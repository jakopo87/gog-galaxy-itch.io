import asyncio
from dataclasses import dataclass
import json
import logging
import os
import sqlite3
import sys
import time
import webbrowser
from typing import List, Dict, Optional

from galaxy.api.plugin import Plugin, create_and_run_plugin
from galaxy.api.consts import Platform, LicenseType, LocalGameState, OSCompatibility
from galaxy.api.types import Authentication, LocalGame, Game, LicenseInfo, GameTime

if sys.platform.startswith("darwin"):
    ITCH_DB_BASE_PATH = os.path.expanduser(
        "~/Library/Application Support/itch/db")
else:
    ITCH_DB_BASE_PATH = os.path.join(os.getenv("appdata"), "itch/db")
ITCH_DB_PATH = ITCH_DB_BASE_PATH + "/butler.db"

GET_OWNED_GAMES_TASK = "GET_OWNED_GAMES_TASK"
GET_OWNED_GAMES_LAST_CHECK = "GET_OWNED_GAMES_LAST_CHECK"
GET_OWNED_GAMES_WAIT_TIME = float(24 * 3600)

GET_LOCAL_GAMES_TASK = "GET_LOCAL_GAMES_TASK"
GET_LOCAL_GAMES_LAST_CHECK = "GET_LOCAL_GAMES_LAST_CHECK"
GET_LOCAL_GAMES_WAIT_TIME = float(1 * 3600)


class ItchIntegration(Plugin):
    async def get_owned_games(self) -> List[Game]:
        self.__running_task[GET_OWNED_GAMES_TASK] = True

        logging.debug("Opening connection to itch butler.db")
        self.itch_db = sqlite3.connect(ITCH_DB_PATH)
        self.itch_db_cursor = self.itch_db.cursor()

        # Import a game if one of those conditions is satisfied:
        # - it's a free game in a collection;
        # - has a download key;
        sql = """
            SELECT games.*
            FROM games
            LEFT JOIN download_keys dk ON games.id = dk.game_id
            LEFT JOIN collection_games cg ON games.id = cg.game_id
            WHERE (cg.collection_id IS NOT NULL AND games.min_price=0)
                OR (dk.id IS NOT NULL)
        """
        resp = list(self.itch_db_cursor.execute(sql))
        self.itch_db.close()
        logging.debug("Closing connection to itch butler.db")

        for row in resp:
            id = row[0]
            title = row[2]
            can_be_bought = True if row[11] == 1 else False
            min_price = row[10]

            logging.debug(f"get_owned_games {id} ({title})")
            license_type = LicenseType.FreeToPlay
            if can_be_bought and min_price > 0:
                license_type = LicenseType.SinglePurchase
            else:
                license_type = LicenseType.FreeToPlay

            self.__owned_games[id] = Game(
                game_id=id, game_title=title, dlcs=None, license_info=LicenseInfo(license_type))

            logging.debug(f"Built {id} ({title})")

        logging.debug("Finished building games")

        self.persistent_cache[GET_OWNED_GAMES_LAST_CHECK] = time.time()
        self.__running_task[GET_OWNED_GAMES_TASK] = False

        return list(self.__owned_games.values())

    async def get_user_data(self):
        logging.debug("get_user_data")

        self.itch_db = sqlite3.connect(ITCH_DB_PATH)
        self.itch_db.row_factory = sqlite3.Row
        self.itch_db_cursor = self.itch_db.cursor()

        sql = """
            SELECT *
            FROM users u
            INNER JOIN profiles p  ON u.id =p.user_id
            order by u.id
            LIMIT 1
        """
        user = self.itch_db_cursor.execute(sql).fetchone()
        logging.debug(user)
        self.itch_db.close()
        return user

    async def get_os_compatibility(self, game_id, context):
        logging.debug(f"get_os_compatibility {game_id}")
        return OSCompatibility.Windows

    async def pass_login_credentials(self):
        user = await self.get_user_data()
        return Authentication(user["id"], user["username"])

    async def get_local_games(self) -> List[LocalGame]:
        self.__running_task[GET_LOCAL_GAMES_TASK] = True

        self.itch_db = sqlite3.connect(ITCH_DB_PATH)
        self.itch_db_cursor = self.itch_db.cursor()

        installed_games = list(
            self.itch_db_cursor.execute("SELECT game_id, verdict FROM caves"))

        self.itch_db.close()
        for game in installed_games:
            game_id = game[0]
            game_json = game[1]

            exe_path = self.__exe_from_json(game_json)

            if not exe_path or not os.path.exists(exe_path):
                continue

            self.__local_games[str(game_id)] = ItchLocalGame(
                game_id=game_id,
                path=exe_path,
                local_game_state=LocalGameState.Installed
            )
            logging.debug(f"get_local_games {game_id}")

        local_games = [game.toGalaxyLocalGame()
                       for game in self.__local_games.values()]

        self.persistent_cache[GET_LOCAL_GAMES_LAST_CHECK] = time.time()
        self.__running_task[GET_LOCAL_GAMES_TASK] = False

        return local_games

    @staticmethod
    def __exe_from_json(json_string):
        data = json.loads(json_string)

        if not data["candidates"] or len(data["candidates"]) == 0:
            return None
        else:
            return os.path.join(data["basePath"], data["candidates"][0]["path"])

    async def launch_game(self, game_id: str) -> None:
        logging.debug(self.__local_games)
        exe_path = self.__local_games[game_id].path
        start = int(time.time())
        proc = await asyncio.create_subprocess_shell(
            exe_path)

        await proc.communicate()  # wait till terminates
        end = int(time.time())

        session_mins_played = int((end - start) / 60)  # secs to mins
        time_played = (self._get_time_played(game_id)
                       or 0) + session_mins_played
        game_time = GameTime(game_id=game_id,
                             time_played=time_played,
                             last_played_time=end)
        self.update_game_time(game_time)

        # store updated times
        self.persistent_cache[self._time_played_key(game_id)] = str(
            time_played)
        self.persistent_cache[self._last_played_time_key(game_id)] = str(end)
        self.push_cache()

    async def get_game_time(self, game_id: str, context: None) -> GameTime:
        return GameTime(
            game_id=game_id,
            time_played=None,
            last_played_time=None,
        )

    def _get_time_played(self, game_id: str) -> Optional[int]:
        key = self._time_played_key(game_id)
        return int(self.persistent_cache[key]
                   ) if key in self.persistent_cache else None

    def _get_last_played_time(self, game_id: str) -> Optional[int]:
        key = self._last_played_time_key(game_id)
        return int(self.persistent_cache[key]
                   ) if key in self.persistent_cache else None

    @staticmethod
    def _time_played_key(game_id: str) -> str:
        return f'time{game_id}'

    @staticmethod
    def _last_played_time_key(game_id: str) -> str:
        return f'last{game_id}'

    async def install_game(self, game_id):
        await webbrowser.open(f"itch://games/{game_id}")
        return

    async def uninstall_game(self, game_id: str):
        await webbrowser.open(f"itch://games/{game_id}")
        return

    def __init__(self, reader, writer, token):
        super().__init__(
            Platform.ItchIo,  # Choose platform from available list
            "0.1",  # Version
            reader,
            writer,
            token)

        self.itch_db = None
        self.itch_db_cursor = None

        self.__owned_games: Dict[str, Game] = {}
        self.__local_games: Dict[str, ItchLocalGame] = {}
        self.__running_task = {
            GET_OWNED_GAMES_TASK: False,
            GET_LOCAL_GAMES_TASK: False,
        }

    async def authenticate(self, stored_credentials=None):
        user = await self.get_user_data()
        return Authentication(user["id"], user["username"])

    def tick(self) -> None:
        now = time.time()
        if (not self.__running_task[GET_OWNED_GAMES_TASK]) and (now - float(self.persistent_cache[GET_OWNED_GAMES_LAST_CHECK]) > GET_OWNED_GAMES_WAIT_TIME):
            self.create_task(self.get_owned_games(), "Check owned games")

        if (not self.__running_task[GET_LOCAL_GAMES_TASK]) and (now - float(self.persistent_cache[GET_LOCAL_GAMES_LAST_CHECK]) > GET_LOCAL_GAMES_WAIT_TIME):
            self.create_task(self.get_local_games(), "Check installed games")


@dataclass
class ItchLocalGame(LocalGame):
    path: str

    def toGalaxyLocalGame(self) -> LocalGame:
        return LocalGame(
            game_id=self.game_id,
            local_game_state=self.local_game_state
        )


def main():
    create_and_run_plugin(ItchIntegration, sys.argv)


# run plugin event loop
if __name__ == "__main__":
    main()
