from sqlalchemy import create_engine
import logging
from .interface import DatabaseConnector


class OracleConnector(DatabaseConnector):
    def connect(self):
        dsn = (
            f"(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)"
            f"(HOST = {self.config['host']})(PORT = {self.config['port']})) "
            f"(CONNECT_DATA = (SID = {self.config['sid']})(SERVER = DEDICATED)))"
        )

        conn_str = f"oracle+oracledb://{self.config['user']}:{self.config['password']}@{dsn}"
        self.engine = create_engine(conn_str)
        logging.info("Connected to Oracle Database")
