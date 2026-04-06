from typing import Dict, Any
from .interface import DatabaseConnector
from .mssql import SQLServerConnector
from .oracle import OracleConnector


class DatabaseFactory:
    """Factory to create specific DB connectors."""

    @staticmethod
    def get_connector(db_config: Dict[str, Any]) -> DatabaseConnector:
        db_type = db_config.get('type')

        if db_type == 'mssql':
            connector = SQLServerConnector(db_config)
        elif db_type == 'oracle':
            connector = OracleConnector(db_config)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        connector.connect()
        return connector
