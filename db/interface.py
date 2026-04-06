from abc import ABC, abstractmethod
import pandas as pd
from sqlalchemy import text
import logging


class DatabaseConnector(ABC):
    """Abstract Base Class defining the contract for all DB interactions."""

    def __init__(self, config: dict):
        self.config = config
        self.engine = None

    @abstractmethod
    def connect(self):
        """Establishes the connection engine."""
        pass

    def read_sql(self, query: str, params=None) -> pd.DataFrame:
        """Reads data into a Pandas DataFrame."""
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(text(query), conn, params=params)
        except Exception as e:
            logging.error(f"Error reading from DB: {e}")
            raise

    def write_sql(self, df: pd.DataFrame, table_name: str, if_exists='append', chunk_size=5000):
        """Generic write using pandas.to_sql."""
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False, chunk_size=chunk_size)
            logging.info(f"Successfully wrote {len(df)} rows to {table_name}")
        except Exception as e:
            logging.error(f"Error writing to DB: {e}")
            raise

    def bulk_insert(self, df: pd.DataFrame, table_name: str):
        """
        Optional optimized bulk insert.
        Default implementation falls back to write_sql.
        Concrete connectors can override for DB-specific bulk behavior.
        """
        logging.info(f"Using default bulk_insert for {table_name}")
        self.write_sql(df, table_name)
