from sqlalchemy import create_engine
import logging
from .interface import DatabaseConnector
import pyodbc


class SQLServerConnector(DatabaseConnector):
    def connect(self):
        conn_str = (
            f"mssql+pyodbc://@{self.config['server']}/{self.config['database']}?"
            f"driver={self.config['driver'].replace(' ', '+')}&trusted_connection=yes"
        )
        # fast_executemany is key for bulk performance
        self.engine = create_engine(conn_str, fast_executemany=True)
        logging.info(f"Connected to SQL Server: {self.config['database']} (fast_executemany enabled)")

    def bulk_insert(self, df, table_name: str):
        """
        Optimized bulk insert into SQL Server using fast_executemany
        and a MAXDOP query hint.
        """
        if df.empty:
            logging.info("bulk_insert called with empty DataFrame; nothing to insert.")
            return

        cols = list(df.columns)
        col_list = ", ".join(f"[{c}]" for c in cols)
        param_list = ", ".join("?" for _ in cols)

        insert_sql = (
            f"INSERT INTO {table_name} WITH (TABLOCK) "
            f"({col_list}) VALUES ({param_list}) OPTION (MAXDOP 8);"
        )

        logging.info(f"Starting bulk insert of {len(df)} rows into {table_name} with MAXDOP 8")

        try:
            with self.engine.raw_connection() as conn:
                cursor = conn.cursor()
                cursor.fast_executemany = True
                data = [tuple(row[col] for col in cols) for _, row in df.iterrows()]
                cursor.executemany(insert_sql, data)
                conn.commit()
            logging.info(f"Bulk insert completed successfully into {table_name}")
        except Exception as e:
            logging.error(f"Bulk insert failed for {table_name}: {e}")
            raise
