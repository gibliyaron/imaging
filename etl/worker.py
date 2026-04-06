import pandas as pd
import logging
from db.factory import DatabaseFactory


class OracleImageRepository:
    """
    Responsible for querying Oracle for imaging data based on Bdika IDs.
    Single responsibility: read from Oracle.
    """

    def __init__(self, config: dict):
        self._config = config
        self._db = DatabaseFactory.get_connector(config['databases']['oracle_prod'])
        self._base_query = config['queries']['read_oracle']

    def fetch_by_ids(self, chunk_ids) -> pd.DataFrame:
        """
        Fetch images for a specific chunk of IDs.
        """
        if not chunk_ids:
            return pd.DataFrame()

        try:
            ids_formatted = ",".join(f"'{str(x)}'" for x in chunk_ids)
            query = self._base_query.format(accession_numbers_wc=ids_formatted)
            df = self._db.read_sql(query)
            logging.info(f"OracleImageRepository: fetched {len(df)} rows for {len(chunk_ids)} IDs")
            return df
        except Exception as e:
            logging.error(f"OracleImageRepository error: {e}")
            return pd.DataFrame()
