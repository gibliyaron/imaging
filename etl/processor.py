import pandas as pd
from db.factory import DatabaseFactory
from etl.oracle_repository import OracleImageRepository


class ETLProcessor:
    """
    Orchestrates the ETL flow:
    1. Read IDs from Onc SQL Server
    2. Fetch imaging data from Oracle in chunks
    3. Join and transform
    4. Bulk insert into HDS_IMG with MAXDOP
    """

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

        self.logger.info("Initializing Database Connections...")
        self.onc_db = DatabaseFactory.get_connector(self.config['databases']['onc_sql'])
        self.hds_db = DatabaseFactory.get_connector(self.config['databases']['hds_img_sql'])
        self.oracle_repo = OracleImageRepository(self.config)

        self.chunk_size = self.config['etl']['chunk_size']
        self.target_table = self.config['queries']['insert_hds']

    @staticmethod
    def _chunk_list(data_list, chunk_size):
        """Split a list into smaller chunks."""
        for i in range(0, len(data_list), chunk_size):
            yield data_list[i:i + chunk_size]

    def _load_onc_ids(self) -> pd.DataFrame:
        """Step 1: Read from Onc SQL Server."""
        self.logger.info("Step 1: Reading IDs from Onc SQL Server...")
        onc_df = self.onc_db.read_sql(self.config['queries']['read_onc'])
        if onc_df.empty:
            self.logger.warning("No records returned from Onc.")
            return pd.DataFrame()

        onc_df.set_index('PB_Bdika_Id', inplace=True)
        self.logger.info(f"Retrieved {len(onc_df)} records from Onc.")
        return onc_df

    # def _fetch_oracle_data(self, all_ids):
    def _etl_data(self, all_ids):
        """Step 2: Fetch from Oracle in chunks (no multiprocessing)."""
        self.logger.info("Step 2: Reading from Oracle in chunks...")
        oracle_frames = []

        for chunk in self._chunk_list(all_ids, self.chunk_size):
            df_chunk = self.oracle_repo.fetch_by_ids(chunk)
        #     if not df_chunk.empty:
        #         oracle_frames.append(df_chunk)

        # if not oracle_frames:
        #     self.logger.warning("No matching data found in Oracle.")
        #     return pd.DataFrame()

        # oracle_df = pd.concat(oracle_frames, ignore_index=True)
            orcale_df = df_chunk
            oracle_df.rename(columns=str.lower, inplace=True)
            if 'bdika_id' in oracle_df.columns:
                oracle_df.set_index('bdika_id', inplace=True)

            self.logger.info(f"Total Oracle records retrieved: {len(oracle_df)}")
            final_df = self._transform(onc_df, oracle_df)
            self._load(final_df)
        # return oracle_df

    def _transform(self, onc_df, oracle_df):
        """Step 3: Join and transform."""
        self.logger.info("Step 3: Merging datasets...")
        if oracle_df.empty or onc_df.empty:
            self.logger.warning("One of the datasets is empty; skipping merge.")
            return pd.DataFrame()

        final_df = oracle_df.join(onc_df[['Baznat_Id']], how='inner')
        final_df.reset_index(inplace=True)
        final_df.rename(columns={'bdika_id': 'Bdika_Id'}, inplace=True)
        self.logger.info(f"After merge, {len(final_df)} records remain.")
        return final_df

    def _load(self, final_df):
        """Step 4: Bulk load to HDS_IMG using SQL Server bulk insert with MAXDOP."""
        if final_df.empty:
            self.logger.warning("Final DataFrame is empty; nothing to load.")
            return

        self.logger.info(
            f"Step 4: Bulk inserting {len(final_df)} records into {self.target_table} with MAXDOP 8..."
        )
        self.hds_db.bulk_insert(final_df, self.target_table)

    def run(self):
        self.logger.info("Starting ETL Process (SQL Server bulk insert)...")

        try:
            onc_df = self._load_onc_ids()
            if onc_df.empty:
                self.logger.warning("Aborting ETL: no Onc data.")
                return

            all_ids = onc_df.index.unique().tolist()
            self._etl_data(all_ids)
            # self._fetch_oracle_data(all_ids)
            # oracle_df = self._fetch_oracle_data(all_ids)
            # final_df = self._transform(onc_df, oracle_df)
            # self._load(final_df)

            self.logger.info("ETL Process Completed Successfully.")
        except Exception as e:
            self.logger.critical(f"Unhandled exception in ETLProcessor.run: {e}")
            raise
