import pandas as pd
from db.factory import DatabaseFactory


class ETLProcessor:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

        # Initialize Connections via Factory
        self.logger.info("Initializing Database Connections...")
        self.onc_db = DatabaseFactory.get_connector(self.config['databases']['onc_sql'])
        self.oracle_db = DatabaseFactory.get_connector(self.config['databases']['oracle_prod'])
        self.hds_db = DatabaseFactory.get_connector(self.config['databases']['hds_img_sql'])

    def _chunk_list(self, data_list, chunk_size):
        """Generator to split a list into chunks."""
        for i in range(0, len(data_list), chunk_size):
            yield data_list[i:i + chunk_size]

    def run(self):
        self.logger.info("Starting ETL Process...")

        # --- Step 1: Read from Onc SQL Server ---
        self.logger.info("Step 1: Reading data from Onc Database...")
        try:
            onc_df = self.onc_db.read_sql(self.config['queries']['read_onc'])
            onc_df.set_index('PB_Bdika_Id', inplace=True)
            self.logger.info(f"Retrieved {len(onc_df)} records from Onc.")
        except Exception as e:
            self.logger.critical(f"Failed Step 1: {e}")
            return

        all_ids = onc_df.index.unique().tolist()
        if not all_ids:
            self.logger.warning("No IDs found in Onc database. Aborting.")
            return

        # --- Step 2: Read from Oracle in Chunks ---
        self.logger.info("Step 2: Reading from Oracle with Chunking...")
        oracle_results = []
        chunk_size = self.config['etl']['chunk_size']
        base_query = self.config['queries']['read_oracle']

        # Use a safe string conversion for IDs to prevent SQL injection issues if IDs are strings
        for chunk in self._chunk_list(all_ids, chunk_size):
            ids_formatted = ",".join(f"'{str(x)}'" for x in chunk)
            query = base_query.format(accession_numbers_wc=ids_formatted)

            chunk_df = self.oracle_db.read_sql(query)
            if not chunk_df.empty:
                oracle_results.append(chunk_df)

        # if not oracle_results:
        #     self.logger.warning("No matching data found in Oracle.")
        #     return

        # oracle_df = pd.concat(oracle_results)
            oracle_df = chunk_df
            oracle_df.set_index('id', inplace=True)
            self.logger.info(f"Retrieved {len(oracle_df)} records from Oracle.")

            # --- Step 3: Join and Transform ---
            self.logger.info("Merging datasets...")
            # Join on Index (PB_Bdika_Id vs id)
            final_df = oracle_df.join(onc_df[['Baznat_Id']], how='inner')
    
            final_df.reset_index(inplace=True)
            final_df.rename(columns={'index': 'id'}, inplace=True)

            # --- Step 4: Load to HDS_IMG ---
            self.logger.info("Step 3: Inserting data into HDS_IMG...")
            target_table = self.config['queries']['insert_hds']
            self.hds_db.write_sql(final_df, target_table, if_exists='append')

        self.logger.info("ETL Process Completed Successfully.")
