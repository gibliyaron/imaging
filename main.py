"""
import math

from Oncology import dwh_data, pacs_data
from Oncology.datfarame_paginator import DataFramePaginator
import pandas as pd

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
PAGE_SIZE = 1000

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    df_onc = dwh_data.get_oncology_tests()
    pages = math.floor(len(df_onc) / PAGE_SIZE) + 1
    page_counter = 1
    while page_counter <= 3:
        paginator = DataFramePaginator(df_onc, page_size=PAGE_SIZE)
        result = paginator.get_page(page_counter)
        # print(result['meta']['total_pages'])
        df_onc_page = pd.DataFrame(result['data'])
        pacs_data.get_oncology_images(df_onc_page)
        page_counter = page_counter + 1
#    print(f"Page {result['meta']['current_page']} of {result['meta']['total_pages']}")
#    print(result['data'])
"""

import yaml
from logger_setup import setup_logger
from etl.processor import ETLProcessor


def main():
    # Load Config
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print("Error: config.yaml not found.")
        return

    # Setup Logger
    logger = setup_logger(config)

    # Initialize and Run Processor
    try:
        processor = ETLProcessor(config, logger)
        processor.run()
    except Exception as e:
        logger.critical(f"Unhandled exception in main execution: {e}")


if __name__ == "__main__":
    main()
