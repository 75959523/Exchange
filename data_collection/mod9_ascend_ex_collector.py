import os
import time

import requests

from config.logger_config import setup_logger
from data_processing.mod9_ascend_ex_processor import filter_symbols, insert_to_db

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("ascend_ex_collector", os.path.join(project_root, 'log', 'app.log'))


def ascend_ex(temp_table_name):
    start_time = time.time()
    try:
        url = "https://ascendex.com/api/pro/v1/spot/ticker"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data = data['data']
            found_records = filter_symbols(data)
            insert_to_db(found_records, temp_table_name)
            end_time = time.time()
            elapsed_time = round(end_time - start_time, 3)
            logger.info(f"-------------------------------------------------- ascend_ex executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error("Failed to get tickers from ascend_ex", e)
