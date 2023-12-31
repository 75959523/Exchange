import os
import time

import requests

from config.logger_config import setup_logger
from data_processing.mod3_huobi_processor import filter_symbols, insert_to_db

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("huobi_collector", os.path.join(project_root, 'log', 'app.log'))


def huobi(temp_table_name):
    start_time = time.time()
    try:
        url = "https://api.huobi.pro/market/tickers"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data = data['data']
            found_records = filter_symbols(data)
            insert_to_db(found_records, temp_table_name)
            end_time = time.time()
            elapsed_time = round(end_time - start_time, 3)
            logger.info(f"-------------------------------------------------- huobi executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error("Failed to get tickers from huobi", e)


