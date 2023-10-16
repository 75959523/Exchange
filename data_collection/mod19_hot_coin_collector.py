import os
import time

import requests

from config.logger_config import setup_logger
from data_processing.mod19_hot_coin_processor import filter_symbols, insert_to_db

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("hot_coin_collector", os.path.join(project_root, 'log', 'app.log'))


def hot_coin(temp_table_name):
    start_time = time.time()
    try:
        url = "https://api.hotcoinfin.com/v1/market/ticker"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['ticker']
            found_records = filter_symbols(data)
            insert_to_db(found_records, temp_table_name)
            end_time = time.time()
            elapsed_time = round(end_time - start_time, 3)
            logger.info(f"-------------------------------------------------- hot_coin executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error("Failed to get tickers from hot_coin", e)
