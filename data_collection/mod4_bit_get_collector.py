import os
import time

import requests

from config.logger_config import setup_logger
from data_processing.mod4_bit_get_processor import insert_to_db, filter_symbols_u, filter_symbols_d, filter_symbols_c

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("bit_get_collector", os.path.join(project_root, 'log', 'app.log'))


def bit_get(temp_table_name):
    try:
        start_time = time.time()
        url = "https://api.bitget.com/api/mix/v1/market/tickers?productType=umcbl"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data = data['data']
            found_records = filter_symbols_u(data)
            insert_to_db(found_records, temp_table_name)

        url = "https://api.bitget.com/api/mix/v1/market/tickers?productType=dmcbl"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data = data['data']
            found_records = filter_symbols_d(data)
            insert_to_db(found_records, temp_table_name)

        url = "https://api.bitget.com/api/mix/v1/market/tickers?productType=cmcbl"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data = data['data']
            found_records = filter_symbols_c(data)
            insert_to_db(found_records, temp_table_name)

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 3)
        logger.info(f"-------------------------------------------------- bit_get executed in {elapsed_time} seconds.")
    except Exception as e:
        logger.error("Failed to get tickers from bit_get", e)

