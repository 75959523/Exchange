import os
import requests

from config.logger_config import setup_logger
from util.depth_util import combined_data

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("bybit_depth", os.path.join(project_root, 'log', 'app.log'))


def bybit(symbol_name, reference):
    symbol_name = symbol_name + reference
    url = f"https://api.bybit.com/v5/market/orderbook?category=spot&limit=50&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['result']
            data['asks'] = data.pop('a')
            data['bids'] = data.pop('b')
            return combined_data(data, reference, 'bybit')
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from bybit Error: {repr(e)}")
