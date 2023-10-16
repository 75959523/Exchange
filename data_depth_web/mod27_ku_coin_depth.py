import os
import requests

from config.logger_config import setup_logger
from util.depth_util import combined_data

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("ku_coin_depth", os.path.join(project_root, 'log', 'app.log'))


def ku_coin(symbol_name, reference):
    symbol_name = symbol_name + '-' + reference
    url = f"https://api.kucoin.com/api/v1/market/orderbook/level2_100?symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['data']
            return combined_data(data, reference, 'kucoin')
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from ku_coin Error: {repr(e)}")
