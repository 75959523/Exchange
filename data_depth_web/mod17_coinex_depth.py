import os
import requests

from config.logger_config import setup_logger
from util.depth_util import combined_data

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("coinex_depth", os.path.join(project_root, 'log', 'app.log'))


def coinex(symbol_name, reference):
    symbol_name = symbol_name + reference
    url = f"https://api.coinex.com/v1/market/depth?limit=50&merge=0&market={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['data']
            return combined_data(data, reference, 'coinex')
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from coinex Error: {repr(e)}")
