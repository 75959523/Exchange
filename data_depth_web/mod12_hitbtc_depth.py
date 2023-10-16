import os
import requests

from config.logger_config import setup_logger
from util.depth_util import combined_data

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("hitbtc_depth", os.path.join(project_root, 'log', 'app.log'))


def hitbtc(symbol_name, reference):
    symbol_name = symbol_name + reference
    url = f"https://api.hitbtc.com/api/3/public/orderbook/{symbol_name}?depth=100"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data['asks'] = data.pop('ask')
            data['bids'] = data.pop('bid')
            return combined_data(data, reference, 'hitbtc')
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from hitbtc Error: {repr(e)}")
