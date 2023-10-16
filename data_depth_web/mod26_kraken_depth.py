import os
import requests

from config.logger_config import setup_logger
from util.depth_util import combined_data

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("kraken_depth", os.path.join(project_root, 'log', 'app.log'))


def kraken(symbol_name, reference):
    symbol_name = symbol_name + reference
    url = f"https://api.kraken.com/0/public/Depth?count=100&pair={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            first_entry = data['result'][symbol_name]
            data = {
                'asks': [ask[:2] for ask in first_entry['asks']],
                'bids': [bid[:2] for bid in first_entry['bids']]
            }
            return combined_data(data, reference, 'kraken')
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from kraken Error: {repr(e)}")
