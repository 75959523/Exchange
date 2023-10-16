import os
import requests

from config.logger_config import setup_logger
from util.depth_util import combined_data

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("mexc_depth", os.path.join(project_root, 'log', 'app.log'))


def mexc(symbol_name, reference):
    symbol_name = symbol_name + reference
    url = f"https://api.mexc.com/api/v3/depth?limit=100&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return combined_data(data, reference, 'mexc')
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from mexc Error: {repr(e)}")
