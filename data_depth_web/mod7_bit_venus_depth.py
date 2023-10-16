import os
import requests

from config.logger_config import setup_logger
from util.depth_util import combined_data

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("bit_venus_depth", os.path.join(project_root, 'log', 'app.log'))


def bit_venus(symbol_name, reference):
    symbol_name = symbol_name + reference
    url = f"https://www.bitvenus.me/openapi/quote/v1/depth?limit=100&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return combined_data(data, reference, 'bitvenus')
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from bit_venus Error: {repr(e)}")
