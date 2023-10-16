import os
import requests

from config.logger_config import setup_logger
from util.depth_util import combined_data

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("digi_finex_depth", os.path.join(project_root, 'log', 'app.log'))


def digi_finex(symbol_name, reference):
    symbol_name = str(symbol_name).lower() + '_' + str(reference).lower()
    url = f"https://openapi.digifinex.com/v3/order_book?limit=100&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data['asks'] = data['asks'][::-1]
            return combined_data(data, reference, 'digifinex')
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from digi_finex Error: {repr(e)}")
