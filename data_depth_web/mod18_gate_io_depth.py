import os
import requests

from config.logger_config import setup_logger
from util.depth_util import combined_data

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("gate_io_depth", os.path.join(project_root, 'log', 'app.log'))


def gate_io(symbol_name, reference):
    symbol_name = symbol_name + '_' + reference
    url = f"https://api.gateio.ws/api/v4/spot/order_book?limit=100&currency_pair={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return combined_data(data, reference, 'gateio')
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from gate_io Error: {repr(e)}")
