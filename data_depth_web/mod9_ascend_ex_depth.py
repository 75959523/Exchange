import os
import requests

from config.logger_config import setup_logger
from util.depth_util import combined_data

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("ascend_ex_depth", os.path.join(project_root, 'log', 'app.log'))


def ascend_ex(symbol_name, reference):
    symbol_name = symbol_name + '/' + reference
    url = f"https://ascendex.com/api/pro/v1/depth?symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['data']['data']
            return combined_data(data, reference, 'ascendex')
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from ascend_ex Error: {repr(e)}")
