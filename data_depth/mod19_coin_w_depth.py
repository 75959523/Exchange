import os
import requests

from config.logger_config import setup_logger

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("coin_w_depth", os.path.join(project_root, 'log', 'app.log'))


def coin_w(symbol_name, reference):
    symbol_name = symbol_name + '_' + reference
    url = f"https://api.coinw.com/api/v1/public?command=returnOrderBook&size=50&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['data']
            return data
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from coin_w Error: {repr(e)}")
