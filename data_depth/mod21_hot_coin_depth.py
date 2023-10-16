import os
import requests

from config.logger_config import setup_logger

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("hot_coin_depth", os.path.join(project_root, 'log', 'app.log'))


def hot_coin(symbol_name, reference):
    symbol_name = str(symbol_name).lower() + '_' + str(reference).lower()
    url = f"https://api.hotcoinfin.com/v1/depth?symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['data']['depth']
            return data
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from hot_coin Error: {repr(e)}")
