import os
import requests

from config.logger_config import setup_logger

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("binance_depth", os.path.join(project_root, 'log', 'app.log'))


def binance(symbol_name, reference):
    symbol_name = symbol_name + reference
    url = f"https://api.binance.com/api/v3/depth?limit=100&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from binance Error: {repr(e)}")



