import os
import requests

from config.logger_config import setup_logger

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("bi_ka_depth", os.path.join(project_root, 'log', 'app.log'))


def bi_ka(symbol_name, reference):
    symbol_name = symbol_name + '_' + reference
    url = f"https://www.bika.one/cmc/spot/orderbook/{symbol_name}?level=3&depth=100"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from bi_ka Error: {repr(e)}")
