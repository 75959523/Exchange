import os
import requests
from config.logger_config import setup_logger

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("okx_depth", os.path.join(project_root, 'log', 'app.log'))


def okx(symbol_name, reference):
    symbol_name = symbol_name + '-' + reference
    url = f"https://www.okx.com/api/v5/market/books?sz=100&instId={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['data'][0]
            data = {
                'asks': [ask[:2] for ask in data['asks']],
                'bids': [bid[:2] for bid in data['bids']]
            }
            return data
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from okx Error: {repr(e)}")
