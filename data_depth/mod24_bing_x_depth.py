import os
import requests

from config.logger_config import setup_logger

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("bing_x_depth", os.path.join(project_root, 'log', 'app.log'))


def bing_x(symbol_name, reference):
    symbol_name = symbol_name + '-' + reference
    url = f"https://open-api.bingx.com/openApi/spot/v1/market/depth?limit=100&symbol={symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['data']
            data['asks'] = data['asks'][::-1]
            return data
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from bing_x Error: {repr(e)}")
