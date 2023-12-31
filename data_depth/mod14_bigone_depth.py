import os
import requests

from config.logger_config import setup_logger

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("bigone_depth", os.path.join(project_root, 'log', 'app.log'))


def bigone(symbol_name, reference):
    symbol_name = symbol_name + '-' + reference
    url = f"https://big.one/api/v3/asset_pairs/{symbol_name}/depth?limit=100"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['data']
            data = {
                "bids": [[entry["price"], entry["quantity"]] for entry in data["bids"]],
                "asks": [[entry["price"], entry["quantity"]] for entry in data["asks"]]
            }
            return data
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from bigone Error: {repr(e)}")
