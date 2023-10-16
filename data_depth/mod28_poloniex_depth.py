import os
import requests

from config.logger_config import setup_logger

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("poloniex_depth", os.path.join(project_root, 'log', 'app.log'))


def poloniex(symbol_name, reference):
    symbol_name = symbol_name + '_' + reference
    url = f"https://api.poloniex.com/markets/{symbol_name}/orderBook?limit=100"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = transform_data(response.json())
            return data
        else:
            logger.error(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to get depth from poloniex Error: {repr(e)}")


def transform_data(data):
    for key in ['asks', 'bids']:
        values = data[key]
        transformed_values = [[values[i], values[i + 1]] for i in range(0, len(values), 2)]
        data[key] = transformed_values
    return data

