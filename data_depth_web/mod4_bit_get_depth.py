import os
import requests

from config.logger_config import setup_logger
from util.depth_util import combined_data

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("bit_get_depth", os.path.join(project_root, 'log', 'app.log'))


# def bit_get(symbol_name, reference):
#     retry_suffixes = ['_UMCBL', '_DMCBL', '_CMCBL']
#
#     for suffix in retry_suffixes:
#         current_symbol_name = symbol_name + reference + suffix
#         url = f"https://api.bitget.com/api/mix/v1/market/depth?limit=100&precision=scale0&symbol={current_symbol_name}"
#
#         try:
#             response = requests.get(url)
#             if response.status_code == 200:
#                 data = response.json()['data']
#                 return combined_data(data, reference, 'bitget')
#             else:
#                 logger.info(f"bit_get change request params {current_symbol_name}")
#         except Exception as e:
#             logger.error(f"Failed to get depth from bit_get using {current_symbol_name} Error: {repr(e)}")

def bit_get(symbol_name, reference):
    current_symbol_name = symbol_name + reference + '_SPBL'
    url = f"https://api.bitget.com/api/spot/v1/market/depth?type=step0&limit=100&symbol={current_symbol_name}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['data']
            return combined_data(data, reference, 'bitget')
    except Exception as e:
        logger.error(f"Failed to get depth from bit_get using {current_symbol_name} Error: {repr(e)}")