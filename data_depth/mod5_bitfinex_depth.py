import os
import requests

from config.logger_config import setup_logger

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("bitfinex_depth", os.path.join(project_root, 'log', 'app.log'))


def bitfinex(symbol_name, reference):
    current_symbol_name = symbol_name + reference
    for attempt in range(2):
        url = f"https://api-pub.bitfinex.com/v2/book/t{current_symbol_name}/P0?len=100"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                bids = []
                asks = []
                for entry in data:
                    price = entry[0]
                    quantity = entry[2]

                    if quantity > 0 and len(bids) < 20:
                        bids.append([str(price), str(quantity)])
                    elif quantity < 0 and len(asks) < 20:
                        asks.append([str(price), str(abs(quantity))])

                    if len(bids) == 20 and len(asks) == 20:
                        break

                data = {
                    "bids": bids,
                    "asks": asks
                }
                return data
            else:
                logger.info(f"bitfinex change request params {current_symbol_name}")
                if attempt == 0:
                    current_symbol_name = symbol_name + ':' + reference
        except Exception as e:
            logger.error(f"Failed to get depth from bitfinex using {current_symbol_name} Error: {repr(e)}")

