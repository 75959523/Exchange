import os

import httpx
from config.logger_config import setup_logger
from proxy_handler.proxy_loader import ProxyRotator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("bitfinex_depth_proxy", os.path.join(project_root, 'log', 'app.log'))

rotator = ProxyRotator()
retry_limit = 3


def bitfinex(symbol_name, reference):
    symbol_name = symbol_name + reference
    url = f"https://api-pub.bitfinex.com/v2/book/t{symbol_name}/P0?len=100"

    for retry in range(retry_limit):
        proxy = rotator.get_random_proxy()
        try:
            with httpx.Client(proxies=proxy, verify=False, timeout=20) as client:
                response = client.get(url)
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

                    transformed_data = {
                        "bids": bids,
                        "asks": asks
                    }
                    return transformed_data
                else:
                    logger.error(f"bitfinex Request failed with status code {response.status_code}")
                    logger.error(f"bitfinex change request params")
                    symbol_name = symbol_name + ':' + reference

                if retry < retry_limit - 1:
                    logger.info(f"bitfinex Retry {retry + 1} for {symbol_name} ...")

        except Exception as e:
            logger.error(f"Failed to get depth from bitfinex Error: {repr(e)}")

    return None
