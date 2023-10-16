import os

import httpx
from config.logger_config import setup_logger
from proxy_handler.proxy_loader import ProxyRotator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("probit_depth_proxy", os.path.join(project_root, 'log', 'app.log'))

rotator = ProxyRotator()
retry_limit = 3


def probit(symbol_name, reference):
    symbol_name = symbol_name + '-' + reference
    url = f"https://api.probit.com/api/exchange/v1/order_book?market_id={symbol_name}"

    for retry in range(retry_limit):
        proxy = rotator.get_random_proxy()
        try:
            with httpx.Client(proxies=proxy, verify=False, timeout=20) as client:
                response = client.get(url)
                if response.status_code == 200:
                    data = response.json()['data']
                    bids = []
                    asks = []
                    for entry in reversed(data):
                        if entry["side"] == "buy" and len(bids) < 20:
                            bids.append([entry["price"], entry["quantity"]])
                        elif entry["side"] == "sell" and len(asks) < 20:
                            asks.append([entry["price"], entry["quantity"]])

                        if len(bids) == 20 and len(asks) == 20:
                            break

                    return {
                        "bids": bids,
                        "asks": asks
                    }
                else:
                    logger.error(f"probit Request failed with status code {response.status_code}")

                if retry < retry_limit - 1:
                    logger.info(f"probit Retry {retry + 1} for {symbol_name} ...")

        except Exception as e:
            logger.error(f"Failed to get depth from probit Error: {repr(e)}")

    return None
