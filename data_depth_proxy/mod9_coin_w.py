import os

import httpx
from config.logger_config import setup_logger
from proxy_handler.proxy_loader import ProxyRotator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("coin_w_depth_proxy", os.path.join(project_root, 'log', 'app.log'))

rotator = ProxyRotator()
retry_limit = 3


def coin_w(symbol_name, reference):
    symbol_name = symbol_name + '_' + reference
    url = f"https://api.coinw.com/api/v1/public?command=returnOrderBook&size=50&symbol={symbol_name}"

    for retry in range(retry_limit):
        proxy = rotator.get_random_proxy()
        try:
            with httpx.Client(proxies=proxy, verify=False, timeout=20) as client:
                response = client.get(url)
                if response.status_code == 200:
                    return response.json()['data']
                else:
                    logger.error(f"coin_w Request failed with status code {response.status_code}")

                if retry < retry_limit - 1:
                    logger.info(f"coin_w Retry {retry + 1} for {symbol_name} ...")

        except Exception as e:
            logger.error(f"Failed to get depth from coin_w Error: {repr(e)}")

    return None
