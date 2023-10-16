import os

import httpx
from config.logger_config import setup_logger
from proxy_handler.proxy_loader import ProxyRotator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("gate_io_depth_proxy", os.path.join(project_root, 'log', 'app.log'))

rotator = ProxyRotator()
retry_limit = 3


def gate_io(symbol_name, reference):
    symbol_name = symbol_name + '_' + reference
    url = f"https://api.gateio.ws/api/v4/spot/order_book?limit=100&currency_pair={symbol_name}"

    for retry in range(retry_limit):
        proxy = rotator.get_random_proxy()
        try:
            with httpx.Client(proxies=proxy, verify=False, timeout=20) as client:
                response = client.get(url)
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"gate_io Request failed with status code {response.status_code}")

                if retry < retry_limit - 1:
                    logger.info(f"gate_io Retry {retry + 1} for {symbol_name} ...")

        except Exception as e:
            logger.error(f"Failed to get depth from gate_io Error: {repr(e)}")

    return None
