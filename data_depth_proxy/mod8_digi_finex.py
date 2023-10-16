import os

import httpx
from config.logger_config import setup_logger
from proxy_handler.proxy_loader import ProxyRotator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("digi_finex_depth_proxy", os.path.join(project_root, 'log', 'app.log'))

rotator = ProxyRotator()
retry_limit = 3


def digi_finex(symbol_name, reference):
    symbol_name = str(symbol_name).lower() + '_' + str(reference).lower()
    url = f"https://openapi.digifinex.com/v3/order_book?limit=100&symbol={symbol_name}"

    for retry in range(retry_limit):
        proxy = rotator.get_random_proxy()
        try:
            with httpx.Client(proxies=proxy, verify=False, timeout=20) as client:
                response = client.get(url)
                if response.status_code == 200:
                    data = response.json()
                    data['asks'] = data['asks'][::-1]
                    return data
                else:
                    logger.error(f"digi_finex Request failed with status code {response.status_code}")

                if retry < retry_limit - 1:
                    logger.info(f"digi_finex Retry {retry + 1} for {symbol_name} ...")

        except Exception as e:
            logger.error(f"Failed to get depth from digi_finex Error: {repr(e)}")

    return None
