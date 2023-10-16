import os

import httpx
from config.logger_config import setup_logger
from proxy_handler.proxy_loader import ProxyRotator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("bing_x_depth_proxy", os.path.join(project_root, 'log', 'app.log'))

rotator = ProxyRotator()
retry_limit = 3


def bing_x(symbol_name, reference):
    symbol_name = symbol_name + '-' + reference
    url = f"https://open-api.bingx.com/openApi/spot/v1/market/depth?limit=100&symbol={symbol_name}"

    for retry in range(retry_limit):
        proxy = rotator.get_random_proxy()
        try:
            with httpx.Client(proxies=proxy, verify=False, timeout=20) as client:
                response = client.get(url)
                if response.status_code == 200:
                    return response.json()['data']
                else:
                    logger.error(f"bing_x Request failed with status code {response.status_code}")

                if retry < retry_limit - 1:
                    logger.info(f"bing_x Retry {retry + 1} for {symbol_name} ...")

        except Exception as e:
            logger.error(f"Failed to get depth from bing_x Error: {repr(e)}")

    return None
