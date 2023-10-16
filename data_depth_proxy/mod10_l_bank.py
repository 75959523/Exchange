import os

import httpx
from config.logger_config import setup_logger
from proxy_handler.proxy_loader import ProxyRotator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("l_bank_depth_proxy", os.path.join(project_root, 'log', 'app.log'))

rotator = ProxyRotator()
retry_limit = 3


def l_bank(symbol_name, reference):
    symbol_name = str(symbol_name).lower() + '_' + str(reference).lower()
    url = f"https://api.lbkex.com/v2/depth.do?size=100&symbol={symbol_name}"

    for retry in range(retry_limit):
        proxy = rotator.get_random_proxy()
        try:
            with httpx.Client(proxies=proxy, verify=False, timeout=20) as client:
                response = client.get(url)
                if response.status_code == 200:
                    return response.json()['data']
                else:
                    logger.error(f"l_bank Request failed with status code {response.status_code}")

                if retry < retry_limit - 1:
                    logger.info(f"l_bank Retry {retry + 1} for {symbol_name} ...")

        except Exception as e:
            logger.error(f"Failed to get depth from l_bank Error: {repr(e)}")

    return None
