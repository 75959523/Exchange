import os
import httpx
from config.logger_config import setup_logger
from proxy_handler.proxy_loader import ProxyRotator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("okx_depth_proxy", os.path.join(project_root, 'log', 'app.log'))

rotator = ProxyRotator()
retry_limit = 3


def okx(symbol_name, reference):
    url = f"https://www.okx.com/api/v5/market/books?sz=100&instId={symbol_name}-{reference}"

    for retry in range(retry_limit):
        proxy = rotator.get_random_proxy()
        try:
            with httpx.Client(proxies=proxy, verify=False, timeout=20) as client:
                response = client.get(url)
                response.raise_for_status()
                data = response.json()
                first_entry = data['data'][0]
                return {
                    'asks': [ask[:2] for ask in first_entry['asks']],
                    'bids': [bid[:2] for bid in first_entry['bids']]
                }

        except httpx.HTTPStatusError:
            logger.error(f"okx Request failed with status code {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to get depth from okx Error: {repr(e)}")

        if retry < retry_limit - 1:
            logger.info(f"okx Retry {retry + 1} for {symbol_name} ...")

    return None

