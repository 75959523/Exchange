import asyncio
import os
import time
import httpx

from config.logger_config import setup_logger
from data_processing_proxy.mod1_coin_w_processor import filter_symbols, insert_to_db
from proxy_handler.proxy_loader import ProxyRotator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("coin_w_collector", os.path.join(project_root, 'log', 'app.log'))

rotator = ProxyRotator(
    os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'proxy_handler', 'mod1_coin_w.txt'))
# rotator = ProxyRotator()
max_concurrent_requests = 100
retry_limit = 3


def coin_w(temp_table_name):
    start_time = time.time()
    try:
        data = asyncio.run(coin_w_symbols())
        if data:
            found_records = filter_symbols(data)
            result = asyncio.run(coin_w_depth(found_records))
            insert_to_db(result, temp_table_name)

            end_time = time.time()
            elapsed_time = round(end_time - start_time, 3)
            logger.info(
                f"-------------------------------------------------- coin_w executed in {elapsed_time} seconds. ----- symbols : {len(found_records)} success : {len(result)}")
    except Exception as e:
        logger.error("Failed to get tickers from coin_w", e)


async def coin_w_symbols():
    proxy = rotator.get_next_proxy()
    url = "https://api.coinw.com/api/v1/public?command=returnSymbol"
    async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=10) as client:
        response = await client.get(url)
        return response.json()['data'] if response.status_code == 200 else None


async def coin_w_depth(found_records):
    url = "https://api.coinw.com/api/v1/public?command=returnOrderBook&size=1&symbol="
    semaphore = asyncio.Semaphore(max_concurrent_requests)
    tasks = [fetch(symbol, url, semaphore) for symbol in found_records]
    results = await asyncio.gather(*tasks)
    return {symbol: result for symbol, result in results if result}


async def fetch(symbol, url, semaphore):
    proxy = rotator.get_next_proxy()

    for retry in range(retry_limit):
        async with semaphore:
            try:
                async with httpx.AsyncClient(proxies=proxy, verify=False, timeout=20) as client:
                    response = await client.get(url + symbol)
                    if response.status_code == 200:
                        # logger.info("success: " + symbol)
                        return symbol, response.json()
                    else:
                        logger.info(f"Request failed {symbol} status code {response.status_code} - coin_w")
                        await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error fetching {symbol}. {repr(e)} - coin_w")
                await asyncio.sleep(0.1)

    return symbol, None
