import os
import time
import requests

from config.logger_config import setup_logger
from database.db_pool import get_connection, release_connection
from data_stablecoin.stable_coin_processing import stable_coin_list

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("stable_coin_special", os.path.join(project_root, 'log', 'app.log'))


def binance_stable_coin():
    try:
        url = "https://api.binance.com/api/v3/ticker/price"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            result = stable_coin_list('binance')
            pairs = [f"{currency[2]}USDT" for currency in result]
            connection = get_connection()
            cursor = connection.cursor()
            for pair in pairs:
                if pair == 'DAIUSDT':
                    continue
                for entry in data:
                    if entry['symbol'] == pair:
                        last_value = entry['price']
                        cursor.execute(
                            "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'binance'",
                            (last_value, pair[:-4]))
                        break
            connection.commit()
            cursor.close()
            release_connection(connection)

    except Exception as e:
        logger.error("Failed to get price from binance", e)


def probit_stable_coin():
    try:
        url = "https://api.probit.com/api/exchange/v1/ticker"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['data']
            result = stable_coin_list('probit')
            pairs = [f"{currency[2]}-USDT" for currency in result]
            connection = get_connection()
            cursor = connection.cursor()
            for pair in pairs:
                for entry in data:
                    if entry['market_id'] == pair:
                        last_value = entry['last']
                        cursor.execute(
                            "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'probit'",
                            (last_value, pair.split('-')[0]))
                        break
            connection.commit()
            cursor.close()
            release_connection(connection)

    except Exception as e:
        logger.error("Failed to get price from probit", e)


def mexc_stable_coin():
    try:
        url = "https://api.mexc.com/api/v3/ticker/price"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            result = stable_coin_list('mexc')
            pairs = [f"{currency[2]}USDT" for currency in result]
            connection = get_connection()
            cursor = connection.cursor()
            for pair in pairs:
                for entry in data:
                    if entry['symbol'] == pair:
                        last_value = entry['price']
                        cursor.execute(
                            "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'mexc'",
                            (last_value, pair[:-4]))
                        break
            connection.commit()
            cursor.close()
            release_connection(connection)

    except Exception as e:
        logger.error("Failed to get price from mexc", e)


def xt_stable_coin():
    try:
        url = "https://sapi.xt.com/v4/public/ticker/price"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['result']
            result = stable_coin_list('xt')
            pairs = [f"{currency[2]}_USDT" for currency in result]
            connection = get_connection()
            cursor = connection.cursor()
            for pair in pairs:
                for entry in data:
                    if entry['s'] == pair.lower():
                        last_value = entry['p']
                        cursor.execute(
                            "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'xt'",
                            (last_value, pair.split('_')[0]))
                        break
            connection.commit()
            cursor.close()
            release_connection(connection)

    except Exception as e:
        logger.error("Failed to get price from xt", e)


def hitbtc_stable_coin():
    try:
        url = "https://api.hitbtc.com/api/3/public/ticker"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            result = stable_coin_list('hitbtc')
            pairs = [f"{currency[2]}USDT" for currency in result]
            connection = get_connection()
            cursor = connection.cursor()
            for pair in pairs:
                if pair in data:
                    last_value = data[pair]['last']
                    cursor.execute(
                        "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'hitbtc'",
                        (last_value, pair[:-4]))

            connection.commit()
            cursor.close()
            release_connection(connection)

    except Exception as e:
        logger.error("Failed to get price from xt", e)


def jubi_stable_coin():
    try:
        url = "https://api.jbex.com/openapi/quote/v1/ticker/price"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            result = stable_coin_list('jubi')
            pairs = [f"{currency[2]}USDT" for currency in result]
            connection = get_connection()
            cursor = connection.cursor()
            for pair in pairs:
                for entry in data:
                    if entry['symbol'] == pair:
                        last_value = entry['price']
                        cursor.execute(
                            "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'jubi'",
                            (last_value, pair[:-4]))
                        break
            connection.commit()
            cursor.close()
            release_connection(connection)

    except Exception as e:
        logger.error("Failed to get price from jubi", e)


def coin_w_stable_coin():
    try:
        url = "https://api.coinw.com/api/v1/public?command=returnTicker"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['data']
            result = stable_coin_list('coinw')
            pairs = [f"{currency[2]}_USDT" for currency in result]
            connection = get_connection()
            cursor = connection.cursor()
            for pair in pairs:
                if pair in data:
                    last_value = data[pair]['last']
                    cursor.execute(
                        "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'coinw'",
                        (last_value, pair.split('_')[0]))

            connection.commit()
            cursor.close()
            release_connection(connection)

    except Exception as e:
        logger.error("Failed to get price from coin_w", e)


def l_bank_stable_coin():
    try:
        connection = get_connection()
        cursor = connection.cursor()

        url_usdc = "https://api.lbkex.com/v2/ticker/24hr.do?symbol=usdc_usdt"
        url_btc = "https://api.lbkex.com/v2/ticker/24hr.do?symbol=btc_usdt"
        url_eth = "https://api.lbkex.com/v2/ticker/24hr.do?symbol=eth_usdt"
        response = requests.get(url_usdc)
        if response.status_code == 200:
            price = response.json()['data'][0]['ticker']['latest']
            cursor.execute(
                "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'lbank'",
                (price, 'USDC'))

        response = requests.get(url_btc)
        if response.status_code == 200:
            price = response.json()['data'][0]['ticker']['latest']
            cursor.execute(
                "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'lbank'",
                (price, 'BTC'))

        response = requests.get(url_eth)
        if response.status_code == 200:
            price = response.json()['data'][0]['ticker']['latest']
            cursor.execute(
                "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'lbank'",
                (price, 'ETH'))

        connection.commit()
        cursor.close()
        release_connection(connection)
    except Exception as e:
        logger.error("Failed to get price from l_bank", e)


def bing_x_stable_coin():
    try:
        url = "https://open-api.bingx.com/openApi/spot/v1/ticker/24hr"
        current_timestamp = int(time.time() * 1000)
        params = {'timestamp': current_timestamp}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()['data']
            result = stable_coin_list('bingx')
            pairs = [f"{currency[2]}-USDT" for currency in result]
            connection = get_connection()
            cursor = connection.cursor()
            for pair in pairs:
                for entry in data:
                    if entry['symbol'] == pair:
                        last_value = entry['lastPrice']
                        cursor.execute(
                            "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'bingx'",
                            (last_value, pair.split('-')[0]))
                        break

            connection.commit()
            cursor.close()
            release_connection(connection)

    except Exception as e:
        logger.error("Failed to get price from bing_x", e)

