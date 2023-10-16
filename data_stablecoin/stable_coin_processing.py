from psycopg2.extras import DictCursor

from database.db_pool import get_connection, release_connection


def stable_coin_list(exchange_name):
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)
    sql_script = f"""SELECT * FROM stablecoin WHERE exchange_name = '{exchange_name}'"""
    cursor.execute(sql_script)
    result = cursor.fetchall()
    cursor.close()
    release_connection(connection)

    return result


def okx_stable_coin(data):
    result = stable_coin_list('okx')
    pairs = [f"{currency[2]}-USDT" for currency in result]

    connection = get_connection()
    cursor = connection.cursor()
    for pair in pairs:
        for entry in data:
            if entry['instId'] == pair:
                last_value = entry['last']
                cursor.execute("UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'okx'",
                               (last_value, pair.split('-')[0]))
                break

    connection.commit()
    cursor.close()
    release_connection(connection)


def ascend_ex_stable_coin(data):
    result = stable_coin_list('ascendex')
    pairs = [f"{currency[2]}-USDT" for currency in result]

    connection = get_connection()
    cursor = connection.cursor()
    for pair in pairs:
        for entry in data:
            if entry[0] == pair:
                last_value = entry[1]['close']
                cursor.execute("UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'ascendex'",
                               (last_value, pair.split('-')[0]))
                break

    connection.commit()
    cursor.close()
    release_connection(connection)


def bit_mark_stable_coin(data):
    result = stable_coin_list('bitmart')
    pairs = [f"{currency[2]}-USDT" for currency in result]

    connection = get_connection()
    cursor = connection.cursor()
    for pair in pairs:
        for entry in data:
            if entry[0] == pair:
                last_value = entry[1]['close_24h']
                cursor.execute("UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'bitmart'",
                               (last_value, pair.split('-')[0]))
                break

    connection.commit()
    cursor.close()
    release_connection(connection)


def bigone_stable_coin(data):
    result = stable_coin_list('bigone')
    pairs = [f"{currency[2]}-USDT" for currency in result]

    connection = get_connection()
    cursor = connection.cursor()
    for pair in pairs:
        for entry in data:
            if entry['asset_pair_name'] == pair:
                last_value = entry['close']
                cursor.execute("UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'bigone'",
                               (last_value, pair.split('-')[0]))
                break

    connection.commit()
    cursor.close()
    release_connection(connection)


def la_token_stable_coin(data):
    result = stable_coin_list('latoken')
    pairs = [f"{currency[2]}-USDT" for currency in result]

    connection = get_connection()
    cursor = connection.cursor()
    for pair in pairs:
        for entry in data:
            if entry[0] == pair:
                last_value = entry[1]['lastPrice']
                cursor.execute("UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'latoken'",
                               (last_value, pair.split('-')[0]))
                break

    connection.commit()
    cursor.close()
    release_connection(connection)


def gate_io_stable_coin(data):
    result = stable_coin_list('gateio')
    pairs = [f"{currency[2]}-USDT" for currency in result]

    connection = get_connection()
    cursor = connection.cursor()
    for pair in pairs:
        for entry in data:
            if entry[0] == pair:
                last_value = entry[1]['last']
                cursor.execute("UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'gateio'",
                               (last_value, pair.split('-')[0]))
                break

    connection.commit()
    cursor.close()
    release_connection(connection)


def hot_coin_stable_coin(data):
    result = stable_coin_list('hotcoin')
    pairs = [f"{currency[2]}-USDT" for currency in result]

    connection = get_connection()
    cursor = connection.cursor()
    for pair in pairs:
        for entry in data:
            if entry[0] == pair:
                last_value = entry[1]['last']
                cursor.execute("UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'hotcoin'",
                               (last_value, pair.split('-')[0]))
                break

    connection.commit()
    cursor.close()
    release_connection(connection)


def digi_finex_stable_coin(data):
    result = stable_coin_list('digifinex')
    pairs = [f"{currency[2]}-USDT" for currency in result]

    connection = get_connection()
    cursor = connection.cursor()
    for pair in pairs:
        for entry in data:
            if entry[0] == pair:
                last_value = entry[1]['last']
                cursor.execute(
                    "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'digifinex'",
                    (last_value, pair.split('-')[0]))
                break

    connection.commit()
    cursor.close()
    release_connection(connection)


def huobi_stable_coin(data):
    result = stable_coin_list('huobi')
    pairs = [f"{currency[2]}-USDT" for currency in result]

    connection = get_connection()
    cursor = connection.cursor()
    for pair in pairs:
        for entry in data:
            if entry[0] == pair:
                last_value = entry[1]['close']
                cursor.execute(
                    "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'huobi'",
                    (last_value, pair.split('-')[0]))
                break

    connection.commit()
    cursor.close()
    release_connection(connection)


def bitfinex_stable_coin(data):
    result = stable_coin_list('bitfinex')
    pairs = [f"{currency[2]}-USD" for currency in result]

    connection = get_connection()
    cursor = connection.cursor()
    for pair in pairs:
        for entry in data:
            if entry[0] == pair:
                last_value = entry[1][7]
                cursor.execute(
                    "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'bitfinex'",
                    (last_value, pair.split('-')[0]))
                break

    connection.commit()
    cursor.close()
    release_connection(connection)


def bybit_stable_coin(data):
    result = stable_coin_list('bybit')
    pairs = [f"{currency[2]}-USDT" for currency in result]

    connection = get_connection()
    cursor = connection.cursor()
    for pair in pairs:
        for entry in data:
            if entry[0] == pair:
                last_value = entry[1]['lastPrice']
                cursor.execute(
                    "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'bybit'",
                    (last_value, pair.split('-')[0]))
                break

    connection.commit()
    cursor.close()
    release_connection(connection)


def coinex_stable_coin(data):
    result = stable_coin_list('coinex')
    pairs = [f"{currency[2]}-USDT" for currency in result]
    connection = get_connection()
    cursor = connection.cursor()
    for dict_data in data:
        for pair, values in dict_data.items():
            if pair in pairs:
                last_value = values['last']
                cursor.execute(
                    "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'coinex'",
                    (last_value, pair.split('-')[0]))

    connection.commit()
    cursor.close()
    release_connection(connection)


def kraken_stable_coin(data):
    result = stable_coin_list('kraken')
    pairs = [f"{currency[2]}-USDT" for currency in result]
    connection = get_connection()
    cursor = connection.cursor()

    for record in data:
        for pair, values in record.items():
            if pair in pairs:
                last_value = values['c'][0]
                cursor.execute(
                    "UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'kraken'",
                    (last_value, pair.split('-')[0]))

    connection.commit()
    cursor.close()
    release_connection(connection)


def ku_coin_stable_coin(data):
    result = stable_coin_list('kucoin')
    pairs = [f"{currency[2]}-USDT" for currency in result]

    connection = get_connection()
    cursor = connection.cursor()
    for pair in pairs:
        for entry in data:
            if entry['symbol'] == pair:
                last_value = entry['last']
                cursor.execute("UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'kucoin'",
                               (last_value, pair.split('-')[0]))
                break

    connection.commit()
    cursor.close()
    release_connection(connection)


def poloniex_stable_coin(data):
    result = stable_coin_list('poloniex')
    pairs = [f"{currency[2]}_USDT" for currency in result]

    connection = get_connection()
    cursor = connection.cursor()
    for pair in pairs:
        for entry in data:
            if entry['symbol'] == pair:
                last_value = entry['close']
                cursor.execute("UPDATE stablecoin SET price = %s WHERE symbol_name = %s AND exchange_name = 'poloniex'",
                               (last_value, pair.split('_')[0]))
                break

    connection.commit()
    cursor.close()
    release_connection(connection)
