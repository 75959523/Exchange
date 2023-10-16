import os

from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection
from util.time_util import get_current_time
from data_stablecoin.stable_coin_special import coin_w_stable_coin
from web_interaction.exclusion import get_filtered_symbols_for_exchange

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("coin_w_processor", os.path.join(project_root, 'log', 'app.log'))


def filter_symbols(data):
    found_records = []
    inst_ids_set = set(item['currencyPair'] for item in data)
    symbols = get_filtered_symbols_for_exchange('coinw')

    for symbol in symbols:
        symbol = str(symbol).replace('-', '_')
        if symbol in inst_ids_set:
            found_records.append(symbol)

    logger.info(f"coin_w - symbols       : {len(data)}")
    logger.info(f"coin_w - symbols found : {len(found_records)}")
    coin_w_stable_coin()
    return found_records


def insert_to_db(found_records, temp_table_name):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query_temp_table = f"""
        INSERT INTO {temp_table_name} (
            symbol_name, reference, ask, bid, ask_size, bid_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, %s, '{current_time}', 'coinw');
    """

    records_to_insert_temp = []
    for symbol_name, result in found_records.items():
        result = result.get('data', {})
        asks = result.get('asks', [])
        bids = result.get('bids', [])

        if not asks or not bids:
            continue

        reference = str(symbol_name).split('_')[1]
        symbol_name = str(symbol_name).split('_')[0]

        try:
            ask_price = asks[0][0] if asks else None
            bid_price = bids[0][0] if bids else None
            ask_size = asks[0][1] if asks else None
            bid_size = bids[0][1] if bids else None

            records_to_insert_temp.append(
                (symbol_name, reference, ask_price, bid_price, ask_size, bid_size)
            )
        except (IndexError, TypeError, ValueError) as e:
            logger.error(f"Error processing ask/bid price for symbol {symbol_name}. Error: {repr(e)}")
            continue

    cursor.executemany(query_temp_table, records_to_insert_temp)
    connection.commit()
    cursor.close()
    release_connection(connection)
