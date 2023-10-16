import os

from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection
from util.time_util import get_current_time
from data_stablecoin.stable_coin_processing import ku_coin_stable_coin
from web_interaction.exclusion import get_filtered_symbols_for_exchange

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("ku_coin_processor", os.path.join(project_root, 'log', 'app.log'))


def filter_symbols(data):
    found_records = []
    inst_ids_set = set(item['symbol'] for item in data)
    symbols = get_filtered_symbols_for_exchange('kucoin')

    for symbol in symbols:
        if symbol in inst_ids_set:
            found_records.append([item for item in data if item['symbol'] == symbol][0])

    logger.info(f"ku_coin - symbols       : {len(data)}")
    logger.info(f"ku_coin - symbols found : {len(found_records)}")
    ku_coin_stable_coin(found_records)
    return found_records


def insert_to_db(found_records, temp_table_name):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO {temp_table_name} (
            symbol_name, reference, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, %s, '{current_time}', 'kucoin');
    """

    records_to_insert = [
        (
            record['symbol'].split('-')[0],
            record['symbol'].split('-')[1],
            record['buy'],
            0,
            record['sell'],
            0
        ) for record in found_records
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)
