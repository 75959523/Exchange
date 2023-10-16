import os

from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection
from util.time_util import get_current_time
from data_stablecoin.stable_coin_processing import okx_stable_coin
from web_interaction.exclusion import get_filtered_symbols_for_exchange

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("okx_processor", os.path.join(project_root, 'log', 'app.log'))


def filter_symbols(data):
    found_records = []
    inst_ids_set = set(item['instId'] for item in data)
    symbols = get_filtered_symbols_for_exchange('okx')

    for symbol in symbols:
        if symbol in inst_ids_set:
            found_records.append([item for item in data if item['instId'] == symbol][0])

    logger.info(f"okx - symbols       : {len(data)}")
    logger.info(f"okx - symbols found : {len(found_records)}")
    okx_stable_coin(found_records)
    return found_records


def insert_to_db(found_records, temp_table_name):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO {temp_table_name} (
            symbol_name, reference, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, %s, '{current_time}', 'okx');
    """

    records_to_insert = [
        (
            record['instId'].split('-')[0],
            record['instId'].split('-')[1],
            record['bidPx'],
            record['bidSz'],
            record['askPx'],
            record['askSz']
        ) for record in found_records
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)
