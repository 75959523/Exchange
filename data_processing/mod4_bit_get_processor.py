import os

from config.logger_config import setup_logger
from data_stablecoin.stable_coin_processing import bit_get_stable_coin
from database.db_pool import release_connection, get_connection
from util.time_util import get_current_time
from web_interaction.exclusion import get_filtered_symbols_for_exchange

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("bit_get_processor", os.path.join(project_root, 'log', 'app.log'))


def filter_symbols(data):
    found_records = []
    inst_ids_set = set(item['symbol'] for item in data)
    symbols = get_filtered_symbols_for_exchange('bitget')

    for symbol in symbols:
        combined_id = str(symbol).replace('-', '')
        if combined_id in inst_ids_set:
            matched_item = [item for item in data if item['symbol'] == combined_id][0]
            found_records.append((symbol, matched_item))

    logger.info(f"bit_get - symbols       : {len(data)}")
    logger.info(f"bit_get - symbols found : {len(found_records)}")
    # bit_get_stable_coin(found_records)
    return found_records


def insert_to_db(found_records, temp_table_name):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO {temp_table_name} (
            symbol_name, reference, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, %s, '{current_time}', 'bitget');
    """

    records_to_insert = [
        (
            record[0].split('-')[0],
            record[0].split('-')[1],
            record[1].get('buyOne', 0),
            record[1].get('bidSz', 0),
            record[1].get('sellOne', 0),
            record[1].get('askSz', 0)
        ) for record in found_records
    ]

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)
