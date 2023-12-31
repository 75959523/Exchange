import os

from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection
from util.time_util import get_current_time
from data_stablecoin.stable_coin_processing import bigone_stable_coin
from web_interaction.exclusion import get_filtered_symbols_for_exchange

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("bigone_processor", os.path.join(project_root, 'log', 'app.log'))


def filter_symbols(data):
    found_records = []
    inst_ids_set = set(item['asset_pair_name'] for item in data)
    symbols = get_filtered_symbols_for_exchange('bigone')

    for symbol in symbols:
        if symbol in inst_ids_set:
            found_records.append([item for item in data if item['asset_pair_name'] == symbol][0])

    logger.info(f"bigone - symbols       : {len(data)}")
    logger.info(f"bigone - symbols found : {len(found_records)}")
    # bigone_stable_coin(found_records)
    return found_records


def insert_to_db(found_records, temp_table_name):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO {temp_table_name} (
            symbol_name, reference, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, %s, '{current_time}', 'bigone');
    """

    records_to_insert = []
    for record in found_records:
        try:
            asset_pair_name = record.get('asset_pair_name', '')
            bid_price = record.get('bid', {}).get('price', '')
            bid_quantity = record.get('bid', {}).get('quantity', '')
            ask_price = record.get('ask', {}).get('price', '')
            ask_quantity = record.get('ask', {}).get('quantity', '')

            if all([asset_pair_name, bid_price, bid_quantity, ask_price, ask_quantity]):
                records_to_insert.append((asset_pair_name.split('-')[0], asset_pair_name.split('-')[1], bid_price, bid_quantity, ask_price, ask_quantity))

        except Exception as e:
            logger.error(f"Error processing record {record}. Error: {e}")

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)


