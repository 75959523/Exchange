import os

from config.logger_config import setup_logger
from database.db_pool import release_connection, get_connection
from util.time_util import get_current_time
from data_stablecoin.stable_coin_special import hitbtc_stable_coin
from web_interaction.exclusion import get_filtered_symbols_for_exchange

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("hitbtc_processor", os.path.join(project_root, 'log', 'app.log'))


def filter_symbols(data):
    found_records = []
    inst_ids_set = set(data.keys())
    symbols = get_filtered_symbols_for_exchange('hitbtc')

    for symbol in symbols:
        combined_id = str(symbol).replace('-', '')
        if combined_id in inst_ids_set:
            found_records.append({symbol: data[combined_id]})

    logger.info(f"hitbtc - symbols       : {len(data)}")
    logger.info(f"hitbtc - symbols found : {len(found_records)}")
    # hitbtc_stable_coin()
    return found_records


def insert_to_db(found_records, temp_table_name):
    current_time = get_current_time()
    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        INSERT INTO {temp_table_name} (
            symbol_name, reference, bid, bid_size, ask, ask_size, update_time, exchange_name
        ) VALUES (%s, %s, %s, %s, %s, %s, '{current_time}', 'hitbtc');
    """

    records_to_insert = []
    for record in found_records:
        symbol_name, data = list(record.items())[0]

        if data.get('ask') and len(data['ask']) > 0:
            ask_price = data['ask'][0][0]
            ask_size = data['ask'][0][1]
        else:
            ask_price = None
            ask_size = None

        if data.get('bid') and len(data['bid']) > 0:
            bid_price = data['bid'][0][0]
            bid_size = data['bid'][0][1]
        else:
            bid_price = None
            bid_size = None

        records_to_insert.append(
            (
                symbol_name.split('-')[0],
                symbol_name.split('-')[1],
                bid_price,
                bid_size,
                ask_price,
                ask_size
            )
        )

    cursor.executemany(query, records_to_insert)
    connection.commit()
    cursor.close()
    release_connection(connection)
