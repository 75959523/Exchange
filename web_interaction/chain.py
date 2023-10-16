from psycopg2.extras import DictCursor

from database.db_pool import release_connection, get_connection
from util.time_util import get_current_time


def add_chain_into_db(data):
    exchange_name = data['exchangeName']
    ccy = data['ccy']
    chain = data['chain']
    address = data['address']
    dep = 'true' if data['dep'] == '是' else 'false'
    wd = 'true' if data['wd'] == '是' else 'false'

    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO chain_custom (exchange_name, ccy, chain, ct_addr, candep, canwd, create_time) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (exchange_name, ccy, chain, address, dep, wd, get_current_time()))
    connection.commit()
    cursor.close()
    release_connection(connection)


def get_chain():
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)
    cursor.execute("SELECT * FROM chain_custom ORDER BY chain_id")
    result = cursor.fetchall()
    cursor.close()
    release_connection(connection)
    return result


def delete_chain_record(chain_id):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("DELETE FROM chain_custom WHERE chain_id = %s", (chain_id,))
    connection.commit()
    cursor.close()
    release_connection(connection)
