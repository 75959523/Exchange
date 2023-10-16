import os

from psycopg2.extras import DictCursor
from config.logger_config import setup_logger
from database.db_pool import get_connection, release_connection

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("db_service", os.path.join(project_root, 'log', 'app.log'))


def fetch_symbol_names(table_name):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute(f"SELECT symbol_name FROM {table_name} order by symbol_id;")
    coins = [row[0] for row in cursor.fetchall()]
    cursor.close()
    release_connection(connection)
    return coins


def get_symbols():
    symbols = fetch_symbol_names("symbols")
    reference = fetch_symbol_names("reference")
    return symbols, reference


def insert_into_db(greater, less, temp_table):
    insert_into_db_execute(greater, temp_table + '_analysis_result')
    insert_into_db_execute(less, temp_table + '_analysis_result_exclusion')


def insert_into_db_execute(data_list, temp_table):
    connection = get_connection()
    cursor = connection.cursor()

    for record in data_list:
        record[10] = round(record[10], 2)
        record[11] = round(record[11], 2)

    placeholder = ', '.join(['%s'] * len(data_list[0]))
    insert_sql = f"INSERT INTO {temp_table} VALUES ({placeholder})"

    cursor.executemany(insert_sql, data_list)
    connection.commit()
    cursor.close()
    release_connection(connection)


def usd_to_cny_rate():
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = """
        SELECT * FROM usd_to_cny_rate
    """

    cursor.execute(sql_script)
    result = cursor.fetchall()
    cursor.close()
    release_connection(connection)

    return result


def truncate_table():
    connection = get_connection()
    cursor = connection.cursor()

    trade_data = f"TRUNCATE TABLE trade_data;"
    trade_data_analysis = f"TRUNCATE TABLE trade_data_analysis;"
    trade_data_analysis_result = f"TRUNCATE TABLE trade_data_analysis_result;"
    trade_data_analysis_result_exclusion = f"TRUNCATE TABLE trade_data_analysis_result_exclusion;"

    cursor.execute(trade_data)
    cursor.execute(trade_data_analysis)
    cursor.execute(trade_data_analysis_result)
    cursor.execute(trade_data_analysis_result_exclusion)
    connection.commit()
    cursor.close()
    release_connection(connection)


def get_result_from_db(temp_table):
    connection = get_connection()
    cursor = connection.cursor(cursor_factory=DictCursor)

    sql_script = f"""
        SELECT * FROM {temp_table}_analysis
    """
    cursor.execute(sql_script)
    result = cursor.fetchall()
    cursor.close()
    release_connection(connection)

    return result

# def create_temp_table():
#     connection = get_connection()
#     cursor = connection.cursor()
#
#     temp_table_name = "t_" + str(uuid.uuid4()).replace("-", "")
#     temp_table_name = "t_" + get_current_time().replace('-', '_').replace(' ', '_').replace(':', '_')
#     trade_data = f"""CREATE TABLE {temp_table_name} AS SELECT * FROM trade_data;"""
#     trade_data_analysis = f"""CREATE TABLE {temp_table_name}_analysis AS SELECT * FROM trade_data_analysis;"""
#     trade_data_analysis_result = f"""CREATE TABLE {temp_table_name}_analysis_result AS SELECT * FROM trade_data_analysis_result;"""
#     trade_data_analysis_result_exclusion = f"""CREATE TABLE {temp_table_name}_analysis_result_exclusion AS SELECT * FROM trade_data_analysis_result_exclusion;"""
#
#     execute(trade_data, trade_data_analysis, trade_data_analysis_result, trade_data_analysis_result_exclusion, cursor)
#     connection.commit()
#     cursor.close()
#     release_connection(connection)
#
#     return temp_table_name
#
#
# def delete_temp_table(temp_table_name):
#     connection = get_connection()
#     cursor = connection.cursor()
#
#     trade_data = f"DROP TABLE IF EXISTS {temp_table_name};"
#     trade_data_analysis = f"DROP TABLE IF EXISTS {temp_table_name}_analysis;"
#     trade_data_analysis_result = f"DROP TABLE IF EXISTS {temp_table_name}_analysis_result;"
#     trade_data_analysis_result_exclusion = f"DROP TABLE IF EXISTS {temp_table_name}_analysis_result_exclusion;"
#
#     execute(trade_data, trade_data_analysis, trade_data_analysis_result, trade_data_analysis_result_exclusion, cursor)
#     connection.commit()
#     cursor.close()
#     release_connection(connection)
#
#
# def execute(trade_data, trade_data_analysis, trade_data_analysis_result, trade_data_analysis_result_exclusion, cursor):
#     cursor.execute(trade_data)
#     cursor.execute(trade_data_analysis)
#     cursor.execute(trade_data_analysis_result)
#     cursor.execute(trade_data_analysis_result_exclusion)
