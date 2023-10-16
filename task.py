import os
import time

from data_analysis.trade_data_analysis import fetch_combined_analysis_data
from data_analysis_filter.trade_data_analysis_filter import filter_result_by_depth
from data_collection.mod18_gate_io_collector import gate_io
from data_collection.mod19_hot_coin_collector import hot_coin
from data_collection.mod20_digi_finex_collector import digi_finex
from data_collection.mod22_kraken_collector import kraken
from data_collection.mod23_ku_coin_collector import ku_coin
from data_collection.mod24_poloniex_collector import poloniex
from data_collection.mod16_la_token_collector import la_token
from data_collection.mod17_coinex_collector import coinex
from data_collection.mod9_ascend_ex_collector import ascend_ex
from data_collection.mod14_bigone_collector import bigone
from data_collection.mod2_binance_collector import binance
from data_collection.mod4_bit_get_collector import bit_get
from data_collection.mod13_bit_mart_collector import bit_mart
from data_collection.mod7_bit_venus_collector import bit_venus
from data_collection.mod5_bitfinex_collector import bitfinex
from data_collection.mod10_bybit_collector import bybit
from data_collection.mod8_deep_coin_collector import deep_coin
from data_collection.mod12_hitbtc_collector import hitbtc
from data_collection.mod3_huobi_collector import huobi
from data_collection.mod15_jubi_collector import jubi
from data_collection.mod6_mexc_collector import mexc
from data_collection.mod1_okx_collector import okx
from data_collection.mod11_xt_collector import xt
from data_collection_proxy.mod1_coin_w_collector import coin_w
from data_collection.mod21_bi_ka_collector import bi_ka
from data_collection_proxy.mod2_l_bank_collector import l_bank
from data_collection_proxy.mod3_bing_x_collector import bing_x
from data_collection_proxy.mod4_probit_collector import probit
from database.db_service import insert_into_db, truncate_table
from web_interaction.exchange import load_exchanges
from web_interaction.exclusion import load_exclusion_list
from concurrent.futures import ThreadPoolExecutor, wait, ProcessPoolExecutor
from util.time_util import get_current_time
from config.logger_config import setup_logger
from config.redis_config import RedisConfig

project_root = os.path.dirname(os.path.abspath(__file__))
logger = setup_logger("core_task", os.path.join(project_root, 'log', 'app.log'))

redis_config = RedisConfig()

exchange_functions = {
    'okx': okx,
    'binance': binance,
    'huobi': huobi,
    'bitget': bit_get,
    'bitfinex': bitfinex,
    'mexc': mexc,
    'bitvenus': bit_venus,
    'deepcoin': deep_coin,
    'ascendex': ascend_ex,
    'bybit': bybit,
    'xt': xt,
    'hitbtc': hitbtc,
    'bitmart': bit_mart,
    'bigone': bigone,
    'jubi': jubi,
    'latoken': la_token,
    'coinex': coinex,
    'gateio': gate_io,
    'coinw': coin_w,
    'bika': bi_ka,
    'hotcoin': hot_coin,
    'digifinex': digi_finex,
    'lbank': l_bank,
    'bingx': bing_x,
    'probit': probit,
    'kraken': kraken,
    'kucoin': ku_coin,
    'poloniex': poloniex
}

special_exchanges = ['coinw', 'lbank', 'bingx', 'probit']


def execute_in_parallel(exchanges, temp_table):
    with ThreadPoolExecutor() as thread_executor, ProcessPoolExecutor() as process_executor:

        futures = [
            # thread_executor.submit(get_usd_to_cny_rate)
        ]
        for item in exchanges:
            exchange_name = item[1]

            if exchange_name in exchange_functions:
                if exchange_name in special_exchanges:
                    # futures.append(process_executor.submit(exchange_functions[exchange_name], temp_table_name))
                    futures.append(
                        thread_executor.submit(exchange_functions[exchange_name], temp_table))

                else:
                    futures.append(
                        thread_executor.submit(exchange_functions[exchange_name], temp_table))

        wait(futures)


def core_task():
    start_time = time.time()

    load_exclusion_list()
    exchanges = load_exchanges()
    # temp_table = create_temp_table()
    truncate_table()

    execute_in_parallel(exchanges, 'trade_data')

    fetch_combined_analysis_data('trade_data')
    greater, less = filter_result_by_depth('trade_data')
    redis_config.set_data('result', greater)

    insert_into_db(greater, less, 'trade_data')

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 3)
    logger.info(f"-------------------------------------------------- all executed in {elapsed_time} seconds.")


def run_task(seconds):
    while True:
        current_time = get_current_time()
        logger.info(f"core_task executed {current_time}")
        core_task()
        time.sleep(seconds)


core_task()
