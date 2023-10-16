import os

from concurrent.futures import ThreadPoolExecutor
from config.logger_config import setup_logger
from data_depth.mod10_bybit_depth import bybit
from data_depth.mod11_xt_depth import xt
from data_depth.mod12_hitbtc_depth import hitbtc
from data_depth.mod13_bit_mart_depth import bit_mart
from data_depth.mod14_bigone_depth import bigone
from data_depth.mod15_jubi_depth import jubi
from data_depth.mod16_la_token_depth import la_token
from data_depth.mod17_coinex_depth import coinex
from data_depth.mod18_gate_io_depth import gate_io
from data_depth.mod19_coin_w_depth import coin_w
from data_depth.mod1_okx_depth import okx
from data_depth.mod20_bi_ka_depth import bi_ka
from data_depth.mod21_hot_coin_depth import hot_coin
from data_depth.mod22_digi_finex_depth import digi_finex
from data_depth.mod23_l_bank_depth import l_bank
from data_depth.mod24_bing_x_depth import bing_x
from data_depth.mod25_probit_depth import probit
from data_depth.mod26_kraken_depth import kraken
from data_depth.mod27_ku_coin_depth import ku_coin
from data_depth.mod28_poloniex_depth import poloniex
from data_depth.mod2_binance_depth import binance
from data_depth.mod3_huobi_depth import huobi
from data_depth.mod4_bit_get_depth import bit_get
from data_depth.mod5_bitfinex_depth import bitfinex
from data_depth.mod6_mexc_depth import mexc
from data_depth.mod7_bit_venus_depth import bit_venus
from data_depth.mod8_deep_coin_depth import deep_coin
from data_depth.mod9_ascend_ex_depth import ascend_ex

from database.db_service import usd_to_cny_rate, get_result_from_db
from util.depth_util import filter_asks, filter_bids, calculate_total
from web_interaction.reference import reference_list
from config.factor_config import TOTAL_PRICE_LIMIT

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("filter_result_by_depth", os.path.join(project_root, 'log', 'app.log'))


def filter_result_by_depth(temp_table):
    data = get_result_from_db(temp_table)
    reference = reference_list()
    usd_to_cny = usd_to_cny_rate()[0][1]

    logger.info(f"Total symbols : {len(data)}")

    args_for_validation = [(record, reference, usd_to_cny) for record in data]
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(validate_depth_data, args_for_validation))

    limit = TOTAL_PRICE_LIMIT
    valid_results = [item for item in results if item]
    greater = [item for item in valid_results if item[10] >= limit and item[11] >= limit]
    less = [item for item in valid_results if item[10] < limit or item[11] < limit]

    logger.info(f"Symbols greater  : {len(greater)}")
    logger.info(f"Symbols less : {len(less)}")

    return greater, less


def validate_depth_data(args):
    record, reference, usd_to_cny = args
    symbol = record[0]
    exchange_ask = record[2]
    exchange_bid = record[3]
    reference_ask = record[5]
    reference_bid = record[7]

    reference_value_ask = [x[3] for x in reference if x[1] == exchange_ask and x[2] == reference_ask][0]
    reference_value_bid = [x[3] for x in reference if x[1] == exchange_bid and x[2] == reference_bid][0]

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            future1 = executor.submit(get_depth_data, exchange_ask, symbol, reference_ask)
            future2 = executor.submit(get_depth_data, exchange_bid, symbol, reference_bid)

            depth_data1 = future1.result()
            depth_data2 = future2.result()

            if not depth_data1 or not depth_data2 or 'asks' not in depth_data1 or 'bids' not in depth_data2:
                logger.info(f"{exchange_ask} {exchange_bid}")
                logger.info(f"{symbol}-{reference_ask} ask_data: {depth_data1}")
                logger.info(f"{symbol}-{reference_bid} bid_data: {depth_data2}")
                return None

            asks_filtered = filter_asks(depth_data1['asks'], float(depth_data1['asks'][0][0]))
            bids_filtered = filter_bids(depth_data2['bids'], float(depth_data2['bids'][0][0]))

            asks_total = calculate_total(asks_filtered, reference_value_ask, usd_to_cny)
            bids_total = calculate_total(bids_filtered, reference_value_bid, usd_to_cny)

            record[10] = asks_total
            record[11] = bids_total
            return record

    except Exception as e:
        logger.error(f"Error occurred while processing data for symbol: {symbol}. Error: {repr(e)}")
        return None


def get_depth_data(exchange_name, symbol_name, reference):
    if exchange_name == 'okx':
        return okx(symbol_name, reference)
    if exchange_name == 'binance':
        return binance(symbol_name, reference)
    if exchange_name == 'huobi':
        return huobi(symbol_name, reference)
    if exchange_name == 'bitget':
        return bit_get(symbol_name, reference)
    if exchange_name == 'bitfinex':
        return bitfinex(symbol_name, reference)
    if exchange_name == 'mexc':
        return mexc(symbol_name, reference)
    if exchange_name == 'bitvenus':
        return bit_venus(symbol_name, reference)
    if exchange_name == 'deepcoin':
        return deep_coin(symbol_name, reference)
    if exchange_name == 'ascendex':
        return ascend_ex(symbol_name, reference)
    if exchange_name == 'bybit':
        return bybit(symbol_name, reference)
    if exchange_name == 'xt':
        return xt(symbol_name, reference)
    if exchange_name == 'hitbtc':
        return hitbtc(symbol_name, reference)
    if exchange_name == 'bitmark':
        return bit_mart(symbol_name, reference)
    if exchange_name == 'bigone':
        return bigone(symbol_name, reference)
    if exchange_name == 'jubi':
        return jubi(symbol_name, reference)
    if exchange_name == 'latoken':
        return la_token(symbol_name, reference)
    if exchange_name == 'coinex':
        return coinex(symbol_name, reference)
    if exchange_name == 'gateio':
        return gate_io(symbol_name, reference)
    if exchange_name == 'coinw':
        return coin_w(symbol_name, reference)
    if exchange_name == 'bika':
        return bi_ka(symbol_name, reference)
    if exchange_name == 'hotcoin':
        return hot_coin(symbol_name, reference)
    if exchange_name == 'digifinex':
        return digi_finex(symbol_name, reference)
    if exchange_name == 'lbank':
        return l_bank(symbol_name, reference)
    if exchange_name == 'bingx':
        return bing_x(symbol_name, reference)
    if exchange_name == 'probit':
        return probit(symbol_name, reference)
    if exchange_name == 'kraken':
        return kraken(symbol_name, reference)
    if exchange_name == 'kucoin':
        return ku_coin(symbol_name, reference)
    if exchange_name == 'poloniex':
        return poloniex(symbol_name, reference)
