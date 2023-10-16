from flask import jsonify

from data_depth_web.mod10_bybit_depth import bybit
from data_depth_web.mod11_xt_depth import xt
from data_depth_web.mod12_hitbtc_depth import hitbtc
from data_depth_web.mod13_bit_mart_depth import bit_mart
from data_depth_web.mod14_bigone_depth import bigone
from data_depth_web.mod15_jubi_depth import jubi
from data_depth_web.mod16_la_token_depth import la_token
from data_depth_web.mod17_coinex_depth import coinex
from data_depth_web.mod18_gate_io_depth import gate_io
from data_depth_web.mod19_coin_w_depth import coin_w
from data_depth_web.mod1_okx_depth import okx
from data_depth_web.mod20_bi_ka_depth import bi_ka
from data_depth_web.mod21_hot_coin_depth import hot_coin
from data_depth_web.mod22_digi_finex_depth import digi_finex
from data_depth_web.mod23_l_bank_depth import l_bank
from data_depth_web.mod24_bing_x_depth import bing_x
from data_depth_web.mod25_probit_depth import probit
from data_depth_web.mod26_kraken_depth import kraken
from data_depth_web.mod27_ku_coin_depth import ku_coin
from data_depth_web.mod28_poloniex_depth import poloniex
from data_depth_web.mod2_binance_depth import binance
from data_depth_web.mod3_huobi_depth import huobi
from data_depth_web.mod4_bit_get_depth import bit_get
from data_depth_web.mod5_bitfinex_depth import bitfinex
from data_depth_web.mod6_mexc_depth import mexc
from data_depth_web.mod7_bit_venus_depth import bit_venus
from data_depth_web.mod8_deep_coin_depth import deep_coin
from data_depth_web.mod9_ascend_ex_depth import ascend_ex


def depth(exchange_name, symbol_name, reference):
    if exchange_name == 'okx':
        return jsonify(okx(symbol_name, reference))
    if exchange_name == 'binance':
        return jsonify(binance(symbol_name, reference))
    if exchange_name == 'huobi':
        return jsonify(huobi(symbol_name, reference))
    if exchange_name == 'bitget':
        return jsonify(bit_get(symbol_name, reference))
    if exchange_name == 'bitfinex':
        return jsonify(bitfinex(symbol_name, reference))
    if exchange_name == 'mexc':
        return jsonify(mexc(symbol_name, reference))
    if exchange_name == 'bitvenus':
        return jsonify(bit_venus(symbol_name, reference))
    if exchange_name == 'deepcoin':
        return jsonify(deep_coin(symbol_name, reference))
    if exchange_name == 'ascendex':
        return jsonify(ascend_ex(symbol_name, reference))
    if exchange_name == 'bybit':
        return jsonify(bybit(symbol_name, reference))
    if exchange_name == 'xt':
        return jsonify(xt(symbol_name, reference))
    if exchange_name == 'hitbtc':
        return jsonify(hitbtc(symbol_name, reference))
    if exchange_name == 'bitmart':
        return jsonify(bit_mart(symbol_name, reference))
    if exchange_name == 'bigone':
        return jsonify(bigone(symbol_name, reference))
    if exchange_name == 'jubi':
        return jsonify(jubi(symbol_name, reference))
    if exchange_name == 'latoken':
        return jsonify(la_token(symbol_name, reference))
    if exchange_name == 'coinex':
        return jsonify(coinex(symbol_name, reference))
    if exchange_name == 'gateio':
        return jsonify(gate_io(symbol_name, reference))
    if exchange_name == 'coinw':
        return jsonify(coin_w(symbol_name, reference))
    if exchange_name == 'bika':
        return jsonify(bi_ka(symbol_name, reference))
    if exchange_name == 'hotcoin':
        return jsonify(hot_coin(symbol_name, reference))
    if exchange_name == 'digifinex':
        return jsonify(digi_finex(symbol_name, reference))
    if exchange_name == 'lbank':
        return jsonify(l_bank(symbol_name, reference))
    if exchange_name == 'bingx':
        return jsonify(bing_x(symbol_name, reference))
    if exchange_name == 'probit':
        return jsonify(probit(symbol_name, reference))
    if exchange_name == 'kraken':
        return kraken(symbol_name, reference)
    if exchange_name == 'kucoin':
        return ku_coin(symbol_name, reference)
    if exchange_name == 'poloniex':
        return poloniex(symbol_name, reference)
