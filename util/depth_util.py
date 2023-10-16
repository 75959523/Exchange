from database.db_service import usd_to_cny_rate
from web_interaction.reference import reference_list

from config.factor_config import ASK_PRICE_FACTOR
from config.factor_config import BID_PRICE_FACTOR


def filter_asks(data, base_price):
    upper_limit = ASK_PRICE_FACTOR * base_price
    return [entry for entry in data if float(entry[0]) <= upper_limit][:100]


def filter_bids(data, base_price):
    lower_limit = BID_PRICE_FACTOR * base_price
    return [entry for entry in data if float(entry[0]) >= lower_limit][:100]


def calculate_total(data, reference_value, usd_to_cny):
    return sum(float(entry[0]) * float(entry[1]) * float(reference_value) * float(usd_to_cny) for entry in data)


def combined_data(data, reference, exchange_name):
    ref_list = reference_list()
    usd_to_cny = usd_to_cny_rate()[0][1]
    ref_value = [x[3] for x in ref_list if x[1] == exchange_name and x[2] == reference][0]
    data['asks_total'] = calculate_total(filter_asks(data['asks'], float(data['asks'][0][0])), ref_value, usd_to_cny)
    data['bids_total'] = calculate_total(filter_bids(data['bids'], float(data['bids'][0][0])), ref_value, usd_to_cny)
    return data
