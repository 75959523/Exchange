import json

from flask import Flask, jsonify, request, make_response
from flask_cors import CORS
from config.redis_config import RedisConfig
from database.db_service import usd_to_cny_rate
from web_interaction.chain import add_chain_into_db, get_chain, delete_chain_record
from web_interaction.depth import depth
from web_interaction.exchange import exchange_list, update_status
from web_interaction.exclusion import exclusion_list, delete_exclusion_record, insert_exclusion_record
from web_interaction.reference import reference_list
from web_interaction.symbol import symbol_list, delete_record, insert_record
from web_interaction.user import get_all_users, add_user_into_db, update_user_info, delete_user_record, user_login

app = Flask(__name__)
CORS(app)
redis_config = RedisConfig()


@app.route('/api/get-analysis-data', methods=['GET'])
def get_analysis_data():
    data = redis_config.get_data('result')
    return jsonify(data)


@app.route('/api/get-depth', methods=['POST'])
def get_depth():
    data = request.json
    exchange_name = data['exchange']
    symbol_name = data['symbol']
    reference = data['reference']
    return depth(exchange_name, symbol_name, reference)


@app.route('/api/get-exchange-list', methods=['GET'])
def get_exchange_list():
    data = exchange_list()
    return jsonify(data)


@app.route('/api/get-exclusion-list', methods=['GET'])
def get_exclusion_list():
    data = exclusion_list()
    return jsonify(data)


@app.route('/api/get-symbol-list', methods=['GET'])
def get_symbol_list():
    data = symbol_list()
    return jsonify(data)


@app.route('/api/get-reference-list', methods=['GET'])
def get_reference_list():
    data = reference_list()
    return jsonify(data)


@app.route('/api/get-user-list', methods=['GET'])
def get_user_list():
    data = get_all_users()
    return jsonify(data)


@app.route('/api/update-exchange-status', methods=['POST'])
def update_exchange_status():
    data = request.json
    exchange_id = data['exchangeId']
    status = data['status']
    update_status(exchange_id, status)
    return "Success", 200


@app.route('/api/add-exclusion', methods=['POST'])
def add_exclusion():
    data = request.json
    insert_exclusion_record(data)
    return "Success", 200


@app.route('/api/delete-exclusion', methods=['POST'])
def delete_exclusion():
    data = request.json
    exclusion_id = data['exclusionId']
    delete_exclusion_record(exclusion_id)
    return "Success", 200


@app.route('/api/add-symbol', methods=['POST'])
def add_symbol():
    data = request.json
    symbol_name = data['symbolName']
    insert_record(symbol_name)
    return "Success", 200


@app.route('/api/delete-symbol', methods=['POST'])
def delete_symbol():
    data = request.json
    symbol_id = data['symbolId']
    delete_record(symbol_id)
    return "Success", 200


@app.route('/api/add-user', methods=['POST'])
def add_user():
    data = request.json
    data = add_user_into_db(data)
    return jsonify(data)


@app.route('/api/delete-user', methods=['POST'])
def delete_user():
    data = request.json
    user_id = data['userId']
    delete_user_record(user_id)
    return "Success", 200


@app.route('/api/update-user', methods=['POST'])
def update_user():
    data = request.json
    update_user_info(data)
    return "Success", 200


@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    user_name = data['username']
    input_password = data['password']

    success, message, user = user_login(user_name, input_password)
    if success:
        del user[2]
        response_data = {
            "status": "success",
            "message": "登陆成功",
            "user": user
        }
        response = make_response(json.dumps(response_data, ensure_ascii=False), 200)
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response
    else:
        return jsonify({"status": "failure", "message": message}), 401


@app.route('/api/get-rate', methods=['GET'])
def get_rate():
    data = usd_to_cny_rate()
    return jsonify(data)


@app.route('/api/add-chain', methods=['POST'])
def add_chain():
    data = request.json
    data = add_chain_into_db(data)
    return jsonify(data)


@app.route('/api/get-chain-list', methods=['GET'])
def get_chain_list():
    data = get_chain()
    return jsonify(data)


@app.route('/api/delete-chain', methods=['POST'])
def delete_chain():
    data = request.json
    chain_id = data['chainId']
    delete_chain_record(chain_id)
    return "Success", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True, use_reloader=False)
