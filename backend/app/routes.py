from flask import Blueprint, request, jsonify
from .tushare.tushare_data import get_stock_data
from .tdengine.tdengine_data import execute_query, insert_data

main = Blueprint('main', __name__)


def init_routes(app):
    app.register_blueprint(main)


@main.route('/')
def index():
    return "Hello, World!"


@main.route('/fetch_stock_data', methods=['GET'])
def fetch_stock_data():
    stock_list, stock_data = get_stock_data()
    insert_data(stock_data)
    return jsonify({'message': 'Data fetched and stored successfully'})


@main.route('/query_stock_data', methods=['POST'])
def query_stock_data():
    query = request.json.get('query')
    result = execute_query(query)
    return jsonify(result)
