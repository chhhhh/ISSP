import tushare as ts
import pandas as pd
from flask import current_app as app


def get_stock_data():
    ts.set_token(app.config['TUSHARE_TOKEN'])
    pro = ts.pro_api()

    # 获取沪深交易所A股列表
    stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code')

    stock_list = stocks['ts_code'].tolist()
    all_stock_data = []

    for ts_code in stock_list:
        df = pro.daily(ts_code=ts_code, start_date='20220101', end_date='20221231')
        all_stock_data.append(df)

    # 保存到本地文件
    stocks.to_csv('data/stock_list.csv', index=False)
    all_data = pd.concat(all_stock_data)
    all_data.to_csv('data/stock_data.csv', index=False)

    return stock_list, all_data.to_dict(orient='records')
