from flask import current_app as app
from taosrest import connect, TaosRestConnection, TaosRestCursor
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_tdengine_connection():
    # conn = taos.connect(
    #     host=app.config['TDENGINE_HOST'],
    #     user=app.config['TDENGINE_USER'],
    #     password=app.config['TDENGINE_PASSWORD'],
    #     database=app.config['TDENGINE_DATABASE']
    # )
    conn = connect(url=app.config['TDENGINE_URL'],
                   user=app.config['TDENGINE_USER'],
                   password=app.config['TDENGINE_PASSWORD'],
                   timeout=30)
    return conn


def execute_query(query):
    conn = get_tdengine_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


def insert_data(data):
    conn = get_tdengine_connection()
    cursor = conn.cursor()

    for record in data:
        query = f"""
        INSERT INTO stock_daily_k VALUES (
            '{record['trade_date']}', '{record['ts_code']}', {record['open']},
            {record['high']}, {record['low']}, {record['close']},
            {record['pre_close']}, {record['change']}, {record['pct_chg']},
            {record['vol']}, {record['amount']}
        )
        """
        cursor.execute(query)

    cursor.close()
    conn.close()


def load_csv_to_memory(file_path):
    """将 CSV 文件加载到内存中"""
    df = pd.read_csv(file_path)
    return df


def create_database_and_table():
    """创建数据库和超级表"""
    conn = get_tdengine_connection()
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {TDENGINE_DATABASE}")
    cursor.execute(f"USE {TDENGINE_DATABASE}")
    cursor.execute("""
        CREATE STABLE IF NOT EXISTS stock_daily_k (
            ts TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            pre_close FLOAT,
            change FLOAT,
            pct_chg FLOAT,
            vol FLOAT,
            amount FLOAT
        ) TAGS (ts_code NCHAR(10))
    """)
    cursor.close()
    conn.close()

def insert_data_to_tdengine(df):
    """将数据批量插入到 TDengine"""
    conn = taosrest.connect(host=TDENGINE_HOST, user=TDENGINE_USER, password=TDENGINE_PASSWORD, database=TDENGINE_DATABASE)
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO {row['ts_code']}
            USING stock_daily_k
            TAGS ('{row['ts_code']}')
            VALUES ('{row['trade_date']}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, 
                    {row['pre_close']}, {row['change']}, {row['pct_chg']}, {row['vol']}, {row['amount']})
        """)
    cursor.close()
    conn.close()