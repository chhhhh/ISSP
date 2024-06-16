from flask import Flask
from backend.app.config import Config
from taosrest import connect, TaosRestConnection, TaosRestCursor
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Queue, Process
import time
from threading import Thread
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class TDEngineClient:
    def __init__(self, config):
        self.url = config.TDENGINE_URL
        self.user = config.TDENGINE_USER
        self.password = config.TDENGINE_PASSWORD
        self.database = config.TDENGINE_DATABASE
        self.conn = self.init_td_engine_connection()
        self.pool = ThreadPoolExecutor(max_workers=10)

    def init_td_engine_connection(self):
        conn = connect(url=self.url,
                       user=self.user,
                       password=self.password,
                       timeout=30)
        return conn

    def create_database(self):
        cursor = self.conn.cursor()
        cursor.execute(f"""CREATE DATABASE IF NOT EXISTS {self.database}""")
        cursor.close()

    def create_super_table(self):
        cursor = self.conn.cursor()
        cursor.execute(f"""            
            CREATE STABLE IF NOT EXISTS {self.database}.stock_daily_k (
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

    def create_tables(self, t_name):
        cursor = self.conn.cursor()
        sql = f"""CREATE TABLE IF NOT EXISTS {self.database}.`{t_name}` USING {self.database}.stock_daily_k TAGS ('{t_name}')
        """
        logging.info(sql)
        try:
            cursor.execute(sql)
        except Exception as e:
            logging.error(f"Error creating table {t_name}: {e}")
        finally:
            cursor.close()

    def write_batch(self, batch):
        cursor = self.conn.cursor()
        if not batch:
            return
        t_name = batch[1]['ts_code'].replace('.', '_')
        self.create_tables(t_name)
        sql = f"""INSERT INTO {self.database}.`{t_name}` VALUES """

        for row in batch:
            trade_date = str(row['trade_date'])  # 确保 trade_date 是字符串
            trade_date = datetime.strptime(trade_date, '%Y%m%d').strftime('%Y-%m-%d %H:%M:%S')
            sql += f" ('{trade_date}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, {row['pre_close']}, {row['change']}, {row['pct_chg']}, {row['vol']}, {row['amount']}) "
        logging.debug(sql)
        logging.info(sql)
        retries = 3
        while retries > 0:
            try:
                cursor.execute(sql)
                break
            except KeyError as e:
                logging.error(f"Data format error: {e}")
            except ValueError as e:
                logging.error(f"Data conversion error: {e}")
            except Exception as e:
                logging.error(f"Error executing batch insert: {e}")
                retries -= 1
                if retries == 0:
                    logging.error("Max retries exceeded, giving up.")
                else:
                    logging.info(f"Retrying... ({3 - retries} attempts left)")
                    time.sleep(1)  # 等待一秒后重试
            finally:
                cursor.close()

    def close_connection(self):
        self.conn.close()


class CSVDataLoader:
    def __init__(self, config):
        self.stock_list_path = config.CSV_CODE_ADDR
        self.data_dir = config.CSV_STOCKS_DIR
        self.stock_list = self.read_stock_list()

    def read_stock_list(self):
        df = pd.read_csv(self.stock_list_path)
        return df['ts_code'].tolist()

    def read_stock_data(self, ts_code):
        file_path = f"{self.data_dir}/{ts_code}.csv"
        df = pd.read_csv(file_path)
        return df


class DataPipeline:
    def __init__(self, config):
        self.config = config
        self.tdengine_client = TDEngineClient(config)
        self.data_loader = CSVDataLoader(config)

        self.csv_file_path = config.CSV_CODE_ADDR
        self.data_dir = config.CSV_STOCKS_DIR

    def run_read_task(self, task_queue, infinity):
        logging.info(f"Read task started.")
        while True:
            for ts_code in self.data_loader.stock_list:
                df = self.data_loader.read_stock_data(ts_code)
                for _, row in df.iterrows():
                    task_queue.put(row)
            if not infinity:
                break
            time.sleep(1)
        logging.info(f"Read task completed.")

    def run_write_task(self, write_task_id, queue, done_queue):
        logging.info(f"Write task {write_task_id} started.")
        batch = []
        while True:
            try:
                while not queue.empty() and len(batch) < self.config.MAX_BATCH_SIZE:
                    batch.append(queue.get(timeout=1))
                if batch:
                    self.tdengine_client.write_batch(batch)
                    batch.clear()
                if done_queue.qsize() > 0:
                    done_queue.get()
                    break
            except Exception as e:
                logging.error(f"Write task {write_task_id}: Error - {e}")
        logging.info(f"Write task {write_task_id} completed.")
        self.tdengine_client.close_connection()

    def main(self, infinity=False):
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        self.tdengine_client.create_database()
        self.tdengine_client.create_super_table()

        done_queue = Queue()
        task_queue = Queue()

        write_threads = [Thread(target=self.run_write_task, args=(i, task_queue, done_queue)) for i in
                         range(self.config.WRITE_TASK_COUNT)]
        for t in write_threads:
            t.start()
            logging.debug(f"Write thread started with id {t.ident}")

        read_threads = []
        for i in range(self.config.READ_TASK_COUNT):
            t = Thread(target=self.run_read_task, args=(task_queue, infinity))
            t.start()
            logging.debug(f"Read thread {i} started with id {t.ident}")
            read_threads.append(t)

        try:
            for t in read_threads:
                t.join()
            for t in write_threads:
                t.join()
            time.sleep(1)
            logging.info("All tasks completed.")

        except KeyboardInterrupt:
            [t.join() for t in read_threads]
            [t.join() for t in write_threads]
            task_queue.join()


if __name__ == '__main__':
    app = Flask(__name__)
    config = Config()
    # 配置TDengine连接
    tdengine_client = TDEngineClient(config)
    # 数据管道
    data_pipeline = DataPipeline(config)
    # 运行数据管道
    data_pipeline.main()
