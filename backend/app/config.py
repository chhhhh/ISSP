import yaml


class Config:
    def __init__(self):
        with open(r'C:\Users\chenhao\PycharmProjects\ISSP\config.yaml', 'r') as file:
            config = yaml.safe_load(file)
        self.TDENGINE_URL = config['tdengine']['url']
        self.TDENGINE_HOST = config['tdengine']['host']
        self.TDENGINE_PORT = config['tdengine']['port']
        self.TDENGINE_USER = config['tdengine']['user']
        self.TDENGINE_PASSWORD = config['tdengine']['password']
        self.TDENGINE_DATABASE = config['tdengine']['database']

        self.TUSHARE_TOKEN = config['tushare']['token']

        self.CSV_CODE_ADDR = config['csv']['code_addr']
        self.CSV_STOCKS_DIR = config['csv']['stocks_dir']

        self.READ_TASK_COUNT = 10
        self.WRITE_TASK_COUNT = 10
        self.MAX_BATCH_SIZE = 3000
