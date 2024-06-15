import yaml


class Config:
    def __init__(self):
        with open('config.yaml', 'r') as file:
            config = yaml.safe_load(file)
        self.tdengine_url = config['tdengine']['url']
        self.tdengine_host = config['tdengine']['host']
        self.tdengine_port = config['tdengine']['port']
        self.tdengine_user = config['tdengine']['user']
        self.tdengine_password = config['tdengine']['password']
        self.tdengine_database = config['tdengine']['database']

        self.tushare_token = config['tushare']['token']
