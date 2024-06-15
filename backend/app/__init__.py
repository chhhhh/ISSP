from flask import Flask
from .routes import main
from .config import Config


def create_app():
    app = Flask(__name__)

    config = Config()
    app.config['TDENGINE_URL'] = config.tdengine_url
    app.config['TDENGINE_HOST'] = config.tdengine_host
    app.config['TDENGINE_PORT'] = config.tdengine_port
    app.config['TDENGINE_USER'] = config.tdengine_user
    app.config['TDENGINE_PASSWORD'] = config.tdengine_password
    app.config['TDENGINE_DATABASE'] = config.tdengine_database
    app.config['TUSHARE_TOKEN'] = config.tushare_token

    app.register_blueprint(main)

    return app
