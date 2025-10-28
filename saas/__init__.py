# saas/__init__.py
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_login import LoginManager
from .config import Config

db = SQLAlchemy()
migrate = Migrate()
login = LoginManager()
login.login_view = 'saas_auth.login'

def create_saas_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)
    migrate.init_app(app, db)
    login.init_app(app)

    # Register SaaS blueprints
    from .auth import auth_bp as saas_auth_bp
    from .billing import billing_bp
    from .main import main_bp as saas_main_bp

    app.register_blueprint(saas_auth_bp, url_prefix='/saas')
    app.register_blueprint(billing_bp, url_prefix='/saas')
    app.register_blueprint(saas_main_bp, url_prefix='/saas')

    return app
