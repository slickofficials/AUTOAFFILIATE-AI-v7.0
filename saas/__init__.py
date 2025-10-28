from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_login import LoginManager

db = SQLAlchemy()
migrate = Migrate()
login = LoginManager()
login.login_view = 'auth.login'

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['PAYSTACK_SECRET_KEY'] = os.getenv('PAYSTACK_SECRET_KEY')
    app.config['PAYSTACK_PUBLIC_KEY'] = os.getenv('PAYSTACK_PUBLIC_KEY')

    db.init_app(app)
    migrate.init_app(app, db)
    login.init_app(app)

    from saas.auth import auth_bp
    from saas.main import main_bp
    from saas.billing import billing_bp
    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(main_bp)
    app.register_blueprint(billing_bp, url_prefix='/billing')

    return app
