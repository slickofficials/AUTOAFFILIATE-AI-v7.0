# saas/models.py
from flask_login import UserMixin
from . import db
from datetime import datetime

class SaaSUser(UserMixin, db.Model):
    __tablename__ = 'saas_users'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(150), unique=True, nullable=False)
    password = db.Column(db.String(150), nullable=False)
    subdomain = db.Column(db.String(50), unique=True)
    paystack_customer_code = db.Column(db.String(100))
    paystack_subscription_code = db.Column(db.String(100))
    status = db.Column(db.String(20), default='active')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def get_id(self):
        return str(self.id)
