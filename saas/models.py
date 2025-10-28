from saas import db
from flask_login import UserMixin
from datetime import datetime

class SaaSUser(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(150), unique=True, nullable=False)
    password = db.Column(db.String(150), nullable=False)
    subdomain = db.Column(db.String(50), unique=True)
    paystack_customer_code = db.Column(db.String(100))
    paystack_subscription_code = db.Column(db.String(100))
    plan_price = db.Column(db.Float, default=150000)  # ₦150k/mo
    status = db.Column(db.String(20), default='active')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def get_id(self):
        return str(self.id) 
