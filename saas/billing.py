# saas/billing.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app
from flask_login import login_required, current_user
import requests
from .models import SaaSUser
from . import db

billing_bp = Blueprint('billing', __name__)

@billing_bp.route('/setup')
@login_required
def setup():
    user = current_user
    return render_template('saas/billing.html', user=user, key=current_app.config['PAYSTACK_PUBLIC_KEY'])

@billing_bp.route('/subscribe', methods=['POST'])
@login_required
def subscribe():
    user = current_user
    plan = "PLN_autopro_150k"  # Your Paystack plan code
    email_token = request.form['email_token']

    url = "https://api.paystack.co/subscription"
    headers = {"Authorization": f"Bearer {current_app.config['PAYSTACK_SECRET_KEY']}"}
    payload = {"customer": user.paystack_customer_code, "plan": plan, "email_token": email_token}
    r = requests.post(url, json=payload, headers=headers)
    if r.status_code == 200:
        data = r.json()['data']
        user.paystack_subscription_code = data['subscription_code']
        user.status = 'active'
        db.session.commit()
        flash('Subscription Active! ₦150,000/mo')
    else:
        flash('Subscription failed')
    return redirect(url_for('saas_main.dashboard'))
