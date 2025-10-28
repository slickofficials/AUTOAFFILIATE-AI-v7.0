from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app
from flask_login import login_required, current_user
import requests
import uuid
from saas.models import SaaSUser
from saas import db

billing_bp = Blueprint('billing', __name__)

@billing_bp.route('/setup')
@login_required
def setup():
    user = current_user
    if not user.paystack_customer_code:
        # Create Paystack Customer
        url = "https://api.paystack.co/customer"
        headers = {"Authorization": f"Bearer {current_app.config['PAYSTACK_SECRET_KEY']}"}
        payload = {"email": user.email, "first_name": user.subdomain}
        r = requests.post(url, json=payload, headers=headers)
        if r.status_code == 201:
            user.paystack_customer_code = r.json()['data']['customer_code']
            db.session.commit()
    return render_template('saas/billing.html', user=user, key=current_app.config['PAYSTACK_PUBLIC_KEY'])

@billing_bp.route('/subscribe', methods=['POST'])
@login_required
def subscribe():
    user = current_user
    plan_code = "PLN_monthly_150k"  # Create in Paystack Dashboard
    email_token = str(uuid.uuid4())

    url = "https://api.paystack.co/subscription"
    headers = {"Authorization": f"Bearer {current_app.config['PAYSTACK_SECRET_KEY']}"}
    payload = {
        "customer": user.paystack_customer_code,
        "plan": plan_code,
        "email_token": email_token
    }
    r = requests.post(url, json=payload, headers=headers)
    if r.status_code == 200:
        data = r.json()['data']
        user.paystack_subscription_code = data['subscription_code']
        db.session.commit()
        flash('Subscription Active! ₦150,000/mo')
    return redirect(url_for('main.dashboard'))
