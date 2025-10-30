# saas/auth.py
from flask import Blueprint, render_template, request, redirect, url_for, flash
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import login_user, logout_user, login_required, current_user
from .models import SaaSUser
from . import db
import requests
import os

auth_bp = Blueprint('saas_auth', __name__)

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        email = request.form['email'].strip()
        subdomain = request.form['subdomain'].strip().lower().replace(' ', '-')
        password = generate_password_hash(request.form['password'])

        if SaaSUser.query.filter_by(email=email).first():
            flash('Email already registered')
            return redirect(url_for('saas_auth.register'))
        if SaaSUser.query.filter_by(subdomain=subdomain).first():
            flash('Subdomain taken')
            return redirect(url_for('saas_auth.register'))

        # Create Paystack Customer
        url = "https://api.paystack.co/customer"
        headers = {"Authorization": f"Bearer {current_app.config['PAYSTACK_SECRET_KEY']}"}
        payload = {"email": email, "first_name": subdomain}
        r = requests.post(url, json=payload, headers=headers)
        customer_code = None
        if r.status_code == 201:
            customer_code = r.json()['data']['customer_code']

        user = SaaSUser(email=email, password=password, subdomain=subdomain, paystack_customer_code=customer_code, status='trial')
        db.session.add(user)
        db.session.commit()
        login_user(user)
        flash('7-Day Free Trial Activated! Pay ₦150k on Day 8.')
        return redirect(url_for('saas_main.dashboard'))

    return render_template('saas/register.html')

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        user = SaaSUser.query.filter_by(email=request.form['email']).first()
        if user and check_password_hash(user.password, request.form['password']):
            login_user(user)
            return redirect(url_for('saas_main.dashboard'))
        flash('Invalid credentials')
    return render_template('saas/login.html')

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('saas_auth.login'))
