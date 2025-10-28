from flask import Blueprint, render_template, request, redirect, url_for, flash
from werkzeug.security import generate_password_hash, check_password_hash
from saas.models import SaaSUser
from saas import db, login
from flask_login import login_user, logout_user, login_required, current_user

auth_bp = Blueprint('auth', __name__)

@login.user_loader
def load_user(id):
    return SaaSUser.query.get(int(id))

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        email = request.form['email']
        password = generate_password_hash(request.form['password'])
        subdomain = request.form['subdomain'].lower().replace(' ', '')
        if SaaSUser.query.filter_by(subdomain=subdomain).first():
            flash('Subdomain taken')
            return redirect(url_for('auth.register'))
        user = SaaSUser(email=email, password=password, subdomain=subdomain)
        db.session.add(user)
        db.session.commit()
        login_user(user)
        return redirect(url_for('billing.setup'))
    return render_template('saas/register.html')

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        user = SaaSUser.query.filter_by(email=request.form['email']).first()
        if user and check_password_hash(user.password, request.form['password']):
            login_user(user)
            return redirect(url_for('main.dashboard'))
        flash('Invalid login')
    return render_template('saas/login.html')

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))
