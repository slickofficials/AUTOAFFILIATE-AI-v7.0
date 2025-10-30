# saas/main.py
from flask import Blueprint, render_template
from flask_login import login_required, current_user

main_bp = Blueprint('saas_main', __name__)

@main_bp.route('/dashboard')
@login_required
def dashboard():
    company = current_user.subdomain.upper() + " HQ"
    return render_template('saas/dashboard.html', company=company, user=current_user)
