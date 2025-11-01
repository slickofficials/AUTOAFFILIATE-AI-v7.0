# app.py - v10.4 $10M EMPIRE | FIXED LOGIN + DASHBOARD + WHATSAPP + IP SECURITY
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, send_from_directory
from flask_compress import Compress
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import os
import redis
import rq
import psycopg
from psycopg.rows import dict_row
import bcrypt
import openai
import tempfile
from google_auth_oauthlib.flow import InstalledAppFlow
import requests
from datetime import datetime, timedelta
from twilio.rest import Client

# === INIT ===
app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'slickofficials_hq_2025')
Compress(app)
COMPANY = "SlickOfficials HQ | Amson Multi Global LTD"

# === CONFIG ===
DB_URL = os.getenv('DATABASE_URL')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
r = redis.from_url(REDIS_URL)
queue = rq.Queue(connection=r)
openai.api_key = os.getenv('OPENAI_API_KEY')

# === SECURITY CONFIG ===
ALLOWED_EMAIL = os.getenv('ALLOWED_EMAIL', 'slickofficials@gmail.com')
ALLOWED_IP = os.getenv('ALLOWED_IP', '102.89.32.32')
ADMIN_PASS = os.getenv('ADMIN_PASS')
SUPPORT_EMAIL = "support@slickofficials.com"

TWILIO_SID = os.getenv('TWILIO_SID')
TWILIO_TOKEN = os.getenv('TWILIO_TOKEN')
YOUR_WHATSAPP = os.getenv('YOUR_WHATSAPP')

# === LOGIN TRACKING ===
failed_logins = {}
LOCKOUT_DURATION = timedelta(hours=24)
MAX_ATTEMPTS = 10

# === FLASK-LOGIN ===
class User(UserMixin):
    def __init__(self, id, email):
        self.id = id
        self.email = email

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

@login_manager.user_loader
def load_user(user_id):
    return User(user_id, ALLOWED_EMAIL)

# === TWILIO ALERT ===
client = Client(TWILIO_SID, TWILIO_TOKEN) if TWILIO_SID and TWILIO_TOKEN else None

def send_alert(title, body):
    if not client or not YOUR_WHATSAPP:
        return
    msg = f"*{title}*\n{body}\nTime: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    try:
        client.messages.create(
            from_='whatsapp:+14155238886',
            body=msg,
            to=YOUR_WHATSAPP
        )
        print(f"[WHATSAPP] {title}")
    except Exception as e:
        print(f"[WHATSAPP FAILED] {e}")

# === IP CHECK ===
def check_access():
    client_ip = request.remote_addr
    now = datetime.now()
    for ip in list(failed_logins):
        if failed_logins[ip]['locked_until'] and failed_logins[ip]['locked_until'] < now:
            del failed_logins[ip]
    if client_ip in failed_logins and failed_logins[client_ip]['locked_until'] and failed_logins[client_ip]['locked_until'] > now:
        mins = int((failed_logins[client_ip]['locked_until'] - now).total_seconds() // 60)
        flash(f"Locked out. Try again in {mins} minutes.")
        return False
    if client_ip != ALLOWED_IP:
        flash("Unauthorized IP access attempt detected.")
        send_alert("BLOCKED ACCESS", f"IP: {client_ip} tried accessing dashboard.")
        return False
    return True

# === SECURITY HEADERS ===
@app.after_request
def add_header(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Cache-Control'] = 'no-store'
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    return response

# === DATABASE ===
def get_db():
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# === PUBLIC PAGES ===
@app.route('/')
def index():
    if check_access():
        return redirect(url_for('login'))
    return render_template('coming_soon.html', company=COMPANY, title="Coming Soon", support_email=SUPPORT_EMAIL)

@app.route('/privacy')
def privacy():
    return render_template('coming_soon.html', company=COMPANY, title="Privacy Policy", support_email=SUPPORT_EMAIL)

@app.route('/terms')
def terms():
    return render_template('coming_soon.html', company=COMPANY, title="Terms of Service", support_email=SUPPORT_EMAIL)

# === LOGIN ===
@app.route('/login', methods=['GET', 'POST'])
def login():
    client_ip = request.remote_addr
    if request.method == 'GET':
        return render_template('login.html', company=COMPANY, title="Private Login", support_email=SUPPORT_EMAIL)

    if client_ip != ALLOWED_IP:
        send_alert("BLOCKED LOGIN ATTEMPT", f"Wrong IP: {client_ip}")
        flash("Access Denied: Invalid IP")
        return render_template('coming_soon.html', company=COMPANY, title="Access Denied", support_email=SUPPORT_EMAIL)

    email = request.form['email'].strip().lower()
    password = request.form['password']

    if client_ip not in failed_logins:
        failed_logins[client_ip] = {'count': 0, 'locked_until': None}

    if email == ALLOWED_EMAIL and password == ADMIN_PASS:
        if client_ip in failed_logins:
            del failed_logins[client_ip]
        user = User(email, ALLOWED_EMAIL)
        login_user(user)
        send_alert("LOGIN SUCCESS", f"Dashboard accessed by {email}\nIP: {client_ip}")
        return redirect(url_for('dashboard'))
    else:
        failed_logins[client_ip]['count'] += 1
        left = MAX_ATTEMPTS - failed_logins[client_ip]['count']
        if left <= 0:
            failed_logins[client_ip]['locked_until'] = datetime.now() + LOCKOUT_DURATION
            send_alert("LOCKOUT TRIGGERED", f"IP: {client_ip}")
            flash("Account locked. Try again in 24 hours.")
        else:
            flash(f"Invalid credentials. {left} attempts left.")
        return render_template('login.html', company=COMPANY, title="Private Login", support_email=SUPPORT_EMAIL)

# === LOGOUT ===
@app.route('/logout')
@login_required
def logout():
    send_alert("LOGGED OUT", f"{current_user.email} logged out.")
    logout_user()
    return redirect(url_for('index'))

# === DASHBOARD ===
@app.route('/dashboard')
@login_required
def dashboard():
    if not check_access():
        return render_template('coming_soon.html', company=COMPANY, support_email=SUPPORT_EMAIL)

    try:
        conn, cur = get_db()
        cur.execute("SELECT COUNT(*) as post_count FROM posts WHERE status='sent'")
        posts_sent = cur.fetchone()['post_count'] or 0
        cur.execute("SELECT COALESCE(SUM(amount), 0) as total_revenue FROM earnings")
        revenue = cur.fetchone()['total_revenue'] or 0
        conn.close()
    except:
        posts_sent = 0
        revenue = 0

    return render_template('dashboard.html',
                           posts_sent=posts_sent,
                           revenue=revenue,
                           company=COMPANY,
                           title="Dashboard | $10M Empire",
                           support_email=SUPPORT_EMAIL)

# === BEAST CAMPAIGN ===
@app.route('/beast_campaign')
@login_required
def beast_campaign():
    if not check_access():
        return render_template('coming_soon.html')
    job = queue.enqueue('worker.run_daily_campaign')
    return jsonify({'status': 'v10.4 $10M BEAST MODE ACTIVE', 'job_id': job.id})

# === HEALTH ===
@app.route('/health')
def health():
    return 'OK', 200

# === 404 ===
@app.errorhandler(404)
def not_found(e):
    return render_template('coming_soon.html', company=COMPANY, support_email=SUPPORT_EMAIL), 404

# === RUN ===
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)), debug=False)
