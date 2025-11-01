# app.py - v14.2 $10M EMPIRE | DASHBOARD AFTER LOGIN | REAL IP FROM X-Forwarded-For (LAST)
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, send_from_directory
from flask_compress import Compress
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import os
import redis
import rq
import psycopg
from psycopg.rows import dict_row
import openai
from datetime import datetime, timedelta
from twilio.rest import Client

# === INIT ===
app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'slickofficials_hq_2025')
Compress(app)
COMPANY = "SlickOfficials HQ | Amson Multi Global LTD"
CONTACT_EMAIL = "support@slickofficials.com"

# === CONFIG ===
DB_URL = os.getenv('DATABASE_URL')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
r = redis.from_url(REDIS_URL)
queue = rq.Queue(connection=r)
openai.api_key = os.getenv('OPENAI_API_KEY')

# === SECURITY CONFIG ===
ALLOWED_EMAIL = os.getenv('ALLOWED_EMAIL')
ALLOWED_IP = os.getenv('ALLOWED_IP')  # e.g., 102.88.34.12
ADMIN_PASS = os.getenv('ADMIN_PASS')

TWILIO_SID = os.getenv('TWILIO_SID')
TWILIO_TOKEN = os.getenv('TWILIO_TOKEN')
YOUR_WHATSAPP = os.getenv('YOUR_WHATSAPP')

# === LOGIN TRACKING ===
failed_logins = {}
LOCKOUT_DURATION = timedelta(hours=24)
MAX_ATTEMPTS = 10

# === FLASK-LOGIN ===
class User(UserMixin):
    def __init__(self, email):
        self.id = email

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

@login_manager.user_loader
def load_user(user_id):
    if user_id == ALLOWED_EMAIL:
        return User(user_id)
    return None

# === TWILIO CLIENT ===
client = Client(TWILIO_SID, TWILIO_TOKEN) if TWILIO_SID and TWILIO_TOKEN else None

# === SEND WHATSAPP ALERT ===
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
    except Exception as e:
        print(f"[WHATSAPP FAILED] {e}")

# === GET REAL CLIENT IP (LAST IN X-Forwarded-For) ===
def get_client_ip():
    if request.headers.getlist("X-Forwarded-For"):
        ips = [ip.strip() for ip in request.headers.getlist("X-Forwarded-For")[0].split(',')]
        return ips[-1]  # LAST IP = REAL CLIENT
    return request.remote_addr

# === ACCESS CHECK ===
def check_access():
    client_ip = get_client_ip()
    now = datetime.now()

    # Clear expired lockouts
    for ip in list(failed_logins):
        if failed_logins[ip]['locked_until'] < now:
            del failed_logins[ip]

    if client_ip in failed_logins and failed_logins[client_ip]['locked_until'] > now:
        mins = int((failed_logins[client_ip]['locked_until'] - now).total_seconds() // 60)
        flash(f"Locked out. Try again in {mins} minutes.")
        return False

    return client_ip == ALLOWED_IP

# === CACHE & SECURITY HEADERS ===
@app.after_request
def add_header(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Cache-Control'] = 'public, max-age=31536000'
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    return response

# === DATABASE ===
def get_db():
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# === SERVE STATIC FILES ===
@app.route('/sitemap.xml')
def sitemap():
    return send_from_directory('.', 'sitemap.xml')

@app.route('/robots.txt')
def robots():
    return send_from_directory('.', 'robots.txt')

# === ROUTES ===

@app.route('/')
def index():
    if check_access():
        return render_template('welcome.html', company=COMPANY, title="Welcome")
    return render_template('coming_soon.html', company=COMPANY, title="Coming Soon")

@app.route('/coming_soon')
def coming_soon():
    return render_template('coming_soon.html', company=COMPANY, title="Coming Soon")

@app.route('/login', methods=['GET', 'POST'])
def login():
    client_ip = get_client_ip()

    if request.method == 'GET':
        return render_template('login.html', company=COMPANY, title="Private Login")

    if client_ip != ALLOWED_IP:
        send_alert("BLOCKED LOGIN", f"Wrong IP: {client_ip}")
        flash("Access Denied: Invalid IP")
        return render_template('coming_soon.html')

    email = request.form['email'].strip().lower()
    password = request.form['password']

    if client_ip not in failed_logins:
        failed_logins[client_ip] = {'count': 0, 'locked_until': None}

    if email == ALLOWED_EMAIL and password == ADMIN_PASS:
        if client_ip in failed_logins:
            del failed_logins[client_ip]
        user = User(email)
        login_user(user)
        send_alert("DASHBOARD ACCESSED", f"IP: {client_ip}")
        return redirect(url_for('dashboard'))
    else:
        failed_logins[client_ip]['count'] += 1
        left = MAX_ATTEMPTS - failed_logins[client_ip]['count']
        if failed_logins[client_ip]['count'] >= 3:
            send_alert("FAILED LOGIN", f"Attempt #{failed_logins[client_ip]['count']}\nIP: {client_ip}")
        if left <= 0:
            failed_logins[client_ip]['locked_until'] = datetime.now() + LOCKOUT_DURATION
            send_alert("LOCKED OUT", f"10 fails\nIP: {client_ip}")
            flash("BANNED: 24hr lock.")
        else:
            flash(f"Invalid. {left} left.")
        return render_template('login.html', company=COMPANY, title="Private Login")

@app.route('/dashboard')
@login_required
def dashboard():
    if not check_access():
        logout_user()
        flash("Session expired or IP changed.")
        return redirect(url_for('login'))

    try:
        conn, cur = get_db()
        cur.execute("SELECT COUNT(*) as post_count FROM posts WHERE status='sent'")
        posts_sent = cur.fetchone()['post_count'] or 0
        cur.execute("SELECT COALESCE(SUM(amount), 0) as total_revenue FROM earnings")
        revenue = cur.fetchone()['total_revenue'] or 0
        cur.execute("SELECT COUNT(*) as ref_count FROM referrals")
        referrals = cur.fetchone()['ref_count'] or 0
        conn.close()
    except Exception as e:
        print(f"DB ERROR: {e}")
        posts_sent = revenue = referrals = 0

    return render_template('dashboard.html',
                         posts_sent=posts_sent,
                         revenue=revenue,
                         referrals=referrals,
                         company=COMPANY,
                         title="Dashboard | $10M Empire")

@app.route('/privacy')
def privacy():
    if check_access():
        return render_template('privacy.html', company=COMPANY, contact_email=CONTACT_EMAIL, title="Privacy Policy")
    return render_template('coming_soon.html')

@app.route('/logout')
@login_required
def logout():
    send_alert("LOGGED OUT", "Session ended.")
    logout_user()
    return redirect(url_for('index'))

@app.route('/health')
def health():
    return 'OK', 200

@app.errorhandler(404)
def not_found(e):
    return render_template('coming_soon.html'), 404

@app.route('/<path:path>')
def catch_all(path):
    return render_template('coming_soon.html')

# === RUN ===
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)), debug=False)
