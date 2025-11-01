# app.py - v10.0 $10M EMPIRE | v7.7 + v9.4 SECURITY + WHATSAPP ALERTS + FORT KNOX
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
import json
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

# === SECURITY CONFIG (v9.4) ===
ALLOWED_EMAIL = os.getenv('ALLOWED_EMAIL')      # ← SET IN RENDER
ALLOWED_IP = os.getenv('ALLOWED_IP')            # ← YOUR IP
ADMIN_PASS = os.getenv('ADMIN_PASS')            # ← YOUR PASSWORD

TWILIO_SID = os.getenv('TWILIO_SID')
TWILIO_TOKEN = os.getenv('TWILIO_TOKEN')
YOUR_WHATSAPP = os.getenv('YOUR_WHATSAPP')      # whatsapp:+234...

# === LOGIN TRACKING ===
failed_logins = {}
LOCKOUT_DURATION = timedelta(hours=24)
MAX_ATTEMPTS = 10

# === FLASK-LOGIN ===
class User(UserMixin):
    def __init__(self, id): self.id = id

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

@login_manager.user_loader
def load_user(user_id): return User(user_id)

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
        print(f"[WHATSAPP] {title}")
    except Exception as e:
        print(f"[WHATSAPP FAILED] {e}")

# === ACCESS CHECK (IP + LOCK) ===
def check_access():
    client_ip = request.remote_addr
    now = datetime.now()

    # Clean expired locks
    for ip in list(failed_logins):
        if failed_logins[ip]['locked_until'] < now:
            del failed_logins[ip]

    # Check lock
    if client_ip in failed_logins and failed_logins[client_ip]['locked_until'] > now:
        mins = int((failed_logins[client_ip]['locked_until'] - now).total_seconds() // 60)
        flash(f"Locked out. Try again in {mins} minutes.")
        return False

    # IP Whitelist
    if client_ip != ALLOWED_IP:
        return False

    return True

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

# === SERVE SITEMAP & ROBOTS ===
@app.route('/sitemap.xml')
def sitemap():
    return send_from_directory('.', 'sitemap.xml')

@app.route('/robots.txt')
def robots():
    return send_from_directory('.', 'robots.txt')

# === PUBLIC PAGES (COMING SOON FOR ALL BUT YOU) ===
@app.route('/')
def index():
    if check_access():
        return redirect(url_for('login'))
    return render_template('coming_soon.html', company=COMPANY, title="Coming Soon")

@app.route('/privacy')
def privacy():
    if check_access():
        return render_template('privacy.html', company=COMPANY, title="Privacy Policy")
    return render_template('coming_soon.html')

@app.route('/terms')
def terms():
    if check_access():
        return render_template('terms.html', company=COMPANY, title="Terms of Service")
    return render_template('coming_soon.html')

# === LOGIN (ONLY YOU) ===
@app.route('/login', methods=['GET', 'POST'])
def login():
    if not check_access():
        return render_template('coming_soon.html')

    client_ip = request.remote_addr

    if request.method == 'POST':
        email = request.form['email'].strip().lower()
        password = request.form['password']

        if client_ip not in failed_logins:
            failed_logins[client_ip] = {'count': 0, 'locked_until': None}

        if email == ALLOWED_EMAIL and password == ADMIN_PASS:
            if client_ip in failed_logins: del failed_logins[client_ip]
            login_user(User(email))
            send_alert("BEAST MODE ON", f"Dashboard accessed\nIP: {client_ip}")
            return redirect(url_for('dashboard'))
        else:
            failed_logins[client_ip]['count'] += 1
            left = MAX_ATTEMPTS - failed_logins[client_ip]['count']
            if failed_logins[client_ip]['count'] >= 3:
                send_alert("FAILED LOGIN", f"Attempt #{failed_logins[client_ip]['count']}\nIP: {client_ip}")
            if left <= 0:
                failed_logins[client_ip]['locked_until'] = datetime.now() + LOCKOUT_DURATION
                send_alert("ACCOUNT LOCKED", f"10 failed attempts\nIP: {client_ip}")
                flash("BANNED: 10 failed attempts. 24hr lock.")
            else:
                flash(f"Invalid. {left} attempts left.")

    return render_template('login.html', company=COMPANY, title="Private Login")

# === LOGOUT ===
@app.route('/logout')
@login_required
def logout():
    send_alert("LOGGED OUT", "Dashboard session ended.")
    logout_user()
    return redirect(url_for('index'))

# === DASHBOARD (ONLY YOU) ===
@app.route('/dashboard')
@login_required
def dashboard():
    if current_user.id != ALLOWED_EMAIL:
        return render_template('coming_soon.html')
    
    user_id = session.get('user_id')
    if not user_id:
        # Fallback: get from DB
        conn, cur = get_db()
        cur.execute("SELECT id FROM users WHERE email = %s", (ALLOWED_EMAIL,))
        user = cur.fetchone()
        conn.close()
        if user:
            user_id = user['id']
            session['user_id'] = user_id

    try:
        conn, cur = get_db()
        cur.execute("SELECT COUNT(*) as post_count FROM posts WHERE status='sent'")
        posts_sent = cur.fetchone()['post_count'] or 0
        cur.execute("SELECT COALESCE(SUM(amount), 0) as total_revenue FROM earnings WHERE user_id = %s", (user_id,))
        revenue = cur.fetchone()['total_revenue'] or 0
        cur.execute("SELECT COUNT(*) as ref_count FROM referrals WHERE referrer_id = %s", (user_id,))
        referrals = cur.fetchone()['ref_count'] or 0
        cur.execute("SELECT COALESCE(SUM(reward), 0) as ref_earnings FROM referrals WHERE referrer_id = %s", (user_id,))
        ref_earnings = cur.fetchone()['ref_earnings'] or 0
        cur.execute("SELECT referred_email, reward, created_at FROM referrals WHERE referrer_id = %s ORDER BY created_at DESC LIMIT 10", (user_id,))
        ref_list = cur.fetchall()
        conn.close()
    except Exception as e:
        posts_sent = revenue = referrals = ref_earnings = 0
        ref_list = []

    return render_template('dashboard.html',
                         posts_sent=posts_sent,
                         revenue=revenue,
                         referrals=referrals,
                         ref_earnings=ref_earnings,
                         ref_list=ref_list,
                         company=COMPANY,
                         title="Dashboard | $10M Empire")

# === ALL OTHER ROUTES (EXISTING) ===
@app.route('/api/stats')
def api_stats():
    if current_user.id != ALLOWED_EMAIL:
        return jsonify({'error': 'Unauthorized'}), 401
    # ... [same as before]
    # (Keep your existing code)

@app.route('/payout', methods=['POST'])
@login_required
def payout():
    if current_user.id != ALLOWED_EMAIL:
        return jsonify({'error': 'Unauthorized'}), 401
    # ... [same as before]

@app.route('/health')
def health():
    return 'OK', 200

@app.route('/beast_campaign')
@login_required
def beast_campaign():
    if current_user.id != ALLOWED_EMAIL:
        return render_template('coming_soon.html')
    job = queue.enqueue('worker.run_daily_campaign')
    return jsonify({'status': 'v10.0 $10M BEAST MODE ACTIVATED', 'job_id': job.id})

@app.route('/youtube_auth')
@login_required
def youtube_auth():
    if current_user.id != ALLOWED_EMAIL:
        return render_template('coming_soon.html')
    # ... [same as before]

@app.route('/youtube_callback')
def youtube_callback():
    # ... [same as before]

@app.route('/miniapp')
def miniapp():
    if check_access():
        return render_template('miniapp.html', company=COMPANY, title="Referral Mini App")
    return render_template('coming_soon.html')

@app.route('/upsell', methods=['POST'])
def upsell():
    # ... [same as before]

@app.errorhandler(404)
def not_found(e):
    if check_access():
        return render_template('404.html', company=COMPANY, title="404"), 404
    return render_template('coming_soon.html'), 404

# === CATCH-ALL FOR ANY OTHER ROUTE ===
@app.route('/<path:path>')
def catch_all(path):
    if check_access() and path in ['privacy', 'terms', 'miniapp']:
        return redirect(url_for(path))
    return render_template('coming_soon.html')

# === RUN ===
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)), debug=False)
