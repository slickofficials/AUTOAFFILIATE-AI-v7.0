# app.py - v10.3 $10M EMPIRE | FIXED LOGIN + DASHBOARD + WHATSAPP + SECURITY
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

# === SECURITY CONFIG ===
ALLOWED_EMAIL = os.getenv('ALLOWED_EMAIL')
ALLOWED_IP = os.getenv('ALLOWED_IP')
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
    def __init__(self, id, email):
        self.id = id
        self.email = email  # ← FIXED: Add email to User

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

@login_manager.user_loader
def load_user(user_id):
    return User(user_id, ALLOWED_EMAIL)  # ← FIXED: Pass email

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

# === ACCESS CHECK (FOR DASHBOARD) ===
def check_access():
    client_ip = request.remote_addr
    now = datetime.now()
    for ip in list(failed_logins):
        if failed_logins[ip]['locked_until'] < now:
            del failed_logins[ip]
    if client_ip in failed_logins and failed_logins[client_ip]['locked_until'] > now:
        mins = int((failed_logins[client_ip]['locked_until'] - now).total_seconds() // 60)
        flash(f"Locked out. Try again in {mins} minutes.")
        return False
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

# === PUBLIC PAGES (COMING SOON) ===
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

# === LOGIN — ALWAYS VISIBLE ===
@app.route('/login', methods=['GET', 'POST'])
def login():
    client_ip = request.remote_addr

    # === SHOW LOGIN TO EVERYONE ===
    if request.method == 'GET':
        return render_template('login.html', company=COMPANY, title="Private Login")

    # === POST: CHECK IP FOR SUBMIT ===
    if client_ip != ALLOWED_IP:
        send_alert("BLOCKED LOGIN ATTEMPT", f"Wrong IP: {client_ip}")
        flash("Access Denied: Invalid IP")
        return render_template('coming_soon.html')

    # === VALIDATE CREDENTIALS ===
    email = request.form['email'].strip().lower()
    password = request.form['password']

    if client_ip not in failed_logins:
        failed_logins[client_ip] = {'count': 0, 'locked_until': None}

    if email == ALLOWED_EMAIL and password == ADMIN_PASS:
        if client_ip in failed_logins: del failed_logins[client_ip]
        user = User(email, ALLOWED_EMAIL)
        login_user(user)
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

# === DASHBOARD ===
@app.route('/dashboard')
@login_required
def dashboard():
    if not check_access():
        return render_template('coming_soon.html')
    
    user_id = session.get('user_id')
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

# === LIVE STATS API ===
@app.route('/api/stats')
@login_required
def api_stats():
    if not check_access():
        return jsonify({'error': 'Unauthorized'}), 401
    
    user_id = session.get('user_id')
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
        conn.close()
        return jsonify({
            'posts_sent': posts_sent,
            'revenue': float(revenue),
            'referrals': referrals,
            'ref_earnings': float(ref_earnings)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# === AUTO-PAYOUT ===
@app.route('/payout', methods=['POST'])
@login_required
def payout():
    if not check_access():
        return jsonify({'error': 'Unauthorized'}), 401
    
    user_id = session.get('user_id')
    amount = request.json.get('amount', 0)
    bank_account = request.json.get('bank_account')
    if amount <= 0 or not bank_account:
        return jsonify({'error': 'Invalid amount or bank'}), 400
    
    conn, cur = get_db()
    cur.execute("SELECT balance FROM users WHERE id = %s", (user_id,))
    user = cur.fetchone()
    if not user or user['balance'] < amount:
        conn.close()
        return jsonify({'error': 'Insufficient balance'}), 400
    
    paystack_secret = os.getenv('PAYSTACK_SECRET_KEY')
    headers = {'Authorization': f'Bearer {paystack_secret}'}
    payout_data = {
        'source': 'balance',
        'amount': int(amount * 100),
        'recipient': bank_account,
        'reason': 'Affiliate earnings'
    }
    r = requests.post('https://api.paystack.co/transfer', headers=headers, json=payout_data, timeout=10)
    
    if r.status_code == 200:
        cur.execute("UPDATE users SET balance = balance - %s WHERE id = %s", (amount, user_id))
        cur.execute("INSERT INTO earnings (user_id, amount, source, created_at) VALUES (%s, %s, %s, %s)", 
                    (user_id, -amount, 'payout', datetime.utcnow()))
        conn.commit()
        conn.close()
        return jsonify({'status': f'₦{amount} paid out!'})
    else:
        conn.close()
        return jsonify({'error': f'Payout failed: {r.text}'}), 500

# === HEALTH ===
@app.route('/health')
def health():
    return 'OK', 200

# === BEAST CAMPAIGN ===
@app.route('/beast_campaign')
@login_required
def beast_campaign():
    if not check_access():
        return render_template('coming_soon.html')
    job = queue.enqueue('worker.run_daily_campaign')
    return jsonify({'status': 'v10.3 $10M BEAST MODE MODE ACTIVATED', 'job_id': job.id})

# === YOUTUBE AUTH ===
@app.route('/youtube_auth')
@login_required
def youtube_auth():
    if not check_access():
        return render_template('coming_soon.html')
    secrets_json = os.getenv('GOOGLE_CLIENT_SECRETS')
    if not secrets_json:
        return "<h1 style='color:red'>ERROR: GOOGLE_CLIENT_SECRETS missing</h1>"

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(secrets_json)
        temp_path = f.name

    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            temp_path,
            scopes=['https://www.googleapis.com/auth/youtube.upload'],
            redirect_uri=f"https://{request.host}/youtube_callback"
        )
        auth_url, _ = flow.authorization_url(prompt='consent')
        os.unlink(temp_path)
        return f'''
        <div style="background:#000;color:#0f0;font-family:Orbitron;text-align:center;padding:50px;">
            <h1>CONNECT YOUTUBE</h1>
            <a href="{auth_url}" target="_blank">
                <button style="padding:18px 40px;background:#f00;color:#fff;border:none;font-size:1.3em;cursor:pointer;border-radius:10px;">
                    AUTHORIZE NOW
                </button>
            </a>
        </div>
        '''
    except Exception as e:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        return f"<h1 style='color:red'>Setup Failed: {str(e)}</h1>"

# === YOUTUBE CALLBACK ===
@app.route('/youtube_callback')
def youtube_callback():
    code = request.args.get('code')
    if not code:
        return "<h1 style='color:red'>Auth Denied</h1>"

    secrets_json = os.getenv('GOOGLE_CLIENT_SECRETS')
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(secrets_json)
        temp_path = f.name

    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            temp_path,
            scopes=['https://www.googleapis.com/auth/youtube.upload'],
            redirect_uri=f"https://{request.host}/youtube_callback"
        )
        flow.fetch_token(code=code)
        creds = flow.credentials
        with open('youtube_token.json', 'w') as f:
            f.write(creds.to_json())
        os.unlink(temp_path)
        return "<h1 style='color:#0f0;font-family:Orbitron'>YouTube Connected! Auto-Upload ACTIVE</h1>"
    except Exception as e:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        return f"<h1 style='color:red'>Token Failed: {str(e)}</h1>"

# === MINI APP ===
@app.route('/miniapp')
def miniapp():
    return render_template('coming_soon.html')

# === UPSELL ===
@app.route('/upsell', methods=['POST'])
def upsell():
    return render_template('coming_soon.html')

# === 404 ===
@app.errorhandler(404)
def not_found(e):
    return render_template('coming_soon.html'), 404

# === CATCH-ALL ===
@app.route('/<path:path>')
def catch_all(path):
    return render_template('coming_soon.html')

# === RUN ===
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)), debug=False)
