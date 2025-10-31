# app.py - v7.7 $10M EMPIRE (SEO + GZIP + LIVE STATS + AUTO-PAYOUT + HEADLESS YOUTUBE + RENDER SAFE)
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, send_from_directory
from flask_compress import Compress  # GZIP = 3x FASTER
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
from datetime import datetime

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

# === FRONT PAGE ===
@app.route('/')
def index():
    return render_template('index.html', company=COMPANY, title="SlickOfficials | $100K/Month Auto Affiliate AI SaaS")

# === STATIC PAGES ===
@app.route('/privacy')
def privacy():
    return render_template('privacy.html', company=COMPANY, title="Privacy Policy")

@app.route('/terms')
def terms():
    return render_template('terms.html', company=COMPANY, title="Terms of Service")

# === LOGIN ===
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email'].strip().lower()
        password = request.form['password'].encode()
        conn, cur = get_db()
        cur.execute("SELECT * FROM users WHERE email = %s", (email,))
        user = cur.fetchone()
        conn.close()
        if user and bcrypt.checkpw(password, user['password'].encode()):
            session['user_id'] = user['id']
            return redirect(url_for('dashboard'))
        flash('Invalid credentials')
    return render_template('login.html', company=COMPANY, title="Login")

@app.route('/logout')
def logout():
    session.pop('user_id', None)
    return redirect(url_for('index'))

# === DASHBOARD ===
@app.route('/dashboard')
def dashboard():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    user_id = session['user_id']
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
def api_stats():
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    user_id = session['user_id']
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

# === AUTO-PAYOUT ENDPOINT ===
@app.route('/payout', methods=['POST'])
def payout():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Login required'}), 401
    
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

# === BEAST CAMPAIGN ===
@app.route('/beast_campaign')
def beast_campaign():
    job = queue.enqueue('worker.run_daily_campaign')
    return jsonify({'status': 'v7.7 $10M BEAST MODE ACTIVATED', 'job_id': job.id})

# === YOUTUBE AUTH (HEADLESS + RENDER SAFE) ===
@app.route('/youtube_auth')
def youtube_auth():
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

# === TELEGRAM MINI APP ===
@app.route('/miniapp')
def miniapp():
    return render_template('miniapp.html', company=COMPANY, title="Referral Mini App")

# === UPSELL ===
@app.route('/upsell', methods=['POST'])
def upsell():
    email = request.json.get('email')
    if not email:
        return jsonify({'error': 'Email required'}), 400
    mailchimp_key = os.getenv('MAILCHIMP_API_KEY')
    list_id = os.getenv('MAILCHIMP_LIST_ID')
    if not mailchimp_key or not list_id:
        return jsonify({'error': 'Mailchimp not configured'}), 500
    url = f"https://us1.api.mailchimp.com/3.0/lists/{list_id}/members"
    headers = {"Authorization": f"apikey {mailchimp_key}", "Content-Type": "application/json"}
    payload = {"email_address": email, "status": "subscribed", "tags": ["affiliate", "beast-mode"]}
    response = requests.post(url, headers=headers, json=payload, timeout=10)
    if response.status_code == 200:
        return jsonify({'status': 'VIP Upsell Email Sent!'})
    return jsonify({'error': 'Email failed: ' + response.text}), 500

# === 404 PAGE ===
@app.errorhandler(404)
def not_found(e):
    return render_template('404.html', company=COMPANY, title="404 - Not Found"), 404

# === RUN ===
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)), debug=False)
