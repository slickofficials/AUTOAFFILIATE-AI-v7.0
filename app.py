# app.py - v7.4 $10M EMPIRE (FULL HEYGEN + YOUTUBE + REFERRAL DASH + EMAIL UPSELL)
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, send_from_directory
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

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'slickofficials_hq_2025')
COMPANY = "Slickofficials HQ | Amson Multi Global LTD"

# CONFIG
DB_URL = os.getenv('DATABASE_URL')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
r = redis.from_url(REDIS_URL)
queue = rq.Queue(connection=r)
openai.api_key = os.getenv('OPENAI_API_KEY')

# DATABASE
def get_db():
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# ROOT → LOGIN
@app.route('/')
def index():
    return redirect(url_for('login'))

# LOGIN
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password'].encode()
        conn, cur = get_db()
        cur.execute("SELECT * FROM users WHERE email = %s", (email,))
        user = cur.fetchone()
        conn.close()
        if user and bcrypt.checkpw(password, user['password'].encode()):
            session['user_id'] = user['id']
            return redirect(url_for('dashboard'))
        flash('Invalid credentials')
    return render_template('login.html', company=COMPANY)

@app.route('/logout')
def logout():
    session.pop('user_id', None)
    return redirect(url_for('login'))

# DASHBOARD
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
        conn.close()
    except Exception as e:
        posts_sent = revenue = referrals = ref_earnings = 0

    return render_template('dashboard.html',
                         posts_sent=posts_sent,
                         revenue=revenue,
                         referrals=referrals,
                         ref_earnings=ref_earnings,
                         company=COMPANY)

# BEAST CAMPAIGN
@app.route('/beast_campaign')
def beast_campaign():
    queue.enqueue('worker.run_daily_campaign', job_timeout=7200)
    return jsonify({'status': 'v7.4 100 SHORTS/DAY ACTIVATED'})

# YOUTUBE AUTH - HEADLESS
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

# REFERRAL SYSTEM
@app.route('/refer', methods=['POST'])
def refer():
    if 'user_id' not in session:
        return jsonify({'error': 'Login required'})

    referred_email = request.json['email']
    referrer_id = session['user_id']

    conn, cur = get_db()
    cur.execute("SELECT id FROM users WHERE email = %s", (referred_email,))
    referred = cur.fetchone()

    if referred:
        cur.execute("INSERT INTO referrals (referrer_id, referred_email, reward) VALUES (%s, %s, 500)", (referrer_id, referred_email))
        conn.commit()
        conn.close()
        queue.enqueue('worker.send_referral_reward', referrer_id, 500)
        return jsonify({'status': 'Referral added! ₦500 pending'})
    
    conn.close()
    return jsonify({'error': 'User not found'})

# PAYSTACK PAYOUT
@app.route('/payout', methods=['POST'])
def payout():
    if 'user_id' not in session:
        return jsonify({'error': 'Login required'})

    amount = request.json['amount']
    bank_account = request.json['bank_account']
    paystack_key = os.getenv('PAYSTACK_SECRET_KEY')
    url = "https://api.paystack.co/transfer"
    headers = {"Authorization": f"Bearer {paystack_key}", "Content-Type": "application/json"}
    payload = {
        "source": "balance",
        "amount": amount * 100,
        "recipient": bank_account,
        "reason": "Affiliate Payout"
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        user_id = session['user_id']
        conn, cur = get_db()
        cur.execute("INSERT INTO earnings (user_id, reference, amount, network) VALUES (%s, %s, %s, 'payout')", 
                    (user_id, response.json()['data']['reference'], -amount))
        conn.commit()
        conn.close()
        return jsonify({'status': 'Payout initiated'})
    
    return jsonify({'error': 'Payout failed: ' + response.text})

# AUTO-EMAIL UPSELL (Mailchimp)
@app.route('/upsell', methods=['POST'])
def upsell():
    email = request.json['email']
    mailchimp_key = os.getenv('MAILCHIMP_API_KEY')
    list_id = os.getenv('MAILCHIMP_LIST_ID')
    url = f"https://us1.api.mailchimp.com/3.0/lists/{list_id}/members"
    headers = {"Authorization": f"apikey {mailchimp_key}"}
    payload = {
        "email_address": email,
        "status": "subscribed",
        "tags": ["affiliate", "beast-mode"]
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        return jsonify({'status': 'Upsell email sent!'})
    return jsonify({'error': 'Email failed'})

# MINI APP
@app.route('/miniapp')
def miniapp():
    return render_template('miniapp.html', company=COMPANY)

# STATIC FILES
@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('static', path)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)), debug=False)
