# app.py - v7.5 $10M EMPIRE | slickofficials.com
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import os
import redis
import rq
import psycopg
from psycopg.rows import dict_row
import bcrypt
import hmac
import hashlib
import openai
import json
import tempfile
from google_auth_oauthlib.flow import InstalledAppFlow
import requests

# === FLASK APP ===
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
    try:
        conn = psycopg.connect(DB_URL, row_factory=dict_row)
        return conn, conn.cursor()
    except Exception as e:
        print(f"[DB] Connection failed: {e}")
        return None, None

# === WELCOME PAGE (ROOT) ===
@app.route('/')
def index():
    return render_template('index.html', company=COMPANY)

# === LOGIN ===
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email'].strip().lower()
        password = request.form['password'].encode()
        conn, cur = get_db()
        if not conn:
            flash('Database error. Try again.')
            return render_template('login.html', company=COMPANY)

        cur.execute("SELECT * FROM users WHERE email = %s", (email,))
        user = cur.fetchone()
        conn.close()

        if user and bcrypt.checkpw(password, user['password'].encode()):
            session['user_id'] = user['id']
            return redirect(url_for('dashboard'))
        flash('Invalid email or password')
    return render_template('login.html', company=COMPANY)

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
    conn, cur = get_db()
    if not conn:
        flash('Database offline')
        return redirect(url_for('index'))

    try:
        cur.execute("SELECT COUNT(*) as post_count FROM posts WHERE status='sent'")
        posts_sent = cur.fetchone()['post_count'] or 0

        cur.execute("SELECT COALESCE(SUM(amount), 0) as total_revenue FROM earnings")
        revenue = cur.fetchone()['total_revenue'] or 0

        cur.execute("SELECT COUNT(*) as ref_count FROM referrals WHERE referrer_id = %s", (user_id,))
        referrals = cur.fetchone()['ref_count'] or 0

        cur.execute("SELECT COALESCE(SUM(reward), 0) as ref_earnings FROM referrals WHERE referrer_id = %s", (user_id,))
        ref_earnings = cur.fetchone()['ref_earnings'] or 0

        cur.execute("SELECT referred_email, reward, created_at FROM referrals WHERE referrer_id = %s ORDER BY created_at DESC LIMIT 10", (user_id,))
        ref_list = cur.fetchall()
        conn.close()
    except Exception as e:
        print(f"[DASHBOARD] Query error: {e}")
        posts_sent = revenue = referrals = ref_earnings = 0
        ref_list = []

    return render_template('dashboard.html',
                         posts_sent=posts_sent,
                         revenue=revenue,
                         referrals=referrals,
                         ref_earnings=ref_earnings,
                         ref_list=ref_list,
                         company=COMPANY)

# === BEAST CAMPAIGN ===
@app.route('/beast_campaign')
def beast_campaign():
    try:
        queue.enqueue('worker.run_daily_campaign')
        return jsonify({'status': 'v7.5 $10M BEAST MODE ACTIVATED'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# === YOUTUBE AUTH (RENDER SAFE) ===
@app.route('/youtube_auth')
def youtube_auth():
    secrets_json = os.getenv('GOOGLE_CLIENT_SECRETS')
    if not secrets_json:
        return "<h1 style='color:red;font-family:sans-serif'>ERROR: GOOGLE_CLIENT_SECRETS missing in Render</h1>"

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(secrets_json)
        temp_path = f.name

    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            temp_path,
            scopes=['https://www.googleapis.com/auth/youtube.upload'],
            redirect_uri=f"https://slickofficials.com/youtube_callback"
        )
        auth_url, _ = flow.authorization_url(prompt='consent')
        os.unlink(temp_path)
        return f'''
        <div style="background:#000;color:#0f0;font-family:monospace;text-align:center;padding:60px;">
            <h1>CONNECT YOUTUBE</h1>
            <p>Authorize Slickofficials AI to upload Shorts</p>
            <a href="{auth_url}" target="_blank">
                <button style="padding:18px 40px;background:#f00;color:#fff;border:none;font-size:1.3em;cursor:pointer;border-radius:12px;margin-top:20px;">
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
        return "<h1 style='color:red'>Authorization Denied</h1>"

    secrets_json = os.getenv('GOOGLE_CLIENT_SECRETS')
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(secrets_json)
        temp_path = f.name

    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            temp_path,
            scopes=['https://www.googleapis.com/auth/youtube.upload'],
            redirect_uri=f"https://slickofficials.com/youtube_callback"
        )
        flow.fetch_token(code=code)
        creds = flow.credentials
        with open('youtube_token.json', 'w') as f:
            f.write(creds.to_json())
        os.unlink(temp_path)
        return '''
        <div style="background:#000;color:#0f0;font-family:monospace;text-align:center;padding:60px;">
            <h1>YouTube Connected!</h1>
            <p>Auto-upload Shorts: <strong>ACTIVE</strong></p>
            <a href="/dashboard">← Back to Dashboard</a>
        </div>
        '''
    except Exception as e:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        return f"<h1 style='color:red'>Token Failed: {str(e)}</h1>"

# === STATIC PAGES ===
@app.route('/privacy')
def privacy():
    return render_template('privacy.html', company=COMPANY)

@app.route('/terms')
def terms():
    return render_template('terms.html', company=COMPANY)

@app.route('/miniapp')
def miniapp():
    return render_template('miniapp.html', company=COMPANY)

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
    headers = {"Authorization": f"apikey {mailchimp_key}"}
    payload = {"email_address": email, "status": "subscribed", "tags": ["affiliate", "slickofficials"]}

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        if response.status_code in [200, 201]:
            return jsonify({'status': 'VIP Upsell Sent!'})
        else:
            return jsonify({'error': response.json().get('detail', 'Mailchimp failed')}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# === PAYSTACK WEBHOOK ===
@app.route('/paystack/webhook', methods=['POST'])
def paystack_webhook():
    payload = request.data
    sig = request.headers.get('x-paystack-signature')
    secret = os.getenv('PAYSTACK_SECRET_KEY')

    if not sig or not secret or hmac.new(secret.encode(), payload, hashlib.sha512).hexdigest() != sig:
        return 'Unauthorized', 401

    event = request.json
    conn, cur = get_db()
    if not conn:
        return 'DB Error', 500

    try:
        if event['event'] == 'subscription.create':
            sub_code = event['data']['subscription_code']
            customer_code = event['data']['customer']['customer_code']
            amount = event['data']['amount'] / 100
            cur.execute(
                "UPDATE saas_users SET paystack_subscription_code = %s, status = 'active', amount = %s WHERE paystack_customer_code = %s",
                (sub_code, amount, customer_code)
            )
            conn.commit()
            queue.enqueue('worker.send_welcome_email', customer_code)

        elif event['event'] == 'charge.success':
            reference = event['data']['reference']
            amount = event['data']['amount'] / 100
            customer_code = event['data']['customer']['customer_code']
            cur.execute(
                "INSERT INTO saas_payments (user_id, reference, amount, status) VALUES ((SELECT id FROM saas_users WHERE paystack_customer_code = %s), %s, %s, 'success')",
                (customer_code, reference, amount)
            )
            conn.commit()

        conn.close()
        return jsonify({'status': 'OK'})
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

# === RUN ===
if __name__ == '__main__':
    port = int(os.getenv('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False)
