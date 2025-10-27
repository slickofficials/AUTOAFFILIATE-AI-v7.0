# app.py - v7.3 $1M/MONTH EMPIRE (HEADLESS YOUTUBE + TELEGRAM + RENDER SAFE)
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
    
    try:
        conn, cur = get_db()
        cur.execute("SELECT COUNT(*) as post_count FROM posts WHERE status='sent'")
        posts_sent = cur.fetchone()['post_count'] or 0
        cur.execute("SELECT COALESCE(SUM(amount), 0) as total_revenue FROM earnings")
        revenue = cur.fetchone()['total_revenue'] or 0
        conn.close()
    except Exception as e:
        posts_sent = 0
        revenue = 0.0

    return render_template('dashboard.html',
                         posts_sent=posts_sent,
                         revenue=revenue,
                         company=COMPANY)

# BEAST CAMPAIGN
@app.route('/beast_campaign')
def beast_campaign():
    queue.enqueue('worker.run_daily_campaign')
    return jsonify({'status': 'v7.3 $1M BEAST MODE ACTIVATED'})

# YOUTUBE AUTH - HEADLESS (RENDER SAFE)
@app.route('/youtube_auth')
def youtube_auth():
    secrets_json = os.getenv('GOOGLE_CLIENT_SECRETS')
    if not secrets_json:
        return "<h1 style='color:red;font-family:Orbitron'>ERROR: GOOGLE_CLIENT_SECRETS missing in Render Env</h1>"

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
            <p>Click below to authorize (opens in new tab):</p>
            <a href="{auth_url}" target="_blank">
                <button style="padding:18px 40px;background:#f00;color:#fff;border:none;font-size:1.3em;cursor:pointer;border-radius:10px;">
                    AUTHORIZE NOW
                </button>
            </a>
            <p><small>After allowing, you'll be redirected back.</small></p>
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
        return "<h1 style='color:red;font-family:Orbitron'>Auth Denied</h1>"

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

@app.route('/terms')
def terms():
    return render_template('terms.html')
    
@app.route('/privacy')
def privacy():
    return render_template('privacy.html')

# MINI APP
@app.route('/miniapp')
def miniapp():
    return render_template('miniapp.html', company=COMPANY)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)), debug=False)
