# app.py - v7.1 $1M/MONTH EMPIRE (FULLY UPDATED + YOUTUBE SECURE)
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
    return jsonify({'status': 'v7.1 $1M BEAST MODE ACTIVATED'})

# YOUTUBE AUTH - SECURE VIA ENV VAR (NO client_secrets.json IN REPO)
@app.route('/youtube_auth')
def youtube_auth():
    secrets_json = os.getenv('GOOGLE_CLIENT_SECRETS')
    if not secrets_json:
        return "<h1 style='color:red;font-family:Orbitron'>ERROR: GOOGLE_CLIENT_SECRETS not set in Render Env</h1>"

    # Write to temp file (safe in Render container)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(secrets_json)
        temp_path = f.name

    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            temp_path,
            scopes=['https://www.googleapis.com/auth/youtube.upload']
        )
        creds = flow.run_local_server(port=0)
        with open('youtube_token.json', 'w') as f:
            f.write(creds.to_json())
        os.unlink(temp_path)  # Delete temp file
        return "<h1 style='color:#0f0;font-family:Orbitron'>YouTube Connected! Shorts Auto-Upload ON</h1>"
    except Exception as e:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        return f"<h1 style='color:red;font-family:Orbitron'>Auth Failed: {str(e)}</h1>"

# MINI APP
@app.route('/miniapp')
def miniapp():
    return render_template('miniapp.html', company=COMPANY)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)), debug=False)
