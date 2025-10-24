# app.py - v7.0 $100K/MONTH EMPIRE
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import os
import requests
import redis
import rq
from datetime import datetime
import psycopg
from psycopg.rows import dict_row
import bcrypt
import hmac
import hashlib
import openai
import google_auth_oauthlib.flow
import json

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
@app.route('/')
def dashboard():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    conn, cur = get_db()
    cur.execute("SELECT COUNT(*) FROM posts WHERE status='sent'")
    posts_sent = cur.fetchone()['count']
    cur.execute("SELECT COALESCE(SUM(amount), 0) FROM earnings")
    revenue = cur.fetchone()['coalesce']
    conn.close()

    return render_template('dashboard.html',
                         posts_sent=posts_sent,
                         revenue=revenue,
                         company=COMPANY)

# PAYSTACK WEBHOOK
@app.route('/paystack/webhook', methods=['POST'])
def paystack_webhook():
    payload = request.data
    sig = request.headers.get('x-paystack-signature')
    secret = os.getenv('PAYSTACK_SECRET_KEY')
    if not sig or not secret or hmac.new(secret.encode(), payload, hashlib.sha512).hexdigest() != sig:
        return 'Unauthorized', 401
    event = request.json
    if event['event'] == 'charge.success':
        ref = event['data']['reference']
        amount = event['data']['amount'] / 100
        conn, cur = get_db()
        cur.execute(
            "INSERT INTO earnings (reference, amount, currency) VALUES (%s, %s, 'NGN')",
            (ref, amount)
        )
        conn.commit()
        conn.close()
        queue.enqueue('worker.send_telegram', f"₦{amount:,} | {ref}")
    return 'OK', 200

# BEAST CAMPAIGN
@app.route('/beast_campaign')
def beast_campaign():
    queue.enqueue('worker.run_daily_campaign')
    return jsonify({'status': f'v7.0 $100K MODE: {COMPANY}'})

# YOUTUBE AUTH
@app.route('/youtube_auth')
def youtube_auth():
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        'client_secrets.json', ['https://www.googleapis.com/auth/youtube.upload'])
    creds = flow.run_local_server(port=0)
    with open('youtube_token.json', 'w') as f:
        f.write(creds.to_json())
    return f"<h1 style='font-family:Orbitron'>YouTube Auth Complete! {COMPANY}</h1>"

# MINI APP
@app.route('/miniapp')
def miniapp():
    return render_template('miniapp.html', company=COMPANY)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)), debug=False)
