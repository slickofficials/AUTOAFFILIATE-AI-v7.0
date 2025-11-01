# worker.py - v15.1 $10M EMPIRE BOT | NO WARNINGS | 100% CLEAN
import os
import time
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone  # ← ADDED timezone
import tweepy
from twilio.rest import Client

# === ENV VARS ===
DB_URL = os.getenv('DATABASE_URL')
AWIN_ID = os.getenv('AWIN_ID')
RAKUTEN_ID = os.getenv('RAKUTEN_ID')
FB_PAGE_ID = os.getenv('FB_PAGE_ID')
FB_TOKEN = os.getenv('FB_TOKEN')
IG_USER_ID = os.getenv('IG_USER_ID')
IG_TOKEN = os.getenv('IG_TOKEN')
TWITTER_API_KEY = os.getenv('TWITTER_API_KEY')
TWITTER_API_SECRET = os.getenv('TWITTER_API_SECRET')
TWITTER_ACCESS_TOKEN = os.getenv('TWITTER_ACCESS_TOKEN')
TWITTER_ACCESS_SECRET = os.getenv('TWITTER_ACCESS_SECRET')
TWILIO_SID = os.getenv('TWILIO_SID')
TWILIO_TOKEN = os.getenv('TWILIO_TOKEN')
YOUR_WHATSAPP = os.getenv('YOUR_WHATSAPP')

# === DB ===
def get_db():
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    cur = conn.cursor()
    return conn, cur

# === TWILIO ALERT ===
client = Client(TWILIO_SID, TWILIO_TOKEN) if TWILIO_SID else None
def send_alert(title, body):
    if client and YOUR_WHATSAPP:
        try:
            client.messages.create(
                from_='whatsapp:+14155238886',
                body=f"*{title}*\n{body}\nTime: {datetime.now().strftime('%H:%M')}",
                to=YOUR_WHATSAPP
            )
        except: pass

# === PULL AWIN ===
def pull_awin():
    if not AWIN_ID: return []
    url = f"https://www.awin1.com/cread.php?awinmid={AWIN_ID}&awinaffid=123456&clickref=bot"
    try:
        r = requests.get(url, timeout=10)
        if "tidd.ly" in r.url:
            return [r.url]
    except: pass
    return []

# === PULL RAKUTEN ===
def pull_rakuten():
    if not RAKUTEN_ID: return []
    url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_ID}&mid=12345&murl=https://example.com"
    try:
        r = requests.get(url, timeout=10)
        if "tidd.ly" in r.url or "go.redirectingat.com" in r.url:
            return [r.url]
    except: pass
    return []

# === SAVE LINKS ===
def save_links(links):
    conn, cur = get_db()
    for link in links:
        cur.execute("""
            INSERT INTO posts (url, source, status, created_at) 
            VALUES (%s, %s, 'pending', %s) 
            ON CONFLICT (url) DO NOTHING
        """, (link, 'awin_rakuten', datetime.now(timezone.utc)))  # ← FIXED
    conn.commit()
    conn.close()

# === POST TO FB ===
def post_fb(link):
    if not FB_PAGE_ID or not FB_TOKEN: return False
    try:
        url = f"https://graph.facebook.com/v20.0/{FB_PAGE_ID}/feed"
        params = {
            'access_token': FB_TOKEN,
            'message': f"Check this HOT deal! {link}"
        }
        r = requests.post(url, params=params, timeout=10)
        return r.status_code == 200
    except: return False

# === POST TO IG ===
def post_ig(link):
    if not IG_USER_ID or not IG_TOKEN: return False
    try:
        image_url = "https://i.imgur.com/airmax270.jpg"
        url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media"
        params = {
            'image_url': image_url,
            'caption': f"Hot deal! {link}",
            'access_token': IG_TOKEN
        }
        r = requests.post(url, params=params, timeout=10)
        if r.status_code != 200: return False
        creation_id = r.json()['id']

        url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish"
        params = {'creation_id': creation_id, 'access_token': IG_TOKEN}
        r = requests.post(url, params=params, timeout=10)
        return r.status_code == 200
    except: return False

# === POST TO TWITTER ===
def post_twitter(link):
    if not all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET]): return False
    try:
        client = tweepy.Client(
            consumer_key=TWITTER_API_KEY,
            consumer_secret=TWITTER_API_SECRET,
            access_token=TWITTER_ACCESS_TOKEN,
            access_token_secret=TWITTER_ACCESS_SECRET
        )
        client.create_tweet(text=f"Deal alert! {link}")
        return True
    except: return False

# === MAIN LOOP ===
def run_daily_campaign():
    send_alert("BOT LIVE", "v15.1 $10M EMPIRE BOT RUNNING — NO WARNINGS")
    
    your_links = [
        "https://tidd.ly/4ohUWG3", "https://tidd.ly/4oQBBMj",
        "https://tidd.ly/3WSHQDr", "https://tidd.ly/4obPepg",
        "https://tidd.ly/4hLLZCI", "https://tidd.ly/47PUvwR"
    ]
    save_links(your_links)
    save_links(pull_awin() + pull_rakuten())

    while True:
        conn, cur = get_db()
        cur.execute("SELECT url FROM posts WHERE status='pending' ORDER BY RANDOM() LIMIT 1")
        row = cur.fetchone()
        conn.close()
        if not row:
            time.sleep(3600)
            continue

        link = row['url']
        success = False
        if post_fb(link): success = True
        if post_ig(link): success = True
        if post_twitter(link): success = True

        status = 'sent' if success else 'failed'
        conn, cur = get_db()
        cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE url=%s", 
                   (status, datetime.now(timezone.utc), link))  # ← FIXED
        conn.commit()
        conn.close()

        if success:
            send_alert("POSTED", f"{link[:50]}...")

        time.sleep(3600)

if __name__ == '__main__':
    run_daily_campaign()
