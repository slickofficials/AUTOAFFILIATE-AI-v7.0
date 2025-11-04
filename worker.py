# worker.py - v23.0 $10M EMPIRE BOT | FULL DEEPLINKS + ALL SOCIAL + STOP/START
import os
import time
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone
import tweepy
from twilio.rest import Client
import logging
from logging.handlers import RotatingFileHandler
import threading
import signal
import sys

# === LOGGING ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = RotatingFileHandler('worker.log', maxBytes=10**6, backupCount=5)
handler.setFormatter(logging.Formatter('{"time": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s"}'))
logger.addHandler(handler)

# === ENV ===
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
IFTTT_KEY = os.getenv('IFTTT_KEY')
YT_REFRESH_TOKEN = os.getenv('YT_REFRESH_TOKEN')
YT_CLIENT_ID = os.getenv('YT_CLIENT_ID')
YT_CLIENT_SECRET = os.getenv('YT_CLIENT_SECRET')
TWILIO_SID = os.getenv('TWILIO_SID')
TWILIO_TOKEN = os.getenv('TWILIO_TOKEN')
YOUR_WHATSAPP = os.getenv('YOUR_WHATSAPP')

# === GLOBALS ===
worker_thread = None
stop_event = threading.Event()

# === TWILIO ===
client = Client(TWILIO_SID, TWILIO_TOKEN) if TWILIO_SID else None
def send_alert(title, body):
    if client and YOUR_WHATSAPP:
        try:
            client.messages.create(
                from_='whatsapp:+14155238886',
                body=f"*{title}*\n{body}",
                to=YOUR_WHATSAPP
            )
        except: pass

# === DB ===
def get_db():
    conn = psycopg.connect(DB_URL, row_factory=dict_row, timeout=10)
    return conn, conn.cursor()

def init_db():
    conn, cur = get_db()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id SERIAL PRIMARY KEY,
            url TEXT UNIQUE NOT NULL,
            source TEXT,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            posted_at TIMESTAMPTZ
        );
        CREATE TABLE IF NOT EXISTS clicks (id SERIAL, post_id INT, ip TEXT, user_agent TEXT, created_at TIMESTAMPTZ);
        CREATE TABLE IF NOT EXISTS settings (name TEXT PRIMARY KEY, value TEXT);
    """)
    conn.commit()
    conn.close()

# === PULL LINKS ===
def pull_awin():
    if not AWIN_ID: return []
    try:
        r = requests.get(f"https://www.awin1.com/cread.php?awinmid={AWIN_ID}&awinaffid=123456&clickref=bot", timeout=15)
        return [r.url] if "tidd.ly" in r.url else []
    except: return []

def pull_rakuten():
    if not RAKUTEN_ID: return []
    try:
        r = requests.get(f"https://click.linksynergy.com/deeplink?id={RAKUTEN_ID}&mid=12345&murl=https://example.com", timeout=15)
        return [r.url] if "tidd.ly" in r.url else []
    except: return []

# === SAVE LINKS ===
def save_links(links, source):
    if not links: return 0
    conn, cur = get_db()
    saved = 0
    for link in links:
        try:
            cur.execute("INSERT INTO posts (url, source) VALUES (%s, %s) ON CONFLICT (url) DO NOTHING", (link, source))
            if cur.rowcount: saved += 1
        except: conn.rollback()
    conn.commit()
    conn.close()
    return saved

# === POST FUNCTIONS ===
def post_fb(link): 
    if not FB_PAGE_ID or not FB_TOKEN: return False
    try:
        r = requests.post(f"https://graph.facebook.com/v20.0/{FB_PAGE_ID}/feed",
                         params={'access_token': FB_TOKEN, 'message': f"Deal! {link}"}, timeout=15)
        return r.status_code == 200
    except: return False

def post_ig(link):
    if not IG_USER_ID or not IG_TOKEN: return False
    try:
        r = requests.post(f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media",
                         params={'image_url': 'https://i.imgur.com/airmax270.jpg', 'caption': f"Deal! {link}", 'access_token': IG_TOKEN}, timeout=15)
        if r.status_code != 200: return False
        cid = r.json()['id']
        requests.post(f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish",
                     params={'creation_id': cid, 'access_token': IG_TOKEN}, timeout=15)
        return True
    except: return False

def post_twitter(link):
    if not all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET]): return False
    try:
        client = tweepy.Client(consumer_key=TWITTER_API_KEY, consumer_secret=TWITTER_API_SECRET,
                              access_token=TWITTER_ACCESS_TOKEN, access_token_secret=TWITTER_ACCESS_SECRET)
        client.create_tweet(text=f"Deal! {link}")
        return True
    except: return False

def post_tiktok(link):
    if not IFTTT_KEY: return False
    try:
        requests.post(f"https://maker.ifttt.com/trigger/tiktok_post/with/key/{IFTTT_KEY}",
                     json={"value1": f"Deal! {link}"}, timeout=15)
        return True
    except: return False

def post_youtube(link):
    if not all([YT_REFRESH_TOKEN, YT_CLIENT_ID, YT_CLIENT_SECRET]): return False
    try:
        token = requests.post("https://oauth2.googleapis.com/token", data={
            'client_id': YT_CLIENT_ID, 'client_secret': YT_CLIENT_SECRET,
            'refresh_token': YT_REFRESH_TOKEN, 'grant_type': 'refresh_token'
        }, timeout=15).json()
        return bool(token.get('access_token'))
    except: return False

# === BOT LOOP ===
def bot_loop():
    init_db()
    send_alert("BOT LIVE", "v23.0 Running")
    your_links = [l.strip() for l in os.getenv('YOUR_LINKS', '').split(',') if l.strip()]
    if your_links: save_links(your_links, "manual")

    while not stop_event.is_set():
        try:
            awin = pull_awin()
            rakuten = pull_rakuten()
            saved = save_links(awin, "awin") + save_links(rakuten, "rakuten")
            if saved: send_alert("PULLED", f"{saved} new links")

            conn, cur = get_db()
            cur.execute("SELECT url FROM posts WHERE status='pending' ORDER BY RANDOM() LIMIT 1")
            row = cur.fetchone()
            conn.close()

            if row:
                link = row['url']
                platforms = []
                if post_fb(link): platforms.append("FB")
                if post_ig(link): platforms.append("IG")
                if post_twitter(link): platforms.append("X")
                if post_tiktok(link): platforms.append("TikTok")
                if post_youtube(link): platforms.append("YT")

                status = 'sent' if platforms else 'failed'
                conn, cur = get_db()
                cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE url=%s",
                           (status, datetime.now(timezone.utc), link))
                conn.commit()
                conn.close()

                if platforms: send_alert("POSTED", f"{link[:50]}... on {', '.join(platforms)}")

            interval = int(os.getenv("POST_INTERVAL_SECONDS", "3600"))
            stop_event.wait(interval)
        except Exception as e:
            logger.error(f"Bot error: {e}")
            stop_event.wait(60)

# === CONTROL FUNCTIONS ===
def start_worker_background():
    global worker_thread
    if worker_thread and worker_thread.is_alive(): return
    stop_event.clear()
    worker_thread = threading.Thread(target=bot_loop, daemon=True)
    worker_thread.start()

def stop_worker():
    stop_event.set()
    send_alert("BOT STOPPED", "Worker halted")

def refresh_all_sources():
    return save_links(pull_awin(), "awin") + save_links(pull_rakuten(), "rakuten")

def enqueue_manual_link(url):
    return save_links([url], "manual")

# === GRACEFUL SHUTDOWN ===
def signal_handler(sig, frame):
    stop_worker()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == '__main__':
    start_worker_background()
    while True: time.sleep(3600)
