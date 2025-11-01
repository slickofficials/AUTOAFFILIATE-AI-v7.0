# worker.py - v13.1 $10M EMPIRE BOT | AWIN + RAKUTEN + FB/IG/TWITTER + 24 POSTS/DAY
import os
import time
import requests
import random
import psycopg
from psycopg.rows import dict_row
from datetime import datetime
from facebook_business.ad_objects.page import Page  # ← FIXED: facebook-business
from facebook_business.api import FacebookAdsApi
import instabot
import tweepy
from twilio.rest import Client

# === ENV VARS ===
DB_URL = os.getenv('DATABASE_URL')
AWIN_ID = os.getenv('AWIN_ID')
RAKUTEN_ID = os.getenv('RAKUTEN_ID')
FB_PAGE_ID = os.getenv('FB_PAGE_ID')
FB_TOKEN = os.getenv('FB_TOKEN')
IG_USER = os.getenv('IG_USER')
IG_PASS = os.getenv('IG_PASS')
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
    return conn, conn.cursor()

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

# === PULL AWIN DEEPLINKS ===
def pull_awin():
    if not AWIN_ID: return []
    url = f"https://www.awin1.com/cread.php?awinmid={AWIN_ID}&awinaffid=123456&clickref=bot"
    try:
        r = requests.get(url, timeout=10)
        if "tidd.ly" in r.url:
            return [r.url]
    except: pass
    return []

# === PULL RAKUTEN DEEPLINKS ===
def pull_rakuten():
    if not RAKUTEN_ID: return []
    url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_ID}&mid=12345&murl=https://example.com"
    try:
        r = requests.get(url, timeout=10)
        if "tidd.ly" in r.url or "go.redirectingat.com" in r.url:
            return [r.url]
    except: pass
    return []

# === SAVE LINKS TO DB ===
def save_links(links):
    conn, cur = get_db()
    for link in links:
        cur.execute("""
            INSERT INTO posts (url, source, status, created_at) 
            VALUES (%s, %s, 'pending', %s) 
            ON CONFLICT (url) DO NOTHING
        """, (link, 'awin_rakuten', datetime.utcnow()))
    conn.commit()
    conn.close()

# === POST TO FB (FIXED) ===
def post_fb(link):
    if not FB_PAGE_ID or not FB_TOKEN: return False
    try:
        FacebookAdsApi.init(access_token=FB_TOKEN)
        page = Page(FB_PAGE_ID)
        page.create_feed(
            fields=[],
            params={'message': f"Check this deal! {link}"}
        )
        return True
    except Exception as e:
        print(f"FB ERROR: {e}")
        return False

# === POST TO IG (FIXED) ===
def post_ig(link):
    if not IG_USER or not IG_PASS: return False
    try:
        bot = instabot.Bot()
        bot.login(username=IG_USER, password=IG_PASS)
        bot.upload_photo("deal.jpg", caption=f"Hot deal! {link}")
        return True
    except Exception as e:
        print(f"IG ERROR: {e}")
        return False

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
    except Exception as e:
        print(f"TWITTER ERROR: {e}")
        return False

# === MAIN BOT LOOP ===
def run_daily_campaign():
    send_alert("BOT STARTED", "v13.1 $10M EMPIRE BOT LIVE")
    
    # === YOUR 17 LINKS ===
    your_links = [
        "https://tidd.ly/4ohUWG3", "https://tidd.ly/4oQBBMj",
        "https://tidd.ly/3WSHQDr", "https://tidd.ly/4obPepg",
        "https://tidd.ly/4hLLZCI", "https://tidd.ly/47PUvwR"
        # ADD ALL 17
    ]
    save_links(your_links)

    # === PULL AWIN + RAKUTEN ===
    awin_links = pull_awin()
    rakuten_links = pull_rakuten()
    save_links(awin_links + rakuten_links)

    # === POST 24x/DAY ===
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
        cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE url=%s", (status, datetime.utcnow(), link))
        conn.commit()
        conn.close()

        if success:
            send_alert("POSTED", f"Deal live: {link[:50]}...")
        
        time.sleep(3600)  # 1 HOUR

if __name__ == '__main__':
    run_daily_campaign()
