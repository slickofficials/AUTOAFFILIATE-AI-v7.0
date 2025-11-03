# worker.py — AutoAffiliate worker (API-powered AWIN + Rakuten version, full debug + clean posting)
import os
import time
import logging
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone, timedelta
from openai import OpenAI

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    logger.error("DATABASE_URL not set — worker will not start (set in env)")

# Affiliate credentials
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")
RAKUTEN_WEBSERVICES_TOKEN = os.getenv("RAKUTEN_WEBSERVICES_TOKEN")

# API keys for AI + social
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
HEYGEN_KEY = os.getenv("HEYGEN_API_KEY")
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_TOKEN = os.getenv("FB_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
IG_TOKEN = os.getenv("IG_TOKEN")
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
YOUTUBE_TOKEN_JSON = os.getenv("YOUTUBE_TOKEN_JSON")
IFTTT_KEY = os.getenv("IFTTT_KEY")

# Alerts
TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
YOUR_WHATSAPP = os.getenv("YOUR_WHATSAPP")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Timing
POST_INTERVAL_SECONDS = int(os.getenv("POST_INTERVAL_SECONDS", "3600"))
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "300"))
DEBUG_REDIRECTS = os.getenv("DEBUG_LOG_REDIRECTS", "False").lower() == "true"

openai_client = OpenAI(api_key=OPENAI_KEY) if OPENAI_KEY else None

_worker_running = False
_stop_requested = False

# ========== DATABASE HELPERS ==========
def get_db_conn():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# ========== ALERTS ==========
def send_alert(title, body):
    logger.info("ALERT: %s — %s", title, body)
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": f"{title}\n{body}"}, timeout=8)
        except Exception:
            logger.exception("Telegram alert failed")

# ========== LINK VALIDATION + DEBUG ==========
def is_valid_https_url(url):
    return bool(url and isinstance(url, str) and url.startswith("https://") and len(url) < 3000)

def resolve_redirect_chain(url):
    """Logs the redirect chain for audit/debug"""
    if not DEBUG_REDIRECTS:
        return url
    try:
        r = requests.get(url, allow_redirects=True, timeout=10)
        chain = [resp.url for resp in r.history] + [r.url]
        logger.info("Redirect chain: %s", " → ".join(chain))
        return r.url
    except Exception:
        logger.exception("Redirect chain check failed")
        return url

# ========== AWIN API ==========
def get_awin_offers(limit=5):
    if not AWIN_PUBLISHER_ID or not AWIN_API_TOKEN:
        logger.debug("AWIN API credentials missing")
        return []
    try:
        url = f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/ads?relationship=joined&limit={limit}"
        headers = {"Authorization": f"Bearer {AWIN_API_TOKEN}"}
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        offers = []
        for item in r.json():
            dest = item.get("destinationUrl")
            if dest and is_valid_https_url(dest):
                deeplink = f"https://www.awin1.com/cread.php?awinaffid={AWIN_PUBLISHER_ID}&ued={dest}"
                offers.append(deeplink)
        logger.info("Pulled %s AWIN offers", len(offers))
        return offers
    except Exception:
        logger.exception("AWIN API error")
        return []

# ========== RAKUTEN API ==========
def get_rakuten_offers(limit=5):
    if not RAKUTEN_CLIENT_ID or not RAKUTEN_WEBSERVICES_TOKEN:
        logger.debug("Rakuten API credentials missing")
        return []
    try:
        url = f"https://api.rakutenmarketing.com/productsearch/1.0?keyword=deal&max={limit}"
        headers = {"Authorization": f"Bearer {RAKUTEN_WEBSERVICES_TOKEN}"}
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        offers = []
        for item in data.get("results", []):
            dest = item.get("linkUrl")
            if dest and is_valid_https_url(dest):
                offers.append(dest)
        logger.info("Pulled %s Rakuten offers", len(offers))
        return offers
    except Exception:
        logger.exception("Rakuten API error")
        return []

# ========== SAVE TO DATABASE ==========
def save_links_to_db(links, source="affiliate"):
    if not links:
        return 0
    conn, cur = get_db_conn()
    added = 0
    for link in links:
        final = resolve_redirect_chain(link)
        if not is_valid_https_url(final):
            logger.debug("Invalid link skipped: %s", final)
            continue
        try:
            cur.execute("""
                INSERT INTO posts (url, source, status, created_at)
                VALUES (%s,%s,'pending',%s)
                ON CONFLICT (url) DO NOTHING
            """, (final, source, datetime.now(timezone.utc)))
            added += 1
        except Exception:
            logger.exception("DB insert failed for %s", final)
    conn.commit(); conn.close()
    logger.info("Saved %s new links from %s", added, source)
    return added

# ========== OPENAI CAPTIONS ==========
def generate_caption(link):
    if not openai_client:
        return f"🔥 Hot deal → {link}"
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":f"Write a 1-sentence catchy promo caption with an emoji and a CTA for this link:\n{link}"}],
            max_tokens=60
        )
        text = resp.choices[0].message.content.strip()
        if link not in text:
            text += f" {link}"
        return text
    except Exception:
        logger.exception("Caption generation failed")
        return f"🔥 Hot deal → {link}"

# ========== SOCIAL POSTING ==========
def post_facebook(msg):
    if not FB_PAGE_ID or not FB_TOKEN:
        return False
    try:
        r = requests.post(f"https://graph.facebook.com/v17.0/{FB_PAGE_ID}/feed",
                          params={"access_token": FB_TOKEN, "message": msg}, timeout=10)
        return r.status_code == 200
    except Exception:
        logger.exception("FB post failed")
        return False

def post_instagram(msg):
    if not IG_USER_ID or not IG_TOKEN:
        return False
    try:
        img = "https://i.imgur.com/airmax270.jpg"
        r = requests.post(f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media",
                          params={"image_url": img, "caption": msg, "access_token": IG_TOKEN}, timeout=15)
        if r.status_code != 200: return False
        media_id = r.json().get("id")
        pub = requests.post(f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media_publish",
                            params={"creation_id": media_id, "access_token": IG_TOKEN}, timeout=10)
        return pub.status_code == 200
    except Exception:
        logger.exception("IG post failed")
        return False

def post_twitter(text):
    try:
        import tweepy
        if not TWITTER_API_KEY: return False
        client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN,
                               consumer_key=TWITTER_API_KEY,
                               consumer_secret=TWITTER_API_SECRET,
                               access_token=TWITTER_ACCESS_TOKEN,
                               access_token_secret=TWITTER_ACCESS_SECRET)
        client.create_tweet(text=text)
        return True
    except Exception:
        logger.exception("Twitter post failed")
        return False

def post_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        return r.status_code == 200
    except Exception:
        logger.exception("Telegram post failed")
        return False

def trigger_ifttt(event, v1=None, v2=None):
    if not IFTTT_KEY: return False
    try:
        r = requests.post(f"https://maker.ifttt.com/trigger/{event}/with/key/{IFTTT_KEY}",
                          json={"value1": v1, "value2": v2}, timeout=8)
        return r.status_code in (200, 202)
    except Exception:
        logger.exception("IFTTT failed")
        return False

# ========== MAIN LOOP ==========
def refresh_all_sources():
    logger.info("Refreshing affiliate sources…")
    links = []
    try:
        links += get_awin_offers()
        links += get_rakuten_offers()
    except Exception:
        logger.exception("Refresh sources failed")
    saved = save_links_to_db(links, source="affiliate")
    send_alert("REFRESH", f"Pulled {len(links)} links, saved {saved}")
    return saved

def post_next_pending():
    conn, cur = get_db_conn()
    cur.execute("SELECT id,url FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1")
    row = cur.fetchone(); conn.close()
    if not row:
        logger.debug("No pending posts")
        return False
    pid, url = row["id"], row["url"]
    caption = generate_caption(url)
    success = any([
        post_facebook(caption),
        post_instagram(caption),
        post_twitter(caption),
        post_telegram(caption)
    ])
    trigger_ifttt("Post_TikTok", caption, url)
    conn, cur = get_db_conn()
    cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE id=%s",
                ("sent" if success else "failed", datetime.now(timezone.utc), pid))
    conn.commit(); conn.close()
    send_alert("POSTED" if success else "FAILED", url)
    return success

def start_worker_background():
    global _worker_running, _stop_requested
    if _worker_running:
        logger.info("Worker already running")
        return
    _worker_running = True; _stop_requested = False
    send_alert("WORKER", "AutoAffiliate started")
    next_pull = datetime.now(timezone.utc)
    while not _stop_requested:
        try:
            if datetime.now(timezone.utc) >= next_pull:
                refresh_all_sources()
                next_pull = datetime.now(timezone.utc) + timedelta(minutes=PULL_INTERVAL_MINUTES)
            if not post_next_pending():
                time.sleep(SLEEP_ON_EMPTY)
            else:
                time.sleep(POST_INTERVAL_SECONDS)
        except Exception:
            logger.exception("Loop error")
            time.sleep(60)
    _worker_running = False
    send_alert("WORKER", "Stopped cleanly")

def stop_worker():
    global _stop_requested
    _stop_requested = True
    logger.info("Stop requested")

if __name__ == "__main__":
    start_worker_background()
