# worker.py — AutoAffiliate worker (Production-ready)
# Full pipeline: DB, AWIN & Rakuten official calls, OpenAI captions, HeyGen, FB/IG/Twitter/Telegram/IFTTT posting
# Requirements: requests, psycopg[binary], openai, tweepy, redis, rq, twilio (optional), python-telegram-bot (optional)
# Put your env vars in the Render / environment

import os
import time
import json
import logging
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone
from typing import List, Optional
from openai import OpenAI

try:
    import tweepy
except Exception:
    tweepy = None

# ---------- Logging ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("worker")

# ---------- Environment / Config ----------
DB_URL = os.getenv("DATABASE_URL")
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")
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
IFTTT_KEY = os.getenv("IFTTT_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DEFAULT_CADENCE_SECONDS = int(os.getenv("DEFAULT_CADENCE_SECONDS", str(3*3600)))
DEBUG_REDIRECTS = os.getenv("DEBUG_REDIRECTS", "0") == "1"

# ---------- Clients ----------
openai_client = OpenAI(api_key=OPENAI_KEY) if OPENAI_KEY else None

# ---------- Worker flags ----------
_worker_running = False
_stop_requested = False

# ---------- Rotation plan ----------
ROTATION = [
    ("awin", "B"),
    ("rakuten", "2"),
    ("awin", "C"),
    ("rakuten", "1"),
    ("awin", "A"),
]

# ---------- Database helpers ----------
def get_db_conn():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

def run_write(sql: str, params=()):
    conn, cur = get_db_conn()
    try:
        cur.execute(sql, params)
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("DB write failed: %s", sql)
        raise
    finally:
        conn.close()

def run_read(sql: str, params=()):
    conn, cur = get_db_conn()
    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
        return rows
    finally:
        conn.close()

def ensure_tables():
    logger.info("Ensuring tables: posts, clicks, settings")
    safe = """
    CREATE TABLE IF NOT EXISTS posts (
        id SERIAL PRIMARY KEY,
        url TEXT UNIQUE NOT NULL,
        source TEXT,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMPTZ DEFAULT now(),
        posted_at TIMESTAMPTZ,
        meta JSONB DEFAULT '{}'::jsonb
    );
    CREATE TABLE IF NOT EXISTS clicks (
        id SERIAL PRIMARY KEY,
        post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
        ip TEXT,
        user_agent TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        setting_key TEXT UNIQUE,
        value TEXT
    );
    """
    conn, cur = get_db_conn()
    try:
        cur.execute(safe)
        conn.commit()
        logger.info("Tables ensured")
    except Exception:
        conn.rollback()
        logger.exception("ensure_tables failed")
    finally:
        conn.close()

ensure_tables()

def db_get_setting(k: str, fallback=None):
    try:
        conn, cur = get_db_conn()
        cur.execute("SELECT value FROM settings WHERE key=%s LIMIT 1", (k,))
        r = cur.fetchone()
        conn.close()
        return r["value"] if r else fallback
    except Exception:
        logger.exception("db_get_setting")
        return fallback

def db_set_setting(k: str, v: str):
    try:
        conn, cur = get_db_conn()
        cur.execute("""
            INSERT INTO settings(key, setting_key, value)
            VALUES(%s,%s,%s)
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, setting_key=EXCLUDED.setting_key
        """, (k, k, str(v)))
        conn.commit()
        conn.close()
        return True
    except Exception:
        logger.exception("db_set_setting")
        return False

POST_INTERVAL_SECONDS = int(db_get_setting("post_interval_seconds", fallback=str(DEFAULT_CADENCE_SECONDS)))

# ---------- Helpers ----------
def requests_get(url, **kwargs):
    kwargs.setdefault("timeout", 15)
    return requests.get(url, **kwargs)

def is_valid_https_url(url: str) -> bool:
    return bool(url and isinstance(url, str) and url.startswith("https://") and len(url) < 4000)

def contains_affiliate_id(url: str) -> bool:
    if not url:
        return False
    u = url.lower()
    if AWIN_PUBLISHER_ID and str(AWIN_PUBLISHER_ID) in u:
        return True
    if RAKUTEN_CLIENT_ID and str(RAKUTEN_CLIENT_ID) in u:
        return True
    affiliate_signals = ["tidd.ly", "linksynergy", "awin", "rakuten", "affiliates", "trk."]
    return any(s in u for s in affiliate_signals)

def follow_and_check(url: str, max_hops=5) -> Optional[str]:
    try:
        r = requests_get(url, allow_redirects=True, timeout=15)
        if DEBUG_REDIRECTS:
            chain = " -> ".join([h.url for h in r.history] + [r.url])
            logger.info("Redirect chain: %s", chain)
        return r.url
    except Exception:
        logger.exception("follow_and_check failed for %s", url)
        return None

def validate_and_normalize_link(url: str) -> Optional[str]:
    if not is_valid_https_url(url):
        return None
    if contains_affiliate_id(url):
        return url
    final = follow_and_check(url)
    if final and contains_affiliate_id(final):
        return final
    return None

def save_links_to_db(links: List[str], source="affiliate"):
    if not links:
        return 0
    conn, cur = get_db_conn()
    added = 0
    attempted = len(links)
    for link in links:
        try:
            norm = validate_and_normalize_link(link)
            if not norm:
                continue
            try:
                cur.execute(
                    "INSERT INTO posts (url, source, status, created_at) VALUES (%s,%s,'pending',%s) ON CONFLICT (url) DO NOTHING",
                    (norm, source, datetime.now(timezone.utc))
                )
                added += 1
            except Exception:
                conn.rollback()
                logger.exception("Insert failed for %s", norm)
        except Exception:
            logger.exception("save_links_to_db outer error")
    try:
        conn.commit()
    except Exception:
        conn.rollback()
    conn.close()
    logger.info("Saved %s validated links from %s (attempted %s)", added, source, attempted)
    return added

# ---------- AWIN ----------
def awin_api_offers(limit=4):
    out = []
    if not AWIN_API_TOKEN or not AWIN_PUBLISHER_ID:
        return out
    headers = {"Authorization": f"Bearer {AWIN_API_TOKEN}", "Accept": "application/json"}
    endpoint = f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/programmes"
    try:
        r = requests_get(endpoint, headers=headers, timeout=12)
        if r.status_code == 200:
            data = r.json()
            for p in data[:limit]:
                u = p.get("url") or p.get("deeplink") or p.get("tracking_url") or p.get("siteUrl")
                if u and is_valid_https_url(u):
                    out.append(u)
    except Exception:
        logger.exception("awin_api_offers error")
    return out[:limit]

def pull_awin_deeplinks(limit=4):
    out = awin_api_offers(limit=limit)
    while len(out) < limit:
        try:
            url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=bot"
            r = requests_get(url, allow_redirects=True, timeout=15)
            final = r.url
            if final and is_valid_https_url(final):
                out.append(final)
        except Exception:
            break
    return out[:limit]

# ---------- Rakuten ----------
def rakuten_api_offers(limit=4):
    out = []
    if not RAKUTEN_SECURITY_TOKEN:
        return out
    headers = {"Authorization": f"Bearer {RAKUTEN_SECURITY_TOKEN}", "Accept": "application/json"}
    endpoint = "https://api.rakutenmarketing.com/linking/v1/offer"
    try:
        r = requests_get(endpoint, headers=headers, timeout=12)
        if r.status_code == 200:
            data = r.json()
            for item in data.get("offers", [])[:limit]:
                u = item.get("deeplink") or item.get("url")
                if u and is_valid_https_url(u):
                    out.append(u)
    except Exception:
        logger.exception("rakuten_api_offers error")
    return out[:limit]

def pull_rakuten_deeplinks(limit=4):
    out = rakuten_api_offers(limit=limit)
    while len(out) < limit:
        try:
            url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl=https://example.com"
            r = requests_get(url, allow_redirects=True, timeout=15)
            final = r.url
            if final and is_valid_https_url(final):
                out.append(final)
        except Exception:
            break
    return out[:limit]

# ---------- OpenAI captions ----------
def generate_caption(link: str) -> str:
    if not openai_client:
        return f"Hot deal — check this out: {link}"
    try:
        prompt = f"Create a short energetic social caption (one sentence, includes 1 emoji, 1 CTA) for this affiliate link:\n{link}"
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content": prompt}],
            max_tokens=60
        )
        text = ""
        if resp and getattr(resp, "choices", None):
            choice = resp.choices[0]
            msg = getattr(choice, "message", None)
            if msg and getattr(msg, "content", None):
                text = msg.content.strip()
        if not text:
            text = f"Hot deal — check this out: {link}"
        if link not in text:
            text = f"{text} {link}"
        return text
    except Exception:
        logger.exception("OpenAI caption failed")
        return f"Hot deal — check this out: {link}"

# ---------- HeyGen ----------
def generate_video(caption: str, link: str) -> Optional[str]:
    if not HEYGEN_KEY:
        return None
    try:
        payload = {"script": caption, "voice": "en_us_1", "format": "mp4", "resolution": "1080p"}
        headers = {"Authorization": f"Bearer {HEYGEN_KEY}", "Content-Type": "application/json"}
        r = requests.post("https://api.heygen.com/v1/video", json=payload, headers=headers, timeout=30)
        if r.status_code == 201:
            data = r.json()
            return data.get("video_url")
    except Exception:
        logger.exception("HeyGen video generation failed")
    return None

# ---------- Social posting ----------
def post_to_facebook(message: str, link: str) -> bool:
    if not FB_TOKEN or not FB_PAGE_ID:
        return False
    try:
        url = f"https://graph.facebook.com/{FB_PAGE_ID}/feed"
        resp = requests.post(url, data={"message": message, "link": link, "access_token": FB_TOKEN}, timeout=15)
        return resp.status_code == 200
    except Exception:
        logger.exception("FB posting error")
        return False

def post_to_twitter(message: str, link: str) -> bool:
    if not TWITTER_BEARER_TOKEN or not tweepy:
        return False
    try:
        client = tweepy.Client(
            bearer_token=TWITTER_BEARER_TOKEN,
            consumer_key=TWITTER_API_KEY,
            consumer_secret=TWITTER_API_SECRET,
            access_token=TWITTER_ACCESS_TOKEN,
            access_token_secret=TWITTER_ACCESS_SECRET
        )
        client.create_tweet(text=f"{message} {link}")
        return True
    except Exception:
        logger.exception("Twitter posting error")
        return False

def post_to_telegram(message: str, link: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": f"{message} {link}"}, timeout=10)
        return resp.status_code == 200
    except Exception:
        logger.exception("Telegram posting error")
        return False

def post_to_ifttt(event_name: str, value1: str, value2: str = "", value3: str = "") -> bool:
    if not IFTTT_KEY:
        return False
    try:
        url = f"https://maker.ifttt.com/trigger/{event_name}/with/key/{IFTTT_KEY}"
        resp = requests.post(url, json={"value1": value1, "value2": value2, "value3": value3}, timeout=10)
        return resp.status_code in (200, 202)
    except Exception:
        logger.exception("IFTTT posting error")
        return False

# ---------- Worker loop ----------
def pull_and_post():
    for source, sub in ROTATION:
        if source == "awin":
            links = pull_awin_deeplinks(limit=4)
        elif source == "rakuten":
            links = pull_rakuten_deeplinks(limit=4)
        else:
            links = []
        if not links:
            continue
        save_links_to_db(links, source=source)
        for link in links:
            caption = generate_caption(link)
            generate_video(caption, link)
            post_to_facebook(caption, link)
            post_to_twitter(caption, link)
            post_to_telegram(caption, link)
            post_to_ifttt("new_affiliate_link", link, caption)
            run_write("UPDATE posts SET status='posted', posted_at=%s WHERE url=%s", (datetime.now(timezone.utc), link))

def start_worker_background():
    global _worker_running, _stop_requested
    if _worker_running:
        return
    _worker_running = True
    while not _stop_requested:
        try:
            pull_and_post()
        except Exception:
            logger.exception("Worker iteration failed")
        time.sleep(POST_INTERVAL_SECONDS)
    _worker_running = False

def stop_worker():
    global _stop_requested
    _stop_requested = True

def get_stats():
    pending = run_read("SELECT COUNT(*) as cnt FROM posts WHERE status='pending'")[0]["cnt"]
    posted = run_read("SELECT COUNT(*) as cnt FROM posts WHERE status='posted'")[0]["cnt"]
    return {"pending": pending, "posted": posted}

if __name__ == "__main__":
    ensure_tables()
    logger.info("Worker loaded, initial stats: %s", get_stats())
    try:
        start_worker_background()
    except KeyboardInterrupt:
        stop_worker()
        logger.info("Worker stopped via KeyboardInterrupt")
