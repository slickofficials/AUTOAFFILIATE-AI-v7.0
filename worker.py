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
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from urllib.parse import quote_plus
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

# AWIN
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")
AWIN_AFFILIATE_ID = os.getenv("AWIN_AFFILIATE_ID")
AWIN_CLICKREF = os.getenv("AWIN_CLICKREF", "autoaffiliate")

# Rakuten (use App Token Key to fetch access tokens; build deeplinks with siteId)
RAKUTEN_SITE_ID = os.getenv("RAKUTEN_SITE_ID")
RAKUTEN_APP_TOKEN_KEY = os.getenv("RAKUTEN_APP_TOKEN_KEY")
RAKUTEN_CLICKREF = os.getenv("RAKUTEN_CLICKREF", "autoaffiliate")

# Social / Content keys
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

DEFAULT_CADENCE_SECONDS = int(os.getenv("DEFAULT_CADENCE_SECONDS", str(3 * 3600)))
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

def ensure_failed_links_table():
    logger.info("Ensuring table: failed_links")
    sql = """
    CREATE TABLE IF NOT EXISTS failed_links (
        id SERIAL PRIMARY KEY,
        source VARCHAR(50) NOT NULL,
        attempted_url TEXT NOT NULL,
        reason TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    """
    conn, cur = get_db_conn()
    try:
        cur.execute(sql)
        conn.commit()
        logger.info("failed_links table ensured")
    except Exception:
        conn.rollback()
        logger.exception("ensure_failed_links_table failed")
    finally:
        conn.close()

ensure_tables()
ensure_failed_links_table()

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
    # AWIN publisher id or affiliate id presence is a signal
    if AWIN_PUBLISHER_ID and str(AWIN_PUBLISHER_ID).lower() in u:
        return True
    if AWIN_AFFILIATE_ID and str(AWIN_AFFILIATE_ID).lower() in u:
        return True
    # Rakuten LinkSynergy site id presence is a signal
    if RAKUTEN_SITE_ID and str(RAKUTEN_SITE_ID).lower() in u:
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
    if not url:
        return None
    # Allow initial url to be http/https, but final saved must be https + affiliate signal
    final = follow_and_check(url) if not is_valid_https_url(url) else url
    if final and is_valid_https_url(final) and contains_affiliate_id(final):
        return final
    return None

def log_failed_link(url: str, source: str, reason: str):
    conn, cur = get_db_conn()
    try:
        cur.execute(
            "INSERT INTO failed_links(source, attempted_url, reason) VALUES (%s,%s,%s)",
            (source, url, reason)
        )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("log_failed_link failed: %s (%s)", url, reason)
    finally:
        conn.close()

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
                log_failed_link(link, source, "Failed validation/normalization")
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
                log_failed_link(norm, source, "DB insert failed")
        except Exception:
            logger.exception("save_links_to_db outer error")
            log_failed_link(link, source, "save_links_to_db outer exception")
    try:
        conn.commit()
    except Exception:
        conn.rollback()
    conn.close()
    logger.info("Saved %s validated links from %s (attempted %s)", added, source, attempted)
    return added
# ---------- AWIN ----------
def awin_api_offers(limit=4):
    """Pull programme metadata and attempt to extract a usable URL (not guaranteed)."""
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
                # Try multiple fields that may contain URLs
                u = p.get("url") or p.get("siteUrl") or p.get("homepageUrl") or p.get("programUrl")
                if u:
                    final = follow_and_check(u)
                    if final and is_valid_https_url(final):
                        out.append(final)
                    else:
                        log_failed_link(u, "awin", "Programme URL invalid or not https")
        else:
            log_failed_link(endpoint, "awin", f"HTTP {r.status_code}")
    except Exception:
        logger.exception("awin_api_offers error")
        log_failed_link(endpoint, "awin", "Exception during programmes call")
    return out[:limit]

def pull_awin_deeplinks(limit=4):
    """Try API offers first, then fall back to cread.php with real awinaffid."""
    out = awin_api_offers(limit=limit)
    # Fallback ensures tracking via your affiliate id (awinaffid must NOT be 0)
    if len(out) < limit and AWIN_PUBLISHER_ID and AWIN_AFFILIATE_ID:
        shortfall = limit - len(out)
        for _ in range(shortfall):
            try:
                url = (
                    f"https://www.awin1.com/cread.php?"
                    f"awinmid={AWIN_PUBLISHER_ID}&awinaffid={AWIN_AFFILIATE_ID}&clickref={AWIN_CLICKREF}"
                )
                r = requests_get(url, allow_redirects=True, timeout=15)
                final = r.url
                if final and is_valid_https_url(final) and contains_affiliate_id(final):
                    out.append(final)
                else:
                    log_failed_link(final or url, "awin", "Fallback link invalid")
            except Exception:
                logger.exception("AWIN fallback error")
                log_failed_link("cread.php", "awin", "Exception during fallback")
    return out[:limit]

# ---------- Rakuten ----------
_rakuten_access_token = None
_rakuten_token_expiry = 0

def get_rakuten_access_token() -> Optional[str]:
    """Obtain/refresh short-lived access token using App Token Key."""
    global _rakuten_access_token, _rakuten_token_expiry
    now = time.time()
    if not _rakuten_access_token or now >= _rakuten_token_expiry:
        try:
            resp = requests.post(
                "https://api.rakutenadvertising.com/token",
                data={"grant_type": "client_credentials", "client_id": RAKUTEN_APP_TOKEN_KEY},
                timeout=10,
            )
            data = resp.json()
            token = data.get("access_token")
            ttl = int(data.get("expires_in", 3600))
            if not token:
                log_failed_link("token", "rakuten", f"Missing access_token: {data}")
                return None
            _rakuten_access_token = token
            _rakuten_token_expiry = now + ttl - 60  # refresh 60s early
            logger.info("Rakuten token refreshed (ttl=%s)", ttl)
        except Exception:
            logger.exception("Rakuten token refresh failed")
            log_failed_link("token", "rakuten", "Exception during token refresh")
            _rakuten_access_token = None
    return _rakuten_access_token

def rakuten_headers():
    token = get_rakuten_access_token()
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"} if token else {"Accept": "application/json"}

def build_rakuten_deeplink(mid: str, dest: str) -> Optional[str]:
    """Commission-eligible deeplink via LinkSynergy."""
    if not (mid and dest and RAKUTEN_SITE_ID):
        return None
    clickref = quote_plus(RAKUTEN_CLICKREF)
    murl = quote_plus(dest)
    return f"https://click.linksynergy.com/deeplink?id={RAKUTEN_SITE_ID}&mid={mid}&u1={clickref}&murl={murl}"

def rakuten_api_offers(limit=4):
    """Fetch advertiser links via Linking API; returns destination + mid for deeplink."""
    out = []
    if not (RAKUTEN_SITE_ID and RAKUTEN_APP_TOKEN_KEY):
        return out
    hdrs = rakuten_headers()
    if "Authorization" not in hdrs:
        log_failed_link("headers", "rakuten", "Missing Bearer authorization")
        return out
    endpoint = "https://api.rakutenadvertising.com/linking/v1/links"
    try:
        r = requests_get(endpoint, headers=hdrs, timeout=12, params={"siteId": RAKUTEN_SITE_ID, "pageSize": limit})
        if r.status_code == 200:
            items = (r.json().get("links") or [])[:limit]
            for item in items:
                dest = item.get("destinationUrl") or item.get("linkUrl")
                mid = item.get("advertiserId") or item.get("mid")
                if dest and mid:
                    out.append({"dest": dest, "mid": str(mid)})
                else:
                    log_failed_link(json.dumps(item)[:500], "rakuten", "Missing dest/mid")
        else:
            log_failed_link(endpoint, "rakuten", f"HTTP {r.status_code}")
    except Exception:
        logger.exception("rakuten_api_offers error")
        log_failed_link(endpoint, "rakuten", "Exception during links call")
    return out

def pull_rakuten_deeplinks(limit=4):
    """Convert offers to deeplinks and validate before save."""
    out = []
    offers = rakuten_api_offers(limit=limit)
    for off in offers[:limit]:
        dl = build_rakuten_deeplink(off["mid"], off["dest"])
        if dl:
            final = follow_and_check(dl)
            if final and is_valid_https_url(final) and contains_affiliate_id(final):
                out.append(final)
            else:
                log_failed_link(dl, "rakuten", "Deeplink invalid after redirects")
    # Strict: no fabricated placeholders
    return out[:limit]
# ---------- OpenAI captions ----------
def generate_caption(link: str) -> str:
    if not openai_client:
        return f"Hot deal — check this out: {link}"
    try:
        prompt = f"Create a short energetic social caption (one sentence, includes 1 emoji, 1 CTA) for this affiliate link:\n{link}"
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
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
        logger.warning("HeyGen non-201: %s %s", r.status_code, r.text[:300])
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
        data = {}
        try:
            data = resp.json()
        except Exception:
            pass
        if resp.status_code == 200 and isinstance(data, dict) and "id" in data:
            logger.info("Posted to Facebook: %s", data["id"])
            return True
        log_failed_link(link, "facebook", f"HTTP {resp.status_code} {str(data)[:300]}")
        return False
    except Exception:
        logger.exception("FB posting error")
        log_failed_link(link, "facebook", "Exception")
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
        resp = client.create_tweet(text=f"{message} {link}")
        tid = None
        if hasattr(resp, "data") and resp.data and "id" in resp.data:
            tid = resp.data["id"]
            logger.info("Posted to Twitter: %s", tid)
            return True
        log_failed_link(link, "twitter", f"No tweet id: {str(resp)[:300]}")
        return False
    except Exception:
        logger.exception("Twitter posting error")
        log_failed_link(link, "twitter", "Exception")
        return False

def post_to_telegram(message: str, link: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": f"{message} {link}"}, timeout=10)
        data = {}
        try:
            data = resp.json()
        except Exception:
            pass
        if resp.status_code == 200 and isinstance(data, dict) and data.get("ok"):
            logger.info("Posted to Telegram")
            return True
        log_failed_link(link, "telegram", f"HTTP {resp.status_code} {str(data)[:300]}")
        return False
    except Exception:
        logger.exception("Telegram posting error")
        log_failed_link(link, "telegram", "Exception")
        return False

def post_to_ifttt(event_name: str, value1: str, value2: str = "", value3: str = "") -> bool:
    if not IFTTT_KEY:
        return False
    try:
        url = f"https://maker.ifttt.com/trigger/{event_name}/with/key/{IFTTT_KEY}"
        resp = requests.post(url, json={"value1": value1, "value2": value2, "value3": value3}, timeout=10)
        if resp.status_code in (200, 202):
            logger.info("Triggered IFTTT event: %s", event_name)
            return True
        log_failed_link(value1, "ifttt", f"HTTP {resp.status_code}")
        return False
    except Exception:
        logger.exception("IFTTT posting error")
        log_failed_link(value1, "ifttt", "Exception")
        return False
# ---------- Dashboard summary ----------
def get_failed_links_summary(days: int = 1):
    since = datetime.now(timezone.utc) - timedelta(days=days)
    rows = run_read(
        "SELECT source, COUNT(*) AS count FROM failed_links WHERE created_at >= %s GROUP BY source",
        (since,),
    )
    summary = {"date": datetime.now(timezone.utc).strftime("%Y-%m-%d"), "sources": {}}
    for r in rows:
        summary["sources"][r["source"]] = r["count"]
    for src in ("awin", "rakuten", "facebook", "twitter", "telegram", "ifttt"):
        summary["sources"].setdefault(src, 0)
    return summary

def print_dashboard_summary(days: int = 1):
    s = get_failed_links_summary(days)
    print("\n=== Affiliate Bot Dashboard Report ===")
    print(f"Date: {s['date']}")
    for source, count in s["sources"].items():
        print(f"{source.capitalize()}: {count} failures in last {days} day(s)")
    print("=====================================\n")

# ---------- Pull + Post ----------
def pull_and_post():
    for source, sub in ROTATION:
        if source == "awin":
            links = pull_awin_deeplinks(limit=4)
        elif source == "rakuten":
            links = pull_rakuten_deeplinks(limit=4)
        else:
            links = []

        if not links:
            log_failed_link(f"{source}-batch", source, "No links pulled")
            continue

        save_links_to_db(links, source=source)

        for link in links:
            caption = generate_caption(link)
            _ = generate_video(caption, link)  # optional, not blocking

            success_fb = post_to_facebook(caption, link)
            success_tw = post_to_twitter(caption, link)
            success_tg = post_to_telegram(caption, link)
            success_ifttt = post_to_ifttt("new_affiliate_link", link, caption)

            logger.info("Post results for %s: FB=%s TW=%s TG=%s IFTTT=%s",
                        link, success_fb, success_tw, success_tg, success_ifttt)

            # Mark as posted only if at least one platform succeeded
            if any([success_fb, success_tw, success_tg, success_ifttt]):
                run_write("UPDATE posts SET status='posted', posted_at=%s WHERE url=%s",
                          (datetime.now(timezone.utc), link))
            else:
                log_failed_link(link, source, "All platform posts failed")

# ---------- Worker loop ----------
def start_worker_background():
    global _worker_running, _stop_requested
    if _worker_running:
        return
    _worker_running = True
    while not _stop_requested:
        try:
            pull_and_post()
            print_dashboard_summary(days=1)
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
    ensure_failed_links_table()
    logger.info("Worker loaded, initial stats: %s", get_stats())
    try:
        start_worker_background()
    except KeyboardInterrupt:
        stop_worker()
        logger.info("Worker stopped via KeyboardInterrupt") 
