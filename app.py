# app.py — AutoAffiliate (production-ready)
# Full pipeline: DB, AWIN & Rakuten official calls, OpenAI captions, HeyGen, FB/IG/Twitter/Telegram/IFTTT posting
# Requirements: requests, psycopg (psycopg3), openai, tweepy (optional), backoff
# Put your env vars in Render / your environment

import os
import time
import json
import logging
import requests
import threading
import traceback
from typing import List, Optional, Tuple
from datetime import datetime, timezone, timedelta

import backoff
import psycopg
from psycopg.rows import dict_row

# Optional imports
try:
    from openai import OpenAI
except Exception:
    OpenAI = None

try:
    import tweepy
except Exception:
    tweepy = None

# ---------- Logging ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("app")

# ---------- Environment / Config ----------
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    logger.error("DATABASE_URL not set — app may fail to start database operations")

# AWIN / Rakuten / OpenAI / HeyGen / Social tokens (keep same names you used)
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")

RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")  # bearer
# Allow override token endpoint if provider changed host/cert (helps when documentation changes)
RAKUTEN_TOKEN_URL = os.getenv("RAKUTEN_TOKEN_URL", "https://api.rakutenadvertising.com/token")
RAKUTEN_OFFERS_URL = os.getenv("RAKUTEN_OFFERS_URL", "https://api.rakutenadvertising.com/linking/v1/offer")

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

TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
YOUR_WHATSAPP = os.getenv("YOUR_WHATSAPP")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

APP_PUBLIC_URL = os.getenv("APP_PUBLIC_URL", "")

# Cadence
DEFAULT_CADENCE_SECONDS = int(os.getenv("DEFAULT_CADENCE_SECONDS", str(3 * 3600)))
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "300"))
DEBUG_REDIRECTS = os.getenv("DEBUG_REDIRECTS", "0") == "1"

# ---------- Clients ----------
openai_client = None
if OpenAI and OPENAI_KEY:
    try:
        openai_client = OpenAI(api_key=OPENAI_KEY)
    except Exception:
        logger.exception("OpenAI client init failed")

# ---------- Worker flags ----------
_worker_thread: Optional[threading.Thread] = None
_worker_stop = threading.Event()

# ---------- Rotation plan ----------
ROTATION = [
    ("awin", "B"),
    ("rakuten", "2"),
    ("awin", "C"),
    ("rakuten", "1"),
    ("awin", "A"),
]

# ---------- Database helpers ----------
def get_db_conn() -> Tuple[psycopg.Connection, psycopg.Cursor]:
    """
    Return psycopg connection, cursor (dict_row)
    """
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    cur = conn.cursor()
    return conn, cur

def ensure_tables():
    """
    Create tables if missing. resilient: won't fail if different schema exists.
    """
    logger.info("Ensuring tables: posts, clicks, settings")
    sql = """
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
    -- settings: keep older and newer names tolerable
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        setting_key TEXT UNIQUE,
        value TEXT
    );
    """
    conn, cur = get_db_conn()
    try:
        cur.execute(sql)
        conn.commit()
        logger.info("Tables ensured")
    except Exception:
        conn.rollback()
        logger.exception("Failed ensure_tables")
    finally:
        conn.close()

# helper that checks whether a column exists in settings (used to avoid UndefinedColumn)
def _settings_has_column(col_name: str) -> bool:
    try:
        conn, cur = get_db_conn()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = 'settings' AND column_name = %s
        """, (col_name,))
        r = cur.fetchone()
        conn.close()
        return bool(r)
    except Exception:
        logger.exception("Failed to check settings schema")
        return False

def db_get_setting(k: str, fallback=None):
    """Try multiple safe queries to avoid undefined-column errors."""
    try:
        conn, cur = get_db_conn()
        # first try primary 'key'
        try:
            cur.execute("SELECT value FROM settings WHERE key=%s LIMIT 1", (k,))
            r = cur.fetchone()
            if r:
                conn.close()
                return r["value"]
        except Exception:
            # log but continue, maybe column doesn't exist
            logger.debug("db_get_setting: primary key query failed (will try fallback). %s", traceback.format_exc())

        # fallback: if column 'setting_key' exists use it
        if _settings_has_column("setting_key"):
            try:
                conn, cur = get_db_conn()
                cur.execute("SELECT value FROM settings WHERE setting_key=%s LIMIT 1", (k,))
                r = cur.fetchone()
                conn.close()
                if r:
                    return r["value"]
            except Exception:
                logger.debug("db_get_setting: fallback query failed.", exc_info=True)

        # last-resort: try any row where value is present (very last fallback)
        conn, cur = get_db_conn()
        cur.execute("SELECT value FROM settings WHERE value IS NOT NULL LIMIT 1")
        r = cur.fetchone()
        conn.close()
        return r["value"] if r else fallback
    except Exception:
        logger.exception("db_get_setting final failure")
        return fallback

def db_set_setting(k: str, v: str):
    """
    Insert or update settings. Write both key and setting_key to be tolerant to legacy code.
    """
    try:
        conn, cur = get_db_conn()
        # ensure both columns exist; if not, try to add setting_key
        if not _settings_has_column("setting_key"):
            try:
                cur.execute("ALTER TABLE settings ADD COLUMN IF NOT EXISTS setting_key TEXT UNIQUE")
                conn.commit()
            except Exception:
                conn.rollback()
                logger.debug("Could not add setting_key column (perhaps permissions).")

        cur.execute("""
            INSERT INTO settings(key, setting_key, value) 
            VALUES (%s, %s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, setting_key = EXCLUDED.setting_key
        """, (k, k, str(v)))
        conn.commit()
        conn.close()
        return True
    except Exception:
        logger.exception("db_set_setting failed")
        return False

# ---------- HTTP helpers ----------
DEFAULT_REQUEST_TIMEOUT = 15

@backoff.on_exception(backoff.expo, (requests.exceptions.RequestException,), max_time=30)
def requests_get(url, **kwargs):
    kwargs.setdefault("timeout", DEFAULT_REQUEST_TIMEOUT)
    return requests.get(url, **kwargs)

@backoff.on_exception(backoff.expo, (requests.exceptions.RequestException,), max_time=30)
def requests_post(url, **kwargs):
    kwargs.setdefault("timeout", DEFAULT_REQUEST_TIMEOUT)
    return requests.post(url, **kwargs)

# ---------- URL helpers ----------
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
    affiliate_signals = ["tidd.ly", "linksynergy", "awin", "rakuten", "affiliates", "trk.", "click.linksynergy"]
    return any(s in u for s in affiliate_signals)

def follow_and_check(url: str, max_hops=5) -> Optional[str]:
    try:
        # follow redirects and return final
        r = requests_get(url, allow_redirects=True, timeout=20)
        if DEBUG_REDIRECTS:
            chain = " -> ".join([h.url for h in r.history] + [r.url])
            logger.info("Redirect chain: %s", chain)
        return r.url
    except Exception:
        logger.exception("follow_and_check failed for %s", url)
        return None

def validate_and_normalize_link(url: str) -> Optional[str]:
    if not is_valid_https_url(url):
        logger.debug("Invalid URL: %s", url)
        return None
    if contains_affiliate_id(url):
        return url
    final = follow_and_check(url)
    if final and contains_affiliate_id(final):
        return final
    logger.debug("Rejected non-affiliate link after redirect check: %s", url)
    return None

# ---------- Save links ----------
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
                logger.debug("Rejected non-affiliate or invalid: %s", link)
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

# ---------- AWIN integration ----------
def awin_api_offers(limit=4):
    out = []
    if not AWIN_API_TOKEN or not AWIN_PUBLISHER_ID:
        logger.debug("AWIN credentials missing")
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
        else:
            logger.warning("AWIN API non-200: %s %s", r.status_code, r.text[:400])
    except Exception:
        logger.exception("awin_api_offers error")
    return out[:limit]

def pull_awin_deeplinks(limit=4):
    out = awin_api_offers(limit=limit)
    if len(out) >= limit:
        return out[:limit]
    # fallback to standard cread redirect
    for _ in range(limit - len(out)):
        try:
            url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=autoaffiliate"
            r = requests_get(url, allow_redirects=True, timeout=12)
            final = r.url
            if final and is_valid_https_url(final):
                out.append(final)
        except Exception:
            logger.exception("AWIN fallback fetch error")
    return out[:limit]

# ---------- Rakuten integration ----------
def get_rakuten_access_token() -> Optional[str]:
    """
    Robust token retrieval with configurable URL (security token flow)
    Some Rakuten regions use different hostnames—RAKUTEN_TOKEN_URL allows override.
    """
    if RAKUTEN_SECURITY_TOKEN:
        # If we already have a bearer token configured in env, use it
        return RAKUTEN_SECURITY_TOKEN

    # If you need to use client credentials flow, set RAKUTEN_APP_TOKEN_KEY or other envs and override URL
    RAKUTEN_APP_TOKEN_KEY = os.getenv("RAKUTEN_APP_TOKEN_KEY")
    if not RAKUTEN_APP_TOKEN_KEY:
        logger.debug("No Rakuten bearer or app token found in env.")
        return None

    try:
        # Respect override if provider changed host/cert
        resp = requests_post(RAKUTEN_TOKEN_URL, data={"grant_type": "client_credentials", "client_id": RAKUTEN_APP_TOKEN_KEY}, timeout=10)
        if resp.status_code in (200, 201):
            j = resp.json()
            token = j.get("access_token") or j.get("token")
            if token:
                logger.info("Rakuten token obtained dynamically")
                return token
            logger.warning("Rakuten token response missing access_token: %s", j)
        else:
            logger.warning("Rakuten token endpoint returned %s: %s", resp.status_code, resp.text[:400])
    except requests.exceptions.SSLError as e:
        logger.exception("Rakuten SSLError — check RAKUTEN_TOKEN_URL or your environment certificate trust: %s", e)
    except Exception:
        logger.exception("Rakuten token fetch failed")
    return None

def rakuten_api_offers(limit=4):
    out = []
    token = get_rakuten_access_token()
    if not token:
        logger.debug("Rakuten credentials missing")
        return out
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    endpoint = RAKUTEN_OFFERS_URL
    try:
        r = requests_get(endpoint, headers=headers, timeout=12)
        if r.status_code == 200:
            data = r.json()
            # find offers depending on response format
            offers = data.get("offers") if isinstance(data, dict) else []
            if not offers and isinstance(data, list):
                offers = data
            for item in (offers or [])[:limit]:
                u = item.get("deeplink") or item.get("url") or item.get("tracking_url")
                if u and is_valid_https_url(u):
                    out.append(u)
        else:
            logger.warning("Rakuten API non-200: %s %s", r.status_code, r.text[:400])
    except requests.exceptions.SSLError as e:
        logger.exception("Rakuten SSL error — this usually means the endpoint host doesn't match cert. If you see Hostname mismatch, set RAKUTEN_TOKEN_URL and RAKUTEN_OFFERS_URL to the correct host for your region.")
    except Exception:
        logger.exception("rakuten_api_offers error")
    return out[:limit]

def pull_rakuten_deeplinks(limit=4):
    out = rakuten_api_offers(limit=limit)
    if len(out) >= limit:
        return out[:limit]
    # fallback LinkSynergy deeplink (if client_id known)
    for _ in range(limit - len(out)):
        try:
            url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl=https://example.com"
            r = requests_get(url, allow_redirects=True, timeout=12)
            final = r.url
            if final and is_valid_https_url(final):
                out.append(final)
        except Exception:
            logger.exception("Rakuten fallback fetch error")
    return out[:limit]

# ---------- OpenAI captions ----------
def generate_caption(link: str) -> str:
    if not openai_client:
        return f"Hot deal — check this out: {link}"
    try:
        prompt = f"Create a short energetic social caption (one sentence, includes 1 emoji, 1 CTA) for this affiliate link:\n{link}"
        # using Chat Completions or Responses — if OpenAI client differs, this will error; we handle it.
        try:
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
            text = text.strip()
        except Exception:
            # fallback create with simpler interface
            resp = openai_client.completions.create(model="gpt-4o-mini", prompt=prompt, max_tokens=60)
            text = (resp.choices[0].text.strip() if resp and getattr(resp, "choices", None) else "").strip()
        if not text:
            text = f"Hot deal — check this out: {link}"
        if link not in text:
            text = f"{text} {link}"
        return text
    except Exception:
        logger.exception("OpenAI caption failed")
        return f"Hot deal — check this out: {link}"

# ---------- HeyGen ----------
def generate_heygen_avatar_video(text: str) -> Optional[str]:
    if not HEYGEN_KEY:
        return None
    try:
        url = "https://api.heygen.com/v1/video/generate"
        headers = {"x-api-key": HEYGEN_KEY, "Content-Type": "application/json"}
        payload = {
            "type": "avatar",
            "script": {"type": "text", "input": text},
            "avatar": "default",
            "voice": {"language": "en-US", "style": "energetic"},
            "output_format": "mp4"
        }
        r = requests_post(url, json=payload, headers=headers, timeout=60)
        if r.status_code in (200, 201):
            data = r.json()
            return data.get("video_url") or data.get("result_url") or data.get("url") or data.get("job_id")
        logger.warning("HeyGen API non-200: %s %s", r.status_code, r.text[:400])
    except Exception:
        logger.exception("HeyGen error")
    return None

# ---------- Social posting helpers ----------
def post_facebook(message: str) -> bool:
    if not FB_PAGE_ID or not FB_TOKEN:
        logger.debug("FB not configured")
        return False
    try:
        endpoint = f"https://graph.facebook.com/v17.0/{FB_PAGE_ID}/feed"
        params = {"access_token": FB_TOKEN, "message": message}
        r = requests_post(endpoint, params=params, timeout=15)
        logger.info("FB post status=%s", r.status_code)
        return r.status_code in (200, 201)
    except Exception:
        logger.exception("FB post failed")
        return False

def post_instagram(caption: str) -> bool:
    if not IG_USER_ID or not (IG_TOKEN or FB_TOKEN):
        logger.debug("IG not configured")
        return False
    try:
        image_url = "https://i.imgur.com/airmax270.jpg"
        create = requests_post(
            f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media",
            params={"image_url": image_url, "caption": caption, "access_token": IG_TOKEN or FB_TOKEN}, timeout=15)
        if create.status_code != 200:
            logger.warning("IG create failed: %s", create.text[:400])
            return False
        creation_id = create.json().get("id")
        publish = requests_post(
            f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media_publish",
            params={"creation_id": creation_id, "access_token": IG_TOKEN or FB_TOKEN}, timeout=15)
        logger.info("IG publish status=%s", publish.status_code)
        return publish.status_code in (200, 201)
    except Exception:
        logger.exception("IG post failed")
        return False

def post_twitter(text: str) -> bool:
    if not tweepy:
        logger.debug("tweepy not installed")
        return False
    try:
        if TWITTER_BEARER_TOKEN:
            client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN,
                                   consumer_key=TWITTER_API_KEY,
                                   consumer_secret=TWITTER_API_SECRET,
                                   access_token=TWITTER_ACCESS_TOKEN,
                                   access_token_secret=TWITTER_ACCESS_SECRET)
            client.create_tweet(text=text)
            logger.info("Tweet posted via v2")
            return True
        if all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET]):
            auth = tweepy.OAuth1UserHandler(TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)
            api = tweepy.API(auth)
            api.update_status(status=text)
            logger.info("Tweet posted via OAuth1")
            return True
    except tweepy.TooManyRequests:
        logger.warning("Twitter rate limit hit")
    except Exception:
        logger.exception("Twitter post failed")
    return False

def post_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram not configured")
        return False
    try:
        resp = requests_post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                             json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        if resp.status_code == 200:
            logger.info("Posted to Telegram")
        return resp.status_code == 200
    except Exception:
        logger.exception("Telegram post failed")
        return False

def trigger_ifttt(event: str, value1=None, value2=None, value3=None) -> bool:
    if not IFTTT_KEY:
        logger.debug("IFTTT not configured")
        return False
    try:
        url = f"https://maker.ifttt.com/trigger/{event}/with/key/{IFTTT_KEY}"
        payload = {}
        if value1 is not None: payload["value1"] = value1
        if value2 is not None: payload["value2"] = value2
        if value3 is not None: payload["value3"] = value3
        r = requests_post(url, json=payload, timeout=10)
        logger.info("IFTTT trigger %s status=%s", event, r.status_code)
        return r.status_code in (200, 202)
    except Exception:
        logger.exception("IFTTT trigger failed")
        return False

def post_youtube_short(title: str, video_url: str) -> bool:
    try:
        post_telegram(f"YouTube (manual): {title}\n{video_url}")
        return True
    except Exception:
        logger.exception("YouTube post fallback failed")
        return False

def send_alert(title: str, body: str):
    logger.info("ALERT: %s — %s", title, body)
    # WhatsApp via Twilio
    if TWILIO_SID and TWILIO_TOKEN and YOUR_WHATSAPP:
        try:
            from twilio.rest import Client
            client = Client(TWILIO_SID, TWILIO_TOKEN)
            client.messages.create(from_='whatsapp:+14155238886', body=f"*{title}*\n{body}", to=YOUR_WHATSAPP)
        except Exception:
            logger.exception("Twilio alert failed")
    # Telegram
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests_post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": f"{title}\n{body}"}, timeout=8)
        except Exception:
            logger.exception("Telegram alert failed")

# ---------- Enqueue manual link ----------
def enqueue_manual_link(url: str, source="manual"):
    if not url:
        raise ValueError("url required")
    norm = validate_and_normalize_link(url)
    if not norm:
        raise ValueError("URL not valid affiliate or could not validate")
    conn, cur = get_db_conn()
    try:
        cur.execute("INSERT INTO posts (url, source, status, created_at) VALUES (%s,%s,'pending',%s) ON CONFLICT (url) DO NOTHING",
                    (norm, source, datetime.now(timezone.utc)))
        conn.commit()
        logger.info("Enqueued manual link %s", norm)
        return {"inserted": True, "url": norm}
    except Exception:
        conn.rollback()
        logger.exception("enqueue_manual_link failed")
        raise
    finally:
        conn.close()

# ---------- Posting pipeline ----------
def post_next_pending() -> bool:
    conn, cur = get_db_conn()
    try:
        cur.execute("SELECT id, url FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED")
        row = cur.fetchone()
        if not row:
            return False
        post_id = row["id"]
        url = row["url"]
        final_url = url
        if not contains_affiliate_id(url):
            maybe = follow_and_check(url)
            if maybe and contains_affiliate_id(maybe):
                final_url = maybe
            else:
                # mark failed
                try:
                    cur.execute("UPDATE posts SET status=%s, meta = jsonb_set(coalesce(meta,'{}'::jsonb), %s, %s, true) WHERE id=%s",
                                ("failed", '{reason}', json.dumps("invalid_affiliate"), post_id))
                    conn.commit()
                except Exception:
                    conn.rollback()
                logger.info("Dropped non-affiliate pending: %s", url)
                return False

        caption = generate_caption(final_url)
        redirect_link = f"{APP_PUBLIC_URL.rstrip('/')}/r/{post_id}" if APP_PUBLIC_URL else final_url
        caption_with_link = f"{caption}\n{redirect_link}"

        # optional HeyGen
        video_ref = None
        try:
            video_ref = generate_heygen_avatar_video(caption)
        except Exception:
            logger.exception("HeyGen generation failed")

        posted_any = False
        # Post to social platforms (try/catch each)
        try:
            if post_facebook(caption_with_link):
                posted_any = True
        except Exception:
            logger.exception("FB posting error")
        try:
            if post_instagram(caption_with_link):
                posted_any = True
        except Exception:
            logger.exception("IG posting error")
        try:
            if post_twitter(caption + " " + redirect_link):
                posted_any = True
        except Exception:
            logger.exception("Twitter posting error")
        try:
            if post_telegram(caption_with_link):
                posted_any = True
        except Exception:
            logger.exception("Telegram posting error")

    # ---------- IFTTT ----------
def trigger_ifttt(event: str, value1=None, value2=None, value3=None) -> bool:
    if not IFTTT_KEY:
        logger.debug("IFTTT not configured")
        return False
    try:
        url = f"https://maker.ifttt.com/trigger/{event}/json/with/key/{IFTTT_KEY}"
        payload = {"value1": value1, "value2": value2, "value3": value3}
        r = requests_post(url, json=payload, timeout=10)
        logger.info("IFTTT trigger %s status=%s", event, r.status_code)
        return r.status_code in (200, 202)
    except Exception:
        logger.exception("IFTTT trigger failed")
        return False


# ---------- Posting pipeline ----------
def post_to_all_socials(caption: str, link: str):
    results = {}

    try:
        results["facebook"] = post_facebook(caption)
    except:
        results["facebook"] = False

    try:
        results["instagram"] = post_instagram(caption)
    except:
        results["instagram"] = False

    try:
        results["twitter"] = post_twitter(caption)
    except:
        results["twitter"] = False

    try:
        results["telegram"] = post_telegram(caption)
    except:
        results["telegram"] = False

    try:
        results["ifttt"] = trigger_ifttt("autoaffiliate", caption, link)
    except:
        results["ifttt"] = False

    logger.info("Social posting results: %s", results)
    return results


# ---------- Worker core ----------
def process_next_post():
    conn, cur = get_db_conn()
    post = None
    try:
        cur.execute("SELECT * FROM posts WHERE status='pending' ORDER BY id ASC LIMIT 1")
        post = cur.fetchone()
        if not post:
            conn.close()
            return False

        post_id = post["id"]
        url = post["url"]

        logger.info("Processing post #%s %s", post_id, url)

        # 1. Generate caption
        caption = generate_caption(url)

        # 2. Generate HeyGen video (optional)
        video_url = generate_heygen_avatar_video(caption)

        # 3. Post everywhere
        results = post_to_all_socials(caption, url)

        # 4. Save result metadata
        cur.execute("""
            UPDATE posts SET 
                status='posted', 
                posted_at=%s, 
                meta=%s
            WHERE id=%s
        """, (datetime.now(timezone.utc),
              json.dumps({"caption": caption, "video": video_url, "results": results}),
              post_id))
        conn.commit()
        conn.close()
        return True

    except Exception:
        logger.exception("process_next_post failure")
        try:
            conn.rollback()
        except:
            pass
        try:
            conn.close()
        except:
            pass
        return False


def worker_loop():
    logger.info("Worker loop started")
    while not _worker_stop.is_set():
        try:
            ok = process_next_post()
            if not ok:
                logger.info("No pending posts — sleeping %s sec", SLEEP_ON_EMPTY)
                time.sleep(SLEEP_ON_EMPTY)
        except Exception:
            logger.exception("worker_loop error")
            time.sleep(10)
    logger.info("Worker loop stopped")


# ---------- Rotation pulling ----------
def run_rotation_once():
    logger.info("Running affiliate rotation")
    for src, code in ROTATION:
        try:
            if src == "awin":
                links = pull_awin_deeplinks()
            else:
                links = pull_rakuten_deeplinks()
            save_links_to_db(links, source=f"{src}-{code}")
        except Exception:
            logger.exception("Rotation step failed for %s-%s", src, code)


def schedule_rotation():
    while not _worker_stop.is_set():
        try:
            run_rotation_once()
        except Exception:
            logger.exception("schedule_rotation error")
        time.sleep(PULL_INTERVAL_MINUTES * 60)


# ---------- Flask API ----------
from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route("/")
def home():
    return jsonify({"status": "running", "worker": _worker_thread.is_alive() if _worker_thread else False})

@app.route("/pull")
def pull_now():
    run_rotation_once()
    return jsonify({"status": "ok", "message": "Pulled affiliate links"})

@app.route("/run-once")
def run_once():
    ok = process_next_post()
    return jsonify({"status": "ok", "processed": ok})

@app.route("/settings", methods=["GET", "POST"])
def api_settings():
    if request.method == "GET":
        k = request.args.get("key")
        return jsonify({"key": k, "value": db_get_setting(k)})
    else:
        body = request.json or {}
        k = body.get("key")
        v = body.get("value")
        db_set_setting(k, v)
        return jsonify({"status": "saved", "key": k, "value": v})


# ---------- App start ----------
def start_worker():
    global _worker_thread
    if _worker_thread and _worker_thread.is_alive():
        return
    t = threading.Thread(target=worker_loop, daemon=True)
    t.start()
    _worker_thread = t


def start_rotation_scheduler():
    t = threading.Thread(target=schedule_rotation, daemon=True)
    t.start()


if __name__ == "__main__":
    ensure_tables()
    start_worker()
    start_rotation_scheduler()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
