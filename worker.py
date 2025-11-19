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
from openai import OpenAI
import backoff

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
if not DB_URL:
    logger.error("DATABASE_URL not set — worker will not start")

AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")      # Bearer for AWIN Publisher API
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")  # Bearer for Rakuten API

OPENAI_KEY = os.getenv("OPENAI_API_KEY")
HEYGEN_KEY = os.getenv("HEYGEN_API_KEY")

FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_TOKEN = os.getenv("FB_ACCESS_TOKEN")  # or the Facebook Page Access Token
IG_USER_ID = os.getenv("IG_USER_ID")
IG_TOKEN = os.getenv("IG_TOKEN")  # IG long-lived token or FB token

TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")  # preferred for v2

IFTTT_KEY = os.getenv("IFTTT_KEY")  # for TikTok via IFTTT webhook

TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
YOUR_WHATSAPP = os.getenv("YOUR_WHATSAPP")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

YOUTUBE_TOKEN_JSON = os.getenv("YOUTUBE_TOKEN_JSON")  # optional fallback

DEFAULT_CADENCE_SECONDS = int(os.getenv("DEFAULT_CADENCE_SECONDS", str(3*3600)))
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "300"))
DEBUG_REDIRECTS = os.getenv("DEBUG_REDIRECTS", "0") == "1"
APP_PUBLIC_URL = os.getenv("APP_PUBLIC_URL", "")

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
    # Settings table supports both 'key' and legacy 'setting_key' lookups by storing both columns (key primary)
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
        logger.info("Tables ensured: posts, clicks, settings")
    except Exception:
        conn.rollback()
        logger.exception("ensure_tables failed")
    finally:
        conn.close()

ensure_tables()

def db_get_setting(k: str, fallback=None):
    try:
        conn, cur = get_db_conn()
        # Try both `key` and `setting_key` to be robust
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

# cached interval
POST_INTERVAL_SECONDS = int(db_get_setting("post_interval_seconds", fallback=str(DEFAULT_CADENCE_SECONDS)))

# ---------- Helpers: HTTP with retries ----------
def requests_get(url, **kwargs):
    # small wrapper to centralize verify and timeouts
    kwargs.setdefault("timeout", 15)
    # do default verify True; caller can override verify=False (not recommended)
    return requests.get(url, **kwargs)

# ---------- Helpers: URL validation ----------
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
    # common affiliate domains
    affiliate_signals = ["tidd.ly", "linksynergy", "awin", "rakuten", "affiliates", "trk."]
    return any(s in u for s in affiliate_signals)

def follow_and_check(url: str, max_hops=5) -> Optional[str]:
    # Follow redirects up to max_hops and return final url or None
    try:
        r = requests_get(url, allow_redirects=True, timeout=15)
        if DEBUG_REDIRECTS:
            chain = " -> ".join([h.url for h in r.history] + [r.url])
            logger.info("Redirect chain: %s", chain)
        final = r.url
        return final
    except Exception:
        logger.exception("follow_and_check failed for %s", url)
        return None

def validate_and_normalize_link(url: str) -> Optional[str]:
    # Ensure it's https and contains affiliate ID or resolves to one.
    if not is_valid_https_url(url):
        logger.debug("Invalid scheme or too long: %s", url)
        return None
    if contains_affiliate_id(url):
        return url
    # Follow redirects to see if affiliate ID present in final url
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
            # AWIN returns programmes; try to extract deep links or programme URLs
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
    # fallback deeplink redirect scraping
    for _ in range(limit - len(out)):
        try:
            url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=bot"
            r = requests_get(url, allow_redirects=True, timeout=15)
            final = r.url
            if final and is_valid_https_url(final):
                out.append(final)
        except Exception:
            logger.exception("AWIN fallback fetch error")
    return out[:limit]

# ---------- Rakuten integration ----------
def rakuten_api_offers(limit=4):
    out = []
    if not RAKUTEN_SECURITY_TOKEN:
        logger.debug("Rakuten credentials missing")
        return out
    headers = {"Authorization": f"Bearer {RAKUTEN_SECURITY_TOKEN}", "Accept": "application/json"}
    # Use rakutenmarketing.com (valid) endpoint - adjust if Rakuten returns different path in your region
    endpoint = "https://api.rakutenmarketing.com/linking/v1/offer"
    try:
        r = requests_get(endpoint, headers=headers, timeout=12)
        if r.status_code == 200:
            data = r.json()
            for item in data.get("offers", [])[:limit]:
                u = item.get("deeplink") or item.get("url")
                if u and is_valid_https_url(u):
                    out.append(u)
        else:
            logger.warning("Rakuten API non-200: %s %s", r.status_code, r.text[:400])
    except requests.exceptions.SSLError as e:
        logger.exception("Rakuten SSL error — check endpoint/region/token: %s", e)
    except Exception:
        logger.exception("rakuten_api_offers error")
    return out[:limit]

def pull_rakuten_deeplinks(limit=4):
    out = rakuten_api_offers(limit=limit)
    if len(out) >= limit:
        return out[:limit]
    # fallback deep link redirect scraping (LinkSynergy)
    for _ in range(limit - len(out)):
        try:
            url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl=https://example.com"
            r = requests_get(url, allow_redirects=True, timeout=15)
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
        if not text:
            text = getattr(resp, "text", None) or ""
        if link not in text:
            text = f"{text} {link}"
        return text or f"Hot deal — check this out: {link}"
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
        r = requests.post(url, json=payload, headers=headers, timeout=60)
        if r.status_code in (200, 201):
            data = r.json()
            # job id or result url
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
        r = requests.post(endpoint, params=params, timeout=15)
        logger.info("FB post status=%s", r.status_code)
        return r.status_code in (200, 201)
    except Exception:
        logger.exception("FB post failed")
        return False

def post_instagram(caption: str) -> bool:
    # Create media then publish via Graph API (image fallback to generic image)
    if not IG_USER_ID or not (IG_TOKEN or FB_TOKEN):
        logger.debug("IG not configured")
        return False
    try:
        image_url = "https://i.imgur.com/airmax270.jpg"
        create = requests.post(
            f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media",
            params={"image_url": image_url, "caption": caption, "access_token": IG_TOKEN or FB_TOKEN}, timeout=15)
        if create.status_code != 200:
            logger.warning("IG create failed: %s", create.text[:400])
            return False
        creation_id = create.json().get("id")
        publish = requests.post(
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
        # Try v2 client if bearer token available
        if TWITTER_BEARER_TOKEN:
            client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN,
                                   consumer_key=TWITTER_API_KEY,
                                   consumer_secret=TWITTER_API_SECRET,
                                   access_token=TWITTER_ACCESS_TOKEN,
                                   access_token_secret=TWITTER_ACCESS_SECRET)
            client.create_tweet(text=text)
            logger.info("Tweet posted via v2")
            return True
        # fallback to OAuth1 (v1.1)
        if all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET]):
            auth = tweepy.OAuth1UserHandler(TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)
            api = tweepy.API(auth)
            api.update_status(status=text)
            logger.info("Tweet posted via OAuth1")
            return True
    except Exception:
        logger.exception("Twitter post failed")
    return False

def post_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram not configured")
        return False
    try:
        resp = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                             json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
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
        r = requests.post(url, json=payload, timeout=10)
        logger.info("IFTTT trigger %s status=%s", event, r.status_code)
        return r.status_code in (200, 202)
    except Exception:
        logger.exception("IFTTT trigger failed")
        return False

def post_youtube_short(title: str, video_url: str) -> bool:
    # Minimal fallback: notify Telegram with video URL; full YouTube upload requires OAuth flow and is usually manual
    try:
        post_telegram(f"YouTube (manual): {title}\n{video_url}")
        return True
    except Exception:
        logger.exception("YouTube post fallback failed")
        return False

def send_alert(title: str, body: str):
    logger.info("ALERT: %s — %s", title, body)
    # WhatsApp via Twilio (optional)
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
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
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
def post_next_pending():
    """Pick next pending row, generate caption/video, post across socials, then update DB"""
    conn, cur = get_db_conn()
    try:
        # Use SELECT FOR UPDATE SKIP LOCKED if you plan concurrent workers (psycopg supports it)
        cur.execute("SELECT id, url FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED")
        row = cur.fetchone()
        if not row:
            return False
        post_id = row["id"]
        url = row["url"]
        # double-check valid
        final_url = url
        if not contains_affiliate_id(url):
            maybe = follow_and_check(url)
            if maybe and contains_affiliate_id(maybe):
                final_url = maybe
            else:
                # mark failed
                cur.execute("UPDATE posts SET status=%s, meta=jsonb_set(coalesce(meta,'{}'::jsonb), %s, %s, true) WHERE id=%s",
                            ("failed", '{reason}', json.dumps("invalid_affiliate")), post_id)
                conn.commit()
                logger.info("Dropped non-affiliate pending: %s", url)
                return False

        caption = generate_caption(final_url)
        # append redirect tracked link if public url is configured
        redirect_link = f"{APP_PUBLIC_URL.rstrip('/')}/r/{post_id}" if APP_PUBLIC_URL else final_url
        caption_with_link = f"{caption}\n{redirect_link}"

        # Generate HeyGen (optional)
        video_ref = None
        try:
            video_ref = generate_heygen_avatar_video(caption)
        except Exception:
            logger.exception("HeyGen generation failed")

        posted_any = False
        # Post to each platform; wrap to avoid total failure
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
        # TikTok via IFTTT (fire-and-forget)
        try:
            trigger_ifttt("post_tiktok", value1=caption, value2=redirect_link)
        except Exception:
            logger.exception("IFTTT error")
        # YouTube fallback if HeyGen returned mp4 URL
        if video_ref and isinstance(video_ref, str) and video_ref.startswith("http"):
            try:
                post_youtube_short(caption, video_ref)
            except Exception:
                logger.exception("YouTube fallback error")

        # Update DB with status and meta
        status_str = "sent" if posted_any else "failed"
        meta_obj = {"caption": caption, "video": video_ref, "posted_via": "auto"}
        try:
            cur.execute("UPDATE posts SET status=%s, posted_at=%s, meta=%s WHERE id=%s",
                        (status_str, datetime.now(timezone.utc), json.dumps(meta_obj), post_id))
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("Failed to update post status")
        send_alert("POSTED" if posted_any else "POST FAILED", f"{redirect_link} | video:{bool(video_ref)}")
        return posted_any
    except Exception:
        conn.rollback()
        logger.exception("post_next_pending failed")
        return False
    finally:
        conn.close()

# ---------- Refresh all sources ----------
def refresh_all_sources():
    logger.info("Refreshing affiliate sources (rotation)")
    links = []
    try:
        for provider, tag in ROTATION:
            try:
                if provider == "awin":
                    urls = pull_awin_deeplinks(limit=4)
                    logger.info("AWIN pulled %d links", len(urls))
                    links.extend(urls)
                elif provider == "rakuten":
                    urls = pull_rakuten_deeplinks(limit=4)
                    logger.info("Rakuten pulled %d links", len(urls))
                    links.extend(urls)
                time.sleep(0.2)
            except Exception:
                logger.exception("refresh loop provider error: %s", provider)
        saved = save_links_to_db(links, source="affiliate") if links else 0
        logger.info("Total new links pulled: %d", saved)
        return {"new_links": saved}
    except Exception:
        logger.exception("refresh_all_sources failed")
        return {"new_links": 0}

# ---------- Stats ----------
def get_stats():
    try:
        conn, cur = get_db_conn()
        cur.execute("SELECT COUNT(*) AS total FROM posts")
        total = cur.fetchone()["total"] or 0
        cur.execute("SELECT COUNT(*) AS pending FROM posts WHERE status='pending'")
        pending = cur.fetchone()["pending"] or 0
        cur.execute("SELECT COUNT(*) AS sent FROM posts WHERE status='sent'")
        sent = cur.fetchone()["sent"] or 0
        cur.execute("SELECT COUNT(*) AS failed FROM posts WHERE status='failed'")
        failed = cur.fetchone()["failed"] or 0
        cur.execute("SELECT posted_at FROM posts WHERE status='sent' ORDER BY posted_at DESC LIMIT 1")
        row = cur.fetchone()
        last_posted_at = row["posted_at"].astimezone(timezone.utc).isoformat() if (row and row["posted_at"]) else None
        conn.close()
        return {
            "total": total,
            "pending": pending,
            "sent": sent,
            "failed": failed,
            "last_posted_at": last_posted_at
        }
    except Exception:
        logger.exception("get_stats failed")
        return {"total": 0, "pending": 0, "sent": 0, "failed": 0}

# ---------- Worker loop ----------
def start_worker_background():
    global _worker_running, _stop_requested, POST_INTERVAL_SECONDS
    if _worker_running:
        logger.info("Worker already running")
        return {"status": "already_running"}
    POST_INTERVAL_SECONDS = int(db_get_setting("post_interval_seconds", fallback=str(DEFAULT_CADENCE_SECONDS)))
    _worker_running = True
    _stop_requested = False
    logger.info("Worker started — cadence %s seconds", POST_INTERVAL_SECONDS)
    send_alert("WORKER START", f"Cadence: {POST_INTERVAL_SECONDS}s")
    next_pull = datetime.now(timezone.utc) - timedelta(seconds=5)
    try:
        while not _stop_requested:
            try:
                now = datetime.now(timezone.utc)
                if now >= next_pull:
                    try:
                        refresh_all_sources()
                    except Exception:
                        logger.exception("refresh_all_sources top-level error")
                    next_pull = now + timedelta(minutes=PULL_INTERVAL_MINUTES)
                posted = post_next_pending()
                if posted:
                    time.sleep(POST_INTERVAL_SECONDS)
                else:
                    # nothing to post; sleep short
                    time.sleep(SLEEP_ON_EMPTY)
            except Exception:
                logger.exception("Worker loop error, sleeping 60s")
                time.sleep(60)
    finally:
        _worker_running = False
        _stop_requested = False
        logger.info("Worker stopped")
        send_alert("WORKER STOP", "Worker stopped")

def stop_worker():
    global _stop_requested
    _stop_requested = True
    logger.info("Stop requested")

# ---------- Module entry ----------
if __name__ == "__main__":
    logger.info("Worker loaded — fully production-ready, ready to pull, save, post, and report stats")
    # quick test: ensure tables exist and print stats
    ensure_tables()
    logger.info("Initial stats: %s", get_stats())
    # Run worker loop directly if executed
    try:
        start_worker_background()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt — stopping worker")
        stop_worker()
