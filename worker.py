# worker.py — AutoAffiliate worker (rotation B → 2 → C → 1 → A; OpenAI; HeyGen; social APIs)
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

# Affiliate IDs and tokens
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")    # e.g. 2615532
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")          # optional (for AWIN API)
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")    # e.g. 4599968
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")  # optional for Rakuten API

# Social & API keys
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
IFTTT_KEY = os.getenv("IFTTT_KEY")  # TikTok via IFTTT

TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
YOUR_WHATSAPP = os.getenv("YOUR_WHATSAPP")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Default cadence: every 3 hours (user selected option 2) unless overridden by DB setting
DEFAULT_CADENCE_SECONDS = 3 * 3600

# Pull interval for refresh (minutes)
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "300"))

# redirect debug toggle (set in env to "1" to log full redirect chains)
DEBUG_REDIRECTS = os.getenv("DEBUG_REDIRECTS", "0") == "1"

# OpenAI client (modern)
openai_client = OpenAI(api_key=OPENAI_KEY) if OPENAI_KEY else None

# Worker control flags
_worker_running = False
_stop_requested = False

# Rotation plan: B → 2 → C → 1 → A
# Represented as tuples (provider, tag) where provider in ("awin","rakuten")
ROTATION = [
    ("awin", "B"),
    ("rakuten", "2"),
    ("awin", "C"),
    ("rakuten", "1"),
    ("awin", "A"),
]

def get_db_conn():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# Helper to safely run DDL/DML and rollback on error
def safe_execute(sql, params=None):
    conn, cur = get_db_conn()
    try:
        cur.execute(sql, params or ())
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("safe_execute failed for sql: %s", sql)
    finally:
        conn.close()

# Ensure required tables (idempotent)
def ensure_tables():
    safe_execute("""
    CREATE TABLE IF NOT EXISTS posts (
        id SERIAL PRIMARY KEY,
        url TEXT UNIQUE NOT NULL,
        source TEXT,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMPTZ DEFAULT now(),
        posted_at TIMESTAMPTZ,
        meta JSONB DEFAULT '{}'::jsonb
    );
    """)
    safe_execute("""
    CREATE TABLE IF NOT EXISTS clicks (
        id SERIAL PRIMARY KEY,
        post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
        ip TEXT,
        user_agent TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    """)
    safe_execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """)

ensure_tables()

# Settings helpers
def db_get_setting(key, fallback=None):
    try:
        conn, cur = get_db_conn()
        cur.execute("SELECT value FROM settings WHERE key=%s", (key,))
        row = cur.fetchone()
        conn.close()
        return row["value"] if row else fallback
    except Exception:
        logger.exception("db_get_setting")
        return fallback

def db_set_setting(key, value):
    try:
        conn, cur = get_db_conn()
        cur.execute("INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", (key, str(value)))
        conn.commit(); conn.close()
        return True
    except Exception:
        logger.exception("db_set_setting")
        return False

# cadence seconds (persisted)
POST_INTERVAL_SECONDS = int(db_get_setting("post_interval_seconds", fallback=str(DEFAULT_CADENCE_SECONDS)))
# allow dashboard to set interval by calling worker's db_set_setting via app

def send_alert(title, body):
    logger.info("ALERT: %s — %s", title, body)
    # Twilio WhatsApp
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

# URL validation + affiliate check
def is_valid_https_url(url):
    return bool(url and isinstance(url, str) and url.startswith("https://") and len(url) < 4000)

def contains_affiliate_id(url):
    if not url: return False
    u = url.lower()
    if AWIN_PUBLISHER_ID and str(AWIN_PUBLISHER_ID) in u: return True
    if RAKUTEN_CLIENT_ID and str(RAKUTEN_CLIENT_ID) in u: return True
    return False

def save_links_to_db(links, source="affiliate"):
    if not links:
        return 0
    conn, cur = get_db_conn()
    added = 0
    attempted = len(links)
    for link in links:
        try:
            if not is_valid_https_url(link):
                logger.debug("Reject invalid: %s", link); continue
            allow = contains_affiliate_id(link) or ("tidd.ly" in link.lower()) or ("linksynergy" in link.lower()) or ("awin" in link.lower()) or ("rakuten" in link.lower())
            if not allow:
                logger.debug("Reject non-affiliate: %s", link); continue
            try:
                cur.execute("INSERT INTO posts (url, source, status, created_at) VALUES (%s,%s,'pending',%s) ON CONFLICT (url) DO NOTHING",
                            (link, source, datetime.now(timezone.utc)))
                added += 1
            except Exception:
                # rollback current transaction and continue
                conn.rollback()
                logger.exception("Insert failed for %s", link)
        except Exception:
            logger.exception("save_links_to_db outer error")
    try:
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    conn.close()
    logger.info("Saved %s validated links from %s (attempted %s)", added, source, attempted)
    return added

# AWIN: attempt API first, fallback to redirect deeplink
def awin_api_offers(limit=4):
    out = []
    if not AWIN_API_TOKEN:
        return out
    try:
        # Example AWIN Publisher API - basic search (this is a template; you may refine with AWIN docs)
        headers = {"Authorization": f"Bearer {AWIN_API_TOKEN}", "Accept":"application/json"}
        # NOTE: endpoint and params depend on AWIN plan — keep conservative search for offers
        endpoint = "https://api.awin.com/publishers/{publisherId}/programmes".replace("{publisherId}", str(AWIN_PUBLISHER_ID))
        r = requests.get(endpoint, headers=headers, timeout=12)
        if r.status_code == 200:
            data = r.json()
            # If AWIN returns programme/product URLs, attempt to extract deeplinks or tracking links
            for p in data[:limit]:
                url = p.get("url") or p.get("deeplink") or p.get("tracking_url")
                if url and is_valid_https_url(url):
                    out.append(url)
        else:
            logger.warning("AWIN API non-200: %s %s", r.status_code, r.text[:300])
    except Exception:
        logger.exception("awin_api_offers error")
    return out

def pull_awin_deeplinks(limit=4):
    out = []
    if not AWIN_PUBLISHER_ID:
        logger.debug("No AWIN_PUBLISHER_ID")
        return out
    # attempt API first
    out += awin_api_offers(limit=limit)
    if len(out) >= limit:
        return out[:limit]
    # fallback: redirect scraping
    for _ in range(limit - len(out)):
        try:
            url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=bot"
            r = requests.get(url, allow_redirects=True, timeout=15)
            final = r.url
            if DEBUG_REDIRECTS:
                logger.info("AWIN redirect chain: %s", " -> ".join([h.url for h in r.history] + [r.url]))
            if final and is_valid_https_url(final):
                out.append(final)
        except Exception:
            logger.exception("AWIN pull error")
    return out

# Rakuten API attempt then redirect fallback
def rakuten_api_offers(limit=4):
    out = []
    if not RAKUTEN_SECURITY_TOKEN:
        return out
    try:
        # Placeholder: Rakuten Advertising API pattern (requires token/auth)
        headers = {"Authorization": f"Bearer {RAKUTEN_SECURITY_TOKEN}", "Accept":"application/json"}
        endpoint = "https://api.rakutenadvertising.com/linking/v1/offer"  # example (adjust to real doc)
        r = requests.get(endpoint, headers=headers, timeout=12)
        if r.status_code == 200:
            data = r.json()
            for item in data.get("offers", [])[:limit]:
                u = item.get("deeplink") or item.get("url")
                if u and is_valid_https_url(u):
                    out.append(u)
        else:
            logger.warning("Rakuten API non-200: %s %s", r.status_code, r.text[:300])
    except Exception:
        logger.exception("rakuten_api_offers error")
    return out

def pull_rakuten_deeplinks(limit=4):
    out = []
    if not RAKUTEN_CLIENT_ID:
        logger.debug("No RAKUTEN_CLIENT_ID")
        return out
    out += rakuten_api_offers(limit=limit)
    if len(out) >= limit:
        return out[:limit]
    for _ in range(limit - len(out)):
        try:
            url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl=https://example.com"
            r = requests.get(url, allow_redirects=True, timeout=15)
            final = r.url
            if DEBUG_REDIRECTS:
                logger.info("Rakuten redirect chain: %s", " -> ".join([h.url for h in r.history] + [r.url]))
            if final and is_valid_https_url(final):
                out.append(final)
        except Exception:
            logger.exception("Rakuten pull error")
    return out

# OpenAI caption generator
def generate_caption(link):
    if not openai_client:
        return f"Hot deal — check this out: {link}"
    try:
        prompt = f"Create a short energetic social caption (one sentence, includes 1 emoji, 1 CTA) for this affiliate link:\n\n{link}"
        # modern usage
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
            text = getattr(resp, "text", None) or ""
        text = text.strip()
        if not text:
            return f"Hot deal — check this out: {link}"
        if link not in text:
            text = f"{text} {link}"
        return text
    except Exception:
        logger.exception("OpenAI caption failed")
        return f"Hot deal — check this out: {link}"

# HeyGen
def generate_heygen_avatar_video(text):
    if not HEYGEN_KEY:
        return None
    try:
        url = "https://api.heygen.com/v1/video/generate"
        headers = {"x-api-key": HEYGEN_KEY, "Content-Type": "application/json"}
        payload = {
            "type": "avatar",
            "script": {"type":"text","input": text},
            "avatar": "default",
            "voice": {"language":"en-US", "style":"energetic"},
            "output_format": "mp4"
        }
        r = requests.post(url, json=payload, headers=headers, timeout=60)
        if r.status_code in (200,201):
            data = r.json()
            return data.get("video_url") or data.get("result_url") or data.get("url") or data.get("job_id")
        logger.warning("HeyGen failed %s %s", r.status_code, r.text[:300])
    except Exception:
        logger.exception("HeyGen error")
    return None

# Social posting helpers
def post_facebook(message):
    if not FB_PAGE_ID or not FB_TOKEN:
        logger.debug("FB not configured")
        return False
    try:
        endpoint = f"https://graph.facebook.com/v17.0/{FB_PAGE_ID}/feed"
        params = {"access_token": FB_TOKEN, "message": message}
        r = requests.post(endpoint, params=params, timeout=15)
        logger.info("FB post status=%s", r.status_code)
        if r.status_code in (200,201):
            return True
        logger.warning("FB response: %s", r.text[:400])
    except Exception:
        logger.exception("FB post failed")
    return False

def post_instagram(caption):
    if not IG_USER_ID or not IG_TOKEN:
        logger.debug("IG not configured")
        return False
    try:
        image_url = "https://i.imgur.com/airmax270.jpg"
        create = requests.post(f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media",
                               params={"image_url": image_url, "caption": caption, "access_token": IG_TOKEN}, timeout=15)
        if create.status_code != 200:
            logger.warning("IG create failed: %s", create.text[:300]); return False
        creation_id = create.json().get("id")
        publish = requests.post(f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media_publish",
                                params={"creation_id": creation_id, "access_token": IG_TOKEN}, timeout=15)
        logger.info("IG publish status=%s", publish.status_code)
        return publish.status_code in (200,201)
    except Exception:
        logger.exception("IG post failed")
    return False

def post_twitter(text):
    try:
        import tweepy
        if TWITTER_BEARER_TOKEN:
            client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN,
                                   consumer_key=TWITTER_API_KEY,
                                   consumer_secret=TWITTER_API_SECRET,
                                   access_token=TWITTER_ACCESS_TOKEN,
                                   access_token_secret=TWITTER_ACCESS_SECRET)
            client.create_tweet(text=text)
            logger.info("Tweet posted via v2")
            return True
        else:
            if not all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET]):
                logger.debug("Twitter creds missing"); return False
            auth = tweepy.OAuth1UserHandler(TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)
            api = tweepy.API(auth)
            api.update_status(status=text)
            logger.info("Tweet posted via OAuth1")
            return True
    except Exception:
        logger.exception("Twitter error")
    return False

def post_telegram(text):
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

def trigger_ifttt(event, value1=None, value2=None, value3=None):
    if not IFTTT_KEY:
        logger.debug("IFTTT not configured")
        return False
    url = f"https://maker.ifttt.com/trigger/{event}/with/key/{IFTTT_KEY}"
    payload = {}
    if value1: payload["value1"] = value1
    if value2: payload["value2"] = value2
    if value3: payload["value3"] = value3
    try:
        r = requests.post(url, json=payload, timeout=8)
        logger.info("IFTTT status=%s", r.status_code)
        return r.status_code in (200,202)
    except Exception:
        logger.exception("IFTTT failed")
    return False

# YouTube fallback
def post_youtube_short(title, video_url):
    if not YOUTUBE_TOKEN_JSON:
        logger.debug("YouTube not configured")
        return False
    try:
        post_telegram(f"YouTube (manual): {title}\n{video_url}")
        return True
    except Exception:
        logger.exception("YouTube fallback failed")
    return False

# Enqueue manual link
def enqueue_manual_link(url):
    if not is_valid_https_url(url):
        raise ValueError("URL must be HTTPS")
    return {"inserted": save_links_to_db([url], source="manual"), "url": url}

# debug helper: fetch redirect chain without following to final (log chain)
def fetch_and_log_redirects(url, timeout=15):
    try:
        r = requests.get(url, allow_redirects=True, timeout=timeout)
        chain = [h.url for h in r.history] + [r.url]
        logger.info("Redirect chain for %s: %s", url, " -> ".join(chain))
        return r.url, chain
    except Exception:
        logger.exception("fetch_and_log_redirects failed")
        return url, []

# Posting pipeline: post the next pending item
def post_next_pending():
    conn, cur = get_db_conn()
    try:
        cur.execute("SELECT id, url FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED")
        row = cur.fetchone()
    except Exception:
        conn.rollback()
        cur = conn.cursor()
        row = None
    conn.close()
    if not row:
        logger.debug("No pending posts")
        return False
    post_id = row["id"]; url = row["url"]
    # ensure https
    if not is_valid_https_url(url):
        logger.warning("Invalid pending; marking failed: %s", url)
        conn, cur = get_db_conn()
        cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE id=%s", ("failed", datetime.now(timezone.utc), post_id))
        conn.commit(); conn.close()
        return False
    # capture redirect chain (debug flag)
    if DEBUG_REDIRECTS:
        final_url, chain = fetch_and_log_redirects(url)
    else:
        final_url = url

    caption = generate_caption(final_url)
    public = os.getenv("APP_PUBLIC_URL") or os.getenv("PUBLIC_URL") or ""
    redirect_link = f"{public.rstrip('/')}/r/{post_id}" if public else url
    caption_with_link = f"{caption}\n{redirect_link}"

    # generate HeyGen video if available (async handled by HeyGen itself)
    video_ref = generate_heygen_avatar_video(caption) if HEYGEN_KEY else None
    video_host_url = video_ref if (video_ref and isinstance(video_ref, str) and video_ref.startswith("http")) else None

    success = False
    # Post to FB/IG/X and Telegram
    try:
        if post_facebook(caption_with_link): success = True
    except Exception:
        logger.exception("FB error")
    try:
        if post_instagram(caption_with_link): success = True
    except Exception:
        logger.exception("IG error")
    try:
        if post_twitter(caption + " " + redirect_link): success = True
    except Exception:
        logger.exception("Twitter error")
    try:
        if post_telegram(caption_with_link): success = True
    except Exception:
        logger.exception("Telegram error")
    # TikTok via IFTTT (fire-and-forget) - only IFTTT
    try:
        trigger_ifttt("Post_TikTok", value1=caption, value2=redirect_link)
    except Exception:
        logger.exception("IFTTT error")
    # YouTube fallback: if we have a HeyGen-hosted mp4 URL (or else skip)
    if video_host_url:
        try:
            post_youtube_short(caption, video_host_url)
        except Exception:
            logger.exception("YouTube post failed")

    # Save post result
    conn, cur = get_db_conn()
    try:
        cur.execute("UPDATE posts SET status=%s, posted_at=%s, meta=jsonb_set(coalesce(meta,'{}'::jsonb), %s, %s, true) WHERE id=%s",
                    ("sent" if success else "failed", datetime.now(timezone.utc), '{posted_via}', ('"auto"' ), post_id))
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Failed to update post status for %s", post_id)
    finally:
        conn.close()

    send_alert("POSTED" if success else "POST FAILED", f"{redirect_link} | vid:{bool(video_host_url)}")
    return success

# Main: refresh all affiliate sources (picked rotation — alternate providers by rotation)
def refresh_all_sources():
    logger.info("Refreshing affiliate sources (rotation mode)")
    links = []
    try:
        # build a small batch by following the rotation order and pulling one from each slot
        for provider, tag in ROTATION:
            if provider == "awin":
                # pull 1 deeplink for AWIN slot
                links += pull_awin_deeplinks(limit=1)
            elif provider == "rakuten":
                links += pull_rakuten_deeplinks(limit=1)
            time.sleep(0.25)  # be gentle
    except Exception:
        logger.exception("refresh_all_sources outer failure")
    saved = save_links_to_db(links, source="affiliate") if links else 0
    send_alert("REFRESH", f"Pulled {len(links)} links, saved {saved}")
    return saved

# Stats
def get_stats():
    s = {"total":0,"pending":0,"sent":0,"failed":0,"last_posted_at":None,"clicks_total":0}
    try:
        conn, cur = get_db_conn()
        cur.execute("SELECT COUNT(*) as c FROM posts")
        s["total"] = cur.fetchone()["c"] or 0
        cur.execute("SELECT COUNT(*) as c FROM posts WHERE status='pending'")
        s["pending"] = cur.fetchone()["c"] or 0
        cur.execute("SELECT COUNT(*) as c FROM posts WHERE status='sent'")
        s["sent"] = cur.fetchone()["c"] or 0
        cur.execute("SELECT COUNT(*) as c FROM posts WHERE status='failed'")
        s["failed"] = cur.fetchone()["c"] or 0
        cur.execute("SELECT posted_at FROM posts WHERE status='sent' ORDER BY posted_at DESC LIMIT 1")
        row = cur.fetchone()
        if row and row["posted_at"]:
            s["last_posted_at"] = row["posted_at"].astimezone(timezone.utc).isoformat()
        cur.execute("SELECT COUNT(*) as c FROM clicks")
        s["clicks_total"] = cur.fetchone()["c"] or 0
        conn.close()
    except Exception:
        logger.exception("get_stats failed")
    return s

# Worker loop control
def start_worker_background():
    global _worker_running, _stop_requested, POST_INTERVAL_SECONDS
    if _worker_running:
        logger.info("Worker already running")
        return
    if not DB_URL:
        logger.error("DATABASE_URL missing; not starting worker")
        return
    # refresh cadence from DB (if changed via dashboard)
    POST_INTERVAL_SECONDS = int(db_get_setting("post_interval_seconds", fallback=str(DEFAULT_CADENCE_SECONDS)))
    _worker_running = True
    _stop_requested = False
    logger.info("Worker starting — cadence: %s seconds", POST_INTERVAL_SECONDS)
    send_alert("WORKER START", "AutoAffiliate worker started")
    next_pull = datetime.now(timezone.utc) - timedelta(seconds=5)
    try:
        while not _stop_requested:
            try:
                now = datetime.now(timezone.utc)
                if now >= next_pull:
                    try:
                        refresh_all_sources()
                    except Exception:
                        logger.exception("refresh_all_sources failed")
                    next_pull = now + timedelta(minutes=PULL_INTERVAL_MINUTES)
                posted = post_next_pending()
                if posted:
                    time.sleep(POST_INTERVAL_SECONDS)
                else:
                    time.sleep(SLEEP_ON_EMPTY)
            except Exception:
                logger.exception("Worker top-level error, sleeping 60s")
                time.sleep(60)
    finally:
        _worker_running = False
        _stop_requested = False
        logger.info("Worker stopped")
        send_alert("WORKER STOPPED", "AutoAffiliate worker stopped")

def stop_worker():
    global _stop_requested
    logger.info("Stop requested")
    _stop_requested = True

if __name__ == "__main__":
    # run worker loop when executed directly
    start_worker_background()
