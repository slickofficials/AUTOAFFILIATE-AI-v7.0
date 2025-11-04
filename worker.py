# worker.py — AutoAffiliate worker (hourly posts, deep link pulls, OpenAI captions, HeyGen avatar)
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

# Affiliate IDs
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")

# API keys & tokens
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
IFTTT_KEY = os.getenv("IFTTT_KEY")  # for TikTok webhook

TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
YOUR_WHATSAPP = os.getenv("YOUR_WHATSAPP")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Defaults, but can be overridden in DB.settings
POST_INTERVAL_SECONDS_DEFAULT = int(os.getenv("POST_INTERVAL_SECONDS", "3600"))
PULL_INTERVAL_MINUTES_DEFAULT = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))
SLEEP_ON_EMPTY_DEFAULT = int(os.getenv("SLEEP_ON_EMPTY", "300"))

DEBUG_REDIRECTS = os.getenv("DEBUG_REDIRECTS", "false").lower() in ("1","true","yes")

# OpenAI client (modern)
openai_client = OpenAI(api_key=OPENAI_KEY) if OPENAI_KEY else None

# Worker control flags
_worker_running = False
_stop_requested = False

def get_db_conn():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

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
    return bool(url and isinstance(url, str) and url.startswith("https://") and len(url) < 3000)

def contains_affiliate_id(url):
    if not url: return False
    u = url.lower()
    if AWIN_PUBLISHER_ID and str(AWIN_PUBLISHER_ID) in u: return True
    if RAKUTEN_CLIENT_ID and str(RAKUTEN_CLIENT_ID) in u: return True
    return False

def fetch_redirect_chain(url, timeout=12):
    """
    Follow redirects and return list of (status_code, location) and final_url.
    Uses HEAD first for speed, falls back to GET if needed.
    """
    chain = []
    final = url
    try:
        # try HEAD first
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        final = r.url
        if r.history:
            for h in r.history:
                chain.append((h.status_code, h.headers.get("location") or h.url))
            chain.append((r.status_code, r.url))
        else:
            chain.append((r.status_code, r.url))
        return chain, final
    except Exception:
        # fallback to GET (some endpoints block HEAD)
        try:
            r = requests.get(url, allow_redirects=True, timeout=timeout)
            final = r.url
            if r.history:
                for h in r.history:
                    chain.append((h.status_code, h.headers.get("location") or h.url))
                chain.append((r.status_code, r.url))
            else:
                chain.append((r.status_code, r.url))
            return chain, final
        except Exception:
            # return minimal info
            return chain, url

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
            # attempt to fetch redirect chain and final url to validate affiliate id
            chain, final = fetch_redirect_chain(link)
            if DEBUG_REDIRECTS:
                logger.debug("Redirect chain for %s: %s", link, chain)
            allow = contains_affiliate_id(final) or contains_affiliate_id(link)
            # also allow known redirect hosts
            if not allow:
                low = (final or "").lower()
                if any(x in low for x in ("tidd.ly","linksynergy","awin","rakuten","click.linksynergy")):
                    allow = True
            if not allow:
                logger.debug("Reject non-affiliate final: %s (final=%s)", link, final); continue
            cur.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    id SERIAL PRIMARY KEY,
                    url TEXT UNIQUE,
                    source TEXT,
                    status TEXT,
                    created_at TIMESTAMP WITH TIME ZONE,
                    posted_at TIMESTAMP WITH TIME ZONE
                )
            """)
            # optional audit table for redirects
            cur.execute("""
                CREATE TABLE IF NOT EXISTS post_redirects (
                    id SERIAL PRIMARY KEY,
                    post_url TEXT,
                    redirect_chain TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
                )
            """)
            cur.execute("INSERT INTO posts (url, source, status, created_at) VALUES (%s,%s,'pending',%s) ON CONFLICT (url) DO NOTHING",
                        (link, source, datetime.now(timezone.utc)))
            # insert redirect chain audit
            if DEBUG_REDIRECTS and chain:
                try:
                    chain_str = " | ".join([f"{st}:{loc}" for st, loc in chain])
                    cur.execute("INSERT INTO post_redirects (post_url, redirect_chain) VALUES (%s,%s)", (link, chain_str))
                except Exception:
                    logger.exception("redirect audit failed")
            added += 1
        except Exception:
            logger.exception("Insert failed for %s", link)
    conn.commit(); conn.close()
    logger.info("Saved %s validated links from %s (attempted %s)", added, source, attempted)
    return added

# AWIN pulls via redirect endpoint (preferred if publisher id)
def pull_awin_deeplinks(limit=4):
    out = []
    if not AWIN_PUBLISHER_ID:
        logger.debug("No AWIN_PUBLISHER_ID")
        return out
    for _ in range(limit):
        try:
            # redairect approach — this reliably produces a deeplink with publisher info in chain
            url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=bot"
            r = requests.get(url, allow_redirects=True, timeout=12)
            final = r.url
            chain = [(h.status_code, h.headers.get("location") or h.url) for h in r.history] + [(r.status_code, r.url)]
            logger.debug("AWIN chain: %s", chain)
            if final and is_valid_https_url(final):
                out.append(final)
        except Exception:
            logger.exception("AWIN pull error")
    return out

# Rakuten pulls via LinkShare/LinkSynergy redirect
def pull_rakuten_deeplinks(limit=4):
    out = []
    if not RAKUTEN_CLIENT_ID:
        logger.debug("No RAKUTEN_CLIENT_ID")
        return out
    for _ in range(limit):
        try:
            url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl=https://example.com"
            r = requests.get(url, allow_redirects=True, timeout=12)
            final = r.url
            chain = [(h.status_code, h.headers.get("location") or h.url) for h in r.history] + [(r.status_code, r.url)]
            logger.debug("Rakuten chain: %s", chain)
            if final and is_valid_https_url(final):
                out.append(final)
        except Exception:
            logger.exception("Rakuten pull error")
    return out

# OpenAI caption generator (modern client)
def generate_caption(link):
    if not openai_client:
        return f"Hot deal — check this out: {link}"
    try:
        prompt = f"Create a short energetic social caption (one sentence, includes 1 emoji, 1 CTA) for this affiliate link:\n\n{link}"
        # modern client usage (OpenAI Python >=1.0)
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content": prompt}],
            max_tokens=60
        )
        # extract text robustly
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

# HeyGen talking avatar (create job; may return url or job id)
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

# Social posting helpers (FB/IG/Twitter/Telegram/YouTube/IFTTT)
def post_facebook(message):
    if not FB_PAGE_ID or not FB_TOKEN:
        logger.debug("FB not configured")
        return False
    try:
        endpoint = f"https://graph.facebook.com/v17.0/{FB_PAGE_ID}/feed"
        params = {"access_token": FB_TOKEN, "message": message}
        r = requests.post(endpoint, params=params, timeout=15)
        logger.info("FB post status=%s", r.status_code)
        if r.status_code == 200:
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
        return publish.status_code == 200
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

# YouTube: placeholder upload flow (requires proper google oauth client + token file)
def post_youtube_short(title, video_url):
    if not YOUTUBE_TOKEN_JSON:
        logger.debug("YouTube not configured")
        return False
    # Implement full youtube upload/shorts logic if you want direct uploads.
    # For now we send the video url as a Telegram/FB message as fallback.
    try:
        post_telegram(f"YouTube (manual): {title}\n{video_url}")
        return True
    except Exception:
        logger.exception("YouTube fallback failed")
    return False

# Enqueue manual link (API helper)
def enqueue_manual_link(url):
    if not is_valid_https_url(url):
        raise ValueError("URL must be HTTPS")
    inserted = save_links_to_db([url], source="manual")
    return {"inserted": inserted, "url": url}

# Posting pipeline — now checks redirect chain/final for affiliate IDs, logs debug
def post_next_pending():
    conn, cur = get_db_conn()
    cur.execute("SELECT id, url FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if not row:
        logger.debug("No pending posts")
        return False
    post_id = row["id"]; url = row["url"]

    # get final redirect chain and ensure final contains affiliate id or known redirect host
    chain, final = fetch_redirect_chain(url)
    if DEBUG_REDIRECTS:
        logger.info("Pending post %s redirect chain: %s", url, chain)
    if not contains_affiliate_id(final) and not contains_affiliate_id(url):
        low = (final or "").lower()
        if not any(x in low for x in ("tidd.ly","linksynergy","awin","rakuten","click.linksynergy")):
            logger.warning("Rejecting pending post: final does not include affiliate id: %s -> %s", url, final)
            conn, cur = get_db_conn()
            cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE id=%s", ("failed", datetime.now(timezone.utc), post_id))
            conn.commit(); conn.close()
            return False

    # Quick validation — ensure https
    if not is_valid_https_url(final):
        logger.warning("Invalid final URL; marking failed: %s", final)
        conn, cur = get_db_conn()
        cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE id=%s", ("failed", datetime.now(timezone.utc), post_id))
        conn.commit(); conn.close()
        return False

    caption = generate_caption(final)
    # build tracking link if APP_PUBLIC_URL set
    public = os.getenv("APP_PUBLIC_URL") or os.getenv("PUBLIC_URL") or ""
    redirect_link = f"{public.rstrip('/')}/r/{post_id}" if public else final
    caption_with_link = f"{caption}\n{redirect_link}"

    # generate HeyGen video job
    video_ref = generate_heygen_avatar_video(caption) if HEYGEN_KEY else None
    video_host_url = video_ref if (video_ref and isinstance(video_ref, str) and video_ref.startswith("http")) else None

    success = False
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
    # TikTok via IFTTT (fire-and-forget)
    try:
        trigger_ifttt("Post_TikTok", value1=caption, value2=redirect_link)
    except Exception:
        logger.exception("IFTTT error")
    # YouTube shorts fallback (attempt)
    if video_host_url:
        try:
            post_youtube_short(caption, video_host_url)
        except Exception:
            logger.exception("YouTube post failed")

    # Update DB status
    conn, cur = get_db_conn()
    cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE id=%s", ("sent" if success else "failed", datetime.now(timezone.utc), post_id))
    conn.commit(); conn.close()
    send_alert("POSTED" if success else "POST FAILED", f"{redirect_link} | vid:{bool(video_host_url)}")
    return success

# refresh all sources and save
def refresh_all_sources():
    logger.info("Refreshing affiliate sources")
    links = []
    try:
        links += pull_awin_deeplinks(limit=4)
    except Exception:
        logger.exception("AWIN refresh error")
    try:
        links += pull_rakuten_deeplinks(limit=4)
    except Exception:
        logger.exception("Rakuten refresh error")
    saved = save_links_to_db(links, source="affiliate") if links else 0
    send_alert("REFRESH", f"Pulled {len(links)} links, saved {saved}")
    return saved

# Stats (for app)
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

# Start / stop — worker reads interval setting from DB.settings; fallback to env defaults
def _read_intervals_from_db():
    try:
        conn, cur = get_db_conn()
        cur.execute("CREATE TABLE IF NOT EXISTS settings (name TEXT PRIMARY KEY, value TEXT)")
        cur.execute("SELECT value FROM settings WHERE name='post_interval_seconds' LIMIT 1")
        r = cur.fetchone()
        post_interval = int(r["value"]) if r and r["value"] else POST_INTERVAL_SECONDS_DEFAULT
        cur.execute("SELECT value FROM settings WHERE name='pull_interval_minutes' LIMIT 1")
        r2 = cur.fetchone()
        pull_interval = int(r2["value"]) if r2 and r2["value"] else PULL_INTERVAL_MINUTES_DEFAULT
        cur.execute("SELECT value FROM settings WHERE name='sleep_on_empty' LIMIT 1")
        r3 = cur.fetchone()
        sleep_empty = int(r3["value"]) if r3 and r3["value"] else SLEEP_ON_EMPTY_DEFAULT
        conn.close()
        return post_interval, pull_interval, sleep_empty
    except Exception:
        logger.exception("read intervals failed; using defaults")
        return POST_INTERVAL_SECONDS_DEFAULT, PULL_INTERVAL_MINUTES_DEFAULT, SLEEP_ON_EMPTY_DEFAULT

def start_worker_background():
    global _worker_running, _stop_requested
    if _worker_running:
        logger.info("Worker already running")
        return
    if not DB_URL:
        logger.error("DATABASE_URL missing; not starting worker")
        return
    _worker_running = True
    _stop_requested = False
    logger.info("Worker starting")
    send_alert("WORKER START", "AutoAffiliate worker started")
    # initial intervals
    post_interval, pull_interval, sleep_on_empty = _read_intervals_from_db()
    next_pull = datetime.now(timezone.utc) - timedelta(seconds=5)
    try:
        while not _stop_requested:
            try:
                # read possibly-updated intervals each loop
                post_interval, pull_interval, sleep_on_empty = _read_intervals_from_db()
                now = datetime.now(timezone.utc)
                if now >= next_pull:
                    try:
                        refresh_all_sources()
                    except Exception:
                        logger.exception("refresh_all_sources failed")
                    next_pull = now + timedelta(minutes=int(pull_interval))
                posted = post_next_pending()
                if posted:
                    time.sleep(int(post_interval))
                else:
                    time.sleep(int(sleep_on_empty))
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
    start_worker_background()
