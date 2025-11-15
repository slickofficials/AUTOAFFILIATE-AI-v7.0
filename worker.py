# worker.py — AutoAffiliate AI v7.0 (multi-provider + social + captions + video + shorts)

import os
import time
import logging
import json
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone
from threading import Thread

# Optional: Tweepy for X/Twitter posting if installed
try:
    import tweepy
except Exception:
    tweepy = None

# Optional: OpenAI client (captions)
try:
    from openai import OpenAI
except Exception:
    OpenAI = None

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

# Environment / credentials
DB_URL = os.getenv("DATABASE_URL")

# Affiliate networks
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")
AWIN_MID = os.getenv("AWIN_MID")  # optional merchant id

RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")  # LinkShare affiliate id
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")
RAKUTEN_WEBSERVICES_TOKEN = os.getenv("RAKUTEN_WEBSERVICES_TOKEN")
RAKUTEN_MID = os.getenv("RAKUTEN_MID")  # optional merchant id

# Social & APIs
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
IG_ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN") or FB_ACCESS_TOKEN
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
IFTTT_KEY = os.getenv("IFTTT_KEY")
TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
WHATSAPP_NUMBER = os.getenv("WHATSAPP_NUMBER")

# Content generation
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
HEYGEN_API_KEY = os.getenv("HEYGEN_API_KEY")

# YouTube Shorts
YOUTUBE_TOKEN_JSON = os.getenv("YOUTUBE_TOKEN_JSON")  # should contain {"access_token": "..."}
YOUTUBE_CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID")

# Misc
PUBLIC_URL = os.getenv("PUBLIC_URL") or os.getenv("APP_PUBLIC_URL") or ""
DEBUG_REDIRECTS = os.getenv("DEBUG_LOG_REDIRECTS", "0") == "1"

# Timing
DEFAULT_CADENCE_SECONDS = int(os.getenv("POST_INTERVAL_SECONDS", "10800"))  # default 3 hours
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "300"))
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))

# Rotation (provider, tag)
ROTATION = [
    ("rakuten", "2"),
    ("awin", "B"),
    ("rakuten", "1"),
    ("awin", "C"),
    ("rakuten", "A"),
]

# Worker state
_worker_running = False
_stop_requested = False

# ---------------------------
# DB helpers and tables
# ---------------------------
def get_db_conn():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

def ensure_tables():
    conn, cur = get_db_conn()
    try:
        cur.execute("""
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
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clicks (
            id SERIAL PRIMARY KEY,
            post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
            ip TEXT,
            user_agent TEXT,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """)
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("ensure_tables failed")
    finally:
        conn.close()

ensure_tables()

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
        cur.execute("""
            INSERT INTO settings(key,value)
            VALUES(%s,%s)
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
        """, (key, str(value)))
        conn.commit()
        conn.close()
        return True
    except Exception:
        logger.exception("db_set_setting")
        return False

try:
    POST_INTERVAL_SECONDS = int(db_get_setting("post_interval_seconds", fallback=str(DEFAULT_CADENCE_SECONDS)))
except Exception:
    POST_INTERVAL_SECONDS = DEFAULT_CADENCE_SECONDS

# ---------------------------
# Alerts
# ---------------------------
def send_alert(title, body):
    logger.info("ALERT: %s — %s", title, body)
    # Telegram
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": f"{title}\n{body}"},
                timeout=10
            )
        except Exception:
            logger.exception("Telegram alert failed")
    # Twilio WhatsApp
    if TWILIO_SID and TWILIO_TOKEN and WHATSAPP_NUMBER:
        try:
            from twilio.rest import Client
            client = Client(TWILIO_SID, TWILIO_TOKEN)
            client.messages.create(
                from_='whatsapp:+14155238886',
                to=f"whatsapp:{WHATSAPP_NUMBER}",
                body=f"*{title}*\n{body}"
            )
        except Exception:
            logger.exception("Twilio WhatsApp alert failed")

# ---------------------------
# Deeplink builders
# ---------------------------
def build_rakuten_deeplink(destination_url, clickref="auto"):
    """
    Classic LinkShare format (valid affiliate link):
    https://click.linksynergy.com/deeplink?id={affiliate_id}&mid={merchant_id}&murl={encoded_destination}&u1={clickref}
    Requires RAKUTEN_CLIENT_ID and optionally RAKUTEN_MID.
    """
    if not RAKUTEN_CLIENT_ID:
        raise RuntimeError("RAKUTEN_CLIENT_ID missing")
    mid = RAKUTEN_MID or ""
    import urllib.parse
    murl = urllib.parse.quote(destination_url, safe="")
    base = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}"
    if mid:
        base += f"&mid={mid}"
    link = f"{base}&murl={murl}&u1={urllib.parse.quote(clickref, safe='')}"
    return link

def build_awin_deeplink(destination_url, clickref="auto"):
    """
    AWIN format (valid affiliate link):
    https://www.awin1.com/cread.php?{awinmid?}&awinaffid={publisher}&clickref={ref}&ued={encoded_destination}
    Requires AWIN_PUBLISHER_ID and optionally AWIN_MID.
    """
    if not AWIN_PUBLISHER_ID:
        raise RuntimeError("AWIN_PUBLISHER_ID missing")
    mid = AWIN_MID or ""
    import urllib.parse
    ued = urllib.parse.quote(destination_url, safe="")
    ref = urllib.parse.quote(clickref, safe="")
    params = f"awinaffid={AWIN_PUBLISHER_ID}&clickref={ref}&ued={ued}"
    if mid:
        params = f"awinmid={mid}&" + params
    return f"https://www.awin1.com/cread.php?{params}"

# ---------------------------
# Enqueue + status helpers
# ---------------------------
def enqueue_manual_link(url, source="manual", meta=None):
    conn, cur = get_db_conn()
    try:
        cur.execute("""
            INSERT INTO posts (url, source, status, created_at, meta)
            VALUES (%s,%s,'pending',now(),%s)
            ON CONFLICT (url) DO NOTHING
        """, (url, source, meta or {}))
        conn.commit()
        return {"url": url, "source": source}
    except Exception:
        conn.rollback()
        logger.exception("enqueue_manual_link failed")
        raise
    finally:
        conn.close()

def mark_post_sent(url):
    conn, cur = get_db_conn()
    try:
        cur.execute("""
            UPDATE posts SET status='sent', posted_at=now() WHERE url=%s
        """, (url,))
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("mark_post_sent failed")
    finally:
        conn.close()

# ---------------------------
# Captions (OpenAI)
# ---------------------------
def generate_caption(dest_url, provider="deal"):
    """Returns a short social caption; falls back gracefully if OpenAI is unavailable."""
    base = f"🔥 New {provider} — Tap to shop: {dest_url}"
    if not OPENAI_API_KEY or OpenAI is None:
        return base
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": f"Write a catchy 1-2 sentence social caption with 3 trending hashtags for this product page:\n{dest_url}"}],
            temperature=0.7,
            max_tokens=80,
        )
        text = (resp.choices[0].message.content or "").strip()
        return text or base
    except Exception:
        logger.exception("OpenAI caption failed")
        return base

# ---------------------------
# HeyGen video (avatar)
# ---------------------------
def generate_heygen_video(caption):
    """
    Submits a short avatar video job to HeyGen and returns a downloadable URL if available.
    This assumes HeyGen API key in HEYGEN_API_KEY. Replace endpoint/params with your HeyGen plan specifics.
    """
    if not HEYGEN_API_KEY:
        return None
    try:
        # Submit job
        submit = requests.post(
            "https://api.heygen.com/v1/video.generate",
            headers={"Authorization": f"Bearer {HEYGEN_API_KEY}", "Content-Type": "application/json"},
            json={"script": caption, "voice": "Ada", "avatar": "Default", "ratio": "9:16", "style": "friendly"},
            timeout=20
        )
        if submit.status_code not in (200, 201):
            logger.error("HeyGen submit failed: %s %s", submit.status_code, submit.text)
            return None
        job_id = submit.json().get("id") or submit.json().get("job_id")
        if not job_id:
            return None

        # Poll status
        for _ in range(20):
            time.sleep(6)
            status = requests.get(
                f"https://api.heygen.com/v1/video.status?id={job_id}",
                headers={"Authorization": f"Bearer {HEYGEN_API_KEY}"},
                timeout=15
            )
            if status.status_code != 200:
                continue
            data = status.json()
            if data.get("state") in ("completed","done") and data.get("video_url"):
                return data["video_url"]
            if data.get("state") in ("failed","error"):
                logger.error("HeyGen job failed: %s", data)
                return None
        return None
    except Exception:
        logger.exception("HeyGen video generation failed")
        return None

# ---------------------------
# YouTube Shorts upload
# ---------------------------
def post_to_youtube_shorts(video_url, title, description="#shorts"):
    """
    Uploads a short video to YouTube using an access_token in YOUTUBE_TOKEN_JSON.
    This expects YOUTUBE_TOKEN_JSON to contain {"access_token": "..."}.
    Downloads the video_url and uploads it to the channel.
    """
    if not (YOUTUBE_TOKEN_JSON and YOUTUBE_CHANNEL_ID and video_url):
        return False
    try:
        token_data = json.loads(YOUTUBE_TOKEN_JSON)
        access_token = token_data.get("access_token")
        if not access_token:
            logger.error("YouTube access_token missing in YOUTUBE_TOKEN_JSON")
            return False

        # Download video
        vid = requests.get(video_url, timeout=60)
        if vid.status_code != 200:
            logger.error("Video download failed for YouTube: %s", vid.status_code)
            return False

        # Resumable upload init
        init = requests.post(
            "https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status",
            headers={"Authorization": f"Bearer {access_token}", "X-Upload-Content-Type": "video/mp4"},
            json={
                "snippet": {"title": title, "description": description, "channelId": YOUTUBE_CHANNEL_ID},
                "status": {"privacyStatus": "public", "selfDeclaredMadeForKids": False}
            },
            timeout=30
        )
        if init.status_code not in (200, 201):
            logger.error("YouTube init failed: %s %s", init.status_code, init.text)
            return False
        upload_url = init.headers.get("Location")
        if not upload_url:
            logger.error("YouTube upload URL missing")
            return False

        # Upload binary
        up = requests.put(upload_url, data=vid.content, headers={"Content-Type": "video/mp4"}, timeout=120)
        ok = up.status_code in (200, 201)
        if not ok:
            logger.error("YouTube upload failed: %s %s", up.status_code, up.text)
        return ok
    except Exception:
        logger.exception("YouTube Shorts upload failed")
        return False

# ---------------------------
# Provider refreshers
# ---------------------------
def refresh_rakuten():
    """
    Build Rakuten deeplinks for a set of destination URLs; replace with your real feed/source.
    """
    sample_urls = [
        "https://www.example-merchant.com/product-1",
        "https://www.example-merchant.com/product-2",
    ]
    saved = 0
    for i, dest in enumerate(sample_urls, start=1):
        try:
            link = build_rakuten_deeplink(dest, clickref=f"rk-{i}-{int(time.time())}")
            enqueue_manual_link(link, source="rakuten", meta={"dest": dest})
            saved += 1
        except Exception:
            logger.exception("refresh_rakuten item failed")
    return saved

def refresh_awin():
    """
    Build AWIN deeplinks for a set of destination URLs; replace with your AWIN feed/source.
    """
    sample_urls = [
        "https://www.awin-merchant.com/product-A",
        "https://www.awin-merchant.com/product-B",
    ]
    saved = 0
    for i, dest in enumerate(sample_urls, start=1):
        try:
            link = build_awin_deeplink(dest, clickref=f"aw-{i}-{int(time.time())}")
            enqueue_manual_link(link, source="awin", meta={"dest": dest})
            saved += 1
        except Exception:
            logger.exception("refresh_awin item failed")
    return saved

def refresh_tiktok():
    """
    Trigger IFTTT webhook for TikTok (optional).
    """
    if not IFTTT_KEY:
        return 0
    try:
        r = requests.post(f"https://maker.ifttt.com/trigger/autoaffiliate/with/key/{IFTTT_KEY}", timeout=10)
        if r.status_code in (200, 202):
            return 1
        logger.error("IFTTT TikTok webhook failed: %s %s", r.status_code, r.text)
        return 0
    except Exception:
        logger.exception("refresh_tiktok failed")
        return 0

def refresh_all_sources():
    saved = 0
    try:
        saved += refresh_rakuten()
    except Exception:
        logger.exception("Rakuten refresh failed")
    try:
        saved += refresh_awin()
    except Exception:
        logger.exception("AWIN refresh failed")
    try:
        saved += refresh_tiktok()
    except Exception:
        logger.exception("TikTok refresh failed")
    send_alert("Refresh complete", f"Saved {saved} links")
    return saved

# ---------------------------
# Social posting
# ---------------------------
def post_to_facebook(link, message):
    if not (FB_PAGE_ID and FB_ACCESS_TOKEN):
        return False
    try:
        resp = requests.post(
            f"https://graph.facebook.com/{FB_PAGE_ID}/feed",
            data={"message": message, "link": link, "access_token": FB_ACCESS_TOKEN},
            timeout=20
        )
        ok = resp.status_code in (200, 201)
        if not ok:
            logger.error("Facebook post failed: %s %s", resp.status_code, resp.text)
        return ok
    except Exception:
        logger.exception("Facebook post error")
        return False

def post_to_instagram(link, caption):
    # IG posting requires media create + publish; this posts caption (with link) as media without image (if allowed).
    if not (IG_USER_ID and IG_ACCESS_TOKEN):
        return False
    try:
        create = requests.post(
            f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media",
            data={"caption": f"{caption}\n{link}", "access_token": IG_ACCESS_TOKEN},
            timeout=20
        )
        if create.status_code not in (200, 201):
            logger.error("Instagram media create failed: %s %s", create.status_code, create.text)
            return False
        media_id = create.json().get("id")
        pub = requests.post(
            f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media_publish",
            data={"creation_id": media_id, "access_token": IG_ACCESS_TOKEN},
            timeout=20
        )
        ok = pub.status_code in (200, 201)
        if not ok:
            logger.error("Instagram publish failed: %s %s", pub.status_code, pub.text)
        return ok
    except Exception:
        logger.exception("Instagram post error")
        return False

def post_to_twitter(link, text):
    # Prefer Tweepy if installed and OAuth1 tokens provided
    if tweepy and TWITTER_API_KEY and TWITTER_API_SECRET and TWITTER_ACCESS_TOKEN and TWITTER_ACCESS_SECRET:
        try:
            auth = tweepy.OAuth1UserHandler(
                TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET
            )
            api = tweepy.API(auth)
            api.update_status(f"{text} {link}")
            return True
        except Exception:
            logger.exception("Twitter post error (tweepy)")
            return False
    logger.warning("Twitter posting skipped (tweepy missing or tokens incomplete)")
    return False

def post_to_telegram(link, text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": f"{text}\n{link}"},
            timeout=10
        )
        return resp.status_code in (200, 201)
    except Exception:
        logger.exception("Telegram post error")
        return False

def post_everywhere(link, caption, video_url=None):
    results = {
        "facebook": post_to_facebook(link, caption),
        "instagram": post_to_instagram(link, caption),
        "twitter": post_to_twitter(link, caption),
        "telegram": post_to_telegram(link, caption),
    }
    logger.info("Social posting results: %s", results)

    # Optional YouTube Shorts if a video is available
    if video_url and YOUTUBE_TOKEN_JSON and YOUTUBE_CHANNEL_ID:
        yt_ok = post_to_youtube_shorts(video_url, title=caption[:90], description=f"{caption}\n#shorts")
        results["youtube_shorts"] = yt_ok
        logger.info("YouTube Shorts upload: %s", yt_ok)

    return results

# ---------------------------
# Worker loop
# ---------------------------
def worker_loop():
    global _worker_running, _stop_requested
    _worker_running = True
    send_alert("Worker started", f"Interval: {POST_INTERVAL_SECONDS}s")
    while not _stop_requested:
        try:
            for provider, tag in ROTATION:
                if _stop_requested:
                    break

                # Destination URLs — replace with your real feed or per-tag mapping
                dest = "https://www.example-merchant.com/product" if provider == "rakuten" else "https://www.example-awin-merchant.com/deal"

                # Generate deeplink
                if provider == "rakuten":
                    link = build_rakuten_deeplink(dest, clickref=f"rk-{tag}-{int(time.time())}")
                    source = f"rakuten-{tag}"
                elif provider == "awin":
                    link = build_awin_deeplink(dest, clickref=f"aw-{tag}-{int(time.time())}")
                    source = f"awin-{tag}"
                else:
                    logger.info("Unknown provider in rotation: %s", provider)
                    continue

                # Enqueue
                enqueue_manual_link(link, source=source, meta={"tag": tag, "dest": dest})

                # Caption
                caption = generate_caption(dest_url=dest, provider=provider)

                # Optional: HeyGen video from caption
                video_url = None
                if HEYGEN_API_KEY:
                    video_url = generate_heygen_video(caption)

                # Post to socials (+ Shorts if video)
                post_everywhere(link, caption, video_url=video_url)

                # Mark as sent
                mark_post_sent(link)

                # Sleep until next rotation step
                time.sleep(POST_INTERVAL_SECONDS)
        except Exception:
            logger.exception("Worker loop error")
            time.sleep(SLEEP_ON_EMPTY)
    _worker_running = False
    send_alert("Worker stopped", "Loop exited")

def start_worker_background():
    global _stop_requested
    _stop_requested = False
    Thread(target=worker_loop, daemon=True).start()

def stop_worker():
    global _stop_requested
    _stop_requested = True
