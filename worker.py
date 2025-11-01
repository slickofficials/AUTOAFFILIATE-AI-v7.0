# worker.py — v16.1 FINAL
import os
import time
import logging
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone, timedelta
from functools import wraps

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

# Environment
DB_URL = os.getenv("DATABASE_URL")

# Awin / Rakuten
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")          # numeric publisher id (for redirect method)
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")                # if you have AWIN API token (not required for redirect)
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")  # optional

# Social tokens
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_TOKEN = os.getenv("FB_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
IG_TOKEN = os.getenv("IG_TOKEN")
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")

# AI & HeyGen
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
HEYGEN_API_KEY = os.getenv("HEYGEN_API_KEY")

# Twilio alerts
TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
YOUR_WHATSAPP = os.getenv("YOUR_WHATSAPP")

# Scheduling
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "120"))
POST_INTERVAL_SECONDS = int(os.getenv("POST_INTERVAL_SECONDS", "3600"))
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "1800"))

# -----------------------
# Utilities
# -----------------------
def retry(times=3, delay=2):
    def deco(fn):
        @wraps(fn)
        def wrapper(*a, **k):
            last = None
            for i in range(times):
                try:
                    return fn(*a, **k)
                except Exception as e:
                    last = e
                    logger.warning("Retry %s/%s for %s after error: %s", i + 1, times, fn.__name__, e)
                    time.sleep(delay * (i + 1))
            logger.exception("Function %s failed after %s attempts", fn.__name__, times)
            raise last
        return wrapper
    return deco

def get_db_conn():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not configured")
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

def send_alert(title, body):
    msg = f"*{title}*\n{body}\nTime: {datetime.now(timezone.utc).astimezone().isoformat()}"
    logger.info("ALERT: %s", title)
    if TWILIO_SID and TWILIO_TOKEN and YOUR_WHATSAPP:
        try:
            from twilio.rest import Client
            client = Client(TWILIO_SID, TWILIO_TOKEN)
            client.messages.create(from_="whatsapp:+14155238886", body=msg, to=YOUR_WHATSAPP)
            logger.info("WhatsApp alert sent")
        except Exception as e:
            logger.exception("Failed sending Twilio alert: %s", e)
    else:
        logger.debug("Twilio not configured; alert payload: %s", msg)

# -----------------------
# DB write (idempotent)
# -----------------------
@retry(times=3, delay=1)
def save_links_to_db(links, source="affiliate"):
    if not links:
        return 0
    conn, cur = get_db_conn()
    added = 0
    for link in links:
        try:
            cur.execute(
                "INSERT INTO posts (url, source, status, created_at) VALUES (%s, %s, 'pending', %s) ON CONFLICT (url) DO NOTHING",
                (link, source, datetime.now(timezone.utc))
            )
            added += 1
        except Exception as e:
            logger.debug("Insert error for %s: %s", link, e)
    conn.commit()
    conn.close()
    logger.info("Attempted to save %s links from %s (approx added=%s)", len(links), source, added)
    return added

# -----------------------
# Awin deeplink pull
# -----------------------
@retry(times=3, delay=2)
def pull_awin_deeplinks():
    """
    Preferred: If you have AWIN API tokens and official endpoints, replace this function
    with the official generate-link endpoint per AWIN docs. As a universal fallback,
    the 'cread.php' redirect produces a publisher deeplink which we capture via requests.
    """
    results = []
    if not AWIN_PUBLISHER_ID:
        logger.debug("AWIN_PUBLISHER_ID not set — skipping")
        return results
    try:
        url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=bot"
        r = requests.get(url, allow_redirects=True, timeout=12)
        final = r.url
        logger.debug("AWIN pull final=%s status=%s", final, r.status_code)
        if final:
            results.append(final)
    except Exception as e:
        logger.exception("AWIN pull error: %s", e)
    return results

# -----------------------
# Rakuten deeplink pull
# -----------------------
@retry(times=3, delay=2)
def pull_rakuten_deeplinks():
    results = []
    if not RAKUTEN_CLIENT_ID:
        logger.debug("RAKUTEN_CLIENT_ID not set — skipping Rakuten")
        return results
    try:
        url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl=https://example.com&afsrc=1"
        r = requests.get(url, allow_redirects=True, timeout=12)
        final = r.url
        logger.debug("Rakuten pull final=%s status=%s", final, r.status_code)
        if final:
            results.append(final)
    except Exception as e:
        logger.exception("Rakuten pull error: %s", e)
    return results

# -----------------------
# HeyGen optional video generation
# -----------------------
def generate_heygen_video(caption):
    """Return video URL or None. Requires HEYGEN_API_KEY in env."""
    if not HEYGEN_API_KEY:
        return None
    try:
        headers = {"Authorization": f"Bearer {HEYGEN_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "script": caption,
            "voice": "en_us_male1",
            "avatar": "default",
            "output_format": "mp4"
        }
        r = requests.post("https://api.heygen.com/v1/video/generate", json=payload, headers=headers, timeout=60)
        if r.status_code == 200:
            data = r.json()
            return data.get("video_url") or data.get("result_url")
        logger.warning("HeyGen response %s: %s", r.status_code, r.text[:200])
    except Exception as e:
        logger.exception("HeyGen error: %s", e)
    return None

# -----------------------
# Posting functions
# -----------------------
def post_facebook(message):
    if not FB_PAGE_ID or not FB_TOKEN:
        return False
    try:
        url = f"https://graph.facebook.com/v20.0/{FB_PAGE_ID}/feed"
        params = {"access_token": FB_TOKEN, "message": message}
        r = requests.post(url, params=params, timeout=15)
        logger.info("FB response %s", r.status_code)
        return r.status_code == 200
    except Exception as e:
        logger.exception("FB post failed: %s", e)
        return False

def post_instagram(caption):
    if not IG_USER_ID or not IG_TOKEN:
        return False
    try:
        image_url = "https://i.imgur.com/airmax270.jpg"
        create_resp = requests.post(f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media",
                                    params={"image_url": image_url, "caption": caption, "access_token": IG_TOKEN}, timeout=15)
        if create_resp.status_code != 200:
            logger.warning("IG create failed: %s", create_resp.text[:200])
            return False
        creation_id = create_resp.json().get("id")
        publish_resp = requests.post(f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish",
                                     params={"creation_id": creation_id, "access_token": IG_TOKEN}, timeout=15)
        logger.info("IG publish status %s", publish_resp.status_code)
        return publish_resp.status_code == 200
    except Exception as e:
        logger.exception("IG post failed: %s", e)
        return False

def post_twitter(text):
    if not all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET]):
        return False
    try:
        import tweepy
        auth = tweepy.OAuth1UserHandler(TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)
        api = tweepy.API(auth)
        api.update_status(text)
        logger.info("Tweet posted")
        return True
    except Exception as e:
        logger.exception("Twitter post failed: %s", e)
        return False

# -----------------------
# OpenAI captioning
# -----------------------
def generate_caption(link):
    if not OPENAI_API_KEY:
        return f"🔥 Hot deal — grab it now: {link}"
    try:
        import openai
        openai.api_key = OPENAI_API_KEY
        prompt = f"Write a short promotional caption (1-2 lines) with an emoji and one hashtag for this affiliate link: {link}"
        # ChatCompletion if available
        resp = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=60
        )
        caption = resp.choices[0].message.content.strip()
        return caption
    except Exception as e:
        logger.exception("OpenAI caption generation failed: %s", e)
        return f"🔥 Hot deal — grab it now: {link}"

# -----------------------
# High-level flows
# -----------------------
def refresh_all_sources():
    logger.info("Refreshing affiliate sources (Awin + Rakuten)")
    links = []
    try:
        links += pull_awin_deeplinks()
    except Exception:
        logger.exception("AWIN pull failed")
    try:
        links += pull_rakuten_deeplinks()
    except Exception:
        logger.exception("Rakuten pull failed")
    if links:
        saved = save_links_to_db(links, source="affiliate")
    else:
        saved = 0
    send_alert("REFRESH", f"Pulled {len(links)} links, saved approx {saved}")
    return saved

def enqueue_manual_link(url):
    added = save_links_to_db([url], source="manual")
    return {"inserted": added, "url": url}

def post_next_pending():
    conn, cur = get_db_conn()
    cur.execute("SELECT url FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if not row:
        logger.debug("No pending rows")
        return False

    url = row["url"]
    caption = generate_caption(url)
    # optional HeyGen video
    video_url = generate_heygen_video(caption) if HEYGEN_API_KEY else None
    if video_url:
        caption_with_video = f"{caption}\nWatch: {video_url}\n{url}"
    else:
        caption_with_video = f"{caption}\n{url}"

    logger.info("Posting URL %s with caption %s", url, caption[:80])

    success = any([
        post_facebook(caption_with_video),
        post_instagram(caption_with_video),
        post_twitter(caption + " " + url)
    ])

    # update DB
    conn, cur = get_db_conn()
    cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE url=%s",
                ("sent" if success else "failed", datetime.now(timezone.utc), url))
    conn.commit()
    conn.close()

    if success:
        send_alert("POSTED", f"{url[:100]}")
    else:
        send_alert("POST FAILED", f"{url[:100]}")
    return success

# -----------------------
# Background loop
# -----------------------
_worker_running = False

def start_worker_background():
    global _worker_running
    if _worker_running:
        logger.info("Worker already running — ignoring start.")
        return
    if not DB_URL:
        logger.error("DATABASE_URL not set — worker will not start")
        return
    _worker_running = True
    logger.info("Worker background loop starting.")
    send_alert("WORKER START", "AutoAffiliate worker started")

    next_pull = datetime.now(timezone.utc) - timedelta(seconds=5)

    while True:
        try:
            now = datetime.now(timezone.utc)
            if now >= next_pull:
                try:
                    refresh_all_sources()
                except Exception as e:
                    logger.exception("Periodic refresh failed: %s", e)
                next_pull = now + timedelta(minutes=PULL_INTERVAL_MINUTES)

            posted = False
            try:
                posted = post_next_pending()
            except Exception as e:
                logger.exception("Posting loop error: %s", e)

            if posted:
                logger.info("Posted an item — sleeping %s seconds", POST_INTERVAL_SECONDS)
                time.sleep(POST_INTERVAL_SECONDS)
            else:
                logger.debug("No posts — sleeping %s seconds", SLEEP_ON_EMPTY)
                time.sleep(SLEEP_ON_EMPTY)

        except Exception as e:
            logger.exception("Worker top-level error: %s", e)
            time.sleep(60)

# CLI entry
if __name__ == "__main__":
    start_worker_background()
