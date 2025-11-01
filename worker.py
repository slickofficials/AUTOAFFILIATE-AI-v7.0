# worker.py — v16 | auto-pull deep links (Awin + Rakuten) + posting + captions
import os
import time
import logging
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone, timedelta
from twilio.rest import Client

# Optional: for retries
from functools import wraps

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

# ENV (set these in Render)
DB_URL = os.getenv("DATABASE_URL")  # postgres connection string
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")  # publisher id (if used)
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")  # if needed for API
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")
RAKUTEN_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")  # depends on API
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # optional
TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
YOUR_WHATSAPP = os.getenv("YOUR_WHATSAPP")

# Social tokens (FB/IG/Twitter) — used in posting functions
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_TOKEN = os.getenv("FB_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
IG_TOKEN = os.getenv("IG_TOKEN")
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")

# Scheduler config
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "120"))  # pull every 2 hours by default
POST_INTERVAL_SECONDS = int(os.getenv("POST_INTERVAL_SECONDS", "3600"))  # post once per hour by default

# Twilio client for alerts
twilio_client = Client(TWILIO_SID, TWILIO_TOKEN) if TWILIO_SID and TWILIO_TOKEN else None

# ----------------------------
# Helpers
# ----------------------------
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
    if twilio_client and YOUR_WHATSAPP:
        try:
            twilio_client.messages.create(from_='whatsapp:+14155238886', body=msg, to=YOUR_WHATSAPP)
            logger.info("WhatsApp alert sent")
        except Exception as e:
            logger.exception("Failed sending WhatsApp alert: %s", e)

# ----------------------------
# Database helpers & schema expectations
# posts table must exist:
# CREATE TABLE posts (
#   id SERIAL PRIMARY KEY,
#   url TEXT UNIQUE NOT NULL,
#   source TEXT,
#   status TEXT DEFAULT 'pending',
#   created_at TIMESTAMPTZ DEFAULT NOW(),
#   posted_at TIMESTAMPTZ
# );
# ----------------------------
@retry(times=3, delay=1)
def save_links_to_db(links, source="affiliate"):
    if not links:
        return 0
    conn, cur = get_db_conn()
    inserted = 0
    for link in links:
        try:
            cur.execute(
                "INSERT INTO posts (url, source, status, created_at) VALUES (%s, %s, 'pending', %s) ON CONFLICT (url) DO NOTHING",
                (link, source, datetime.now(timezone.utc))
            )
            # count affected rows via rowcount if driver supports it; psycopg rowcount after insert returns -1 sometimes
            inserted += 1
        except Exception as e:
            logger.debug("Insert conflict or error for %s: %s", link, e)
    conn.commit()
    conn.close()
    logger.info("Saved %s links to DB (source=%s)", len(links), source)
    return inserted

# ----------------------------
# Affiliate API pulls
# ----------------------------
@retry(times=3, delay=2)
def pull_awin_deeplinks():
    """
    Pull sample deeplink from AWIN. Replace with actual AWIN API endpoints as required by your contract.
    This function attempts to use AWIN redirect to produce an affiliate deeplink.
    """
    links = []
    if not AWIN_PUBLISHER_ID:
        logger.info("AWIN_PUBLISHER_ID not set — skipping AWIN pull")
        return links
    try:
        # Example: AWIN redirect method — adapt to your publisher token / docs
        url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=bot"
        resp = requests.get(url, allow_redirects=True, timeout=10)
        final = resp.url
        logger.debug("AWIN pulled url: %s (status=%s)", final, resp.status_code)
        if final and "tidd.ly" in final or final.startswith("http"):
            links.append(final)
    except Exception as e:
        logger.exception("AWIN pull failed: %s", e)
    return links

@retry(times=3, delay=2)
def pull_rakuten_deeplinks():
    """
    Pull sample Rakuten deeplink. Replace with your Rakuten format.
    """
    links = []
    if not RAKUTEN_CLIENT_ID:
        logger.info("Rakuten client id not set — skipping Rakuten pull")
        return links
    try:
        # Example placeholder flow — adapt to your specific Rakuten deeplink URL pattern
        url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl=https://example.com&afsrc=1"
        resp = requests.get(url, allow_redirects=True, timeout=10)
        final = resp.url
        logger.debug("Rakuten pulled url: %s (status=%s)", final, resp.status_code)
        if final:
            links.append(final)
    except Exception as e:
        logger.exception("Rakuten pull failed: %s", e)
    return links

# ----------------------------
# Posting functions (FB/IG/Twitter)
# ----------------------------
def post_facebook(message):
    if not FB_PAGE_ID or not FB_TOKEN:
        logger.debug("FB credentials not set")
        return False
    try:
        url = f"https://graph.facebook.com/v20.0/{FB_PAGE_ID}/feed"
        params = {"access_token": FB_TOKEN, "message": message}
        r = requests.post(url, params=params, timeout=15)
        logger.info("FB post status %s", r.status_code)
        if r.status_code == 200:
            return True
        logger.warning("FB response: %s", r.text)
    except Exception as e:
        logger.exception("FB post failed: %s", e)
    return False

def post_instagram(caption):
    if not IG_USER_ID or not IG_TOKEN:
        logger.debug("IG credentials not set")
        return False
    try:
        image_url = "https://i.imgur.com/airmax270.jpg"
        create_resp = requests.post(f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media",
                                    params={"image_url": image_url, "caption": caption, "access_token": IG_TOKEN}, timeout=15)
        if create_resp.status_code != 200:
            logger.warning("IG create failed: %s", create_resp.text)
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
        logger.debug("Twitter credentials not set")
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

# ----------------------------
# Caption generation (OpenAI)
# ----------------------------
def generate_caption(link):
    if not OPENAI_API_KEY:
        return f"🔥 Hot deal — grab it now: {link}"
    try:
        import openai
        openai.api_key = OPENAI_API_KEY
        prompt = f"Write a short promotional caption (1-2 lines) for this affiliate link: {link}. Add 1 emoji and 1 hashtag."
        res = openai.ChatCompletion.create(model="gpt-4o-mini", messages=[{"role":"user","content":prompt}], max_tokens=60)
        caption = res.choices[0].message.content.strip()
        return caption
    except Exception as e:
        logger.exception("OpenAI caption failed: %s", e)
        return f"🔥 Hot deal — grab it now: {link}"

# ----------------------------
# High-level flows
# ----------------------------
def refresh_all_sources():
    """Pull from all affiliate sources and save to DB."""
    logger.info("Refreshing affiliate sources...")
    links = []
    links += pull_awin_deeplinks()
    links += pull_rakuten_deeplinks()
    saved = save_links_to_db(links, source="affiliate")
    send_alert("REFRESH", f"Pulled {len(links)} links, saved ~{saved} new rows")
    return saved

def enqueue_manual_link(url):
    """Insert and return a pseudo-job id (for compatibility)"""
    inserted = save_links_to_db([url], source="manual")
    return {"inserted": inserted, "url": url}

def post_next_pending():
    """Pick a pending post, generate caption, post to channels, update DB."""
    conn, cur = get_db_conn()
    cur.execute("SELECT url FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if not row:
        logger.debug("No pending rows")
        return False

    url = row["url"]
    caption = generate_caption(url)
    logger.info("Posting URL %s with caption %s", url, caption)

    success = any([
        post_facebook(caption + "\n" + url),
        post_instagram(caption + "\n" + url),
        post_twitter(caption + " " + url)
    ])

    conn, cur = get_db_conn()
    cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE url=%s", ("sent" if success else "failed", datetime.now(timezone.utc), url))
    conn.commit()
    conn.close()

    if success:
        send_alert("POSTED", f"{url[:80]}")
    else:
        send_alert("FAILED POST", f"{url[:80]}")

    return success

# ----------------------------
# Background runner (entrypoint)
# ----------------------------
_worker_running = False

def start_worker_background():
    global _worker_running
    if _worker_running:
        logger.info("Worker already running — ignoring start.")
        return
    _worker_running = True
    logger.info("Worker background loop starting.")
    # Run a loop: refresh affiliate links on interval, post on interval
    next_pull = datetime.now(timezone.utc)
    while True:
        try:
            now = datetime.now(timezone.utc)
            if now >= next_pull:
                try:
                    refresh_all_sources()
                except Exception as e:
                    logger.exception("Periodic refresh failed: %s", e)
                next_pull = now + timedelta(minutes=PULL_INTERVAL_MINUTES)

            # attempt a single post (if any)
            try:
                posted = post_next_pending()
                if posted:
                    # if posted, wait full post interval
                    logger.info("Posted something — sleeping %s seconds", POST_INTERVAL_SECONDS)
                    time.sleep(POST_INTERVAL_SECONDS)
                else:
                    # no post — sleep shorter and loop
                    time.sleep(600)
            except Exception as e:
                logger.exception("Posting loop error: %s", e)
                time.sleep(120)

        except Exception as e:
            logger.exception("Worker top-level loop failed: %s", e)
            time.sleep(60)
