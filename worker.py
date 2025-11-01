# worker.py — v17 FINAL
"""
Full worker:
- Pulls Awin & Rakuten deeplinks (official-style when token provided)
- Saves to Postgres (idempotent)
- Generates captions with OpenAI
- Generates short HeyGen videos (optional)
- Posts to FB/IG/Twitter
- Sends Twilio WhatsApp alerts
- Background loop with refresh and posting intervals
"""

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

# AWIN
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")  # optional: use official AWIN Generate Link API if available

# RAKUTEN
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")  # optional

# Social
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_TOKEN = os.getenv("FB_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
IG_TOKEN = os.getenv("IG_TOKEN")

TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

# AI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
HEYGEN_API_KEY = os.getenv("HEYGEN_API_KEY")

# Twilio
TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
YOUR_WHATSAPP = os.getenv("YOUR_WHATSAPP")

# Scheduling
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "120"))
POST_INTERVAL_SECONDS = int(os.getenv("POST_INTERVAL_SECONDS", "3600"))
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "1800"))

# -----------------------
# Helpers
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
                    logger.warning("Retry %s/%s for %s after error: %s", i+1, times, fn.__name__, e)
                    time.sleep(delay * (i+1))
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
            logger.exception("Twilio alert failed: %s", e)
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
            logger.debug("Insert conflict/error for %s: %s", link, e)
    conn.commit()
    conn.close()
    logger.info("Attempted save %s links from %s (approx added=%s)", len(links), source, added)
    return added

# -----------------------
# AWIN pull (official style if token present)
# -----------------------
@retry(times=3, delay=2)
def pull_awin_deeplinks(limit=5):
    results = []
    if AWIN_API_TOKEN:
        # Attempt official AWIN Link Generator (example flow — adapt if AWIN docs differ)
        try:
            headers = {"Authorization": f"Bearer {AWIN_API_TOKEN}", "Accept": "application/json"}
            # AWIN may provide endpoints to generate links to merchants; placeholder endpoint below:
            endpoint = f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/links"
            # This request body is an example — adjust to AWIN's API contract if needed
            payload = {"limit": limit}
            r = requests.get(endpoint, headers=headers, params=payload, timeout=12)
            if r.ok:
                data = r.json()
                # flatten possible link fields - adapt to actual response keys
                for item in data.get("links", [])[:limit]:
                    url = item.get("url") or item.get("deeplink")
                    if url:
                        results.append(url)
                logger.info("AWIN API pulled %s links", len(results))
                return results
            else:
                logger.warning("AWIN API responded %s: %s", r.status_code, r.text[:200])
        except Exception as e:
            logger.exception("AWIN API error: %s", e)
    # Fallback: use redirect method to produce a publisher deeplink
    if AWIN_PUBLISHER_ID:
        try:
            url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=bot"
            r = requests.get(url, allow_redirects=True, timeout=12)
            final = r.url
            if final:
                results.append(final)
                logger.info("AWIN redirect produced link")
        except Exception as e:
            logger.exception("AWIN redirect error: %s", e)
    return results

# -----------------------
# Rakuten pull (official style if token present)
# -----------------------
@retry(times=3, delay=2)
def pull_rakuten_deeplinks(limit=5):
    results = []
    # If Rakuten has a webservices token, use official endpoints (placeholder example)
    if RAKUTEN_SECURITY_TOKEN:
        try:
            headers = {"Authorization": f"Bearer {RAKUTEN_SECURITY_TOKEN}"}
            # placeholder endpoint (adapt to Rakuten docs if you have a product/offers endpoint)
            endpoint = f"https://api.rakutenadvertising.com/link/v1/products"
            params = {"limit": limit}
            r = requests.get(endpoint, headers=headers, params=params, timeout=12)
            if r.ok:
                data = r.json()
                for item in data.get("items", [])[:limit]:
                    url = item.get("url") or item.get("deepLink")
                    if url:
                        results.append(url)
                logger.info("Rakuten API pulled %s links", len(results))
                return results
            else:
                logger.warning("Rakuten API responded %s: %s", r.status_code, r.text[:200])
        except Exception as e:
            logger.exception("Rakuten API error: %s", e)
    # Fallback: linksynergy redirect
    if RAKUTEN_CLIENT_ID:
        try:
            url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl=https://example.com&afsrc=1"
            r = requests.get(url, allow_redirects=True, timeout=12)
            final = r.url
            if final:
                results.append(final)
                logger.info("Rakuten redirect produced link")
        except Exception as e:
            logger.exception("Rakuten redirect error: %s", e)
    return results

# -----------------------
# HeyGen video generation
# -----------------------
def generate_heygen_video(caption):
    if not HEYGEN_API_KEY:
        return None
    try:
        headers = {"Authorization": f"Bearer {HEYGEN_API_KEY}", "Content-Type": "application/json"}
        payload = {"script": caption, "voice": "en_us_male1", "avatar": "default", "output_format": "mp4"}
        r = requests.post("https://api.heygen.com/v1/video/generate", json=payload, headers=headers, timeout=60)
        if r.ok:
            data = r.json()
            return data.get("video_url") or data.get("result_url")
        else:
            logger.warning("HeyGen response %s: %s", r.status_code, r.text[:200])
    except Exception as e:
        logger.exception("HeyGen error: %s", e)
    return None

# -----------------------
# Posting functions
# -----------------------
def post_facebook(message):
    if not FB_PAGE_ID or not FB_TOKEN:
        logger.debug("Facebook not configured")
        return False
    try:
        url = f"https://graph.facebook.com/v20.0/{FB_PAGE_ID}/feed"
        params = {"access_token": FB_TOKEN, "message": message}
        r = requests.post(url, params=params, timeout=15)
        logger.info("FB post status=%s", r.status_code)
        if r.status_code == 200:
            return True
        logger.warning("FB response: %s", r.text[:200])
    except Exception as e:
        logger.exception("FB error: %s", e)
    return False

def post_instagram(caption):
    if not IG_USER_ID or not IG_TOKEN:
        logger.debug("Instagram not configured")
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
        logger.info("IG publish status=%s", publish_resp.status_code)
        return publish_resp.status_code == 200
    except Exception as e:
        logger.exception("IG error: %s", e)
    return False

def post_twitter(text):
    # prefer v2 client create_tweet if bearer or OAuth2 app provided, else fallback to OAuth1.0a
    try:
        if TWITTER_BEARER_TOKEN:
            # use tweepy.Client.create_tweet
            import tweepy
            client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN,
                                   consumer_key=TWITTER_API_KEY,
                                   consumer_secret=TWITTER_API_SECRET,
                                   access_token=TWITTER_ACCESS_TOKEN,
                                   access_token_secret=TWITTER_ACCESS_SECRET)
            client.create_tweet(text=text)
            logger.info("Tweet posted via Client.create_tweet")
            return True
        else:
            # fallback to OAuth1
            if not all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET]):
                logger.debug("Twitter credentials missing")
                return False
            import tweepy
            auth = tweepy.OAuth1UserHandler(TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)
            api = tweepy.API(auth)
            api.update_status(status=text)
            logger.info("Tweet posted via API.update_status")
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
        prompt = f"Write a short, punchy promotional caption for this affiliate link: {link}. Include 1 emoji and 1 trending hashtag."
        resp = openai.ChatCompletion.create(model="gpt-4o-mini", messages=[{"role":"user", "content": prompt}], max_tokens=60)
        caption = resp.choices[0].message.content.strip()
        return caption
    except Exception as e:
        logger.exception("OpenAI failed: %s", e)
        return f"🔥 Hot deal — grab it now: {link}"

# -----------------------
# High-level flows
# -----------------------
def refresh_all_sources():
    logger.info("Refreshing affiliate sources")
    links = []
    try:
        a = pull_awin_deeplinks(limit=5)
        links += a
    except Exception:
        logger.exception("AWIN pull failed")
    try:
        r = pull_rakuten_deeplinks(limit=5)
        links += r
    except Exception:
        logger.exception("Rakuten pull failed")
    if links:
        saved = save_links_to_db(links, source="affiliate")
    else:
        saved = 0
    send_alert("REFRESH", f"Pulled {len(links)} links, saved approx {saved}")
    return saved

def enqueue_manual_link(url):
    inserted = save_links_to_db([url], source="manual")
    return {"inserted": inserted, "url": url}

def post_next_pending():
    conn, cur = get_db_conn()
    cur.execute("SELECT url FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if not row:
        logger.debug("No pending")
        return False

    url = row["url"]
    caption = generate_caption(url)
    video_url = generate_heygen_video(caption) if HEYGEN_API_KEY else None
    if video_url:
        caption_full = f"{caption}\nWatch: {video_url}\n{url}"
    else:
        caption_full = f"{caption}\n{url}"

    logger.info("Posting %s", url)
    success = False
    try:
        if post_facebook(caption_full):
            success = True
    except Exception:
        logger.exception("FB post error")
    try:
        if post_instagram(caption_full):
            success = True
    except Exception:
        logger.exception("IG post error")
    try:
        if post_twitter(caption + " " + url):
            success = True
    except Exception:
        logger.exception("Twitter post error")

    conn, cur = get_db_conn()
    cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE url=%s", ("sent" if success else "failed", datetime.now(timezone.utc), url))
    conn.commit()
    conn.close()

    if success:
        send_alert("POSTED", f"{url[:120]}")
    else:
        send_alert("POST FAILED", f"{url[:120]}")

    return success

# -----------------------
# Background runner
# -----------------------
_worker_running = False

def start_worker_background():
    global _worker_running
    if _worker_running:
        logger.info("Worker already running")
        return
    if not DB_URL:
        logger.error("DATABASE_URL missing; worker won't start")
        return
    _worker_running = True
    logger.info("Worker starting")
    send_alert("WORKER START", "AutoAffiliate worker started")

    next_pull = datetime.now(timezone.utc) - timedelta(seconds=5)

    while True:
        try:
            now = datetime.now(timezone.utc)
            if now >= next_pull:
                try:
                    refresh_all_sources()
                except Exception:
                    logger.exception("Periodic refresh failed")
                next_pull = now + timedelta(minutes=PULL_INTERVAL_MINUTES)

            posted = post_next_pending()
            if posted:
                logger.info("Posted; sleeping %s seconds", POST_INTERVAL_SECONDS)
                time.sleep(POST_INTERVAL_SECONDS)
            else:
                logger.debug("No post; sleeping %s seconds", SLEEP_ON_EMPTY)
                time.sleep(SLEEP_ON_EMPTY)

        except Exception as e:
            logger.exception("Worker top-level error: %s", e)
            time.sleep(60)

# CLI entry
if __name__ == "__main__":
    start_worker_background()
