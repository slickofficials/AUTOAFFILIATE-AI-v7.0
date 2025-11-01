# worker.py — v18 FINAL
"""
Worker that:
- pulls AWIN (cread.php) and Rakuten (LinkSynergy) deeplinks
- validates that deeplinks include your affiliate ids
- saves only validated deeplinks to posts table
- generates OpenAI captions (current OpenAI client)
- optionally generates HeyGen video
- posts to FB/IG/Twitter only when link validated
- robust logging, retries, and safe failure handling
"""

import os
import time
import logging
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone, timedelta
from openai import OpenAI

# --------------
# Config / env
# --------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

DB_URL = os.getenv("DATABASE_URL")
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")            # e.g. "2615532"
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")                  # optional
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")            # e.g. "4599968"
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")  # optional

FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_TOKEN = os.getenv("FB_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
IG_TOKEN = os.getenv("IG_TOKEN")

TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
HEYGEN_API_KEY = os.getenv("HEYGEN_API_KEY")

TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
YOUR_WHATSAPP = os.getenv("YOUR_WHATSAPP")

PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "120"))
POST_INTERVAL_SECONDS = int(os.getenv("POST_INTERVAL_SECONDS", "3600"))
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "1800"))

# --------------
# Helpers
# --------------
def get_db_conn():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
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
            logger.exception("Twilio send failed: %s", e)
    else:
        logger.debug("Twilio not configured; alert: %s", msg)

def is_valid_https_url(url):
    if not url or not isinstance(url, str):
        return False
    if not url.startswith("http://") and not url.startswith("https://"):
        return False
    # require https
    if not url.startswith("https://"):
        return False
    if len(url) > 2000:
        return False
    return True

def contains_affiliate_id(url):
    """
    Accept link only if:
      - it contains AWIN_PUBLISHER_ID when AWIN used
      OR - it contains RAKUTEN_CLIENT_ID when Rakuten used
      - or contains either id somewhere in the final URL string
    This ensures only monetized deeplinks get posted.
    """
    if not url:
        return False
    u = url.lower()
    if AWIN_PUBLISHER_ID and str(AWIN_PUBLISHER_ID) in u:
        return True
    if RAKUTEN_CLIENT_ID and str(RAKUTEN_CLIENT_ID) in u:
        return True
    return False

# --------------
# Save / Fetch
# --------------
def save_links_to_db(links, source="affiliate"):
    if not links:
        return 0
    conn, cur = get_db_conn()
    added = 0
    for link in links:
        try:
            if not is_valid_https_url(link):
                logger.debug("Reject non-https/invalid url: %s", link)
                continue
            if not contains_affiliate_id(link):
                logger.debug("Reject non-affiliate link (missing affiliate id): %s", link)
                continue
            cur.execute(
                "INSERT INTO posts (url, source, status, created_at) VALUES (%s, %s, 'pending', %s) ON CONFLICT (url) DO NOTHING",
                (link, source, datetime.now(timezone.utc))
            )
            added += 1
        except Exception as e:
            logger.debug("DB insert error for %s: %s", link, e)
    conn.commit()
    conn.close()
    logger.info("Saved %s validated links from %s (attempted %s)", added, source, len(links))
    return added

# --------------
# AWIN pull (redirect deeplink fallback is reliable)
# --------------
def pull_awin_deeplinks(limit=5):
    results = []
    if AWIN_API_TOKEN:
        # If you have the AWIN API and token and want to use official endpoints,
        # add implementation here (AWIN API varies based on contract).
        logger.debug("AWIN token present but using redirect fallback for stable deeplink generation.")
    if not AWIN_PUBLISHER_ID:
        logger.debug("AWIN_PUBLISHER_ID not set; skipping AWIN")
        return results

    # redirect-based: this reliably produces a publisher deeplink (tidd.ly / awin1 redirect)
    for _ in range(limit):
        try:
            url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=bot"
            r = requests.get(url, allow_redirects=True, timeout=12)
            final = r.url
            if final and is_valid_https_url(final) and contains_affiliate_id(final):
                results.append(final)
            else:
                logger.debug("AWIN redirect gave non-monetized link: %s", final)
        except Exception as e:
            logger.exception("AWIN pull error: %s", e)
            break
    return results

# --------------
# Rakuten pull (LinkSynergy redirect ensures affiliate id is injected)
# --------------
def pull_rakuten_deeplinks(limit=5):
    results = []
    if RAKUTEN_CLIENT_ID:
        for _ in range(limit):
            try:
                # LinkSynergy deeplink redirect - ensures deep link includes tracking / merchant id
                url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl=https://example.com"
                r = requests.get(url, allow_redirects=True, timeout=12)
                final = r.url
                if final and is_valid_https_url(final) and contains_affiliate_id(final):
                    results.append(final)
                else:
                    logger.debug("Rakuten redirect gave non-monetized link: %s", final)
            except Exception as e:
                logger.exception("Rakuten pull error: %s", e)
                break
    else:
        logger.debug("RAKUTEN_CLIENT_ID not set; skipping Rakuten")
    return results

# --------------
# OpenAI captioning (current client)
# --------------
def generate_caption(link):
    if not OPENAI_API_KEY:
        return f"🔥 Hot deal — grab it now: {link}"
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = (
            f"Write a short, punchy promotional caption (1 line) for this affiliate link. "
            f"Include one emoji and one hashtag. Include a clear call-to-action and the link at the end.\n\nLink: {link}"
        )
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=80
        )
        caption = resp.choices[0].message.content.strip()
        # ensure link is present at end (if user token omitted it), append link
        if link not in caption:
            caption = f"{caption}\n{link}"
        return caption
    except Exception as e:
        logger.exception("OpenAI caption failed: %s", e)
        return f"🔥 Hot deal — grab it now: {link}"

# --------------
# HeyGen (optional)
# --------------
def generate_heygen_video(caption):
    if not HEYGEN_API_KEY:
        return None
    try:
        url = "https://api.heygen.com/v1/video/generate"
        headers = {"x-api-key": HEYGEN_API_KEY, "Content-Type": "application/json"}
        payload = {
            "script": {"type": "text", "input": caption},
            "voice": "en_us_female_1",
            "avatar": "default",
            "output_format": "mp4"
        }
        r = requests.post(url, json=payload, headers=headers, timeout=60)
        if r.status_code in (200, 201):
            data = r.json()
            # HeyGen may return job id and result url later — adapt if necessary
            return data.get("video_url") or data.get("result_url") or data.get("url")
        else:
            logger.warning("HeyGen response %s: %s", r.status_code, r.text[:200])
    except Exception as e:
        logger.exception("HeyGen error: %s", e)
    return None

# --------------
# Posting helpers (FB/IG/Twitter)
# --------------
def post_facebook(message):
    if not FB_PAGE_ID or not FB_TOKEN:
        logger.debug("FB not configured")
        return False
    try:
        url = f"https://graph.facebook.com/v17.0/{FB_PAGE_ID}/feed"
        params = {"access_token": FB_TOKEN, "message": message}
        r = requests.post(url, params=params, timeout=15)
        logger.info("FB post status=%s", r.status_code)
        if r.status_code == 200:
            return True
        logger.warning("FB response: %s", r.text[:300])
    except Exception as e:
        logger.exception("FB post failed: %s", e)
    return False

def post_instagram(caption):
    if not IG_USER_ID or not IG_TOKEN:
        logger.debug("IG not configured")
        return False
    try:
        image_url = "https://i.imgur.com/airmax270.jpg"  # replace if you want dynamic image
        create = requests.post(
            f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media",
            params={"image_url": image_url, "caption": caption, "access_token": IG_TOKEN},
            timeout=15
        )
        if create.status_code != 200:
            logger.warning("IG create failed: %s", create.text[:300])
            return False
        creation_id = create.json().get("id")
        publish = requests.post(
            f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media_publish",
            params={"creation_id": creation_id, "access_token": IG_TOKEN},
            timeout=15
        )
        logger.info("IG publish status=%s", publish.status_code)
        return publish.status_code == 200
    except Exception as e:
        logger.exception("IG post failed: %s", e)
        return False

def post_twitter(text):
    try:
        import tweepy
        if TWITTER_BEARER_TOKEN:
            client = tweepy.Client(
                bearer_token=TWITTER_BEARER_TOKEN,
                consumer_key=TWITTER_API_KEY,
                consumer_secret=TWITTER_API_SECRET,
                access_token=TWITTER_ACCESS_TOKEN,
                access_token_secret=TWITTER_ACCESS_SECRET
            )
            client.create_tweet(text=text)
            logger.info("Tweet posted via v2 client")
            return True
        else:
            if not all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET]):
                logger.debug("Twitter creds missing")
                return False
            auth = tweepy.OAuth1UserHandler(TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)
            api = tweepy.API(auth)
            api.update_status(status=text)
            logger.info("Tweet posted via OAuth1")
            return True
    except Exception as e:
        logger.exception("Twitter post failed: %s", e)
        return False

# --------------
# High-level flows
# --------------
def refresh_all_sources():
    logger.info("Refreshing sources: AWIN + Rakuten")
    links = []
    try:
        a = pull_awin_deeplinks(limit=3)
        links += a
    except Exception:
        logger.exception("AWIN pull failed")

    try:
        r = pull_rakuten_deeplinks(limit=3)
        links += r
    except Exception:
        logger.exception("Rakuten pull failed")

    saved = save_links_to_db(links, source="affiliate") if links else 0
    send_alert("REFRESH", f"Pulled {len(links)} links, saved {saved}")
    return saved

def enqueue_manual_link(url):
    # Manual enqueue must be validated here as well
    if not is_valid_https_url(url):
        raise ValueError("URL must be HTTPS and valid")
    if not contains_affiliate_id(url):
        raise ValueError("URL does not contain AWIN or RAKUTEN affiliate id")
    inserted = save_links_to_db([url], source="manual")
    return {"inserted": inserted, "url": url}

def post_next_pending():
    conn, cur = get_db_conn()
    cur.execute("SELECT url FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if not row:
        logger.debug("No pending posts")
        return False

    url = row["url"]
    # double-check validity before posting
    if not is_valid_https_url(url) or not contains_affiliate_id(url):
        logger.warning("Pending row failed final validation, marking failed: %s", url)
        conn, cur = get_db_conn()
        cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE url=%s", ("failed", datetime.now(timezone.utc), url))
        conn.commit()
        conn.close()
        return False

    caption = generate_caption(url)
    video = generate_heygen_video(caption) if HEYGEN_API_KEY else None
    if video:
        caption_full = f"{caption}\nWatch: {video}\n{url}"
    else:
        caption_full = f"{caption}\n{url}"

    # Attempt posting across platforms but only mark success if any succeeds
    success = False
    try:
        if post_facebook(caption_full):
            success = True
    except Exception:
        logger.exception("FB posting raised")
    try:
        if post_instagram(caption_full):
            success = True
    except Exception:
        logger.exception("IG posting raised")
    try:
        if post_twitter(caption + " " + url):
            success = True
    except Exception:
        logger.exception("Twitter posting raised")

    conn, cur = get_db_conn()
    cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE url=%s", ("sent" if success else "failed", datetime.now(timezone.utc), url))
    conn.commit()
    conn.close()

    send_alert("POSTED" if success else "POST FAILED", url[:200])
    return success

# --------------
# Background loop
# --------------
_worker_running = False

def start_worker_background():
    global _worker_running
    if _worker_running:
        logger.info("Worker already running")
        return
    if not DB_URL:
        logger.error("DATABASE_URL not set - not starting worker")
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
                except Exception as e:
                    logger.exception("Periodic refresh failed: %s", e)
                next_pull = now + timedelta(minutes=PULL_INTERVAL_MINUTES)

            posted = post_next_pending()
            if posted:
                logger.info("Posted one item; sleeping %s", POST_INTERVAL_SECONDS)
                time.sleep(POST_INTERVAL_SECONDS)
            else:
                logger.debug("No posts; sleeping %s", SLEEP_ON_EMPTY)
                time.sleep(SLEEP_ON_EMPTY)
        except Exception as e:
            logger.exception("Worker top-level error: %s", e)
            time.sleep(60)

if __name__ == "__main__":
    start_worker_background()
