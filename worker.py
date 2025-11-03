# worker.py — AutoAffiliate worker (hourly posts, deeplink pulls, OpenAI captions, HeyGen avatar)
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

# === CONFIG / ENV ===
DB_URL = os.getenv("DATABASE_URL")
DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")       # e.g. 2615532
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")             # optional — for AWIN official API
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")       # e.g. 4599968
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")  # optional for Rakuten official API

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
IFTTT_KEY = os.getenv("IFTTT_KEY")  # IFTTT maker key for TikTok

TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
YOUR_WHATSAPP = os.getenv("YOUR_WHATSAPP")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

POST_INTERVAL_SECONDS = int(os.getenv("POST_INTERVAL_SECONDS", "3600"))
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "300"))

# OpenAI modern client
openai_client = OpenAI(api_key=OPENAI_KEY) if OPENAI_KEY else None

# worker state
_worker_running = False
_stop_requested = False

if not DB_URL:
    logger.error("DATABASE_URL not set — worker will not start (set in env)")

# DB helper
def get_db_conn():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# Alerts (twilio + telegram)
def send_alert(title, body):
    logger.info("ALERT: %s — %s", title, body)
    if TWILIO_SID and TWILIO_TOKEN and YOUR_WHATSAPP:
        try:
            from twilio.rest import Client as TwClient
            cli = TwClient(TWILIO_SID, TWILIO_TOKEN)
            cli.messages.create(from_='whatsapp:+14155238886', body=f"*{title}*\n{body}", to=YOUR_WHATSAPP)
        except Exception:
            logger.exception("Twilio alert failed")
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": f"{title}\n{body}"}, timeout=8)
        except Exception:
            logger.exception("Telegram alert failed")

# URL validation + affiliate check (strict)
def is_valid_https_url(url):
    return bool(url and isinstance(url, str) and url.startswith("https://") and len(url) < 3000)

def url_contains_affiliate(url):
    if not url:
        return False
    u = url.lower()
    if AWIN_PUBLISHER_ID and str(AWIN_PUBLISHER_ID) in u:
        return True
    if RAKUTEN_CLIENT_ID and str(RAKUTEN_CLIENT_ID) in u:
        return True
    # Known redirect / short domains we accept (redirect fallback)
    if any(d in u for d in ("tidd.ly", "awin1.com", "linksynergy.com", "click.linksynergy.com", "rakuten", "go.redirectingat.com")):
        return True
    return False

# Helper to log redirect chain for debugging
def fetch_redirect_chain(url, timeout=15):
    """Follow redirects and return (final_url, chain_list). Chain includes tuples (url, status_code)."""
    try:
        session = requests.Session()
        resp = session.get(url, allow_redirects=True, timeout=timeout)
        chain = []
        # requests stores history
        for r in resp.history:
            chain.append((r.url, r.status_code))
        chain.append((resp.url, resp.status_code))
        if DEBUG:
            logger.debug("Redirect chain for %s -> %s : %s", url, resp.url, chain)
        return resp.url, chain
    except Exception:
        logger.exception("fetch_redirect_chain failed for %s", url)
        return None, []

# Save links to DB but only if they pass validation and affiliate check (final url check)
def save_links_to_db(links, source="affiliate"):
    if not links:
        return 0
    conn, cur = get_db_conn()
    added = 0
    attempted = len(links)
    for raw in links:
        try:
            if not is_valid_https_url(raw):
                if DEBUG: logger.debug("Reject invalid scheme/length: %s", raw)
                continue
            final, chain = fetch_redirect_chain(raw)
            # if fetch failed, skip
            if not final:
                if DEBUG: logger.debug("Reject due to no final redirect: %s", raw)
                continue
            # final must contain affiliate id OR be a known affiliate redirect host (enforced)
            if not url_contains_affiliate(final):
                # As last resort: check origin raw contains affiliate id
                if not url_contains_affiliate(raw):
                    logger.warning("Rejected non-affiliate final url: %s (chain: %s)", final, chain)
                    continue
            # Insert (use UTC)
            cur.execute(
                "INSERT INTO posts (url, source, status, created_at) VALUES (%s,%s,'pending',%s) ON CONFLICT (url) DO NOTHING",
                (final, source, datetime.now(timezone.utc))
            )
            added += 1
            if DEBUG:
                logger.debug("Inserted url: %s (from raw %s) — chain: %s", final, raw, chain)
        except Exception:
            logger.exception("Insert failed for %s", raw)
    conn.commit(); conn.close()
    logger.info("Saved %s validated links from %s (attempted %s)", added, source, attempted)
    return added

# === AWIN / Rakuten: try API when possible, else redirect fallback ===

def pull_awin_api(limit=10):
    """
    Attempt AWIN API retrieval when AWIN_API_TOKEN and AWIN_PUBLISHER_ID present.
    NOTE: AWIN has a few endpoints depending on use-case. This function attempts a sensible default
    if token present, but you should replace the endpoint and parsing according to AWIN docs for your account.
    """
    results = []
    if not AWIN_API_TOKEN or not AWIN_PUBLISHER_ID:
        if DEBUG: logger.debug("AWIN API not configured; skipping API pull")
        return results
    try:
        # Placeholder AWIN endpoint — you should replace with the exact endpoint per AWIN docs for offers/creative links.
        # Example (not guaranteed): "https://api.awin.com/publishers/{publisher_id}/v2/offers"
        endpoint = f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/offers"
        headers = {"Authorization": f"Bearer {AWIN_API_TOKEN}", "Accept": "application/json"}
        r = requests.get(endpoint, headers=headers, timeout=12, params={"limit": limit})
        if r.status_code == 200:
            data = r.json()
            # Try a few keys commonly used; adapt to AWIN response shape
            items = data.get("offers") or data.get("results") or data.get("data") or []
            for it in items[:limit]:
                # Try to extract a deeplink/url:
                url = it.get("deepLink") or it.get("deeplink") or it.get("url") or it.get("link")
                if url:
                    results.append(url)
        else:
            logger.warning("AWIN API responded %s: %s", r.status_code, r.text[:300])
    except Exception:
        logger.exception("AWIN API pull failed")
    return results

def pull_awin_deeplinks(limit=4):
    """
    Redirect-based AWIN pull (reliable deeplink generation using publisher redirect)
    """
    out = []
    if not AWIN_PUBLISHER_ID:
        if DEBUG: logger.debug("No AWIN_PUBLISHER_ID; skipping AWIN redirects")
        return out
    for _ in range(limit):
        try:
            url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=bot"
            final, chain = fetch_redirect_chain(url, timeout=12)
            if final and is_valid_https_url(final):
                out.append(final)
                if DEBUG:
                    logger.debug("AWIN redirect pulled final: %s (chain: %s)", final, chain)
        except Exception:
            logger.exception("AWIN redirect error")
    return out

def pull_rakuten_api(limit=10):
    """
    Attempt Rakuten API retrieval if security token present. Replace endpoint/headers with Rakuten docs if you have proper credentials.
    """
    results = []
    if not RAKUTEN_SECURITY_TOKEN or not RAKUTEN_CLIENT_ID:
        if DEBUG: logger.debug("Rakuten API not configured; skipping API pull")
        return results
    try:
        # Placeholder Rakuten endpoint (customize per your Rakuten account)
        endpoint = "https://api.rakutenadvertising.com/link/v1/products"
        headers = {"Authorization": f"Bearer {RAKUTEN_SECURITY_TOKEN}"}
        r = requests.get(endpoint, headers=headers, timeout=12, params={"limit": limit})
        if r.status_code == 200:
            data = r.json()
            items = data.get("products") or data.get("data") or []
            for it in items[:limit]:
                url = it.get("deepLink") or it.get("link") or it.get("url")
                if url:
                    results.append(url)
        else:
            logger.warning("Rakuten API responded %s: %s", r.status_code, r.text[:300])
    except Exception:
        logger.exception("Rakuten API pull failed")
    return results

def pull_rakuten_deeplinks(limit=4):
    out = []
    if not RAKUTEN_CLIENT_ID:
        if DEBUG: logger.debug("No RAKUTEN_CLIENT_ID; skipping Rakuten redirects")
        return out
    for _ in range(limit):
        try:
            url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl=https://example.com"
            final, chain = fetch_redirect_chain(url, timeout=12)
            if final and is_valid_https_url(final):
                out.append(final)
                if DEBUG:
                    logger.debug("Rakuten redirect pulled final: %s (chain: %s)", final, chain)
        except Exception:
            logger.exception("Rakuten redirect error")
    return out

# OpenAI caption generator (modern)
def generate_caption(link):
    if not openai_client:
        return f"Hot deal — check this out: {link}"
    try:
        prompt = f"Create a short energetic social caption (one sentence, includes 1 emoji, 1 CTA) for this affiliate link:\n\n{link}"
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

# Posting helpers
def post_facebook(message):
    if not FB_PAGE_ID or not FB_TOKEN:
        if DEBUG: logger.debug("FB not configured")
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
        if DEBUG: logger.debug("IG not configured")
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
        if DEBUG: logger.debug("Telegram not configured")
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
        if DEBUG: logger.debug("IFTTT not configured")
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

# YouTube (fallback)
def post_youtube_short(title, video_url):
    if not YOUTUBE_TOKEN_JSON:
        if DEBUG: logger.debug("YouTube not configured")
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
    inserted = save_links_to_db([url], source="manual")
    return {"inserted": inserted, "url": url}

# Posting pipeline
def post_next_pending():
    conn, cur = get_db_conn()
    cur.execute("SELECT id, url FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if not row:
        if DEBUG: logger.debug("No pending posts")
        return False
    post_id = row["id"]; url = row["url"]
    # quick validation — ensure https
    if not is_valid_https_url(url):
        logger.warning("Invalid pending; marking failed: %s", url)
        conn, cur = get_db_conn()
        cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE id=%s", ("failed", datetime.now(timezone.utc), post_id))
        conn.commit(); conn.close()
        return False

    # Debug: log redirect chain for audit
    final, chain = fetch_redirect_chain(url)
    if DEBUG:
        logger.debug("Posting next pending id=%s url=%s final=%s chain=%s", post_id, url, final, chain)

    caption = generate_caption(final or url)
    public = os.getenv("APP_PUBLIC_URL") or os.getenv("PUBLIC_URL") or ""
    redirect_link = f"{public.rstrip('/')}/r/{post_id}" if public else url
    caption_with_link = f"{caption}\n{redirect_link}"

    # generate HeyGen video job (optional)
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
    logger.info("post_next_pending finished id=%s success=%s", post_id, success)
    return success

# Refresh all sources
def refresh_all_sources():
    logger.info("Refreshing affiliate sources")
    links = []
    # First attempt APIs (if configured) for richer offers
    try:
        api_awin_links = pull_awin_api(limit=6)
        if api_awin_links:
            links += api_awin_links
    except Exception:
        logger.exception("AWIN API error")
    try:
        api_rak_links = pull_rakuten_api(limit=6)
        if api_rak_links:
            links += api_rak_links
    except Exception:
        logger.exception("Rakuten API error")
    # Always use redirect fallback too (keeps it working even without API tokens)
    try:
        links += pull_awin_deeplinks(limit=4)
    except Exception:
        logger.exception("AWIN redirect fallback failed")
    try:
        links += pull_rakuten_deeplinks(limit=4)
    except Exception:
        logger.exception("Rakuten redirect fallback failed")
    saved = save_links_to_db(links, source="affiliate") if links else 0
    send_alert("REFRESH", f"Pulled {len(links)} links, saved {saved}")
    return saved

# Stats helper for app
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

# Start/stop worker
def start_worker_background():
    global _worker_running, _stop_requested, POST_INTERVAL_SECONDS
    if _worker_running:
        logger.info("Worker already running")
        return
    if not DB_URL:
        logger.error("DATABASE_URL missing; not starting worker")
        return
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

# Setter for interval (so app.py can POST /api/interval)
def set_post_interval_seconds(sec):
    global POST_INTERVAL_SECONDS
    try:
        sec = int(sec)
        if sec < 5:
            raise ValueError("interval too small")
        POST_INTERVAL_SECONDS = sec
        logger.info("Updated POST_INTERVAL_SECONDS -> %s", POST_INTERVAL_SECONDS)
        return True
    except Exception as e:
        logger.exception("set_post_interval_seconds failed: %s", e)
        return False

if __name__ == "__main__":
    start_worker_background()
