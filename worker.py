# worker.py — AutoAffiliate worker (OpenAI captions + HeyGen videos + AWIN/Rakuten rotation + social posting)
import os
import time
import json
import logging
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

DATABASE_URL = os.getenv("DATABASE_URL")

# Affiliates
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")
RAKUTEN_WEBSERVICES_TOKEN = os.getenv("RAKUTEN_WEBSERVICES_TOKEN")

# AI + video
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
HEYGEN_API_KEY = os.getenv("HEYGEN_API_KEY")

# Social
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
IG_TOKEN = os.getenv("IG_TOKEN")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")
IFTTT_KEY = os.getenv("IFTTT_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

APP_PUBLIC_URL = os.getenv("APP_PUBLIC_URL") or os.getenv("PUBLIC_URL") or ""

# cadence
POST_INTERVAL_SECONDS = int(os.getenv("POST_INTERVAL_SECONDS", "10800"))  # default 3 hours
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "300"))
DEBUG_REDIRECTS = os.getenv("DEBUG_REDIRECTS", "0") == "1"

# rotation mapping (B -> 2 -> C -> 1 -> A)
ROTATION = [("awin","B"), ("rakuten","2"), ("awin","C"), ("rakuten","1"), ("awin","A")]

# Worker control
_worker_running = False
_stop_requested = False

# DB helpers
def get_db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    return conn, conn.cursor()

def ensure_tables():
    try:
        conn, cur = get_db_conn()
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
        conn.commit(); conn.close()
    except Exception:
        logger.exception("ensure_tables failed")

ensure_tables()

# safe insert
def save_links_to_db(links, source="affiliate"):
    if not links:
        return 0
    conn, cur = get_db_conn()
    added = 0
    attempted = len(links)
    for link in links:
        try:
            url = link if isinstance(link, str) else link.get("url")
            meta = {}
            if isinstance(link, dict):
                meta = {k:v for k,v in link.items() if k!="url"}
            if not url or not url.startswith("http"):
                logger.debug("Reject invalid: %s", url); continue
            try:
                cur.execute("INSERT INTO posts (url, source, status, created_at, meta) VALUES (%s,%s,'pending',%s,%s) ON CONFLICT (url) DO NOTHING",
                            (url, source, datetime.now(timezone.utc), json.dumps(meta)))
                added += 1
            except Exception:
                conn.rollback()
                logger.exception("Insert failed for %s", url)
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

# ---------- AWIN helpers ----------
def pull_awin_deeplinks(limit=4):
    out = []
    # API attempt
    if AWIN_API_TOKEN and AWIN_PUBLISHER_ID:
        try:
            hdr = {"Authorization": f"Bearer {AWIN_API_TOKEN}", "Accept":"application/json"}
            endpoint = f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/programmes"
            r = requests.get(endpoint, headers=hdr, timeout=12)
            if r.status_code == 200:
                data = r.json() or []
                for item in data[:limit]:
                    url = item.get("url") or item.get("clickThroughUrl")
                    if url:
                        out.append(url)
            else:
                logger.info("AWIN API non-200: %s", r.status_code)
        except Exception:
            logger.exception("awin_api error")
    # fallback redirect
    if len(out) < limit and AWIN_PUBLISHER_ID:
        for _ in range(limit - len(out)):
            try:
                url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=bot"
                r = requests.get(url, allow_redirects=True, timeout=15)
                final = r.url
                if DEBUG_REDIRECTS:
                    logger.info("AWIN chain: %s", " -> ".join([h.url for h in r.history] + [r.url]))
                if final and final.startswith("http"):
                    out.append(final)
            except Exception:
                logger.exception("awin fallback error")
    return out[:limit]

# ---------- Rakuten helpers ----------
def get_rakuten_access_token():
    if not RAKUTEN_SECURITY_TOKEN:
        raise RuntimeError("RAKUTEN_SECURITY_TOKEN not set")
    try:
        url = "https://api.linksynergy.com/token"
        headers = {"Authorization": f"Bearer {RAKUTEN_SECURITY_TOKEN}", "Content-Type": "application/x-www-form-urlencoded"}
        data = {"scope": RAKUTEN_CLIENT_ID} if RAKUTEN_CLIENT_ID else {}
        r = requests.post(url, headers=headers, data=data, timeout=12)
        r.raise_for_status()
        return r.json().get("access_token")
    except Exception:
        logger.exception("get_rakuten_access_token failed")
        raise

def generate_rakuten_deeplink(advertiser_id, destination_url):
    try:
        token = get_rakuten_access_token()
        url = f"https://api.rakutenmarketing.com/deeplink/v1/{advertiser_id}"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        params = {"url": destination_url}
        r = requests.get(url, headers=headers, params=params, timeout=12)
        r.raise_for_status()
        return r.json().get("link") or r.json().get("deeplink") or r.json().get("url")
    except Exception:
        logger.exception("generate_rakuten_deeplink failed — falling back to redirect")
        if RAKUTEN_CLIENT_ID:
            return f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl={quote_plus(destination_url)}"
        return destination_url

def pull_rakuten_deeplinks(limit=4):
    out = []
    try:
        if RAKUTEN_SECURITY_TOKEN:
            hdr = {"Authorization": f"Bearer {RAKUTEN_SECURITY_TOKEN}", "Accept": "application/json"}
            endpoint = "https://api.rakutenmarketing.com/linking/v1/offer"
            r = requests.get(endpoint, headers=hdr, timeout=12)
            if r.status_code == 200:
                data = r.json()
                items = data.get("offers") or data.get("data") or []
                for item in items[:limit]:
                    url = item.get("deeplink") or item.get("clickUrl") or item.get("url")
                    if url:
                        out.append(url)
            else:
                logger.info("Rakuten API non-200: %s", r.status_code)
    except requests.exceptions.SSLError:
        logger.exception("Rakuten SSL error (host mismatch). Falling back.")
    except Exception:
        logger.exception("rakuten_api_offers error")
    # fallback redirect
    if len(out) < limit and RAKUTEN_CLIENT_ID:
        for _ in range(limit - len(out)):
            try:
                # origin url for redirect resolution
                murl = "https://example.com"
                url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl={murl}"
                r = requests.get(url, allow_redirects=True, timeout=15)
                final = r.url
                if DEBUG_REDIRECTS:
                    logger.info("Rakuten chain: %s", " -> ".join([h.url for h in r.history] + [r.url]))
                if final and final.startswith("http"):
                    out.append(final)
            except Exception:
                logger.exception("rakuten fallback error")
    return out[:limit]

# ---------- OpenAI caption ----------
def generate_caption_using_openai(url, sample_title=None):
    if not OPENAI_API_KEY:
        return f"Hot deal — check this out: {url}"
    try:
        messages = [
            {"role":"system", "content": "You are a short social caption generator. Output one short sentence, include exactly one emoji and one CTA. Append 2 hashtags separated by spaces."},
            {"role":"user", "content": f"Create a short energetic caption for this affiliate link:\n\n{sample_title or ''}\n{url}"}
        ]
        payload = {"model": "gpt-4o-mini", "messages": messages, "max_tokens": 80, "temperature": 0.8}
        r = requests.post("https://api.openai.com/v1/chat/completions",
                          headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                          json=payload, timeout=12)
        r.raise_for_status()
        data = r.json()
        text = ""
        if "choices" in data and len(data["choices"])>0:
            msg = data["choices"][0].get("message") or {}
            text = msg.get("content") or data["choices"][0].get("text","")
        text = (text or "").strip()
        if not text:
            text = f"Hot deal — check this out: {url}"
        if url not in text:
            text = f"{text} {url}"
        return text
    except Exception:
        logger.exception("OpenAI caption error")
        return f"Hot deal — check this out: {url}"

# ---------- HeyGen ----------
def generate_heygen_video(text):
    if not HEYGEN_API_KEY:
        return None
    try:
        endpoint = "https://api.heygen.com/v1/video/generate"
        headers = {"x-api-key": HEYGEN_API_KEY, "Content-Type": "application/json"}
        payload = {
            "type": "avatar",
            "script": {"type":"text","input": text},
            "avatar": "default",
            "voice": {"language":"en-US", "style":"energetic"},
            "output_format": "mp4"
        }
        r = requests.post(endpoint, headers=headers, json=payload, timeout=60)
        if r.status_code in (200,201):
            dd = r.json()
            return dd.get("video_url") or dd.get("result_url") or dd.get("url") or dd.get("job_id")
        logger.warning("HeyGen returned %s %s", r.status_code, r.text[:300])
    except Exception:
        logger.exception("HeyGen error")
    return None

# ---------- Social helpers ----------
def post_facebook(message):
    if not FB_PAGE_ID or not FB_ACCESS_TOKEN:
        logger.debug("FB not configured"); return False
    try:
        endpoint = f"https://graph.facebook.com/v17.0/{FB_PAGE_ID}/feed"
        params = {"access_token": FB_ACCESS_TOKEN, "message": message}
        r = requests.post(endpoint, params=params, timeout=15)
        logger.info("FB status=%s text=%s", r.status_code, r.text[:200])
        return r.status_code in (200,201)
    except Exception:
        logger.exception("FB post failed")
        return False

def post_instagram(caption):
    if not IG_USER_ID or not IG_TOKEN:
        logger.debug("IG not configured"); return False
    try:
        create = requests.post(f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media",
                               params={"image_url": "https://i.imgur.com/airmax270.jpg", "caption": caption, "access_token": IG_TOKEN},
                               timeout=15)
        if create.status_code not in (200,201):
            logger.warning("IG create failed %s %s", create.status_code, create.text[:300])
            return False
        creation_id = create.json().get("id")
        publish = requests.post(f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media_publish",
                                params={"creation_id": creation_id, "access_token": IG_TOKEN}, timeout=15)
        logger.info("IG publish status=%s", publish.status_code)
        return publish.status_code in (200,201)
    except Exception:
        logger.exception("IG post failed")
        return False

def post_x(text):
    try:
        import tweepy
        if TWITTER_BEARER_TOKEN:
            client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN,
                                   consumer_key=TWITTER_API_KEY,
                                   consumer_secret=TWITTER_API_SECRET,
                                   access_token=TWITTER_ACCESS_TOKEN,
                                   access_token_secret=TWITTER_ACCESS_SECRET)
            client.create_tweet(text=text)
            logger.info("Posted to X via tweepy v2")
            return True
        else:
            if not all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET]):
                logger.debug("Twitter creds missing"); return False
            auth = tweepy.OAuth1UserHandler(TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)
            api = tweepy.API(auth)
            api.update_status(status=text)
            logger.info("Posted to X via OAuth1")
            return True
    except Exception:
        logger.exception("X/Twitter post failed")
        return False

def post_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram not configured"); return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        return r.status_code == 200
    except Exception:
        logger.exception("Telegram failed")
        return False

def trigger_ifttt(event="tiktok_post", value1=None):
    if not IFTTT_KEY:
        logger.debug("IFTTT not configured"); return False
    try:
        url = f"https://maker.ifttt.com/trigger/{event}/with/key/{IFTTT_KEY}"
        r = requests.post(url, json={"value1": value1}, timeout=8)
        logger.info("IFTTT status=%s", r.status_code)
        return r.status_code in (200,202)
    except Exception:
        logger.exception("IFTTT failed")
        return False

def post_youtube_fallback(title, video_url):
    try:
        post_telegram(f"YouTube (manual): {title}\n{video_url}")
        return True
    except Exception:
        logger.exception("YouTube fallback failed")
        return False

# ---------- Post pipeline ----------
def post_next_pending():
    try:
        conn, cur = get_db_conn()
    except Exception:
        logger.exception("DB not available")
        return False
    try:
        cur.execute("SELECT id, url, meta FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED")
        row = cur.fetchone()
        if not row:
            conn.close()
            return False
        post_id = row["id"]
        url = row["url"]
        meta = row.get("meta") or {}

        final_url = url
        if DEBUG_REDIRECTS:
            try:
                r = requests.get(url, allow_redirects=True, timeout=12)
                final_url = r.url
                logger.info("Redirect chain: %s", " -> ".join([h.url for h in r.history] + [r.url]))
            except Exception:
                logger.exception("redirect chain failed; using original url")

        caption = generate_caption_using_openai(final_url, sample_title=meta.get("title"))
        public_redirect = f"{APP_PUBLIC_URL.rstrip('/')}/r/{post_id}" if APP_PUBLIC_URL else final_url
        caption_with_link = f"{caption}\n{public_redirect}"

        video_ref = None
        if HEYGEN_API_KEY:
            try:
                video_ref = generate_heygen_video(caption)
            except Exception:
                logger.exception("heygen generate error")

        success = False
        try:
            if post_facebook(caption_with_link): success = True
        except Exception:
            logger.exception("fb post error")
        try:
            if post_instagram(caption_with_link): success = True
        except Exception:
            logger.exception("ig post error")
        try:
            if post_x(caption + " " + public_redirect): success = True
        except Exception:
            logger.exception("x post error")
        try:
            if post_telegram(caption_with_link): success = True
        except Exception:
            logger.exception("tg post error")
        try:
            trigger_ifttt(value1=caption)  # tiktok via IFTTT
        except Exception:
            logger.exception("ifttt error")
        if video_ref:
            try:
                post_youtube_fallback(caption, video_ref)
            except Exception:
                logger.exception("yt fallback error")

        status = "sent" if success else "failed"
        now = datetime.now(timezone.utc)
        try:
            cur.execute("UPDATE posts SET status=%s, posted_at=%s, meta = jsonb_set(coalesce(meta,'{}'::jsonb), %s, %s, true) WHERE id=%s",
                        (status, now, '{posted_via}', json.dumps("auto"), post_id))
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("update post status failed")
        finally:
            conn.close()

        logger.info("POST %s id=%s final=%s video=%s", status, post_id, final_url, bool(video_ref))
        return success
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception("post_next_pending top error")
        try:
            conn.close()
        except Exception:
            pass
        return False

# ---------- Refresh rotation ----------
def refresh_all_sources():
    logger.info("Refreshing affiliate sources (rotation)")
    links = []
    for provider, tag in ROTATION:
        try:
            if provider == "awin":
                links += pull_awin_deeplinks(limit=1)
            elif provider == "rakuten":
                links += pull_rakuten_deeplinks(limit=1)
            time.sleep(0.2)
        except Exception:
            logger.exception("refresh slot error")
    saved = save_links_to_db(links, source="affiliate") if links else 0
    logger.info("REFRESH complete pulled=%s saved=%s", len(links), saved)
    return saved

# ---------- Enqueue manual ----------
def enqueue_manual_link(url, source="manual"):
    if not url:
        raise ValueError("url required")
    conn, cur = get_db_conn()
    try:
        cur.execute("INSERT INTO posts (url, source, status, created_at) VALUES (%s,%s,'pending',now()) ON CONFLICT (url) DO NOTHING",
                    (url, source))
        conn.commit()
        return {"url": url, "source": source}
    finally:
        conn.close()

# ---------- Stats for app ----------
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

# ---------- Worker loop control ----------
def start_worker_background():
    global _worker_running, _stop_requested, POST_INTERVAL_SECONDS
    if _worker_running:
        logger.info("Worker already running")
        return
    _stop_requested = False
    _worker_running = True
    # load cadence from settings table if present
    try:
        conn, cur = get_db_conn()
        cur.execute("SELECT value FROM settings WHERE key='post_interval_seconds' LIMIT 1")
        r = cur.fetchone()
        if r and r["value"]:
            POST_INTERVAL_SECONDS = int(r["value"])
        conn.close()
    except Exception:
        logger.debug("no post interval setting or failed to read")
    logger.info("Worker starting — cadence: %s seconds", POST_INTERVAL_SECONDS)
    try:
        send_alert = lambda t,b: logger.info("ALERT: %s — %s", t, b)
        send_alert("WORKER START", "AutoAffiliate worker started")
        next_pull = datetime.now(timezone.utc) - timedelta(seconds=5)
        while not _stop_requested:
            try:
                now = datetime.now(timezone.utc)
                if now >= next_pull:
                    try:
                        refresh_all_sources()
                    except Exception:
                        logger.exception("refresh_all_sources error")
                    next_pull = now + timedelta(minutes=PULL_INTERVAL_MINUTES)
                posted = post_next_pending()
                if posted:
                    time.sleep(POST_INTERVAL_SECONDS)
                else:
                    time.sleep(SLEEP_ON_EMPTY)
            except Exception:
                logger.exception("Worker top-level exception")
                time.sleep(60)
    finally:
        _worker_running = False
        _stop_requested = False
        logger.info("Worker stopped")
        logger.info("ALERT: WORKER STOPPED — AutoAffiliate worker stopped")

def stop_worker():
    global _stop_requested
    logger.info("Stop requested")
    _stop_requested = True

# run directly
if __name__ == "__main__":
    try:
        start_worker_background()
    except KeyboardInterrupt:
        stop_worker()
