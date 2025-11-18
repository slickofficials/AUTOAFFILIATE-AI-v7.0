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
from typing import Optional

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

DATABASE_URL = os.getenv("DATABASE_URL")

# Affiliates
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")
AWIN_CLICKREF = os.getenv("AWIN_CLICKREF", "autoaffiliate")
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")  # legacy alias for site id
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")
RAKUTEN_WEBSERVICES_TOKEN = os.getenv("RAKUTEN_WEBSERVICES_TOKEN")
RAKUTEN_SITE_ID = os.getenv("RAKUTEN_SITE_ID") or RAKUTEN_CLIENT_ID
RAKUTEN_CLICKREF = os.getenv("RAKUTEN_CLICKREF", "autoaffiliate")

# AI + video
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
HEYGEN_API_KEY = os.getenv("HEYGEN_API_KEY")

# Social
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
IG_TOKEN = os.getenv("IG_TOKEN") or FB_ACCESS_TOKEN
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")
IFTTT_KEY = os.getenv("IFTTT_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Alerts (WhatsApp via Twilio)
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM")
TWILIO_WHATSAPP_TO = os.getenv("TWILIO_WHATSAPP_TO")

APP_PUBLIC_URL = os.getenv("APP_PUBLIC_URL") or os.getenv("PUBLIC_URL") or ""

# Cadence
POST_INTERVAL_SECONDS = int(os.getenv("POST_INTERVAL_SECONDS", "10800"))  # default 3 hours
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "300"))
DEBUG_REDIRECTS = os.getenv("DEBUG_REDIRECTS", "0") == "1"

# Channel pause flags
DISABLE_FB = os.getenv("DISABLE_FB", "0") == "1"
DISABLE_IG = os.getenv("DISABLE_IG", "0") == "1"
DISABLE_X = os.getenv("DISABLE_X", "0") == "1"
DISABLE_TIKTOK = os.getenv("DISABLE_TIKTOK", "0") == "1"
DISABLE_TELEGRAM = os.getenv("DISABLE_TELEGRAM", "0") == "1"
DISABLE_YOUTUBE = os.getenv("DISABLE_YOUTUBE", "0") == "1"

# Rotation mapping (B -> 2 -> C -> 1 -> A)
ROTATION = [("awin","B"), ("rakuten","2"), ("awin","C"), ("rakuten","1"), ("awin","A")]

# Worker control
_worker_running = False
_stop_requested = False

# ---------- DB helpers ----------
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

# ---------- Safe insert ----------
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

# ---------- AWIN helpers (improved) ----------
def pull_awin_deeplinks(limit=4):
    out = []
    if AWIN_API_TOKEN and AWIN_PUBLISHER_ID:
        try:
            endpoint = f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/products?accessToken={AWIN_API_TOKEN}&pageSize={limit}"
            r = requests.get(endpoint, timeout=12)
            if r.status_code == 200:
                data = r.json() or {}
                for item in (data.get("products") or [])[:limit]:
                    url = item.get("url") or item.get("clickThroughUrl")
                    if url:
                        out.append(url)
            else:
                logger.info("AWIN API non-200: %s %s", r.status_code, r.text[:200])
        except Exception:
            logger.exception("awin_api error")
    # fallback redirect to keep rotation alive
    if len(out) < limit and AWIN_PUBLISHER_ID:
        for _ in range(limit - len(out)):
            try:
                url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref={AWIN_CLICKREF}"
                r = requests.get(url, allow_redirects=True, timeout=15)
                final = r.url
                if DEBUG_REDIRECTS:
                    logger.info("AWIN chain: %s", " -> ".join([h.url for h in r.history] + [r.url]))
                if final and final.startswith("http"):
                    out.append(final)
            except Exception:
                logger.exception("awin fallback error")
    return out[:limit]

# ---------- Rakuten helpers (fixed) ----------
RAKUTEN_BASES = [
    "https://api.rakutenadvertising.com",   # current
    "https://api.rakutenmarketing.com",     # legacy fallback
]

def generate_rakuten_deeplink(advertiser_mid: Optional[str], destination_url: str) -> str:
    """
    LinkShare-style deeplink:
    https://click.linksynergy.com/deeplink?id=<SITE_ID>&mid=<MID>&u1=<clickref>&murl=<encoded target>
    Falls back to destination_url if site_id missing.
    """
    try:
        if not destination_url:
            return ""
        site_id = RAKUTEN_SITE_ID
        if not site_id:
            return destination_url
        clickref = quote_plus(RAKUTEN_CLICKREF)
        murl = quote_plus(destination_url)
        if advertiser_mid:
            return f"https://click.linksynergy.com/deeplink?id={site_id}&mid={advertiser_mid}&u1={clickref}&murl={murl}"
        return f"https://click.linksynergy.com/deeplink?id={site_id}&u1={clickref}&murl={murl}"
    except Exception:
        logger.exception("generate_rakuten_deeplink error")
        return destination_url

def pull_rakuten_deeplinks(limit=4):
    """
    Pull offers from Rakuten Advertising Linking API using Security/WebServices token directly.
    Tries both /offer and /links to accommodate tenant variations. Always keeps rotation alive with fallback.
    """
    out = []
    token = RAKUTEN_SECURITY_TOKEN or RAKUTEN_WEBSERVICES_TOKEN
    site_id = RAKUTEN_SITE_ID
    if not token or not site_id:
        logger.info("Rakuten not configured: missing token or site_id")
        return out

    headers_bearer = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    headers_raw = {"Accept": "application/json", "Authorization": token}
    paths = ["/linking/v1/offer", "/linking/v1/links"]

    for base in RAKUTEN_BASES:
        for path in paths:
            endpoint = f"{base}{path}?siteId={site_id}&pageSize={limit}"
            try:
                r = requests.get(endpoint, headers=headers_bearer, timeout=12)
                if r.status_code == 401:
                    r = requests.get(endpoint, headers=headers_raw, timeout=12)
                if r.status_code != 200:
                    logger.info("Rakuten API non-200: %s %s", r.status_code, r.text[:200])
                    continue

                data = r.json()
                items = data.get("data") or data.get("links") or data.get("offers") or []
                for item in items[:limit]:
                    dest = item.get("destinationUrl") or item.get("linkUrl") or item.get("url")
                    mid = item.get("advertiserId") or item.get("mid")
                    dl = generate_rakuten_deeplink(mid, dest) if dest else None
                    if dl and dl.startswith("http"):
                        out.append(dl)

                if out:
                    return out[:limit]
            except requests.exceptions.RequestException as e:
                logger.warning("Rakuten request error %s: %s", endpoint, e)
            except Exception:
                logger.exception("rakuten_api_offers parsing error")

    # Fallback: keep worker alive with a safe redirect
    if len(out) < limit and site_id:
        for _ in range(limit - len(out)):
            try:
                murl = "https://www.rakuten.com"
                url = f"https://click.linksynergy.com/deeplink?id={site_id}&murl={quote_plus(murl)}"
                r = requests.get(url, allow_redirects=True, timeout=12)
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
    if DISABLE_FB:
        logger.info("FB paused by flag"); return False
    if not FB_PAGE_ID or not FB_ACCESS_TOKEN:
        logger.debug("FB not configured"); return False
    if not (message or "").strip():
        logger.debug("FB empty message, skip"); return False
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
    if DISABLE_IG:
        logger.info("IG paused by flag"); return False
    if not IG_USER_ID or not IG_TOKEN:
        logger.debug("IG not configured"); return False
    if not (caption or "").strip():
        logger.debug("IG empty caption, skip"); return False
    try:
        # NOTE: replace image_url with your offer's image_url if available
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
    if DISABLE_X:
        logger.info("X/Twitter paused by flag"); return False
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
    if DISABLE_TELEGRAM:
        logger.info("Telegram paused by flag"); return False
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
    if DISABLE_TIKTOK:
        logger.info("TikTok/IFTTT paused by flag"); return False
    if not IFTTT_KEY:
        logger.debug("IFTTT not configured"); return False
    try:
        url = f"https://maker.ifttt.com/trigger/{event}/with/key/{IFTTT_KEY}"
        r = requests.post(url, json={"value1": value1 or ""}, timeout=8)
        logger.info("IFTTT status=%s", r.status_code)
        return r.status_code in (200,202)
    except Exception:
        logger.exception("IFTTT failed")
        return False

def post_youtube_fallback(title, video_url):
    if DISABLE_YOUTUBE:
        logger.info("YouTube paused by flag"); return False
    try:
        post_telegram(f"YouTube (manual): {title}\n{video_url}")
        return True
    except Exception:
        logger.exception("YouTube fallback failed")
        return False

# ---------- Health checks ----------
def health_affiliates():
    ok = True
    details = {}
    # AWIN token minimal check
    if AWIN_API_TOKEN and AWIN_PUBLISHER_ID:
        try:
            r = requests.get(f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/programmes?accessToken={AWIN_API_TOKEN}", timeout=10)
            details["awin"] = f"{r.status_code}"
            ok &= (r.status_code == 200)
        except Exception as e:
            details["awin"] = f"error {e}"; ok = False
    else:
        details["awin"] = "missing creds"; ok = False

    # Rakuten token minimal check
    token = RAKUTEN_SECURITY_TOKEN or RAKUTEN_WEBSERVICES_TOKEN
    if token and RAKUTEN_SITE_ID:
        try:
            r = requests.get(f"https://api.rakutenadvertising.com/linking/v1/offer?siteId={RAKUTEN_SITE_ID}&pageSize=1",
                             headers={"Authorization": f"Bearer {token}"}, timeout=10)
            details["rakuten"] = f"{r.status_code}"
            ok &= (r.status_code == 200)
        except Exception as e:
            details["rakuten"] = f"error {e}"; ok = False
    else:
        details["rakuten"] = "missing creds"; ok = False

    return ok, details

def health_social():
    ok = True
    details = {}
    # Facebook
    if FB_PAGE_ID and FB_ACCESS_TOKEN and not DISABLE_FB:
        try:
            r = requests.get(f"https://graph.facebook.com/v17.0/{FB_PAGE_ID}", params={"access_token": FB_ACCESS_TOKEN}, timeout=10)
            details["facebook"] = f"{r.status_code}"
            ok &= (r.status_code == 200)
        except Exception as e:
            details["facebook"] = f"error {e}"; ok = False
    else:
        details["facebook"] = "missing or paused"; ok &= True

    # Instagram
    if IG_USER_ID and IG_TOKEN and not DISABLE_IG:
        try:
            r = requests.get(f"https://graph.facebook.com/v17.0/{IG_USER_ID}", params={"access_token": IG_TOKEN}, timeout=10)
            details["instagram"] = f"{r.status_code}"
            ok &= (r.status_code == 200)
        except Exception as e:
            details["instagram"] = f"error {e}"; ok = False
    else:
        details["instagram"] = "missing or paused"; ok &= True

    # X/Twitter
    if not DISABLE_X:
        details["twitter"] = "configured" if (TWITTER_BEARER_TOKEN or (TWITTER_API_KEY and TWITTER_API_SECRET and TWITTER_ACCESS_TOKEN and TWITTER_ACCESS_SECRET)) else "missing"
        ok &= details["twitter"] == "configured"
    else:
        details["twitter"] = "paused"

    # Telegram
    details["telegram"] = "configured" if (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and not DISABLE_TELEGRAM) else "missing/paused"
    ok &= details["telegram"] == "configured"

    # IFTTT/TikTok
    details["ifttt"] = "configured" if (IFTTT_KEY and not DISABLE_TIKTOK) else "missing/paused"
    ok &= details["ifttt"] == "configured"

    # YouTube fallback
    details["youtube"] = "fallback-enabled" if not DISABLE_YOUTUBE else "paused"

    return ok, details

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

        caption = generate_caption_using_openai(final_url, sample_title=meta.get("title")) or ""
        public_redirect = f"{APP_PUBLIC_URL.rstrip('/')}/r/{post_id}" if APP_PUBLIC_URL else final_url

        caption_with_link = caption.strip()
        if public_redirect not in caption_with_link:
            caption_with_link = f"{caption_with_link}\n{public_redirect}".strip()

        # Avoid posting if caption empty
        if not caption_with_link:
            logger.warning("Empty caption generated; marking as failed")
            now_ts = datetime.now(timezone.utc)
            try:
                cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE id=%s", ("failed", now_ts, post_id))
                conn.commit()
            except Exception:
                conn.rollback()
            conn.close()
            return False

        video_ref = None
        if HEYGEN_API_KEY and not DISABLE_YOUTUBE:
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
        now_ts = datetime.now(timezone.utc)
        try:
            cur.execute("UPDATE posts SET status=%s, posted_at=%s, meta = jsonb_set(coalesce(meta,'{}'::jsonb), %s, %s, true) WHERE id=%s",
                        (status, now_ts, '{posted_via}', json.dumps("auto"), post_id))
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

# ---------- Rotation dispatcher ----------
def pull_links(source, tag, limit=4):
    try:
        if source == "awin":
            links = pull_awin_deeplinks(limit=limit)
        elif source == "rakuten":
            links = pull_rakuten_deeplinks(limit=limit)
        else:
            logger.warning("Unknown source %s", source)
            links = []
        if links:
            save_links_to_db(links, source=f"{source}:{tag}")
        return links
    except Exception:
        logger.exception("pull_links error for %s:%s", source, tag)
        return []

# ---------- Refresh rotation ----------
def refresh_all_sources():
    logger.info("Refreshing affiliate sources (rotation)")
    links = []
    for provider, tag in ROTATION:
        try:
            batch = pull_links(provider, tag, limit=1)
            if batch:
                links += batch
            time.sleep(0.2)
        except Exception:
            logger.exception("refresh slot error")
    logger.info("REFRESH complete pulled=%s saved=%s", len(links), len(links))
    return len(links)

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

# ---------- Health summary for dashboard ----------
def health_summary():
    aff_ok, aff_details = health_affiliates()
    soc_ok, soc_details = health_social()
    return {
        "affiliates_ok": aff_ok,
        "affiliates": aff_details,
        "social_ok": soc_ok,
        "social": soc_details,
        "flags": {
            "FB": DISABLE_FB,
            "IG": DISABLE_IG,
            "X": DISABLE_X,
            "TIKTOK": DISABLE_TIKTOK,
            "TELEGRAM": DISABLE_TELEGRAM,
            "YOUTUBE": DISABLE_YOUTUBE,
        }
    }

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

# ---------- CLI helpers (optional) ----------
def pause_channel(name: str, pause: bool):
    name = name.lower()
    val = "1" if pause else "0"
    if name == "fb": os.environ["DISABLE_FB"] = val
    elif name == "ig": os.environ["DISABLE_IG"] = val
    elif name == "x": os.environ["DISABLE_X"] = val
    elif name == "tiktok": os.environ["DISABLE_TIKTOK"] = val
    elif name == "telegram": os.environ["DISABLE_TELEGRAM"] = val
    elif name == "youtube": os.environ["DISABLE_YOUTUBE"] = val
    logger.info("Channel %s pause=%s", name, pause)

# run directly
if __name__ == "__main__":
    try:
        start_worker_background()
    except KeyboardInterrupt:
        stop_worker()
