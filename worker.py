# worker.py — AutoAffiliate worker (AWIN + Rakuten A/B + rotation + social posts + DB-safe)
# Requirements: requests, psycopg, python-dotenv (optional), OpenAI key (optional), tweepy (optional)
import os
import time
import logging
import json
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone, timedelta
from requests.exceptions import SSLError, RequestException

# ---------- CONFIG ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

# Env / credentials
DB_URL = os.getenv("DATABASE_URL")

# AWIN
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")      # e.g. 2615532
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")            # optional, if available

# Rakuten
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")      # e.g. 4599968 (linksynergy redirect)
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")  # bearer for Rakuten APIs (Link Locator / Product Search)
RAKUTEN_WEBSERVICES_TOKEN = os.getenv("RAKUTEN_WEBSERVICES_TOKEN")  # alternative token sometimes used

# OpenAI (for captions)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# HeyGen (optional avatar video)
HEYGEN_KEY = os.getenv("HEYGEN_API_KEY")

# Social tokens
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
IG_TOKEN = os.getenv("IG_TOKEN")

# Twitter / X
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# IFTTT for TikTok
IFTTT_KEY = os.getenv("IFTTT_KEY")

# YouTube token (optional fallback)
YOUTUBE_TOKEN_JSON = os.getenv("YOUTUBE_TOKEN_JSON")

# Public URL used for redirect routing on posts
APP_PUBLIC_URL = os.getenv("APP_PUBLIC_URL") or os.getenv("PUBLIC_URL") or ""

# Worker timing
DEFAULT_CADENCE_SECONDS = int(os.getenv("POST_INTERVAL_SECONDS", "10800"))  # default 3 hours
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "300"))

# Debug
DEBUG_REDIRECTS = os.getenv("DEBUG_REDIRECTS", "0") == "1"

# Rotation plan (example: B → 2 → C → 1 → A). You can change the provider/tag tuples.
ROTATION = [
    ("awin", "B"),
    ("rakuten", "2"),
    ("awin", "C"),
    ("rakuten", "1"),
    ("awin", "A"),
]

# ---------- DB helpers ----------
def get_db_conn():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

def safe_exec(sql, params=None):
    conn, cur = get_db_conn()
    try:
        cur.execute(sql, params or ())
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("safe_exec failed")
        raise
    finally:
        conn.close()

def ensure_tables():
    # idempotent table creation
    safe_exec("""
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
    safe_exec("""
    CREATE TABLE IF NOT EXISTS clicks (
        id SERIAL PRIMARY KEY,
        post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
        ip TEXT,
        user_agent TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    """)
    safe_exec("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """)

ensure_tables()

def db_get_setting(key, fallback=None):
    try:
        conn, cur = get_db_conn()
        cur.execute("SELECT value FROM settings WHERE key=%s", (key,))
        r = cur.fetchone()
        conn.close()
        return r["value"] if r else fallback
    except Exception:
        logger.exception("db_get_setting")
        return fallback

def db_set_setting(key, value):
    try:
        conn, cur = get_db_conn()
        cur.execute("INSERT INTO settings (key,value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", (key, str(value)))
        conn.commit()
        conn.close()
        return True
    except Exception:
        logger.exception("db_set_setting")
        return False

# cadence persisted
POST_INTERVAL_SECONDS = int(db_get_setting("post_interval_seconds", fallback=str(DEFAULT_CADENCE_SECONDS)))

# ---------- Utility / validation ----------
def is_valid_https_url(u):
    return bool(u and isinstance(u, str) and u.startswith("https://") and len(u) < 4000)

def contains_affiliate_id(url):
    if not url: return False
    u = url.lower()
    if AWIN_PUBLISHER_ID and str(AWIN_PUBLISHER_ID) in u: return True
    if RAKUTEN_CLIENT_ID and str(RAKUTEN_CLIENT_ID) in u: return True
    return False

# ---------- Save links ----------
def save_links_to_db(links, source="affiliate"):
    """links: iterable of URLs or dicts {'url':..., 'meta':...}"""
    if not links:
        return 0
    conn, cur = get_db_conn()
    added = 0
    attempted = len(links)
    for item in links:
        try:
            if isinstance(item, dict):
                url = item.get("url")
                meta = item.get("meta", {})
            else:
                url = item
                meta = {}
            if not is_valid_https_url(url):
                logger.debug("Reject invalid: %s", url)
                continue
            allow = contains_affiliate_id(url) or ("tidd.ly" in url.lower()) or ("linksynergy" in url.lower()) or ("awin" in url.lower()) or ("rakuten" in url.lower())
            if not allow:
                logger.debug("Reject non-affiliate: %s", url)
                continue
            try:
                cur.execute("INSERT INTO posts (url, source, status, created_at, meta) VALUES (%s,%s,'pending',%s,%s) ON CONFLICT (url) DO NOTHING",
                            (url, source, datetime.now(timezone.utc), json.dumps(meta)))
                added += 1
            except Exception:
                # rollback current transaction so subsequent inserts work
                conn.rollback()
                logger.exception("Insert failed for %s", url)
        except Exception:
            logger.exception("save_links_to_db outer")
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
def awin_api_offers(limit=4):
    out = []
    if not AWIN_API_TOKEN or not AWIN_PUBLISHER_ID:
        return out
    try:
        # Template AWIN endpoint (may vary by plan); this attempts to get programmes or campaigns.
        endpoint = f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/programmes"
        headers = {"Authorization": f"Bearer {AWIN_API_TOKEN}", "Accept": "application/json"}
        r = requests.get(endpoint, headers=headers, timeout=12)
        if r.status_code == 200:
            data = r.json()
            # try to extract click/landing URLs if present
            for item in (data or [])[:limit]:
                url = item.get("deeplink") or item.get("tracking_url") or item.get("url")
                if url and is_valid_https_url(url):
                    out.append({"url": url, "meta": {"provider":"awin","raw": item}})
        else:
            logger.warning("AWIN API non-200: %s %s", r.status_code, r.text[:200])
    except Exception:
        logger.exception("awin_api_offers error")
    return out

def pull_awin_deeplinks(limit=4):
    out = []
    # try API first
    out += awin_api_offers(limit=limit)
    if len(out) >= limit:
        return out[:limit]
    if not AWIN_PUBLISHER_ID:
        return out
    for _ in range(limit - len(out)):
        try:
            url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=bot"
            r = requests.get(url, allow_redirects=True, timeout=15)
            final = r.url
            if DEBUG_REDIRECTS:
                logger.info("AWIN redirect chain: %s", " -> ".join([h.url for h in r.history] + [r.url]))
            if final and is_valid_https_url(final):
                out.append({"url": final, "meta": {"provider":"awin","method":"redirect"}})
        except Exception:
            logger.exception("AWIN pull error")
    return out

# ---------- Rakuten helpers (Product Search A + Link Locator B + redirect fallback) ----------
def rakuten_linklocator(url_to_convert=None, advertiser_id=None):
    """
    Link-Locator (B) — create affiliate deep-link for an existing advertiser URL.
    API: https://api.rakutenmarketing.com/linklocator/1.0/getLink.json
    (This function is a best-effort template; adjust to Rakuten docs/params)
    """
    out = []
    if not RAKUTEN_SECURITY_TOKEN:
        return out
    try:
        endpoint = "https://api.rakutenmarketing.com/linklocator/1.0/getLink.json"
        headers = {"Authorization": f"Bearer {RAKUTEN_SECURITY_TOKEN}", "Accept": "application/json"}
        params = {}
        if url_to_convert:
            params["url"] = url_to_convert
        if advertiser_id:
            params["advertiserId"] = advertiser_id
        r = requests.get(endpoint, headers=headers, params=params, timeout=12)
        if r.status_code == 200:
            data = r.json()
            # structure may vary; attempt common fields
            link = None
            if isinstance(data, dict):
                link = data.get("link") or data.get("affiliateLink") or data.get("clickUrl") or data.get("url")
            if link and is_valid_https_url(link):
                out.append({"url": link, "meta": {"provider":"rakuten","method":"linklocator","raw": data}})
        else:
            logger.warning("Rakuten LinkLocator non-200: %s %s", r.status_code, r.text[:300])
        return out
    except SSLError as e:
        logger.exception("rakuten_linklocator SSLError")
        raise
    except Exception:
        logger.exception("rakuten_linklocator error")
        return out

def rakuten_product_search(limit=4, keywords=None):
    """
    Product Search (A) — search products/offers.
    Template endpoint: https://api.rakutenmarketing.com/productsearch/1.0
    (Adjust query params to the exact Rakuten docs for your account.)
    """
    out = []
    if not RAKUTEN_SECURITY_TOKEN:
        return out
    try:
        endpoint = "https://api.rakutenmarketing.com/productsearch/1.0"
        headers = {"Authorization": f"Bearer {RAKUTEN_SECURITY_TOKEN}", "Accept": "application/json"}
        params = {"limit": limit}
        if keywords:
            params["q"] = keywords
        r = requests.get(endpoint, headers=headers, params=params, timeout=12)
        if r.status_code == 200:
            data = r.json()
            # attempt to parse product objects
            products = data.get("products") or data.get("data") or data.get("items") or []
            for p in products[:limit]:
                url = p.get("clickUrl") or p.get("deepLink") or p.get("url") or p.get("link")
                title = p.get("name") or p.get("title") or p.get("productName") or ""
                if url and is_valid_https_url(url):
                    out.append({"url": url, "meta": {"provider":"rakuten","method":"product_search","title": title, "raw": p}})
        else:
            logger.warning("Rakuten product_search non-200: %s %s", r.status_code, r.text[:300])
    except SSLError:
        logger.exception("rakuten_product_search SSLError")
        raise
    except Exception:
        logger.exception("rakuten_product_search error")
    return out

def pull_rakuten_deeplinks(limit=4):
    out = []
    # 1) preferred: LinkLocator for deep-link (B)
    try:
        for _ in range(limit):
            # try linklocator without a URL (some accounts support 'random' or 'offer' endpoints; else try product search)
            res = rakuten_linklocator()
            if res:
                out.extend(res)
            else:
                break
        if len(out) >= limit:
            return out[:limit]
    except SSLError:
        logger.warning("Rakuten LinkLocator SSLError; will fallback to productsearch/redirects")

    # 2) product search (A)
    try:
        ps = rakuten_product_search(limit=limit)
        if ps:
            out.extend(ps)
        if len(out) >= limit:
            return out[:limit]
    except SSLError:
        logger.warning("Rakuten Product Search SSLError; will fallback to redirect scraping")

    # 3) redirect fallback via LinkSynergy deeplink generator
    if RAKUTEN_CLIENT_ID:
        for _ in range(limit - len(out)):
            try:
                url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl=https://example.com"
                r = requests.get(url, allow_redirects=True, timeout=15)
                final = r.url
                if DEBUG_REDIRECTS:
                    logger.info("Rakuten redirect chain: %s", " -> ".join([h.url for h in r.history] + [r.url]))
                if final and is_valid_https_url(final):
                    out.append({"url": final, "meta": {"provider":"rakuten","method":"redirect"}})
            except Exception:
                logger.exception("Rakuten redirect fallback error")
    return out

# ---------- OpenAI caption (REST call) ----------
def generate_caption(link):
    if not OPENAI_API_KEY:
        return f"Hot deal — check it out: {link}"
    try:
        endpoint = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        prompt = f"Write a short energetic social caption (one sentence, include one emoji and one CTA) for this affiliate link:\n\n{link}"
        payload = {
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 60,
            "temperature": 0.8
        }
        r = requests.post(endpoint, headers=headers, json=payload, timeout=10)
        if r.status_code == 200:
            d = r.json()
            # Try to extract message content robustly
            choice = (d.get("choices") or [{}])[0]
            msg = choice.get("message") or choice.get("text") or {}
            text = ""
            if isinstance(msg, dict):
                text = msg.get("content") or msg.get("text") or ""
            else:
                text = str(msg or "")
            text = text.strip()
            if not text:
                # fallback: use top-level text
                text = d.get("text") or ""
            if link not in text:
                text = f"{text} {link}"
            return text
        else:
            logger.warning("OpenAI non-200: %s %s", r.status_code, r.text[:200])
    except Exception:
        logger.exception("generate_caption failed")
    return f"Hot deal — check it out: {link}"

# ---------- HeyGen avatar video (optional) ----------
def generate_heygen_avatar_video(text):
    if not HEYGEN_KEY:
        return None
    try:
        url = "https://api.heygen.com/v1/video/generate"
        headers = {"x-api-key": HEYGEN_KEY, "Content-Type": "application/json"}
        payload = {"type": "avatar", "script": {"type": "text", "input": text}, "avatar": "default", "voice": {"language":"en-US","style":"energetic"}, "output_format":"mp4"}
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        if r.status_code in (200,201):
            data = r.json()
            return data.get("video_url") or data.get("result_url") or data.get("url") or data.get("job_id")
        logger.warning("HeyGen failed %s %s", r.status_code, r.text[:200])
    except Exception:
        logger.exception("HeyGen error")
    return None

# ---------- Social posting helpers ----------
def post_facebook(message):
    if not FB_PAGE_ID or not FB_ACCESS_TOKEN:
        logger.debug("FB not configured")
        return False
    try:
        endpoint = f"https://graph.facebook.com/v17.0/{FB_PAGE_ID}/feed"
        params = {"access_token": FB_ACCESS_TOKEN, "message": message}
        r = requests.post(endpoint, params=params, timeout=15)
        logger.info("FB post status=%s", r.status_code)
        return r.status_code in (200,201)
    except Exception:
        logger.exception("FB post failed")
        return False

def post_instagram(caption):
    if not IG_USER_ID or not IG_TOKEN:
        logger.debug("IG not configured")
        return False
    try:
        # Simple image-less placeholder strategy (Graph API requires media upload flow).
        image_url = "https://i.imgur.com/airmax270.jpg"
        create = requests.post(f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media",
                               params={"image_url": image_url, "caption": caption, "access_token": IG_TOKEN}, timeout=15)
        if create.status_code != 200:
            logger.warning("IG create failed: %s", create.text[:300])
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
    # X (formerly Twitter) v2 - requires bearer token or OAuth client
    try:
        if TWITTER_BEARER_TOKEN:
            headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}", "Content-Type":"application/json"}
            r = requests.post("https://api.twitter.com/2/tweets", headers=headers, json={"text": text}, timeout=10)
            logger.info("X post status=%s", r.status_code)
            return r.status_code in (200,201)
        # fallback: OAuth1 via tweepy could be added if needed
        logger.debug("X not configured with bearer token")
    except Exception:
        logger.exception("X post failed")
    return False

def post_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram not configured")
        return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        return r.status_code == 200
    except Exception:
        logger.exception("Telegram post failed")
        return False

def trigger_ifttt(event, value1=None, value2=None, value3=None):
    if not IFTTT_KEY:
        logger.debug("IFTTT not configured")
        return False
    url = f"https://maker.ifttt.com/trigger/{event}/with/key/{IFTTT_KEY}"
    payload = {}
    if value1 is not None: payload["value1"] = value1
    if value2 is not None: payload["value2"] = value2
    if value3 is not None: payload["value3"] = value3
    try:
        r = requests.post(url, json=payload, timeout=8)
        logger.info("IFTTT status=%s", r.status_code)
        return r.status_code in (200,202)
    except Exception:
        logger.exception("IFTTT failed")
        return False

def post_youtube_short(title, video_url):
    # Placeholder: implement full OAuth upload flow if you want direct uploads
    if not YOUTUBE_TOKEN_JSON:
        return False
    try:
        post_telegram(f"YT (manual): {title}\n{video_url}")
        return True
    except Exception:
        logger.exception("YouTube fallback failed")
        return False

# ---------- Redirect / debugging helper ----------
def fetch_redirect_chain(url, timeout=15):
    try:
        r = requests.get(url, allow_redirects=True, timeout=timeout)
        chain = [h.url for h in r.history] + [r.url]
        return r.url, chain
    except Exception:
        logger.exception("fetch_redirect_chain failed for %s", url)
        return url, []

# ---------- Posting pipeline ----------
def post_next_pending():
    # pick a pending post for update (SKIP LOCKED to support concurrency)
    conn, cur = get_db_conn()
    try:
        cur.execute("SELECT id, url FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED")
        row = cur.fetchone()
    except Exception:
        conn.rollback()
        logger.exception("DB select failed")
        row = None
    finally:
        conn.close()
    if not row:
        logger.debug("No pending posts")
        return False

    post_id = row["id"]
    url = row["url"]
    if not is_valid_https_url(url):
        logger.warning("Invalid pending url; marking failed: %s", url)
        conn, cur = get_db_conn()
        cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE id=%s", ("failed", datetime.now(timezone.utc), post_id))
        conn.commit(); conn.close()
        return False

    # Optionally fetch redirect chain to ensure final destination
    final_url = url
    if DEBUG_REDIRECTS:
        f, chain = fetch_redirect_chain(url)
        logger.info("Redirect chain for post %s: %s", post_id, " -> ".join(chain))
        final_url = f or final_url

    caption = generate_caption(final_url)
    public = APP_PUBLIC_URL.rstrip('/') if APP_PUBLIC_URL else ""
    redirect_link = f"{public}/r/{post_id}" if public else final_url
    caption_with_link = f"{caption}\n{redirect_link}"

    # Try HeyGen (non-blocking in sense we simply request and store returned URL)
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
        if post_x(caption + " " + redirect_link): success = True
    except Exception:
        logger.exception("X error")
    try:
        if post_telegram(caption_with_link): success = True
    except Exception:
        logger.exception("Telegram error")
    # TikTok via IFTTT (fire-and-forget)
    try:
        trigger_ifttt("Post_TikTok", value1=caption, value2=redirect_link)
    except Exception:
        logger.exception("IFTTT error")
    # YouTube fallback
    if video_host_url:
        try:
            post_youtube_short(caption, video_host_url)
        except Exception:
            logger.exception("YouTube post failed")

    # Update DB with status & meta
    conn, cur = get_db_conn()
    try:
        meta_json = json.dumps({"posted_via": "auto", "video": bool(video_host_url)})
        cur.execute("UPDATE posts SET status=%s, posted_at=%s, meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb WHERE id=%s",
                    ("sent" if success else "failed", datetime.now(timezone.utc), meta_json, post_id))
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Failed to update post status for %s", post_id)
    finally:
        conn.close()

    send_alert("POSTED" if success else "POST FAILED", f"{redirect_link} | vid:{bool(video_host_url)}")
    return success

def send_alert(title, body):
    # lightweight alert logger (worker may still send Twilio/Telegram if configured)
    logger.info("ALERT: %s — %s", title, body)
    # Telegram inline alert (best-effort)
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": f"{title}\n{body}"}, timeout=8)
        except Exception:
            logger.exception("Telegram alert failed")

# ---------- Refreshing sources with rotation ----------
def refresh_all_sources():
    logger.info("Refreshing affiliate sources according to rotation")
    links = []
    for provider, tag in ROTATION:
        try:
            if provider == "awin":
                pulled = pull_awin_deeplinks(limit=1)
                links.extend(pulled)
            elif provider == "rakuten":
                # attempt Rakuten LinkLocator first, product search second, redirect third
                try:
                    rl = rakuten_linklocator()
                    if rl:
                        links.extend(rl)
                        continue
                except SSLError:
                    logger.warning("Rakuten LinkLocator SSLError -> fallback")

                try:
                    ps = rakuten_product_search(limit=1)
                    if ps:
                        links.extend(ps)
                        continue
                except SSLError:
                    logger.warning("Rakuten ProductSearch SSLError -> fallback")

                # final fallback: redirect
                pulled = pull_rakuten_deeplinks(limit=1)
                links.extend(pulled)
            time.sleep(0.25)  # be gentle
        except Exception:
            logger.exception("Error pulling from %s", provider)
    saved = save_links_to_db(links, source="affiliate") if links else 0
    send_alert("REFRESH", f"Pulled {len(links)} links, saved {saved}")
    return saved

# ---------- Stats for monitoring ----------
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
_worker_running = False
_stop_requested = False

def start_worker_background():
    global _worker_running, _stop_requested, POST_INTERVAL_SECONDS
    if _worker_running:
        logger.info("Worker already running")
        return
    if not DB_URL:
        logger.error("DATABASE_URL missing; not starting worker")
        return
    # refresh cadence from DB in case changed
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

# ---------- CLI run ----------
if __name__ == "__main__":
    start_worker_background()
