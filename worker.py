# worker.py — AutoAffiliate worker (OpenAI captions + HeyGen videos + AWIN/Rakuten rotation + social posting)
import os
import time
import json
import logging
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone, timedelta

# ---------------------------
# Config / env
# ---------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

DATABASE_URL = os.getenv("DATABASE_URL")

# Affiliate
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")
RAKUTEN_WEBSERVICES_TOKEN = os.getenv("RAKUTEN_WEBSERVICES_TOKEN")

# OpenAI + HeyGen
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
HEYGEN_API_KEY = os.getenv("HEYGEN_API_KEY")

# Social keys
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
IG_TOKEN = os.getenv("IG_TOKEN")
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
IFTTT_KEY = os.getenv("IFTTT_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
YOUTUBE_TOKEN_JSON = os.getenv("YOUTUBE_TOKEN_JSON")

APP_PUBLIC_URL = os.getenv("APP_PUBLIC_URL") or os.getenv("PUBLIC_URL") or ""

# Cadence & options
DEFAULT_CADENCE_SECONDS = int(os.getenv("POST_INTERVAL_SECONDS", "10800"))
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "300"))
DEBUG_REDIRECTS = os.getenv("DEBUG_REDIRECTS", "0") == "1"

# rotation
ROTATION = [("awin","B"), ("rakuten","2"), ("awin","C"), ("rakuten","1"), ("awin","A")]

# ---------------------------
# DB helpers
# ---------------------------
if not DATABASE_URL:
    logger.warning("DATABASE_URL not set. DB ops will fail.")

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

# ---------------------------
# DB save helper
# ---------------------------
def save_links_to_db(links, source="affiliate"):
    if not links:
        return 0
    conn, cur = get_db_conn()
    added = 0
    for link in links:
        try:
            url = link if isinstance(link, str) else link.get("url")
            meta = {}
            if isinstance(link, dict):
                meta = {k:v for k,v in link.items() if k!="url"}
            if not url or not url.startswith("http"):
                continue
            try:
                cur.execute("INSERT INTO posts (url, source, status, created_at, meta) VALUES (%s,%s,'pending',%s,%s) ON CONFLICT (url) DO NOTHING",
                            (url, source, datetime.now(timezone.utc), json.dumps(meta)))
                added += 1
            except Exception:
                conn.rollback()
        except Exception:
            continue
    try:
        conn.commit()
    except Exception:
        try: conn.rollback()
        except: pass
    conn.close()
    logger.info("Saved %s validated links from %s", added, source)
    return added

# ---------------------------
# AWIN pull
# ---------------------------
def pull_awin_deeplinks(limit=4):
    out = []
    if AWIN_API_TOKEN and AWIN_PUBLISHER_ID:
        try:
            hdr = {"Authorization": f"Bearer {AWIN_API_TOKEN}", "Accept":"application/json"}
            endpoint = f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/programmes"
            r = requests.get(endpoint, headers=hdr, timeout=12)
            if r.status_code == 200:
                data = r.json() or []
                for item in data[:limit]:
                    url = item.get("url") or item.get("clickThroughUrl")
                    if url: out.append(url)
        except Exception:
            logger.exception("awin_api error")
    if len(out)<limit and AWIN_PUBLISHER_ID:
        for _ in range(limit-len(out)):
            try:
                url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=bot"
                r = requests.get(url, allow_redirects=True, timeout=15)
                final = r.url
                if DEBUG_REDIRECTS:
                    chain = [h.url for h in r.history]+[r.url]
                    logger.info("AWIN chain: %s", " -> ".join(chain))
                if final.startswith("http"): out.append(final)
            except Exception:
                continue
    return out[:limit]

# ---------------------------
# Rakuten pull
# ---------------------------
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
                    if url: out.append(url)
    except Exception:
        logger.exception("rakuten_api error")
    if len(out)<limit and RAKUTEN_CLIENT_ID:
        for _ in range(limit-len(out)):
            try:
                url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl=https://example.com"
                r = requests.get(url, allow_redirects=True, timeout=15)
                final = r.url
                if final.startswith("http"): out.append(final)
            except Exception:
                continue
    return out[:limit]

# ---------------------------
# OpenAI caption
# ---------------------------
def generate_caption_using_openai(url, sample_title=None):
    if not OPENAI_API_KEY:
        return f"Hot deal — check this out: {url}"
    try:
        messages = [
            {"role":"system", "content": "You are a short social caption generator. One sentence, one emoji, one CTA, 2 hashtags."},
            {"role":"user", "content": f"Create a short caption for:\n{sample_title or ''}\n{url}"}
        ]
        payload = {"model": "gpt-4o-mini", "messages": messages, "max_tokens":80, "temperature":0.8}
        r = requests.post("https://api.openai.com/v1/chat/completions",
                          headers={"Authorization": f"Bearer {OPENAI_API_KEY}","Content-Type":"application/json"},
                          json=payload, timeout=12)
        r.raise_for_status()
        data = r.json()
        text = ""
        if "choices" in data and len(data["choices"])>0:
            msg = data["choices"][0].get("message") or {}
            text = msg.get("content") or data["choices"][0].get("text","")
        text = (text or "").strip()
        if not text: text=f"Hot deal — check this out: {url}"
        if url not in text: text=f"{text} {url}"
        return text
    except Exception:
        return f"Hot deal — check this out: {url}"

# ---------------------------
# HeyGen video
# ---------------------------
def generate_heygen_video(text):
    if not HEYGEN_API_KEY: return None
    try:
        endpoint="https://api.heygen.com/v1/video/generate"
        headers={"x-api-key":HEYGEN_API_KEY,"Content-Type":"application/json"}
        payload={"type":"avatar","script":{"type":"text","input":text},"avatar":"default",
                 "voice":{"language":"en-US","style":"energetic"},"output_format":"mp4"}
        r = requests.post(endpoint, headers=headers, json=payload, timeout=60)
        if r.status_code in (200,201):
            return r.json().get("video_url") or r.json().get("result_url") or r.json().get("url") or r.json().get("job_id")
    except Exception:
        return None

# ---------------------------
# Social posts
# ---------------------------
def post_facebook(msg):
    if not FB_PAGE_ID or not FB_ACCESS_TOKEN: return False
    try:
        r=requests.post(f"https://graph.facebook.com/v17.0/{FB_PAGE_ID}/feed",
                        params={"access_token":FB_ACCESS_TOKEN,"message":msg},timeout=15)
        return r.status_code in (200,201)
    except Exception: return False

def post_instagram(caption):
    if not IG_USER_ID or not IG_TOKEN: return False
    try:
        create=requests.post(f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media",
                             params={"image_url":"https://i.imgur.com/airmax270.jpg","caption":caption,"access_token":IG_TOKEN},
                             timeout=15)
        if create.status_code not in (200,201): return False
        publish=requests.post(f"https://graph.facebook.com/v17.0/{IG_USER_ID}/media_publish",
                              params={"creation_id":create.json().get("id"),"access_token":IG_TOKEN},timeout=15)
        return publish.status_code in (200,201)
    except Exception: return False

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
            return True
        else:
            if not all([TWITTER_API_KEY,TWITTER_API_SECRET,TWITTER_ACCESS_TOKEN,TWITTER_ACCESS_SECRET]): return False
            auth=tweepy.OAuth1UserHandler(TWITTER_API_KEY,TWITTER_API_SECRET,TWITTER_ACCESS_TOKEN,TWITTER_ACCESS_SECRET)
            api=tweepy.API(auth)
            api.update_status(status=text)
            return True
    except Exception: return False

def post_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return False
    try:
        r=requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                        json={"chat_id":TELEGRAM_CHAT_ID,"text":text},timeout=10)
        return r.status_code==200
    except Exception: return False

def post_ifttt_tiktok(caption):
    if not IFTTT_KEY: return False
    try:
        url=f"https://maker.ifttt.com/trigger/tiktok_post/with/key/{IFTTT_KEY}"
        r=requests.post(url,json={"value1":caption},timeout=8)
        return r.status_code in (200,202)
    except Exception: return False

def post_youtube_fallback(title, video_url):
    try:
        post_telegram(f"Youtube (manual): {title}\n{video_url}")
        return True
    except Exception: return False

# ---------------------------
# Post pipeline
# ---------------------------
def post_next_pending():
    try:
        conn, cur=get_db_conn()
        cur.execute("SELECT id,url,meta FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED")
        row=cur.fetchone()
        if not row: conn.close(); return False
        post_id=row["id"]; url=row["url"]; meta=row.get("meta") or {}
        final_url=url
        if DEBUG_REDIRECTS:
            try: r=requests.get(url,allow_redirects=True,timeout=12); final_url=r.url
            except: pass
        caption=generate_caption_using_openai(final_url,sample_title=meta.get("title"))
        public_redirect=f"{APP_PUBLIC_URL.rstrip('/')}/r/{post_id}" if APP_PUBLIC_URL else final_url
        caption_with_link=f"{caption}\n{public_redirect}"
        video_ref=generate_heygen_video(caption) if HEYGEN_API_KEY else None
        success=False
        if post_facebook(caption_with_link): success=True
        if post_instagram(caption_with_link): success=True
        if post_x(caption+" "+public_redirect): success=True
        if post_telegram(caption_with_link): success=True
        post_ifttt_tiktok(caption)
        if video_ref: post_youtube_fallback(caption,video_ref)
        status="sent" if success else "failed"; now=datetime.now(timezone.utc)
        try:
            cur.execute("UPDATE posts SET status=%s, posted_at=%s, meta=jsonb_set(coalesce(meta,'{}'::jsonb),%s,%s,true) WHERE id=%s",
                        (status,now,'{posted_via}',json.dumps("auto"),post_id))
            conn.commit()
        except: conn.rollback()
        conn.close()
        logger.info("POST %s id=%s url=%s video=%s", status, post_id, final_url, bool(video_ref))
        return success
    except Exception: return False

# ---------------------------
# Refresh sources
# ---------------------------
def refresh_all_sources():
    logger.info("Refreshing affiliate sources (rotation)")
    links=[]
    for provider,_ in ROTATION:
        try:
            if provider=="awin": links+=pull_awin_deeplinks(limit=1)
            elif provider=="rakuten": links+=pull_rakuten_deeplinks(limit=1)
            time.sleep(0.2)
        except Exception: continue
    if links: saved=save_links_to_db(links,source="affiliate")
    else: saved=0
    logger.info("REFRESH complete pulled=%s saved=%s",len(links),saved)
    return saved

# ---------------------------
# Worker loop
# ---------------------------
_worker_running=False
_stop_requested=False

def start_worker_background():
    global _worker_running,_stop_requested
    if _worker_running: return
    _worker_running=True; _stop_requested=False
    next_pull=datetime.now(timezone.utc)-timedelta(seconds=5)
    try:
        while not _stop_requested:
            now=datetime.now(timezone.utc)
            if now>=next_pull: refresh_all_sources(); next_pull=now+timedelta(minutes=PULL_INTERVAL_MINUTES)
            posted=post_next_pending()
            time.sleep(DEFAULT_CADENCE_SECONDS if posted else SLEEP_ON_EMPTY)
    finally: _worker_running=False; _stop_requested=False

def stop_worker():
    global _stop_requested; _stop_requested=True

if __name__=="__main__":
    try: start_worker_background()
    except KeyboardInterrupt: stop_worker()
