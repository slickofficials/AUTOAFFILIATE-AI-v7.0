# worker.py — AutoAffiliate worker
import os, time, json, logging, requests, psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone
from urllib.parse import quote_plus
import tweepy

LOG_LEVEL = os.getenv("LOG_LEVEL","INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

DATABASE_URL = os.getenv("DATABASE_URL")

# Affiliate creds
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")
AWIN_CLICKREF = os.getenv("AWIN_CLICKREF","autoaffiliate")
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")
RAKUTEN_WEBSERVICES_TOKEN = os.getenv("RAKUTEN_WEBSERVICES_TOKEN")
RAKUTEN_SITE_ID = os.getenv("RAKUTEN_SITE_ID") or os.getenv("RAKUTEN_CLIENT_ID")
RAKUTEN_CLICKREF = os.getenv("RAKUTEN_CLICKREF","autoaffiliate")

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

APP_PUBLIC_URL = os.getenv("APP_PUBLIC_URL") or os.getenv("PUBLIC_URL") or ""

# Cadence
POST_INTERVAL_SECONDS = int(os.getenv("POST_INTERVAL_SECONDS","10800"))
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES","60"))
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY","300"))

# Pause flags
DISABLE_FB = os.getenv("DISABLE_FB","0")=="1"
DISABLE_IG = os.getenv("DISABLE_IG","0")=="1"
DISABLE_X = os.getenv("DISABLE_X","0")=="1"
DISABLE_TIKTOK = os.getenv("DISABLE_TIKTOK","0")=="1"
DISABLE_TELEGRAM = os.getenv("DISABLE_TELEGRAM","0")=="1"
DISABLE_YOUTUBE = os.getenv("DISABLE_YOUTUBE","0")=="1"

ROTATION = [("awin","B"),("rakuten","2"),("awin","C"),("rakuten","1"),("awin","A")]

_worker_running=False; _stop_requested=False

# ---------- DB ----------
def get_db_conn():
    conn = psycopg.connect(DATABASE_URL,row_factory=dict_row); return conn, conn.cursor()

def ensure_tables():
    conn,cur=get_db_conn()
    cur.execute("""CREATE TABLE IF NOT EXISTS posts(
        id SERIAL PRIMARY KEY,url TEXT UNIQUE NOT NULL,source TEXT,
        status TEXT DEFAULT 'pending',created_at TIMESTAMPTZ DEFAULT now(),
        posted_at TIMESTAMPTZ,meta JSONB DEFAULT '{}'::jsonb);""")
    cur.execute("""CREATE TABLE IF NOT EXISTS clicks(
        id SERIAL PRIMARY KEY,post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
        ip TEXT,user_agent TEXT,created_at TIMESTAMPTZ DEFAULT now());""")
    cur.execute("""CREATE TABLE IF NOT EXISTS settings(
        key TEXT PRIMARY KEY,value TEXT);""")
    conn.commit(); conn.close()
ensure_tables()

def save_links_to_db(links,source="affiliate"):
    if not links: return 0
    conn,cur=get_db_conn(); added=0
    for url in links:
        try:
            cur.execute("INSERT INTO posts(url,source,status,created_at) VALUES(%s,%s,'pending',%s) ON CONFLICT DO NOTHING",
                        (url,source,datetime.now(timezone.utc))); added+=1
        except Exception: conn.rollback()
    conn.commit(); conn.close(); return added
# ---------- AWIN ----------
def pull_awin_deeplinks(limit=4):
    out=[]
    if AWIN_API_TOKEN and AWIN_PUBLISHER_ID:
        try:
            r=requests.get(f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/products?accessToken={AWIN_API_TOKEN}&pageSize={limit}",timeout=12)
            if r.status_code==200:
                for item in (r.json().get("products") or [])[:limit]:
                    url=item.get("url") or item.get("clickThroughUrl")
                    if url: out.append(url)
        except Exception: logger.exception("AWIN API error")
    if len(out)<limit and AWIN_PUBLISHER_ID:
        for _ in range(limit-len(out)):
            try:
                r=requests.get(f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref={AWIN_CLICKREF}",allow_redirects=True,timeout=15)
                if r.url.startswith("http"): out.append(r.url)
            except Exception: logger.exception("AWIN fallback error")
    return out[:limit]
# ---------- Rakuten ----------
def generate_rakuten_deeplink(mid,dest):
    if not dest: return ""
    site_id=RAKUTEN_SITE_ID; clickref=quote_plus(RAKUTEN_CLICKREF); murl=quote_plus(dest)
    if mid: return f"https://click.linksynergy.com/deeplink?id={site_id}&mid={mid}&u1={clickref}&murl={murl}"
    return f"https://click.linksynergy.com/deeplink?id={site_id}&u1={clickref}&murl={murl}"

def pull_rakuten_deeplinks(limit=4):
    out=[]; token=RAKUTEN_SECURITY_TOKEN or RAKUTEN_WEBSERVICES_TOKEN; site_id=RAKUTEN_SITE_ID
    if not token or not site_id: return out
    headers={"Authorization":f"Bearer {token}","Accept":"application/json"}
    for path in ["/linking/v1/offer","/linking/v1/links"]:
        try:
            r=requests.get(f"https://api.rakutenadvertising.com{path}?siteId={site_id}&pageSize={limit}",headers=headers,timeout=12,verify=False)
            if r.status_code==200:
                items=r.json().get("data") or r.json().get("links") or r.json().get("offers") or []
                for item in items[:limit]:
                    dest=item.get("destinationUrl") or item.get("linkUrl") or item.get("url")
                    mid=item.get("advertiserId") or item.get("mid")
                    dl=generate_rakuten_deeplink(mid,dest)
                    if dl: out.append(dl)
        except Exception: logger.exception("Rakuten error")
    if len(out)<limit and site_id:
        out.append(f"https://click.linksynergy.com/deeplink?id={site_id}&murl={quote_plus('https://www.rakuten.com')}")
    return out[:limit]
# ---------- Caption ----------
def generate_caption_using_openai(url,title=None):
    if not OPENAI_API_KEY: return f"Hot deal — check this out: {url}"
    try:
        payload={"model":"gpt-4o-mini","messages":[
            {"role":"system","content":"Generate one short caption with emoji and hashtags."},
            {"role":"user","content":f"{title or ''} {url}"}], "max_tokens":80,"temperature":0.8}
        r=requests.post("https://api.openai.com/v1/chat/completions",
                        headers={"Authorization":f"Bearer {OPENAI_API_KEY}","Content-Type":"application/json"},
                        json=payload,timeout=12)
        txt=r.json()["choices"][0]["message"]["content"].strip()
        return txt if txt else f"Hot deal — check this out: {url}"
    except Exception: return f"Hot deal — check this out: {url}"

# ---------- HeyGen ----------
def generate_heygen_video(text):
    if not HEYGEN_API_KEY: return None
    try:
        r=requests.post("https://api.heygen.com/v1/video/generate",
                        headers={"x-api-key":HEYGEN_API_KEY,"Content-Type":"application/json"},
                        json={"type":"avatar","script":{"type":"text","input":text},"avatar":"default","voice":{"language":"en-US"}},timeout=30)
        if r.status_code in (200,201): return r.json().get("video_url")
    except Exception: logger.exception

    # ---------- Worker loop ----------
def post_next_pending():
    conn, cur = get_db_conn()
    cur.execute("SELECT * FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1")
    row = cur.fetchone()
    if not row:
        conn.close()
        return False

    post_id, url = row["id"], row["url"]
    caption = generate_caption_using_openai(url)
    video_url = generate_heygen_video(caption)
    final_url = f"{APP_PUBLIC_URL}/r/{post_id}" if APP_PUBLIC_URL else url

    ok_fb = post_fb(caption, final_url)
    ok_ig = post_ig(caption, final_url)
    ok_x = post_x(caption, final_url)
    ok_ifttt = post_ifttt(caption, final_url)
    ok_tg = post_telegram(caption, final_url)

    cur.execute(
        "UPDATE posts SET status='sent', posted_at=%s, meta=%s WHERE id=%s",
        (datetime.now(timezone.utc), json.dumps({"video": video_url}), post_id),
    )
    conn.commit()
    conn.close()
    logger.info("POST sent id=%s final=%s video=%s", post_id, final_url, bool(video_url))
    return True

def refresh_all_sources():
    logger.info("Refreshing affiliate sources (rotation)")
    for src, tag in ROTATION:
        if src == "awin":
            links = pull_awin_deeplinks(1)
        else:
            links = pull_rakuten_deeplinks(1)
        saved = save_links_to_db(links, f"{src}:{tag}")
        logger.info("Saved %s validated links from %s:%s", saved, src, tag)
    logger.info("REFRESH complete")

def worker_loop():
    global _worker_running, _stop_requested
    _worker_running = True
    _stop_requested = False
    logger.info("Worker starting — cadence: %s seconds", POST_INTERVAL_SECONDS)
    while not _stop_requested:
        try:
            ok = post_next_pending()
            if not ok:
                logger.info("No pending posts, sleeping %s sec", SLEEP_ON_EMPTY)
                time.sleep(SLEEP_ON_EMPTY)
            else:
                time.sleep(POST_INTERVAL_SECONDS)
        except Exception:
            logger.exception("worker_loop error")
            time.sleep(30)
    _worker_running = False
    logger.info("Worker stopped")

def start_worker_background():
    import threading
    threading.Thread(target=worker_loop, daemon=True).start()

def stop_worker():
    global _stop_requested
    _stop_requested = True
# ---------- Stats / Health ----------
def get_stats():
    conn, cur = get_db_conn()
    cur.execute("SELECT COUNT(*) FROM posts"); total = cur.fetchone()["count"]
    cur.execute("SELECT COUNT(*) FROM posts WHERE status='pending'"); pending = cur.fetchone()["count"]
    cur.execute("SELECT COUNT(*) FROM posts WHERE status='sent'"); sent = cur.fetchone()["count"]
    cur.execute("SELECT COUNT(*) FROM clicks"); clicks = cur.fetchone()["count"]
    cur.execute("SELECT MAX(posted_at) FROM posts WHERE posted_at IS NOT NULL"); last = cur.fetchone()["max"]
    conn.close()
    return {
        "total": total,
        "pending": pending,
        "sent": sent,
        "clicks_total": clicks,
        "last_posted_at": last.isoformat() if last else None,
    }

def health_summary():
    return {
        "fb_disabled": DISABLE_FB,
        "ig_disabled": DISABLE_IG,
        "x_disabled": DISABLE_X,
        "tiktok_disabled": DISABLE_TIKTOK,
        "telegram_disabled": DISABLE_TELEGRAM,
        "youtube_disabled": DISABLE_YOUTUBE,
        "worker_running": _worker_running,
    }

def pause_channel(name, pause=True):
    global DISABLE_FB, DISABLE_IG, DISABLE_X, DISABLE_TIKTOK, DISABLE_TELEGRAM, DISABLE_YOUTUBE
    if name == "fb": DISABLE_FB = pause
    elif name == "ig": DISABLE_IG = pause
    elif name == "x": DISABLE_X = pause
    elif name == "tiktok": DISABLE_TIKTOK = pause
    elif name == "telegram": DISABLE_TELEGRAM = pause
    elif name == "youtube": DISABLE_YOUTUBE = pause
