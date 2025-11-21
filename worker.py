import os, time, json, logging, threading, requests, psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus
from typing import List, Optional

from flask import Flask, request, jsonify

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

# ---------- Logging ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("worker")

# ---------- Environment ----------
DB_URL = os.getenv("DATABASE_URL")

AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")
AWIN_AFFILIATE_ID = os.getenv("AWIN_AFFILIATE_ID")
AWIN_CLICKREF = os.getenv("AWIN_CLICKREF", "autoaffiliate")

RAKUTEN_SITE_ID = os.getenv("RAKUTEN_SITE_ID")
RAKUTEN_APP_TOKEN_KEY = os.getenv("RAKUTEN_APP_TOKEN_KEY")
RAKUTEN_REFRESH_TOKEN = os.getenv("RAKUTEN_REFRESH_TOKEN")
RAKUTEN_CLICKREF = os.getenv("RAKUTEN_CLICKREF", "autoaffiliate")

OPENAI_KEY = os.getenv("OPENAI_API_KEY")
HEYGEN_KEY = os.getenv("HEYGEN_API_KEY")

FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_TOKEN = os.getenv("FB_ACCESS_TOKEN")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
IFTTT_KEY = os.getenv("IFTTT_KEY")

DEFAULT_CADENCE_SECONDS = int(os.getenv("DEFAULT_CADENCE_SECONDS", "1800"))

openai_client = OpenAI(api_key=OPENAI_KEY) if (OPENAI_KEY and OpenAI) else None

_worker_running = False
_stop_requested = False

app = Flask(__name__)
def get_db_conn():
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

def run_write(sql, params=()):
    conn, cur = get_db_conn()
    try:
        cur.execute(sql, params); conn.commit()
    except Exception:
        conn.rollback(); logger.exception("DB write failed"); raise
    finally: conn.close()

def run_read(sql, params=()):
    conn, cur = get_db_conn()
    cur.execute(sql, params); rows = cur.fetchall(); conn.close(); return rows

def ensure_tables():
    schema = """
    CREATE TABLE IF NOT EXISTS posts (
      id SERIAL PRIMARY KEY, url TEXT UNIQUE NOT NULL, source TEXT,
      status TEXT DEFAULT 'pending', created_at TIMESTAMPTZ DEFAULT now(),
      posted_at TIMESTAMPTZ, meta JSONB DEFAULT '{}'::jsonb);
    CREATE TABLE IF NOT EXISTS failed_links (
      id SERIAL PRIMARY KEY, source TEXT, attempted_url TEXT,
      reason TEXT, created_at TIMESTAMPTZ DEFAULT now());
    CREATE TABLE IF NOT EXISTS social_logs (
      id SERIAL PRIMARY KEY, platform TEXT, source TEXT, url TEXT,
      payload JSONB, status TEXT, created_at TIMESTAMPTZ DEFAULT now());
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY, setting_key TEXT UNIQUE, value TEXT);
    """
    conn, cur = get_db_conn(); cur.execute(schema); conn.commit(); conn.close()

ensure_tables()
def requests_get(url, **kwargs): kwargs.setdefault("timeout", 15); return requests.get(url, **kwargs)
def is_valid_https_url(u): return bool(u and u.startswith("https://") and len(u)<4000)
def contains_affiliate_id(u):
    if not u: return False
    u=u.lower()
    return any(s in u for s in [str(AWIN_PUBLISHER_ID), str(AWIN_AFFILIATE_ID), str(RAKUTEN_SITE_ID),
                                "linksynergy","awin","rakuten","tidd.ly","trk."])
def follow_and_check(u):
    try: r=requests_get(u,allow_redirects=True); return r.url
    except: logger.exception("follow_and_check failed"); return None
def is_live_url(u):
    try: r=requests.get(u,timeout=10); return r.status_code==200
    except: return False
def validate_and_normalize_link(u):
    f=follow_and_check(u) if not is_valid_https_url(u) else u
    return f if f and is_valid_https_url(f) and contains_affiliate_id(f) and is_live_url(f) else None
def log_failed_link(u,src,reason):
    try: run_write("INSERT INTO failed_links(source,attempted_url,reason) VALUES(%s,%s,%s)",(src,u,reason))
    except: logger.exception("log_failed_link failed")
def save_links_to_db(links,source="affiliate"):
    if not links: return 0
    conn,cur=get_db_conn(); added=0
    for l in links:
        norm=validate_and_normalize_link(l)
        if not norm: log_failed_link(l,source,"Invalid"); continue
        try: cur.execute("INSERT INTO posts(url,source,status,created_at) VALUES(%s,%s,'pending',%s) ON CONFLICT DO NOTHING",(norm,source,datetime.now(timezone.utc))); added+=1
        except: conn.rollback(); log_failed_link(norm,source,"Insert fail")
    conn.commit(); conn.close(); return added
def pull_awin_deeplinks(limit=4):
    out=[]
    if AWIN_PUBLISHER_ID and AWIN_AFFILIATE_ID:
        for _ in range(limit):
            url=f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid={AWIN_AFFILIATE_ID}&clickref={AWIN_CLICKREF}"
            r=requests_get(url,allow_redirects=True); f=r.url
            if f and validate_and_normalize_link(f): out.append(f)
            else: log_failed_link(f or url,"awin","Fallback invalid")
    return out

_rakuten_access_token=None; _rakuten_token_expiry=0
def rakuten_refresh_access_token():
    global _rakuten_access_token,_rakuten_token_expiry
    scope=RAKUTEN_SITE_ID
    headers={"Authorization":f"Bearer {RAKUTEN_APP_TOKEN_KEY}","Content-Type":"application/x-www-form-urlencoded"}
    body=f"grant_type=refresh_token&refresh_token={quote_plus(RAKUTEN_REFRESH_TOKEN)}&scope={quote_plus(scope)}"
    r=requests.post("https://api.linksynergy.com/token",headers=headers,data=body,timeout=20)
    j=r.json(); tok=j.get("access_token"); ttl=int(j.get("expires_in",3600))
    _rakuten_access_token=tok; _rakuten_token_expiry=time.time()+ttl-60
    os.environ["RAKUTEN_ACCESS_TOKEN"]=tok; os.environ["RAKUTEN_REFRESH_TOKEN"]=j.get("refresh_token",RAKUTEN_REFRESH_TOKEN)
    return tok
def get_rakuten_access_token():
    return _rakuten_access_token if _rakuten_access_token and time.time()<_rakuten_token_expiry else rakuten_refresh_access_token()
def rakuten_product_search(keyword,max_results=10):
    tok=get_rakuten_access_token(); 
    if not tok: return []
    url=f"https://api.linksynergy.com/productsearch?keyword={quote_plus(keyword)}&max={int(max_results)}"
    headers={"Authorization":f"Bearer {tok}"}
    resp=requests.get(url,headers=headers,timeout=20)
    if resp.status_code!=200: log_failed_link(url,"rakuten",f"HTTP {resp.status_code}"); return []
    data=resp.json(); links=[]
    for item in data.get("data",[]):
        link=item.get("linkUrl") or item.get("url")
        if link and validate_and_normalize_link(link): links.append(link)
    return links
def generate_caption(link: str) -> str:
    if not openai_client: return f"Hot deal — check this out: {link}"
    try:
        prompt=f"Create a short energetic social caption (one sentence, include 1 emoji, 1 CTA) for:\n{link}"
        resp=openai_client.chat.completions.create(model="gpt-4o-mini",messages=[{"role":"user","content":prompt}],max_tokens=60)
        text=resp.choices[0].message.content.strip() if resp and resp.choices else ""
        if not text: text=f"Hot deal — check this out: {link}"
        if link not in text: text=f"{text} {link}"
        return text
    except: logger.exception("OpenAI caption failed"); return f"Hot deal — check this out: {link}"
def post_to_facebook(message: str, link: str) -> dict:
    if not FB_TOKEN or not FB_PAGE_ID: return {"error":"FB creds not set"}
    try:
        url=f"https://graph.facebook.com/{FB_PAGE_ID}/feed"
        resp=requests.post(url,data={"message":message,"link":link,"access_token":FB_TOKEN},timeout=15)
        data=resp.json() if resp.headers.get("content-type","").startswith("application/json") else {"status_code":resp.status_code}
        if resp.status_code==200 and "id" in data: logger.info("Posted to Facebook: %s",data["id"])
        else: log_failed_link(link,"facebook",f"HTTP {resp.status_code}")
        return data
    except Exception:
        logger.exception("FB posting error"); log_failed_link(link,"facebook","Exception"); return {"error":"fb_exception"}

def post_to_twitter(message: str, link: str) -> dict:
    if not TWITTER_BEARER_TOKEN: return {"error":"Twitter bearer not set"}
    try:
        url="https://api.twitter.com/2/tweets"
        headers={"Authorization":f"Bearer {TWITTER_BEARER_TOKEN}"}
        resp=requests.post(url,headers=headers,json={"text":f"{message} {link}"},timeout=20)
        data=resp.json() if resp.headers.get("content-type","").startswith("application/json") else {"status_code":resp.status_code}
        if resp.status_code in (200,201): logger.info("Posted to Twitter: %s",data.get("data",{}).get("id"))
        else: log_failed_link(link,"twitter",f"HTTP {resp.status_code}")
        return data
    except Exception:
        logger.exception("Twitter posting error"); log_failed_link(link,"twitter","Exception"); return {"error":"twitter_exception"}

def post_to_telegram(message: str, link: str) -> dict:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return {"error":"Telegram creds not set"}
    try:
        url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp=requests.post(url,data={"chat_id":TELEGRAM_CHAT_ID,"text":f"{message} {link}"},timeout=10)
        data=resp.json() if resp.headers.get("content-type","").startswith("application/json") else {"status_code":resp.status_code}
        if resp.status_code==200 and data.get("ok"): logger.info("Posted to Telegram")
        else: log_failed_link(link,"telegram",f"HTTP {resp.status_code}")
        return data
    except Exception:
        logger.exception("Telegram posting error"); log_failed_link(link,"telegram","Exception"); return {"error":"telegram_exception"}

def post_to_ifttt(event_name: str, value1: str, value2="", value3="") -> dict:
    if not IFTTT_KEY: return {"error":"IFTTT_KEY not set"}
    try:
        url=f"https://maker.ifttt.com/trigger/{event_name}/with/key/{IFTTT_KEY}"
        resp=requests.post(url,json={"value1":value1,"value2":value2,"value3":value3},timeout=10)
        if resp.status_code in (200,202): logger.info("Triggered IFTTT event: %s",event_name)
        else: log_failed_link(value1,"ifttt",f"HTTP {resp.status_code}")
        return {"status_code":resp.status_code}
    except Exception:
        logger.exception("IFTTT posting error"); log_failed_link(value1,"ifttt","Exception"); return {"error":"ifttt_exception"}
def pull_and_post():
    for source, sub in [("awin","B"),("rakuten","2")]:
        links=[]
        if source=="awin": links=pull_awin_deeplinks(limit=4)
        elif source=="rakuten": links=rakuten_product_search("laptop",max_results=4)
        if not links:
            log_failed_link(f"{source}-batch",source,"No links pulled"); continue
        save_links_to_db(links,source=source)
        for link in links:
            caption=generate_caption(link)
            _=generate_video(caption,link)  # optional
            fb=post_to_facebook(caption,link)
            tw=post_to_twitter(caption,link)
            tg=post_to_telegram(caption,link)
            ifttt=post_to_ifttt("new_affiliate_link",link,caption)
            logger.info("Post results: FB=%s TW=%s TG=%s IFTTT=%s",fb,tw,tg,ifttt)
            if any([not fb.get("error"),not tw.get("error"),not tg.get("error"),not ifttt.get("error")]):
                run_write("UPDATE posts SET status='posted',posted_at=%s WHERE url=%s",(datetime.now(timezone.utc),link))
            else: log_failed_link(link,source,"All platform posts failed")
def start_worker_background():
    global _worker_running,_stop_requested
    if _worker_running: return
    _worker_running=True
    while not _stop_requested:
        try: pull_and_post()
        except Exception: logger.exception("Worker iteration failed")
        time.sleep(DEFAULT_CADENCE_SECONDS)
    _worker_running=False

def stop_worker(): 
    global _stop_requested; _stop_requested=True

@app.route("/status")
def status(): 
    return jsonify({"running":_worker_running,"interval":DEFAULT_CADENCE_SECONDS})
@app.route("/start",methods=["POST"])
def start(): threading.Thread(target=start_worker_background,daemon=True).start(); return jsonify({"message":"Worker started"})
@app.route("/stop",methods=["POST"])
def stop(): stop_worker(); return jsonify({"message":"Worker stop requested"})
def main():
    ensure_tables()
    logger.info("Worker loaded, posts: %s", run_read("SELECT COUNT(*) as cnt FROM posts")[0]["cnt"])
    port=int(os.getenv("PORT","5000"))
    app.run(host="0.0.0.0",port=port)

if __name__=="__main__":
    try: main()
    except KeyboardInterrupt: stop_worker(); logger.info("Worker stopped via KeyboardInterrupt")
