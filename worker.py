# worker.py — v20 FINAL (Talking avatar HeyGen + hourly posts + click tracking)
import os
import time
import logging
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone, timedelta
from openai import OpenAI
import tempfile, shutil, json

# Optional Google for YouTube uploads
try:
    from google.oauth2.credentials import Credentials as GoogleCredentials
    from googleapiclient.discovery import build as gbuild
    from googleapiclient.http import MediaFileUpload
    GOOGLE_AVAILABLE = True
except Exception:
    GOOGLE_AVAILABLE = False

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

DB_URL = os.getenv("DATABASE_URL")

# Affiliate IDs (required)
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")
RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")

# Social & AI keys
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
HEYGEN_API_KEY = os.getenv("HEYGEN_API_KEY")
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
FB_TOKEN = os.getenv("FB_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
IG_TOKEN = os.getenv("IG_TOKEN")
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
YOUR_WHATSAPP = os.getenv("YOUR_WHATSAPP")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
YOUTUBE_TOKEN_JSON = os.getenv("YOUTUBE_TOKEN_JSON")
YOUTUBE_UPLOAD_ENABLED = GOOGLE_AVAILABLE and bool(YOUTUBE_TOKEN_JSON)

# Intervals
POST_INTERVAL_SECONDS = int(os.getenv("POST_INTERVAL_SECONDS", "3600"))  # 1 hour by default
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))   # refresh every hour
SLEEP_ON_EMPTY = int(os.getenv("SLEEP_ON_EMPTY", "300"))

# OpenAI client
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

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
        except Exception as e:
            logger.exception("Twilio send failed: %s", e)
    # Telegram mini-app alert
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            tg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            requests.post(tg_url, json={"chat_id": TELEGRAM_CHAT_ID, "text": f"{title}\n{body}"}, timeout=10)
        except Exception:
            logger.exception("Telegram alert failed")

def is_valid_https_url(url):
    return bool(url and isinstance(url, str) and url.startswith("https://") and len(url) < 2000)

def contains_affiliate_id(url):
    if not url: return False
    u = url.lower()
    if AWIN_PUBLISHER_ID and str(AWIN_PUBLISHER_ID) in u:
        return True
    if RAKUTEN_CLIENT_ID and str(RAKUTEN_CLIENT_ID) in u:
        return True
    return False

def save_links_to_db(links, source="affiliate"):
    if not links:
        return 0
    conn, cur = get_db_conn()
    added = 0
    for link in links:
        try:
            if not is_valid_https_url(link):
                logger.debug("Reject invalid: %s", link); continue
            if not contains_affiliate_id(link):
                logger.debug("Reject not affiliate: %s", link); continue
            cur.execute("INSERT INTO posts (url, source, status, created_at) VALUES (%s,%s,'pending',%s) ON CONFLICT (url) DO NOTHING",
                        (link, source, datetime.now(timezone.utc)))
            added += 1
        except Exception:
            logger.exception("Insert failed for %s", link)
    conn.commit(); conn.close()
    logger.info("Saved %s validated links from %s (attempted %s)", added, source, len(links))
    return added

# AWIN redirect deeplink fallback
def pull_awin_deeplinks(limit=4):
    results = []
    if not AWIN_PUBLISHER_ID:
        logger.debug("No AWIN_PUBLISHER_ID set"); return results
    for _ in range(limit):
        try:
            url = f"https://www.awin1.com/cread.php?awinmid={AWIN_PUBLISHER_ID}&awinaffid=0&clickref=bot"
            r = requests.get(url, allow_redirects=True, timeout=12)
            final = r.url
            if final and is_valid_https_url(final) and contains_affiliate_id(final):
                results.append(final)
            else:
                logger.debug("AWIN redirect not affiliate: %s", final)
        except Exception:
            logger.exception("AWIN pull error"); break
    return results

# Rakuten redirect fallback
def pull_rakuten_deeplinks(limit=4):
    results = []
    if not RAKUTEN_CLIENT_ID:
        logger.debug("No RAKUTEN_CLIENT_ID set"); return results
    for _ in range(limit):
        try:
            url = f"https://click.linksynergy.com/deeplink?id={RAKUTEN_CLIENT_ID}&mid=0&murl=https://example.com"
            r = requests.get(url, allow_redirects=True, timeout=12)
            final = r.url
            if final and is_valid_https_url(final) and contains_affiliate_id(final):
                results.append(final)
            else:
                logger.debug("Rakuten redirect not affiliate: %s", final)
        except Exception:
            logger.exception("Rakuten pull error"); break
    return results

# OpenAI caption
def generate_caption(link):
    if not openai_client:
        return f"🔥 Hot deal — grab it now: {link}"
    try:
        prompt = f"Write one short punchy social caption (1 line) for this affiliate link. Include 1 emoji, 1 hashtag, a CTA, and then the link.\n\nLink: {link}"
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":prompt}],
            max_tokens=80
        )
        caption = resp.choices[0].message.content.strip()
        if link not in caption:
            caption = f"{caption} {link}"
        return caption
    except Exception:
        logger.exception("OpenAI caption failed")
        return f"🔥 Hot deal — grab it now: {link}"

# HeyGen talking avatar generation
def generate_heygen_avatar_video(caption):
    if not HEYGEN_API_KEY:
        logger.debug("HeyGen key not set"); return None
    try:
        url = "https://api.heygen.com/v1/video/generate"
        headers = {"x-api-key": HEYGEN_API_KEY, "Content-Type": "application/json"}
        payload = {
            "type": "avatar",
            "script": {"type":"text","input": caption},
            "avatar": "default",   # can be customized if you have templates
            "voice": {"language":"en-US", "style":"energetic"},
            "output_format": "mp4"
        }
        r = requests.post(url, json=payload, headers=headers, timeout=90)
        if r.status_code in (200,201):
            data = r.json()
            # HeyGen often returns job id; may need polling. Try common keys.
            return data.get("video_url") or data.get("result_url") or data.get("url") or data.get("job_id")
        else:
            logger.warning("HeyGen returned %s: %s", r.status_code, r.text[:300])
    except Exception:
        logger.exception("HeyGen error")
    return None

# Posting helpers (FB/IG/X)
def post_facebook(message):
    if not FB_PAGE_ID or not FB_TOKEN:
        logger.debug("FB not configured"); return False
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
        logger.debug("IG not configured"); return False
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

# YouTube upload (Shorts)
def upload_youtube_shorts(video_url, title, description):
    if not YOUTUBE_UPLOAD_ENABLED:
        logger.debug("YouTube disabled"); return False
    tmpdir = tempfile.mkdtemp(prefix="yts_")
    local_path = os.path.join(tmpdir, "short.mp4")
    try:
        # download
        with requests.get(video_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        creds_info = json.loads(YOUTUBE_TOKEN_JSON)
        creds = GoogleCredentials.from_authorized_user_info(creds_info)
        youtube = gbuild("youtube", "v3", credentials=creds)
        body = {"snippet": {"title": title, "description": description, "tags":["deals","shorts"]}, "status":{"privacyStatus":"public"}}
        media = MediaFileUpload(local_path, chunksize=-1, resumable=True, mimetype="video/mp4")
        req = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        resp = None
        while True:
            status, resp = req.next_chunk()
            if status:
                logger.info("Upload progress: %s%%", int(status.progress()*100))
            if resp:
                break
        video_id = resp.get("id")
        shutil.rmtree(tmpdir, ignore_errors=True)
        return f"https://youtu.be/{video_id}"
    except Exception:
        logger.exception("YouTube upload failed")
        try: shutil.rmtree(tmpdir, ignore_errors=True)
        except: pass
    return False

# Pull & save
def refresh_all_sources():
    logger.info("Refreshing AWIN + Rakuten sources")
    links = []
    try:
        links += pull_awin_deeplinks(limit=4)
    except Exception:
        logger.exception("AWIN failed")
    try:
        links += pull_rakuten_deeplinks(limit=4)
    except Exception:
        logger.exception("Rakuten failed")
    saved = save_links_to_db(links, source="affiliate") if links else 0
    send_alert("REFRESH", f"Pulled {len(links)} links, saved {saved}")
    return saved

def enqueue_manual_link(url):
    if not is_valid_https_url(url):
        raise ValueError("URL must be HTTPS")
    if not contains_affiliate_id(url):
        raise ValueError("URL missing affiliate id")
    inserted = save_links_to_db([url], source="manual")
    return {"inserted": inserted, "url": url}

# Posting pipeline: pick oldest pending, double-validate, generate caption & video, post, update DB
def post_next_pending():
    conn, cur = get_db_conn()
    cur.execute("SELECT id, url FROM posts WHERE status='pending' ORDER BY created_at ASC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if not row:
        logger.debug("No pending posts")
        return False
    post_id = row["id"]
    url = row["url"]
    # final validation
    if not is_valid_https_url(url) or not contains_affiliate_id(url):
        logger.warning("Invalid pending, marking failed: %s", url)
        conn, cur = get_db_conn()
        cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE id=%s", ("failed", datetime.now(timezone.utc), post_id))
        conn.commit(); conn.close()
        return False
    caption = generate_caption(url)
    # Use redirect tracking link for socials so clicks go through /r/<id>
    domain = os.getenv("APP_PUBLIC_URL") or os.getenv("PUBLIC_URL") or ""
    if domain:
        redirect_link = f"{domain.rstrip('/')}/r/{post_id}"
    else:
        # fallback to direct affiliate if no public domain is set
        redirect_link = url
    caption_with_link = f"{caption}\n{redirect_link}"
    # Generate HeyGen talking avatar video
    video_ref = generate_heygen_avatar_video(caption) if HEYGEN_API_KEY else None
    video_host_url = None
    if video_ref:
        # If video_ref is a URL we can upload to YouTube or include it; otherwise log job id
        if video_ref.startswith("http"):
            video_host_url = video_ref
    # Post to social platforms - success if any platform accepted
    success = False
    try:
        if post_facebook(caption_with_link):
            success = True
    except Exception:
        logger.exception("FB post error")
    try:
        if post_instagram(caption_with_link):
            success = True
    except Exception:
        logger.exception("IG post error")
    try:
        if post_twitter(caption + " " + redirect_link):
            success = True
    except Exception:
        logger.exception("Twitter post error")
    # Upload to YouTube Shorts if we have a video URL
    yt_link = None
    if video_host_url and YOUTUBE_UPLOAD_ENABLED:
        try:
            yt_link = upload_youtube_shorts(video_host_url, title="Deal — check this out!", description=caption_with_link)
            if yt_link:
                success = True
        except Exception:
            logger.exception("YouTube upload error")
    # record result in DB
    conn, cur = get_db_conn()
    cur.execute("UPDATE posts SET status=%s, posted_at=%s WHERE id=%s", ("sent" if success else "failed", datetime.now(timezone.utc), post_id))
    conn.commit(); conn.close()
    # Alerts: send to WhatsApp & Telegram
    extra = f" | yt:{yt_link}" if yt_link else ""
    send_alert("POSTED" if success else "POST FAILED", f"{redirect_link}{extra}")
    return success

# Provide stats to app.py dashboard
def get_stats():
    stat = {
        "total_links": 0, "pending":0, "sent":0, "failed":0,
        "last_posted_at": None, "next_post_in_seconds": None, "clicks_total":0
    }
    try:
        conn, cur = get_db_conn()
        cur.execute("SELECT COUNT(*) as c FROM posts")
        stat["total_links"] = cur.fetchone()["c"] or 0
        cur.execute("SELECT COUNT(*) as c FROM posts WHERE status='pending'")
        stat["pending"] = cur.fetchone()["c"] or 0
        cur.execute("SELECT COUNT(*) as c FROM posts WHERE status='sent'")
        stat["sent"] = cur.fetchone()["c"] or 0
        cur.execute("SELECT COUNT(*) as c FROM posts WHERE status='failed'")
        stat["failed"] = cur.fetchone()["c"] or 0
        cur.execute("SELECT posted_at FROM posts WHERE status='sent' ORDER BY posted_at DESC LIMIT 1")
        row = cur.fetchone()
        if row and row["posted_at"]:
            stat["last_posted_at"] = row["posted_at"].astimezone(timezone.utc).isoformat()
        cur.execute("SELECT COUNT(*) as c FROM clicks")
        stat["clicks_total"] = cur.fetchone()["c"] or 0
        conn.close()
        # compute approximate next_post_in_seconds: use POST_INTERVAL_SECONDS minus time since last post (if exists)
        if stat["last_posted_at"]:
            last = datetime.fromisoformat(stat["last_posted_at"])
            last_ts = last.replace(tzinfo=timezone.utc).timestamp()
            now_ts = datetime.now(timezone.utc).timestamp()
            elapsed = now_ts - last_ts
            next_in = max(0, POST_INTERVAL_SECONDS - int(elapsed))
            stat["next_post_in_seconds"] = next_in
        else:
            stat["next_post_in_seconds"] = 0
    except Exception:
        logger.exception("Stats gather failed")
    return stat

# Background loop
_worker_running = False
def start_worker_background():
    global _worker_running
    if _worker_running:
        logger.info("Worker already running"); return
    if not DB_URL:
        logger.error("DATABASE_URL missing; not starting"); return
    _worker_running = True
    logger.info("Worker starting (hourly posts, talking avatar HeyGen)")
    send_alert("WORKER START", "AutoAffiliate worker started (1-hour cadence)")
    next_pull = datetime.now(timezone.utc) - timedelta(seconds=5)
    last_post_time = None
    while True:
        try:
            now = datetime.now(timezone.utc)
            if now >= next_pull:
                try:
                    refresh_all_sources()
                except Exception:
                    logger.exception("Refresh failed")
                next_pull = now + timedelta(minutes=PULL_INTERVAL_MINUTES)
            # attempt posting once per POST_INTERVAL_SECONDS window
            posted = post_next_pending()
            if posted:
                last_post_time = datetime.now(timezone.utc)
                time.sleep(POST_INTERVAL_SECONDS)
            else:
                time.sleep(SLEEP_ON_EMPTY)
        except Exception:
            logger.exception("Worker top-level error")
            time.sleep(60)

if __name__ == "__main__":
    start_worker_background()
