# worker.py - v11.2 FINAL: 11 LINKS → AUTO POSTS → PAID CLICKS 24/7
import os, sys, time, random, requests
from datetime import datetime
from openai import OpenAI
import tweepy

# === LOGGING ===
os.environ['PYTHONUNBUFFERED'] = '1'
def log(msg): 
    print(f"[MONEY] {datetime.now().strftime('%H:%M:%S')} | {msg}")
    sys.stdout.flush()

log("SLICKOFFICIALS v11.2 — 11 LINKS AUTO EMPIRE")

# === ENV KEYS ===
required = {k: os.getenv(k) for k in [
    'DATABASE_URL', 'OPENAI_API_KEY', 'TWITTER_API_KEY', 'TWITTER_API_SECRET',
    'TWITTER_ACCESS_TOKEN', 'TWITTER_ACCESS_SECRET', 'TWITTER_BEARER_TOKEN',
    'FB_ACCESS_TOKEN', 'IG_USER_ID', 'FB_PAGE_ID', 'IFTTT_KEY'
]}

# === CLIENTS ===
openai_client = OpenAI(api_key=required['OPENAI_API_KEY']) if required['OPENAI_API_KEY'] else None

# === FALLBACK LINK (IF DB EMPTY) ===
FALLBACK = ("Mystery Deal", "https://tidd.ly/3J1KeV2")

# === MAIN AUTO LOOP ===
while True:
    log("RUN STARTED — PULLING PAID LINK")
    
    # === PULL FROM DB (FIXED: DICT ROWS) ===
    product, deeplink = FALLBACK
    try:
        import psycopg
        from psycopg.rows import dict_row
        conn = psycopg.connect(required['DATABASE_URL'], row_factory=dict_row)
        with conn.cursor() as cur:
            cur.execute("SELECT product_name, deeplink FROM affiliate_links WHERE active = TRUE ORDER BY RANDOM() LIMIT 1")
            row = cur.fetchone()
            if row:
                product = row['product_name']
                deeplink = row['deeplink']
                log(f"PRODUCT: {product}")
                log(f"PAID LINK: {deeplink}")
            else:
                log("NO LINKS IN DB — USING FALLBACK")
    except Exception as e: 
        log(f"DB ERROR: {e} → USING FALLBACK")

    # === GENERATE VIRAL AI POST WITH LINK ===
    content = f"70% OFF {product}! Shop now: {deeplink} #ad"
    if openai_client:
        try:
            resp = openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": f"Make this viral, urgent, exciting: {content}"}],
                max_tokens=100
            )
            content = resp.choices[0].message.content.strip()
            if deeplink not in content:
                content = f"{content.split('#ad')[0].strip()} {deeplink} #ad"
            content = content[:280]
        except Exception as e:
            log(f"OPENAI ERROR: {e}")
    log(f"POST: {content}")

    # === POST TO X (WITH RETRY + RATE LIMIT FIX) ===
    for attempt in range(3):
        try:
            x = tweepy.Client(
                consumer_key=required['TWITTER_API_KEY'],
                consumer_secret=required['TWITTER_API_SECRET'],
                access_token=required['TWITTER_ACCESS_TOKEN'],
                access_token_secret=required['TWITTER_ACCESS_SECRET'],
                bearer_token=required['TWITTER_BEARER_TOKEN']
            )
            tweet = x.create_tweet(text=content)
            log(f"X POSTED: https://x.com/i/web/status/{tweet.data['id']}")
            break
        except Exception as e:
            log(f"X ATTEMPT {attempt+1} FAILED: {e}")
            if "429" in str(e):
                log("RATE LIMIT — SLEEPING 60 SEC")
                time.sleep(60)
            else:
                break

    # === POST TO FACEBOOK & INSTAGRAM ===
    img = "https://i.imgur.com/airmax270.jpg"
    if all([required['FB_ACCESS_TOKEN'], required['IG_USER_ID'], required['FB_PAGE_ID']]):
        try:
            r = requests.post(
                f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media",
                data={'image_url': img, 'caption': content, 'access_token': required['FB_ACCESS_TOKEN']},
                timeout=30
            )
            if r.status_code == 200 and 'id' in r.json():
                requests.post(
                    f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media_publish",
                    data={'creation_id': r.json()['id'], 'access_token': required['FB_ACCESS_TOKEN']}
                )
                log("INSTAGRAM POSTED")
        except Exception as e: log(f"IG ERROR: {e}")

        try:
            requests.post(
                f"https://graph.facebook.com/v20.0/{required['FB_PAGE_ID']}/photos",
                data={'url': img, 'caption': content, 'access_token': required['FB_ACCESS_TOKEN']},
                timeout=30
            )
            log("FACEBOOK POSTED")
        except Exception as e: log(f"FB ERROR: {e}")

    # === POST TO TIKTOK VIA IFTTT ===
    if required['IFTTT_KEY']:
        try:
            requests.post(
                f"https://maker.ifttt.com/trigger/tiktok_post/with/key/{required['IFTTT_KEY']}",
                json={"value1": content, "value2": img},
                timeout=30
            )
            log("TIKTOK SENT")
        except Exception as e: log(f"TIKTOK ERROR: {e}")

    # === SLEEP 6 HOURS ===
    log("RUN COMPLETE — SLEEPING 6 HOURS")
    time.sleep(6 * 60 * 60)
