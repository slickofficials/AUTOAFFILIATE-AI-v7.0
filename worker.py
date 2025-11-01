# worker.py - v8.4 — 24 POSTS/DAY + INSTAGRAM + $10M EMPIRE
import os
import sys
import time
import json
import requests
from datetime import datetime

# === LOGGING ===
os.environ['PYTHONUNBUFFERED'] = '1'
def log(msg):
    print(f"[MONEY] {datetime.now().strftime('%H:%M:%S')} | {msg}")
    sys.stdout.flush()

log("SLICKOFFICIALS v8.4 — 24 POSTS/DAY — $10K/MONTH MODE ENGAGED")

# === IMPORTS ===
try:
    import psycopg
    from psycopg.rows import dict_row
    from openai import OpenAI
    import tweepy
    log("MODULES LOADED")
except Exception as e:
    log(f"IMPORT FAILED: {e}")
    sys.exit(1)

# === ENV ===
required = {k: os.getenv(k) for k in [
    'DATABASE_URL', 'OPENAI_API_KEY', 'TWITTER_API_KEY', 'TWITTER_API_SECRET',
    'TWITTER_ACCESS_TOKEN', 'TWITTER_ACCESS_SECRET', 'TWITTER_BEARER_TOKEN',
    'FB_ACCESS_TOKEN', 'IG_USER_ID', 'FB_PAGE_ID', 'IFTTT_KEY'
]}

# === CLIENTS ===
openai_client = OpenAI(api_key=required['OPENAI_API_KEY']) if required['OPENAI_API_KEY'] else None

# === MAIN LOOP — 24 POSTS/DAY ===
while True:
    log("RUN STARTED — PULLING LINK + POSTING TO ALL")

    # === DB LINK ===
    product = "NeckHammock"
    deeplink = "https://tidd.ly/4qyhB2L"
    try:
        conn = psycopg.connect(required['DATABASE_URL'], row_factory=dict_row)
        with conn.cursor() as cur:
            cur.execute("SELECT product_name, deeplink FROM affiliate_links WHERE active = TRUE ORDER BY RANDOM() LIMIT 1")
            row = cur.fetchone()
            if row:
                product = row['product_name']
                deeplink = row['deeplink']
        log(f"PRODUCT: {product}")
        log(f"PAID LINK: {deeplink}")
    except Exception as e:
        log(f"DB ERROR: {e}")

    # === AI CONTENT ===
    content = f"70% OFF {product}! Shop now: {deeplink} #ad"
    if openai_client:
        try:
            resp = openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": f"Viral post for {product}. Include link: {deeplink}. Max 280. #ad"}],
                max_tokens=100
            )
            content = resp.choices[0].message.content.strip()[:270] + f" {deeplink} #ad"
        except Exception as e:
            log(f"OPENAI ERROR: {e}")
    log(f"POST: {content}")

    # === X POST ===
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
    except Exception as e:
        log(f"X ERROR: {e}")

    # === INSTAGRAM POST ===
    if all([required['FB_ACCESS_TOKEN'], required['IG_USER_ID']]):
        img = "https://i.imgur.com/airmax270.jpg"  # Replace with your image
        try:
            r = requests.post(
                f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media",
                params={
                    'image_url': img,
                    'caption': content,
                    'access_token': required['FB_ACCESS_TOKEN']
                },
                timeout=30
            )
            if r.status_code == 200:
                creation_id = r.json()['id']
                requests.post(
                    f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media_publish",
                    params={'creation_id': creation_id, 'access_token': required['FB_ACCESS_TOKEN']},
                    timeout=30
                )
                log("INSTAGRAM POSTED WITH LINK")
            else:
                log(f"INSTAGRAM ERROR: {r.status_code} - {r.text}")
        except Exception as e:
            log(f"INSTAGRAM EXCEPTION: {e}")

    # === FACEBOOK POST ===
    if required['FB_ACCESS_TOKEN'] and required['FB_PAGE_ID']:
        img = "https://i.imgur.com/airmax270.jpg"
        try:
            r = requests.post(
                f"https://graph.facebook.com/v20.0/{required['FB_PAGE_ID']}/photos",
                params={
                    'url': img,
                    'caption': content,
                    'access_token': required['FB_ACCESS_TOKEN']
                },
                timeout=30
            )
            if r.status_code == 200:
                log("FACEBOOK POSTED WITH LINK")
            else:
                log(f"FACEBOOK ERROR: {r.status_code}")
        except Exception as e:
            log(f"FACEBOOK EXCEPTION: {e}")

    # === TIKTOK ===
    if required['IFTTT_KEY']:
        try:
            requests.post(
                f"https://maker.ifttt.com/trigger/tiktok_post/with/key/{required['IFTTT_KEY']}",
                json={"value1": content, "value2": img},
                timeout=30
            )
            log("TIKTOK SENT WITH LINK")
        except Exception as e:
            log(f"TIKTOK ERROR: {e}")

    # === SLEEP 1 HOUR (24 POSTS/DAY) ===
    log("RUN COMPLETE — SLEEPING 1 HOUR")
    time.sleep(60 * 60)  # 1 HOUR
