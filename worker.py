# worker.py - v12.0 FINAL: FULL AUTO DEEP LINKS → X, IG, FB, TIKTOK → PAID CLICKS 24/7
import os
import sys
import time
import random
import requests
from datetime import datetime

# === FORCE PRINTS ===
os.environ['PYTHONUNBUFFERED'] = '1'
def log(msg):
    print(f"[MONEY] {datetime.now().strftime('%H:%M:%S')} | {msg}")
    sys.stdout.flush()

log("SLICKOFFICIALS v12.0 — FULL AUTO DEEP LINK EMPIRE STARTED")

# === IMPORTS WITH ERROR HANDLING ===
try:
    import psycopg
    from psycopg.rows import dict_row
    from openai import OpenAI
    import tweepy
    log("ALL MODULES LOADED")
except Exception as e:
    log(f"IMPORT FAILED: {e}")
    sys.exit(1)

# === ENV VARIABLES ===
required = {
    'DATABASE_URL': os.getenv('DATABASE_URL'),
    'OPENAI_API_KEY': os.getenv('OPENAI_API_KEY'),
    'TWITTER_API_KEY': os.getenv('TWITTER_API_KEY'),
    'TWITTER_API_SECRET': os.getenv('TWITTER_API_SECRET'),
    'TWITTER_ACCESS_TOKEN': os.getenv('TWITTER_ACCESS_TOKEN'),
    'TWITTER_ACCESS_SECRET': os.getenv('TWITTER_ACCESS_SECRET'),
    'TWITTER_BEARER_TOKEN': os.getenv('TWITTER_BEARER_TOKEN'),
    'FB_ACCESS_TOKEN': os.getenv('FB_ACCESS_TOKEN'),
    'IG_USER_ID': os.getenv('IG_USER_ID'),
    'FB_PAGE_ID': os.getenv('FB_PAGE_ID'),
    'IFTTT_KEY': os.getenv('IFTTT_KEY')
}

# === CLIENTS ===
openai_client = OpenAI(api_key=required['OPENAI_API_KEY']) if required['OPENAI_API_KEY'] else None

# === FALLBACK (IF DB DOWN) ===
FALLBACK_PRODUCT = "Kila Custom Insoles"
FALLBACK_LINK = "https://tidd.ly/3J1KeV2"

# === MAIN LOOP ===
while True:
    log("RUN STARTED — PULLING DEEP LINK FROM DB")

    # === PULL DEEP LINK FROM DB (AWIN + RAKUTEN) ===
    product = FALLBACK_PRODUCT
    deeplink = FALLBACK_LINK

    try:
        conn = psycopg.connect(required['DATABASE_URL'], row_factory=dict_row)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT product_name, deeplink 
                FROM affiliate_links 
                WHERE active = TRUE 
                  AND network IN ('awin', 'rakuten')
                ORDER BY RANDOM() 
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                product = row['product_name']
                deeplink = row['deeplink']
                log(f"PRODUCT: {product}")
                log(f"PAID LINK: {deeplink}")
            else:
                log("NO ACTIVE LINKS IN DB — USING FALLBACK")
    except Exception as e:
        log(f"DB ERROR: {e} → USING FALLBACK")

    # === GENERATE VIRAL AI CONTENT WITH LINK ===
    base_content = f"70% OFF {product}! Shop now: {deeplink} #ad"
    content = base_content

    if openai_client:
        try:
            prompt = f"Write a viral, urgent, exciting social media post. Must include this EXACT link: {deeplink}. Max 270 chars. End with #ad"
            resp = openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=120
            )
            ai_text = resp.choices[0].message.content.strip()
            if deeplink in ai_text:
                content = ai_text[:280]
            else:
                content = f"{ai_text.split('#ad')[0].strip()} {deeplink} #ad"
            content = content[:280]
            log("AI CONTENT GENERATED")
        except Exception as e:
            log(f"OPENAI FAILED: {e}")
    log(f"POST: {content}")

    # === POST TO X (WITH 429 RETRY) ===
    x_posted = False
    for attempt in range(3):
        try:
            client = tweepy.Client(
                consumer_key=required['TWITTER_API_KEY'],
                consumer_secret=required['TWITTER_API_SECRET'],
                access_token=required['TWITTER_ACCESS_TOKEN'],
                access_token_secret=required['TWITTER_ACCESS_SECRET'],
                bearer_token=required['TWITTER_BEARER_TOKEN']
            )
            tweet = client.create_tweet(text=content)
            log(f"X POSTED: https://x.com/i/web/status/{tweet.data['id']}")
            x_posted = True
            break
        except Exception as e:
            error_msg = str(e)
            log(f"X ATTEMPT {attempt+1} FAILED: {error_msg}")
            if "429" in error_msg:
                log("RATE LIMIT — SLEEPING 60 SEC")
                time.sleep(60)
            else:
                break

    # === POST TO INSTAGRAM & FACEBOOK ===
    img_url = "https://i.imgur.com/airmax270.jpg"  # Replace with your image
    if all([required['FB_ACCESS_TOKEN'], required['IG_USER_ID'], required['FB_PAGE_ID']]):
        # Instagram
        try:
            r = requests.post(
                f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media",
                data={
                    'image_url': img_url,
                    'caption': content,
                    'access_token': required['FB_ACCESS_TOKEN']
                },
                timeout=30
            )
            if r.status_code == 200 and 'id' in r.json():
                requests.post(
                    f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media_publish",
                    data={'creation_id': r.json()['id'], 'access_token': required['FB_ACCESS_TOKEN']},
                    timeout=30
                )
                log("INSTAGRAM POSTED")
        except Exception as e:
            log(f"INSTAGRAM ERROR: {e}")

        # Facebook Page
        try:
            requests.post(
                f"https://graph.facebook.com/v20.0/{required['FB_PAGE_ID']}/photos",
                data={
                    'url': img_url,
                    'caption': content,
                    'access_token': required['FB_ACCESS_TOKEN']
                },
                timeout=30
            )
            log("FACEBOOK POSTED")
        except Exception as e:
            log(f"FACEBOOK ERROR: {e}")

    # === POST TO TIKTOK VIA IFTTT ===
    if required['IFTTT_KEY']:
        try:
            requests.post(
                f"https://maker.ifttt.com/trigger/tiktok_post/with/key/{required['IFTTT_KEY']}",
                json={"value1": content, "value2": img_url},
                timeout=30
            )
            log("TIKTOK SENT")
        except Exception as e:
            log(f"TIKTOK ERROR: {e}")

    # === DONE ===
    log("RUN COMPLETE — SLEEPING 6 HOURS")
    time.sleep(6 * 60 * 60)  # 6 HOURS
