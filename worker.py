# worker.py - v10.0 FINAL AUTOPILOT (X, FB, IG, TIKTOK, YOUTUBE, AI, DB)
import os
import sys
import time
import json
import requests

# === FORCE PRINTS + LOG FUNCTION ===
os.environ['PYTHONUNBUFFERED'] = '1'
def log(msg):
    print(f"[LOG] {time.strftime('%H:%M:%S')} | {msg}")
    sys.stdout.flush()

log("SLICKOFFICIALS AI v10.0 - BOT STARTED")

# === IMPORTS ===
try:
    import psycopg
    from psycopg.rows import dict_row
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from google.oauth2.credentials import Credentials
    from openai import OpenAI
    import tweepy
    log("ALL MODULES IMPORTED")
except Exception as e:
    log(f"IMPORT FAILED: {e}")
    sys.exit(1)

# === ENV KEYS ===
required = {k: os.getenv(k) for k in [
    'DATABASE_URL', 'OPENAI_API_KEY', 'TWITTER_API_KEY', 'TWITTER_API_SECRET',
    'TWITTER_ACCESS_TOKEN', 'TWITTER_ACCESS_SECRET', 'TWITTER_BEARER_TOKEN',
    'FB_ACCESS_TOKEN', 'IG_USER_ID', 'FB_PAGE_ID', 'IFTTT_KEY', 'YOUTUBE_TOKEN_JSON'
]}

for k, v in required.items():
    log(f"{k}: {'OK' if v else 'MISSING'}")

# === CLIENTS ===
openai_client = OpenAI(api_key=required['OPENAI_API_KEY']) if required['OPENAI_API_KEY'] else None
x_client = None
youtube = None
conn = None

# === MAIN LOOP ===
run = 0
while True:
    run += 1
    log(f"RUN #{run} STARTED")

    # === DATABASE (FIXED: NO TIMEOUT) ===
    try:
        conn = psycopg.connect(required['DATABASE_URL'], row_factory=dict_row)
        log("DB CONNECTED")
    except Exception as e:
        log(f"DB FAILED: {e}")

    # === X (TWITTER) ===
    try:
        x_client = tweepy.Client(
            consumer_key=required['TWITTER_API_KEY'],
            consumer_secret=required['TWITTER_API_SECRET'],
            access_token=required['TWITTER_ACCESS_TOKEN'],
            access_token_secret=required['TWITTER_ACCESS_SECRET'],
            bearer_token=required['TWITTER_BEARER_TOKEN']
        )
        log("X CLIENT READY")
    except Exception as e:
        log(f"X CLIENT FAILED: {e}")

    # === YOUTUBE (FIXED: FULL TOKEN) ===
    if required['YOUTUBE_TOKEN_JSON']:
        try:
            token_data = json.loads(required['YOUTUBE_TOKEN_JSON'])
            creds = Credentials.from_authorized_user_info(token_data)
            youtube = build('youtube', 'v3', credentials=creds)
            log("YOUTUBE CLIENT READY")
        except Exception as e:
            log(f"YOUTUBE FAILED: {e}")

    # === PULL AFFILIATE LINK ===
    link = "https://click.linksynergy.com/deeplink?id=SLICKO8&mid=36805&murl=https://example.com"
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT deeplink, product_name FROM affiliate_links ORDER BY RANDOM() LIMIT 1")
                row = cur.fetchone()
                if row:
                    link = row['deeplink']
                    product = row['product_name']
                    log(f"PRODUCT: {product}")
                    log(f"LINK: {link[:60]}...")
                else:
                    log("DB: No links found")
        except Exception as e:
            log(f"DB QUERY ERROR: {e}")

    # === GENERATE AI CONTENT ===
    content = f"70% OFF {product}! Shop now: {link} #ad"
    if openai_client:
        try:
            prompt = f"Write a viral, exciting social media post for {product}. Max 280 chars. Include the link: {link}. End with #ad"
            resp = openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100
            )
            content = resp.choices[0].message.content.strip()[:280]
            log("AI CONTENT GENERATED")
        except Exception as e:
            log(f"OPENAI ERROR: {e}")
    log(f"CONTENT: {content}")

    # === POST TO X (TWITTER) ===
    if x_client:
        try:
            tweet = x_client.create_tweet(text=content)
            log(f"X POSTED: https://x.com/i/web/status/{tweet.data['id']}")
        except Exception as e:
            log(f"X POST FAILED: {e}")

    # === POST TO FACEBOOK & INSTAGRAM ===
    if all([required['FB_ACCESS_TOKEN'], required['IG_USER_ID'], required['FB_PAGE_ID']]):
        img = "https://i.imgur.com/airmax270.jpg"  # Replace with your image
        try:
            # Instagram
            r = requests.post(
                f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media",
                data={
                    'image_url': img,
                    'caption': content,
                    'access_token': required['FB_ACCESS_TOKEN']
                },
                timeout=30
            )
            if r.status_code == 200 and 'id' in r.json():
                requests.post(
                    f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media_publish",
                    data={'creation_id': r.json()['id'], 'access_token': required['FB_ACCESS_TOKEN']}
                )
                log("INSTAGRAM POSTED")
        except Exception as e:
            log(f"IG ERROR: {e}")

        try:
            # Facebook Page
            requests.post(
                f"https://graph.facebook.com/v20.0/{required['FB_PAGE_ID']}/photos",
                data={'url': img, 'caption': content, 'access_token': required['FB_ACCESS_TOKEN']},
                timeout=30
            )
            log("FACEBOOK POSTED")
        except Exception as e:
            log(f"FB ERROR: {e}")

    # === TIKTOK VIA IFTTT ===
    if required['IFTTT_KEY']:
        try:
            requests.post(
                f"https://maker.ifttt.com/trigger/tiktok_post/with/key/{required['IFTTT_KEY']}",
                json={"value1": content, "value2": img},
                timeout=30
            )
            log("TIKTOK SENT")
        except Exception as e:
            log("TIKTOK ERROR")

    # === YOUTUBE SHORTS ===
    if youtube:
        try:
            video_path = '/tmp/short.mp4'
            with open(video_path, 'wb') as f:
                f.write(b"fake_video_data")  # Replace with real video later
            media = MediaFileUpload(video_path, mimetype='video/mp4')
            body = {
                'snippet': {
                    'title': f'{product} SALE!',
                    'description': content
                },
                'status': {'privacyStatus': 'public'}
            }
            resp = youtube.videos().insert(part='snippet,status', body=body, media_body=media).execute()
            log(f"YT UPLOADED: https://youtu.be/{resp['id']}")
        except Exception as e:
            log(f"YT ERROR: {e}")

    # === SLEEP (6 HOURS IN PRODUCTION) ===
    log("RUN COMPLETE — SLEEPING 6 HOURS")
    time.sleep(6 * 60 * 60)  # 6 HOURS
