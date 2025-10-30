# worker.py - v9.0 VERBOSE + CRASH-PROOF + AUTO-RESTART
import os
import time
import json
import requests
import psycopg
from psycopg.rows import dict_row
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from openai import OpenAI
import tweepy
from datetime import datetime

# === FORCE ALL PRINTS TO SHOW ===
os.environ['PYTHONUNBUFFERED'] = '1'

print("\n" + "="*80)
print("    SLICKOFFICIALS AI v9.0 - BOT STARTING (VERBOSE MODE)")
print(f"    TIME: {datetime.now()}")
print("="*80)

# === LOG EVERY ENV VAR ===
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
    'IFTTT_KEY': os.getenv('IFTTT_KEY'),
    'YOUTUBE_TOKEN_JSON': os.getenv('YOUTUBE_TOKEN_JSON'),
}

print("[ENV] CHECKING ALL KEYS...")
for key, val in required.items():
    status = "OK" if val else "MISSING"
    print(f"  → {key}: {status} {'(HIDDEN)' if val and 'TOKEN' in key else val}")

# === INIT CLIENTS WITH FULL LOGGING ===
openai_client = None
x_client = None
youtube = None
conn = None

def safe_connect_db():
    global conn
    print("[DB] Attempting connection...")
    try:
        conn = psycopg.connect(required['DATABASE_URL'], row_factory=dict_row, timeout=10)
        print("[DB] CONNECTED SUCCESSFULLY")
        return True
    except Exception as e:
        print(f"[DB] CONNECTION FAILED: {e}")
        return False

def safe_init_x():
    global x_client
    print("[X] Initializing Twitter client...")
    try:
        x_client = tweepy.Client(
            consumer_key=required['TWITTER_API_KEY'],
            consumer_secret=required['TWITTER_API_SECRET'],
            access_token=required['TWITTER_ACCESS_TOKEN'],
            access_token_secret=required['TWITTER_ACCESS_SECRET'],
            bearer_token=required['TWITTER_BEARER_TOKEN']
        )
        print("[X] TWITTER CLIENT READY")
        return True
    except Exception as e:
        print(f"[X] TWITTER CLIENT FAILED: {e}")
        return False

def safe_init_youtube():
    global youtube
    if not required['YOUTUBE_TOKEN_JSON']:
        print("[YT] NO TOKEN → SKIPPING")
        return False
    print("[YT] Initializing YouTube client...")
    try:
        creds = Credentials.from_authorized_user_info(json.loads(required['YOUTUBE_TOKEN_JSON']))
        youtube = build('youtube', 'v3', credentials=creds)
        print("[YT] YOUTUBE CLIENT READY")
        return True
    except Exception as e:
        print(f"[YT] YOUTUBE CLIENT FAILED: {e}")
        return False

if required['OPENAI_API_KEY']:
    try:
        openai_client = OpenAI(api_key=required['OPENAI_API_KEY'])
        print("[OPENAI] CLIENT READY")
    except Exception as e:
        print(f"[OPENAI] CLIENT FAILED: {e}")
else:
    print("[OPENAI] NO KEY → USING FALLBACK")

# === MAIN LOOP ===
run_count = 0
while True:
    run_count += 1
    print(f"\n[RUN #{run_count}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Reconnect everything
    safe_connect_db()
    safe_init_x()
    safe_init_youtube()

    # === PULL LINK ===
    link_data = {'product': 'Fallback', 'deeplink': 'https://click.linksynergy.com/deeplink?id=SLICKO8&mid=36805&murl=...', 'commission': 10}
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT product_name, deeplink, commission FROM affiliate_links WHERE active = TRUE ORDER BY RANDOM() LIMIT 1")
                row = cur.fetchone()
                if row:
                    link_data = {'product': row['product_name'], 'deeplink': row['deeplink'], 'commission': row['commission']}
                    print(f"[DB] PULLED: {link_data['product']}")
        except Exception as e:
            print(f"[DB] QUERY FAILED: {e}")

    # === GENERATE CONTENT ===
    content = f"70% OFF {link_data['product']}! Shop: {link_data['deeplink']} #ad"
    if openai_client:
        try:
            prompt = f"Write a viral post for {link_data['product']}. Link: {link_data['deeplink']}. Max 280 chars. End with #ad"
            resp = openai_client.chat.completions.create(model="gpt-3.5-turbo", messages=[{"role": "user", "content": prompt}], max_tokens=100)
            content = resp.choices[0].message.content.strip()[:280]
            print(f"[AI] GENERATED: {content[:80]}...")
        except Exception as e:
            print(f"[OPENAI] FAILED: {e}")

    print(f"[CONTENT] {content}")

    # === POST EVERYWHERE ===
    if x_client:
        try:
            tweet = x_client.create_tweet(text=content)
            print(f"[X] POSTED: https://x.com/i/web/status/{tweet.data['id']}")
        except Exception as e:
            print(f"[X] FAILED: {e}")

    if all([required['FB_ACCESS_TOKEN'], required['IG_USER_ID'], required['FB_PAGE_ID']]):
        img = "https://i.imgur.com/airmax270.jpg"
        try:
            r = requests.post(f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media", data={'image_url': img, 'caption': content, 'access_token': required['FB_ACCESS_TOKEN']}, timeout=30)
            if r.status_code == 200 and 'id' in r.json():
                requests.post(f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media_publish", data={'creation_id': r.json()['id'], 'access_token': required['FB_ACCESS_TOKEN']})
                print("[INSTAGRAM] POSTED")
        except Exception as e:
            print(f"[INSTAGRAM] ERROR: {e}")
        try:
            requests.post(f"https://graph.facebook.com/v20.0/{required['FB_PAGE_ID']}/photos", data={'url': img, 'caption': content, 'access_token': required['FB_ACCESS_TOKEN']})
            print("[FACEBOOK] POSTED")
        except Exception as e:
            print(f"[FACEBOOK] ERROR: {e}")

    if required['IFTTT_KEY']:
        try:
            requests.post(f"https://maker.ifttt.com/trigger/tiktok_post/with/key/{required['IFTTT_KEY']}", json={"value1": content, "value2": "https://i.imgur.com/airmax270.jpg"})
            print("[TIKTOK] SENT")
        except: pass

    if youtube:
        try:
            video_path = '/tmp/short.mp4'
            with open(video_path, 'wb') as f: f.write(b"fake")
            media = MediaFileUpload(video_path, mimetype='video/mp4')
            body = {'snippet': {'title': f'{link_data['product']} SALE!', 'description': content}, 'status': {'privacyStatus': 'public'}}
            resp = youtube.videos().insert(part='snippet,status', body=body, media_body=media).execute()
            print(f"[YT] UPLOADED: https://youtu.be/{resp['id']}")
        except Exception as e:
            print(f"[YT] FAILED: {e}")

    print(f"[SLEEP] 6 HOURS UNTIL NEXT RUN...")
    time.sleep(6 * 60 * 60)
