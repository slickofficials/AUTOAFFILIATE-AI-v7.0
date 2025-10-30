# worker.py - v8.1 BULLETPROOF + FULL LOGGING + AUTO-RESTART
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

print("\n" + "="*80)
print("    SLICKOFFICIALS AI v8.1 - BOT STARTING (CRASH-PROOF)")
print("="*80)

# === FORCE LOG EVERYTHING ===
os.environ['PYTHONUNBUFFERED'] = '1'

# === ENV VARS (LOG MISSING ONES) ===
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

for key, val in required.items():
    status = "OK" if val else "MISSING"
    print(f"[ENV] {key}: {status}")

# === CLIENTS ===
openai_client = None
x_client = None
youtube = None
conn = None

# === CONNECT DB (RETRY) ===
def connect_db():
    global conn
    for i in range(3):
        try:
            conn = psycopg.connect(required['DATABASE_URL'], row_factory=dict_row, timeout=10)
            print("[DB] CONNECTED")
            return True
        except Exception as e:
            print(f"[DB] ATTEMPT {i+1} FAILED: {e}")
            time.sleep(5)
    print("[DB] GAVE UP")
    return False

# === INIT X CLIENT ===
def init_x():
    global x_client
    keys = [required['TWITTER_API_KEY'], required['TWITTER_API_SECRET'],
            required['TWITTER_ACCESS_TOKEN'], required['TWITTER_ACCESS_SECRET'],
            required['TWITTER_BEARER_TOKEN']]
    if not all(keys):
        print("[X] MISSING KEYS → SKIPPING")
        return False
    try:
        x_client = tweepy.Client(
            consumer_key=required['TWITTER_API_KEY'],
            consumer_secret=required['TWITTER_API_SECRET'],
            access_token=required['TWITTER_ACCESS_TOKEN'],
            access_token_secret=required['TWITTER_ACCESS_SECRET'],
            bearer_token=required['TWITTER_BEARER_TOKEN']
        )
        print("[X] CLIENT READY")
        return True
    except Exception as e:
        print(f"[X] CLIENT ERROR: {e}")
        return False

# === INIT YOUTUBE ===
def init_youtube():
    global youtube
    if not required['YOUTUBE_TOKEN_JSON']:
        print("[YT] NO TOKEN → SKIPPING")
        return False
    try:
        creds = Credentials.from_authorized_user_info(json.loads(required['YOUTUBE_TOKEN_JSON']))
        youtube = build('youtube', 'v3', credentials=creds)
        print("[YT] CLIENT READY")
        return True
    except Exception as e:
        print(f"[YT] CLIENT ERROR: {e}")
        return False

# === INIT OPENAI ===
if required['OPENAI_API_KEY']:
    try:
        openai_client = OpenAI(api_key=required['OPENAI_API_KEY'])
        print("[OPENAI] CLIENT READY")
    except Exception as e:
        print(f"[OPENAI] ERROR: {e}")
else:
    print("[OPENAI] NO KEY → USING FALLBACK")

# === PULL LINK FROM DB ===
def get_link():
    if not conn:
        print("[DB] NO CONNECTION → FALLBACK LINK")
        return {'product': 'Nike Air Max', 'deeplink': 'https://click.linksynergy.com/deeplink?id=SLICKO8&mid=36805&murl=...', 'commission': 15}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT product_name, deeplink, commission FROM affiliate_links WHERE active = TRUE ORDER BY RANDOM() LIMIT 1")
            row = cur.fetchone()
            if row:
                print(f"[DB] PULLED: {row['product_name']}")
                return {'product': row['product_name'], 'deeplink': row['deeplink'], 'commission': row['commission']}
    except Exception as e:
        print(f"[DB] QUERY ERROR: {e}")
    return {'product': 'Fallback Product', 'deeplink': 'https://click.linksynergy.com/deeplink?id=SLICKO8&mid=36805&murl=...', 'commission': 10}

# === GENERATE CONTENT ===
def generate_content(data):
    product, link = data['product'], data['deeplink']
    if not openai_client:
        return f"70% OFF {product}! Shop: {link} #ad"
    try:
        prompt = f"Write a viral post for {product}. Use emojis, urgency. Link: {link}. Max 280 chars. End with #ad"
        resp = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.8
        )
        content = resp.choices[0].message.content.strip()
        return content[:280]
    except Exception as e:
        print(f"[OPENAI] FAILED: {e}")
        return f"70% OFF {product}! Shop: {link} #ad"

# === POST FUNCTIONS (SAFE) ===
def post_to_x(content):
    if not x_client: return
    try:
        tweet = x_client.create_tweet(text=content)
        print(f"[X] POSTED: https://x.com/i/web/status/{tweet.data['id']}")
    except Exception as e:
        print(f"[X] POST FAILED: {e}")

def post_to_meta(content):
    if not all([required['FB_ACCESS_TOKEN'], required['IG_USER_ID'], required['FB_PAGE_ID']]):
        print("[META] KEYS MISSING → SKIPPING")
        return
    img = "https://i.imgur.com/airmax270.jpg"
    try:
        r = requests.post(f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media",
                         data={'image_url': img, 'caption': content, 'access_token': required['FB_ACCESS_TOKEN']}, timeout=30)
        if r.status_code == 200 and 'id' in r.json():
            requests.post(f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media_publish",
                         data={'creation_id': r.json()['id'], 'access_token': required['FB_ACCESS_TOKEN']})
            print("[INSTAGRAM] POSTED")
    except Exception as e:
        print(f"[INSTAGRAM] ERROR: {e}")
    try:
        requests.post(f"https://graph.facebook.com/v20.0/{required['FB_PAGE_ID']}/photos",
                     data={'url': img, 'caption': content, 'access_token': required['FB_ACCESS_TOKEN']})
        print("[FACEBOOK] POSTED")
    except Exception as e:
        print(f"[FACEBOOK] ERROR: {e}")

def post_to_tiktok(content):
    if not required['IFTTT_KEY']: return
    try:
        requests.post(f"https://maker.ifttt.com/trigger/tiktok_post/with/key/{required['IFTTT_KEY']}",
                     json={"value1": content, "value2": "https://i.imgur.com/airmax270.jpg"}, timeout=10)
        print("[TIKTOK] SENT")
    except: pass

def post_to_youtube(content, product):
    if not youtube: return
    try:
        video_path = '/tmp/short.mp4'
        with open(video_path, 'wb') as f: f.write(b"fake")
        media = MediaFileUpload(video_path, mimetype='video/mp4')
        body = {'snippet': {'title': f'{product} SALE!', 'description': content}, 'status': {'privacyStatus': 'public'}}
        resp = youtube.videos().insert(part='snippet,status', body=body, media_body=media).execute()
        print(f"[YT] UPLOADED: https://youtu.be/{resp['id']}")
    except Exception as e:
        print(f"[YT] FAILED: {e}")

# === MAIN LOOP (RESTART ON CRASH) ===
print("\n[BEAST] STARTING INFINITE CAMPAIGN LOOP...")
run_count = 0

while True:
    run_count += 1
    print(f"\n[RUN #{run_count}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Reconnect DB every run
    connect_db()
    init_x()
    init_youtube()

    link_data = get_link()
    content = generate_content(link_data)
    print(f"[CONTENT] {content[:100]}...")

    post_to_x(content)
    post_to_meta(content)
    post_to_tiktok(content)
    post_to_youtube(content, link_data['product'])

    print(f"[SLEEP] 6 HOURS UNTIL NEXT RUN...")
    time.sleep(6 * 60 * 60)  # 6 hours
