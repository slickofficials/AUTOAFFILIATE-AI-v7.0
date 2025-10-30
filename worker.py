# worker.py - v8.0 INFINITE LOOP + ALL PLATFORMS + DB LINKS
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
import random

print("\n" + "="*80)
print("    SLICKOFFICIALS AI v8.0 - BOT SERVICE STARTED (INFINITE LOOP)")
print("="*80)

# === ENV VARS ===
OPENAI_KEY = os.getenv('OPENAI_API_KEY')
DB_URL = os.getenv('DATABASE_URL')
FB_TOKEN = os.getenv('FB_ACCESS_TOKEN')
IG_USER_ID = os.getenv('IG_USER_ID')
FB_PAGE_ID = os.getenv('FB_PAGE_ID')
IFTTT_KEY = os.getenv('IFTTT_KEY')
YOUTUBE_JSON = os.getenv('YOUTUBE_TOKEN_JSON')

TWITTER_API_KEY = os.getenv('TWITTER_API_KEY')
TWITTER_API_SECRET = os.getenv('TWITTER_API_SECRET')
TWITTER_ACCESS_TOKEN = os.getenv('TWITTER_ACCESS_TOKEN')
TWITTER_ACCESS_SECRET = os.getenv('TWITTER_ACCESS_SECRET')
TWITTER_BEARER_TOKEN = os.getenv('TWITTER_BEARER_TOKEN')

# === CLIENTS ===
openai_client = OpenAI(api_key=OPENAI_KEY) if OPENAI_KEY else None
x_client = None
youtube = None

# === CONNECT DB ===
def connect_db():
    try:
        conn = psycopg.connect(DB_URL, row_factory=dict_row)
        print("[DB] Connected")
        return conn
    except Exception as e:
        print(f"[DB] FAILED: {e}")
        return None

conn = connect_db()

# === X CLIENT ===
def init_x():
    global x_client
    if all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET, TWITTER_BEARER_TOKEN]):
        try:
            x_client = tweepy.Client(
                consumer_key=TWITTER_API_KEY,
                consumer_secret=TWITTER_API_SECRET,
                access_token=TWITTER_ACCESS_TOKEN,
                access_token_secret=TWITTER_ACCESS_SECRET,
                bearer_token=TWITTER_BEARER_TOKEN
            )
            print("[X] Client READY")
        except Exception as e:
            print(f"[X] ERROR: {e}")
    else:
        print("[X] Missing keys")

init_x()

# === YOUTUBE ===
if YOUTUBE_JSON:
    try:
        creds = Credentials.from_authorized_user_info(json.loads(YOUTUBE_JSON))
        youtube = build('youtube', 'v3', credentials=creds)
        print("[YT] Client READY")
    except Exception as e:
        print(f"[YT] ERROR: {e}")

# === PULL RANDOM LINK ===
def get_link():
    global conn
    if not conn:
        conn = connect_db()
    if not conn:
        return {'product': 'Nike Air Max', 'deeplink': 'https://click.linksynergy.com/deeplink?id=SLICKO8&mid=36805&murl=...', 'commission': 15}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT product_name, deeplink, commission FROM affiliate_links WHERE active = TRUE ORDER BY RANDOM() LIMIT 1")
            row = cur.fetchone()
            if row:
                return {'product': row['product_name'], 'deeplink': row['deeplink'], 'commission': row['commission']}
    except Exception as e:
        print(f"[DB] ERROR: {e}")
    return {'product': 'Fallback', 'deeplink': 'https://click.linksynergy.com/deeplink?id=SLICKO8&mid=36805&murl=...', 'commission': 10}

# === GENERATE CONTENT ===
def generate_content(link_data):
    product, link, comm = link_data['product'], link_data['deeplink'], link_data['commission']
    if not openai_client:
        return f"70% OFF {product}! Shop: {link} #ad"
    try:
        prompt = f"Write a viral post for {product} at {comm}% commission. Use urgency, emojis. Link: {link}. Max 280 chars. End with #ad"
        resp = openai_client.chat.completions.create(model="gpt-3.5-turbo", messages=[{"role": "user", "content": prompt}], max_tokens=100)
        content = resp.choices[0].message.content.strip()
        return content[:280] if len(content) <= 280 else content[:277] + "..."
    except Exception as e:
        print(f"[OPENAI] ERROR: {e}")
        return f"70% OFF {product}! Shop: {link} #ad"

# === POST FUNCTIONS ===
def post_to_x(content):
    if not x_client: return
    try:
        tweet = x_client.create_tweet(text=content)
        print(f"[X] POSTED: https://x.com/i/web/status/{tweet.data['id']}")
    except Exception as e:
        print(f"[X] FAILED: {e}")

def post_to_meta(content):
    if not all([FB_TOKEN, IG_USER_ID, FB_PAGE_ID]): return
    img = "https://i.imgur.com/airmax270.jpg"
    try:
        ig = requests.post(f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media", data={'image_url': img, 'caption': content, 'access_token': FB_TOKEN}).json()
        if 'id' in ig:
            requests.post(f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish", data={'creation_id': ig['id'], 'access_token': FB_TOKEN})
            print("[INSTAGRAM] POSTED")
    except: pass
    try:
        requests.post(f"https://graph.facebook.com/v20.0/{FB_PAGE_ID}/photos", data={'url': img, 'caption': content, 'access_token': FB_TOKEN})
        print("[FACEBOOK] POSTED")
    except: pass

def post_to_tiktok(content):
    if not IFTTT_KEY: return
    try:
        requests.post(f"https://maker.ifttt.com/trigger/tiktok_post/with/key/{IFTTT_KEY}", json={"value1": content, "value2": "https://i.imgur.com/airmax270.jpg"})
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

# === MAIN LOOP ===
while True:
    print(f"\n[BEAST] CAMPAIGN RUN @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    link_data = get_link()
    content = generate_content(link_data)
    print(f"[CONTENT] {content[:100]}...")
    
    post_to_x(content)
    post_to_meta(content)
    post_to_tiktok(content)
    post_to_youtube(content, link_data['product'])
    
    print(f"[SLEEP] Waiting 6 hours...")
    time.sleep(6 * 60 * 60)  # 6 hours
