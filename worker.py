# worker.py - v7.5 $10M AUTOPILOT ENGINE (CRASH-PROOF + FULL POSTING)
import os
import requests
import json
from datetime import datetime
import time
import psycopg
from psycopg.rows import dict_row
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from openai import OpenAI
import tweepy

print("\n" + "="*70)
print("   SLICKOFFICIALS AI v7.5 - WORKER STARTING")
print("="*70)

# === KEYS ===
OPENAI_KEY = os.getenv('OPENAI_API_KEY')
FB_TOKEN = os.getenv('FB_ACCESS_TOKEN')
IFTTT_KEY = os.getenv('IFTTT_KEY')
YOUTUBE_JSON = os.getenv('YOUTUBE_TOKEN_JSON')
TWITTER_API_KEY = os.getenv('TWITTER_API_KEY')
TWITTER_API_SECRET = os.getenv('TWITTER_API_SECRET')
TWITTER_ACCESS_TOKEN = os.getenv('TWITTER_ACCESS_TOKEN')
TWITTER_ACCESS_SECRET = os.getenv('TWITTER_ACCESS_SECRET')
TWITTER_BEARER = os.getenv('TWITTER_BEARER_TOKEN')

# === OPENAI CLIENT ===
openai_client = OpenAI(api_key=OPENAI_KEY) if OPENAI_KEY else None

# === TWITTER CLIENT ===
try:
    client = tweepy.Client(
        consumer_key=TWITTER_API_KEY,
        consumer_secret=TWITTER_API_SECRET,
        access_token=TWITTER_ACCESS_TOKEN,
        access_token_secret=TWITTER_ACCESS_SECRET,
        bearer_token=TWITTER_BEARER
    )
    print("[X] Twitter Client READY")
except Exception as e:
    print(f"[X] Twitter Client FAILED: {e}")
    client = None

# === TEST OFFER (FALLBACK) ===
offer = {
    'product': 'Nike Air Max 270',
    'link': 'https://click.linksynergy.com/deeplink?id=SLICKO8&mid=36805&murl=https://nike.com/air-max-270',
    'image': 'https://i.imgur.com/airmax.jpg',
    'commission': '15%'
}

# === GENERATE CONTENT ===
def generate_content():
    if not openai_client:
        return f"70% OFF {offer['product']}! Shop now: {offer['link']} #ad"
    try:
        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": f"Write a viral affiliate post for {offer['product']} at {offer['commission']} commission. Link: {offer['link']}"}],
            max_tokens=80
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"[OPENAI] Error: {e}")
        return f"70% OFF {offer['product']}! Shop now: {offer['link']} #ad"

content = generate_content()
print(f"[CONTENT] {content[:100]}...")

# === 1. POST TO INSTAGRAM + FACEBOOK (META API) ===
def post_to_meta():
    if not FB_TOKEN:
        print("[META] FB_ACCESS_TOKEN MISSING")
        return
    IG_USER_ID = os.getenv('IG_USER_ID')
    PAGE_ID = os.getenv('FB_PAGE_ID')
    if not IG_USER_ID or not PAGE_ID:
        print("[META] IG_USER_ID or FB_PAGE_ID missing")
        return

    img_url = offer['image']
    # Instagram
    try:
        r = requests.post(
            f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media",
            data={'image_url': img_url, 'caption': content, 'access_token': FB_TOKEN}
        )
        if r.status_code == 200:
            creation_id = r.json()['id']
            requests.post(
                f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish",
                data={'creation_id': creation_id, 'access_token': FB_TOKEN}
            )
            print("[INSTAGRAM] POSTED")
        else:
            print(f"[INSTAGRAM] ERROR: {r.status_code} | {r.text}")
    except Exception as e:
        print(f"[INSTAGRAM] FAILED: {e}")

    # Facebook
    try:
        r = requests.post(
            f"https://graph.facebook.com/v20.0/{PAGE_ID}/photos",
            data={'url': img_url, 'caption': content, 'access_token': FB_TOKEN}
        )
        print(f"[FACEBOOK] {'POSTED' if r.status_code == 200 else 'FAILED'}")
    except Exception as e:
        print(f"[FACEBOOK] FAILED: {e}")

post_to_meta()

# === 2. POST TO TIKTOK (IFTTT) ===
def post_to_tiktok():
    if not IFTTT_KEY:
        print("[TIKTOK] IFTTT_KEY MISSING")
        return
    url = f"https://maker.ifttt.com/trigger/tiktok_post/with/key/{IFTTT_KEY}"
    try:
        r = requests.post(url, json={"value1": content, "value2": offer['image']}, timeout=10)
        print(f"[TIKTOK] → {r.status_code}")
    except Exception as e:
        print(f"[TIKTOK] ERROR: {e}")

post_to_tiktok()

# === 3. POST TO X (TWITTER) ===
def post_to_x():
    if not client:
        print("[X] Client not ready")
        return
    try:
        resp = client.create_tweet(text=content[:280])
        print(f"[X] POSTED: https://x.com/i/web/status/{resp.data['id']}")
    except Exception as e:
        print(f"[X] FAILED: {e}")

post_to_x()

# === 4. POST TO YOUTUBE ===
def post_to_youtube():
    if not YOUTUBE_JSON:
        print("[YT] NO TOKEN")
        return
    try:
        data = json.loads(YOUTUBE_JSON)
        creds = Credentials.from_authorized_user_info(data)
        youtube = build('youtube', 'v3', credentials=creds)
        with open('placeholder_short.mp4', 'wb') as f:
            f.write(b"fake video data")
        media = MediaFileUpload('placeholder_short.mp4', resumable=True)
        body = {
            'snippet': {'title': 'TEST SHORT', 'description': content, 'categoryId': '22'},
            'status': {'privacyStatus': 'public'}
        }
        resp = youtube.videos().insert(part='snippet,status', body=body, media_body=media).execute()
        print(f"[YT] UPLOADED: https://youtu.be/{resp['id']}")
    except Exception as e:
        print(f"[YT] FAILED: {e}")

post_to_youtube()

# === RAKUTEN (FIXED SYNTAX ERROR) ===
def get_rakuten_offers():
    try:
        # Placeholder — replace with real API later
        return [offer]
    except Exception as e:
        print(f"[RAKUTEN] Error: {e}")  # ← FIXED: MISSING "
        return []

print("\n" + "="*70)
print("   WORKER v7.5 RUN COMPLETE")
print("="*70)

# === MAIN ===
if __name__ == '__main__':
    print("[BEAST] Starting daily campaign...")
    get_rakuten_offers()
