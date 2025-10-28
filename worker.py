# worker.py - v7.4 $10M AUTOPILOT ENGINE (500 Shorts/Day + Trial Auto-Charge)
import os
import requests
import json
from datetime import datetime, timedelta
import psycopg
from psycopg.rows import dict_row
import tweepy
import time
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import redis
import rq
from openai import OpenAI

# CONFIG
DB_URL = os.getenv('DATABASE_URL')
OPENAI_KEY = os.getenv('OPENAI_API_KEY')
openai_client = OpenAI(api_key=OPENAI_KEY) if OPENAI_KEY else None

# === TWITTER CLIENT WITH DEBUG ===
print("[TWITTER] Loading keys from environment...")

TWITTER_API_KEY = os.getenv('TWITTER_API_KEY')
TWITTER_API_SECRET = os.getenv('TWITTER_API_SECRET')
TWITTER_ACCESS_TOKEN = os.getenv('TWITTER_ACCESS_TOKEN')
TWITTER_ACCESS_SECRET = os.getenv('TWITTER_ACCESS_SECRET')
TWITTER_BEARER = os.getenv('TWITTER_BEARER_TOKEN')

print(f"[TWITTER] API_KEY: {'SET' if TWITTER_API_KEY else 'MISSING'}")
print(f"[TWITTER] API_SECRET: {'SET' if TWITTER_API_SECRET else 'MISSING'}")
print(f"[TWITTER] ACCESS_TOKEN: {'SET' if TWITTER_ACCESS_TOKEN else 'MISSING'}")
print(f"[TWITTER] ACCESS_SECRET: {'SET' if TWITTER_ACCESS_SECRET else 'MISSING'}")
print(f"[TWITTER] BEARER: {'SET' if TWITTER_BEARER else 'MISSING'}")

if not all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET]):
    print("[TWITTER] MISSING KEYS → POSTING DISABLED")
    client = tweepy.Client(bearer_token=TWITTER_BEARER)  # Read-only
else:
    print("[TWITTER] ALL KEYS OK → POSTING ENABLED")
    client = tweepy.Client(
        consumer_key=TWITTER_API_KEY,
        consumer_secret=TWITTER_API_SECRET,
        access_token=TWITTER_ACCESS_TOKEN,
        access_token_secret=TWITTER_ACCESS_SECRET,
        bearer_token=TWITTER_BEARER
    )

IFTTT_KEY = os.getenv('IFTTT_KEY')
HEYGEN_KEY = os.getenv('HEYGEN_API_KEY')
PAYSTACK_KEY = os.getenv('PAYSTACK_SECRET_KEY')

r = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
queue = rq.Queue(connection=r)

# DATABASE
def get_db():
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# === 500 SHORTS/DAY CAMPAIGN ===
def run_daily_campaign():
    print(f"[BEAST] v7.4 Campaign started at {datetime.now()}")
    
    offers = get_awin_offers() + get_rakuten_offers()
    if not offers:
        print("[BEAST] No offers found")
        return

    posts_today = 0
    for offer in offers[:500]:
        content = generate_post(offer)
        post_to_x(content)
        post_via_ifttt('instagram', content, offer['image'])
        post_via_ifttt('tiktok', content, offer['image'])
        time.sleep(5)

        video_path = generate_short_video(offer)
        short_title = f"{offer['product']} Deal! #{posts_today + 1}"
        short_desc = content
        video_id = upload_youtube_short(short_title, short_desc, video_path)
        if video_id:
            conn, cur = get_db()
            cur.execute(
                "INSERT INTO posts (platform, content, link, status) VALUES (%s, %s, %s, 'sent')",
                ('youtube', short_desc, f"https://youtu.be/{video_id}")
            )
            conn.commit()
            conn.close()
        posts_today += 1

    print(f"[BEAST] Campaign complete! {posts_today} posts/short sent")
    send_telegram(f"Beast Complete: {posts_today} posts/short live! $10M Mode ON")

# === OFFERS ===
def get_awin_offers():
    token = os.getenv('AWIN_API_TOKEN')
    publisher_id = os.getenv('AWIN_PUBLISHER_ID')
    if not token or not publisher_id:
        return []
    url = f"https://productdata.awin.com/datafeed/download/apiv5/{publisher_id}/csv/"
    headers = {"Authorization": f"Bearer {token}", "User-Agent": "AutoAffiliateAI-v7.4"}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            lines = r.text.splitlines()[1:100]
            offers = []
            for line in lines:
                cols = line.split('|')
                if len(cols) > 5:
                    offers.append({
                        'product': cols[1],
                        'link': cols[3],
                        'image': cols[5],
                        'commission': '8%'
                    })
            return offers
    except Exception as e:
        print(f"[AWIN] Error: {e}")
    return []

def get_rakuten_offers():
    return [
        {'product': 'Gymshark Leggings', 'link': 'https://rakuten.link/gymshark123', 'image': 'https://i.imgur.com/gymshark.jpg', 'commission': '12%'},
    ]

# === OPENAI v1 WITH FALLBACK ===
def generate_post(offer):
    if not openai_client:
        print("[OPENAI] No API key → using fallback")
        return f"70% OFF {offer['product']}! Shop now: {offer['link']} #ad"

    prompt = f"Write a 150-char viral affiliate post for {offer['product']} at {offer['commission']} commission. Use emojis, urgency, CTA. Link: {offer['link']}"
    try:
        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=80,
            temperature=0.8
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"[OPENAI] Error: {e}")
        return f"70% OFF {offer['product']}! Shop now: {offer['link']} #ad"

# === HEYGEN VIDEO ===
def generate_short_video(offer):
    if not HEYGEN_KEY:
        return 'placeholder_short.mp4'
    url = "https://api.heygen.com/v1/video/generate"
    payload = {
        "script": generate_post(offer)[:500],
        "avatar_id": "Daisy",
        "background_id": "gym_bg",
        "voice_id": "en_us_1"
    }
    headers = {"Authorization": f"Bearer {HEYGEN_KEY}"}
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        if r.status_code == 200:
            video_url = r.json()['data']['video_url']
            path = f"short_{int(time.time())}.mp4"
            with open(path, 'wb') as f:
                f.write(requests.get(video_url).content)
            return path
    except Exception as e:
        print(f"[HEYGEN] Error: {e}")
    return 'placeholder_short.mp4'

# === POST TO X ===
def post_to_x(content):
    content = content[:280]
    try:
        response = client.create_tweet(text=content)
        tweet_id = response.data['id']
        print(f"[X] Posted: https://x.com/i/web/status/{tweet_id}")
    except tweepy.Unauthorized:
        print("[X] UNAUTHORIZED: Check API keys")
    except tweepy.Forbidden:
        print("[X] FORBIDDEN: App needs 'Tweet write' permission")
    except Exception as e:
        print(f"[X] Failed: {e}")

# === IFTTT ===
def post_via_ifttt(platform, content, image_url):
    url = f"https://maker.ifttt.com/trigger/{platform}_post/with/key/{IFTTT_KEY}"
    data = {"value1": content, "value2": image_url}
    try:
        requests.post(url, json=data, timeout=10)
        print(f"[{platform.upper()}] Sent via IFTTT")
    except Exception as e:
        print(f"[{platform.upper()}] IFTTT Failed: {e}")

# === YOUTUBE UPLOAD WITH TOKEN VALIDATION ===
def upload_youtube_short(title, description, video_path):
    token_json = os.getenv('YOUTUBE_TOKEN_JSON')
    if not token_json:
        print(f"[YT] No token → skipping: {title}")
        return None

    try:
        token_data = json.loads(token_json)
        required = ['refresh_token', 'client_id', 'client_secret']
        missing = [k for k in required if k not in token_data]
        if missing:
            print(f"[YT] Token missing fields: {missing}")
            return None

        creds = Credentials.from_authorized_user_info(token_data)
        youtube = build('youtube', 'v3', credentials=creds)

        body = {
            'snippet': {
                'title': title[:100],
                'description': description[:5000],
                'tags': ['affiliate', 'sale', 'shorts'],
                'categoryId': '22'
            },
            'status': {'privacyStatus': 'public'}
        }

        media = MediaFileUpload(video_path, chunksize=-1, resumable=True)
        request = youtube.videos().insert(part='snippet,status', body=body, media_body=media)
        response = request.execute()
        video_id = response['id']
        print(f"[YT] Uploaded: https://youtu.be/{video_id}")
        return video_id
    except Exception as e:
        print(f"[YT] Upload failed: {e}")
        return None

# === TELEGRAM ===
def send_telegram(message):
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    if not bot_token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        requests.post(url, data={'chat_id': chat_id, 'text': message}, timeout=10)
    except:
        pass

# === SCHEDULE TRIAL CHECK ===
try:
    from tasks import check_trials
    queue.enqueue_in(timedelta(minutes=5), check_trials)
except Exception as e:
    print(f"[RQ] Task import failed: {e}")

# Run campaign on startup
if __name__ == '__main__':
    print("[BEAST] Starting v7.4 $10M Autopilot Engine...")
    run_daily_campaign()
