# worker.py - v7.3 $10M AUTOPILOT ENGINE (AI + YouTube Upload + IFTTT + 100 Shorts/Day)
import os
import requests
import openai
from datetime import datetime
import psycopg
from psycopg.rows import dict_row
import tweepy
import time
import json
import tempfile
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# CONFIG
DB_URL = os.getenv('DATABASE_URL')
OPENAI_KEY = os.getenv('OPENAI_API_KEY')
openai.api_key = OPENAI_KEY

TWITTER_BEARER = os.getenv('TWITTER_BEARER_TOKEN')
client = tweepy.Client(bearer_token=TWITTER_BEARER)

IFTTT_KEY = os.getenv('IFTTT_KEY')

HEYGEN_KEY = os.getenv('HEYGEN_API_KEY')

PAYSTACK_KEY = os.getenv('PAYSTACK_SECRET_KEY')

# DATABASE
def get_db():
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# Awin Offers
def get_awin_offers():
    token = os.getenv('AWIN_API_TOKEN')
    publisher_id = os.getenv('AWIN_PUBLISHER_ID')
    if not token or not publisher_id:
        return []
    url = f"https://productdata.awin.com/datafeed/download/apiv5/{publisher_id}/csv/"
    headers = {"Authorization": f"Bearer {token}", "User-Agent": "AutoAffiliateAI-v7.3"}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            lines = r.text.splitlines()[1:50]  # More offers for scaling
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

# Rakuten Offers
def get_rakuten_offers():
    return [
        {'product': 'Gymshark Leggings', 'link': 'https://rakuten.link/gymshark123', 'image': 'https://i.imgur.com/gymshark.jpg', 'commission': '12%'},
        # Add more placeholder or real offers for scaling
    ]

# Generate AI Post
def generate_post(offer):
    prompt = f"Write a 150-char viral affiliate post for {offer['product']} at {offer['commission']} commission. Use emojis, urgency, CTA. Link: {offer['link']}"
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=80
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"[OPENAI] Error: {e}")
        return f"🔥 70% OFF {offer['product']}! Shop now: {offer['link']} #ad"

# Generate Short Video with HeyGen
# In worker.py — generate_short_video()
def generate_short_video(offer):
    heygen_key = os.getenv('HEYGEN_API_KEY')
    if not heygen_key:
        return 'placeholder_short.mp4'
    
    url = "https://api.heygen.com/v1/video/generate"
    payload = {
        "script": generate_post(offer)[:500],
        "avatar_id": "Daisy",  # Your avatar
        "background_id": "gym_bg",
        "voice_id": "en_us_1"
    }
    headers = {"Authorization": f"Bearer {heygen_key}"}
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

# Post to X
def post_to_x(content):
    try:
        client.create_tweet(text=content[:280])
        print(f"[X] Posted: {content[:50]}...")
    except Exception as e:
        print(f"[X] Failed: {e}")

# Post via IFTTT
def post_via_ifttt(platform, content, image_url):
    url = f"https://maker.ifttt.com/trigger/{platform}_post/with/key/{IFTTT_KEY}"
    data = {"value1": content, "value2": image_url}
    try:
        requests.post(url, json=data, timeout=10)
        print(f"[{platform.upper()}] Sent via IFTTT")
    except Exception as e:
        print(f"[{platform.upper()}] IFTTT Failed: {e}")

# YouTube Shorts Upload
def upload_youtube_short(title, description, video_path):
    if not os.path.exists('youtube_token.json'):
        print("[YT] Token missing — auth first")
        return None

    with open('youtube_token.json') as f:
        creds = Credentials.from_authorized_user_info(json.load(f))
    youtube = build('youtube', 'v3', credentials=creds)

    body = {
        'snippet': {'title': title, 'description': description, 'tags': ['affiliate', 'sale', 'fitness'], 'categoryId': '22'},
        'status': {'privacyStatus': 'public'}
    }
    media = MediaFileUpload(video_path, chunksize=-1, resumable=True)
    request = youtube.videos().insert(part='snippet,status', body=body, media_body=media)
    response = request.execute()
    print(f"[YT] Uploaded Short: {response['id']}")
    return response['id']

# Telegram Alert
def send_telegram(message):
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    if not bot_token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        requests.post(url, data={'chat_id': chat_id, 'text': message})
    except: pass

# Referral System (₦500/user)
def process_referral(referrer_id, referred_email):
    conn, cur = get_db()
    cur.execute("SELECT id FROM users WHERE email = %s", (referred_email,))
    referred = cur.fetchone()
    if referred:
        cur.execute("INSERT INTO referrals (referrer_id, referred_email, reward) VALUES (%s, %s, 500)", (referrer_id, referred_email))
        conn.commit()
        send_telegram(f"New Referral: ₦500 added to {referrer_id}")
    conn.close()

# Paystack Payouts
def payout_user(user_id, amount):
    bank_account = os.getenv('USER_BANK_ACCOUNT')  # Get from DB in real
    url = "https://api.paystack.co/transfer"
    headers = {"Authorization": f"Bearer {PAYSTACK_KEY}", "Content-Type": "application/json"}
    payload = {
        "source": "balance",
        "amount": amount * 100,
        "recipient": bank_account,
        "reason": "Referral Payout"
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        conn, cur = get_db()
        cur.execute("INSERT INTO earnings (reference, amount, network) VALUES (%s, %s, 'payout')", (response.json()['data']['reference'], -amount))
        conn.commit()
        conn.close()
        send_telegram(f"Payout Complete: ₦{amount} to {user_id}")
        return True
    return False

# MAIN CAMPAIGN (100 SHORTS/DAY)
def run_daily_campaign():
    print(f"[BEAST] v7.3 Campaign started at {datetime.now()}")
    
    offers = get_awin_offers() + get_rakuten_offers()
    if not offers:
        print("[BEAST] No offers found")
        return

    posts_today = 0
    for offer in offers[:100]:  # 100 Shorts/Day
        content = generate_post(offer)
        post_to_x(content)
        post_via_ifttt('instagram', content, offer['image'])
        post_via_ifttt('tiktok', content, offer['image'])
        time.sleep(30)  # Rate limit

        # Generate & Upload YouTube Short
        video_path = generate_short_video(offer)
        short_title = f"{offer['product']} Deal! {posts_today + 1}"
        short_desc = content
        video_id = upload_youtube_short(short_title, short_desc, video_path)
        if video_id:
            conn, cur = get_db()
            cur.execute("INSERT INTO posts (platform, content, link, status) VALUES (%s, %s, %s, 'sent')", ('youtube', short_desc, video_id))
            conn.commit()
            conn.close()
        posts_today += 1

    print(f"[BEAST] Campaign complete! {posts_today} posts/short sent")
    send_telegram(f"Beast Complete: {posts_today} posts/short live! $10M Mode ON")
