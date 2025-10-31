# worker.py - v7.5 $10M AUTOPILOT ENGINE (X + FB + IG + TIKTOK + YOUTUBE + 500 SHORTS/DAY)
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

IFTTT_KEY = os.getenv('IFTTT_KEY')  # For TikTok

HEYGEN_KEY = os.getenv('HEYGEN_API_KEY')

FB_ACCESS_TOKEN = os.getenv('FB_ACCESS_TOKEN')  # Long-lived Page Token
IG_USER_ID = os.getenv('IG_USER_ID')            # Instagram Business Account ID
FB_PAGE_ID = os.getenv('FB_PAGE_ID')            # Facebook Page ID

# DATABASE
def get_db():
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# Error Retry Wrapper (3 tries)
def with_retry(func, *args, **kwargs):
    retries = 3
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"[RETRY] Failed {func.__name__} (Attempt {attempt+1}/{retries}): {e}")
            time.sleep(5 * (attempt + 1))  # Exponential backoff
    print(f"[RETRY] Gave up on {func.__name__}")
    return None

# Awin Offers
def get_awin_offers():
    token = os.getenv('AWIN_API_TOKEN')
    publisher_id = os.getenv('AWIN_PUBLISHER_ID')
    if not token or not publisher_id:
        return []
    url = f"https://productdata.awin.com/datafeed/download/apiv5/{publisher_id}/csv/"
    headers = {"Authorization": f"Bearer {token}", "User-Agent": "AutoAffiliateAI-v7.5"}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            lines = r.text.splitlines()[1:500]  # 500 for scale
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
    # Placeholder — add real API call with your keys
    return [
        {'product': 'Gymshark Leggings', 'link': 'https://rakuten.link/gymshark123', 'image': 'https://i.imgur.com/gymshark.jpg', 'commission': '12%'}
    ] * 500  # 500 for scale

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
        return f"70% OFF {offer['product']}! Shop now: {offer['link']} #ad"

# Generate Short Video with HeyGen (Error Retry)
def generate_short_video(offer):
    def _generate():
        url = "https://api.heygen.com/v1/video/generate"
        payload = {
            "script": generate_post(offer)[:500],
            "avatar_id": "Daisy",
            "background_id": "gym_bg",
            "voice_id": "en_us_1"
        }
        headers = {"Authorization": f"Bearer {HEYGEN_KEY}"}
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        if r.status_code == 200:
            video_url = r.json()['data']['video_url']
            path = f"short_{int(time.time())}.mp4"
            with open(path, 'wb') as f:
                f.write(requests.get(video_url).content)
            return path
        raise Exception(r.text)

    return with_retry(_generate, offer)

# Post to X (Error Retry)
def post_to_x(content):
    def _post():
        client.create_tweet(text=content[:280])
        print(f"[X] Posted: {content[:50]}...")
    with_retry(_post, content)

# Post to Facebook (Error Retry)
def post_to_facebook(content, image_url):
    def _post():
        url = f"https://graph.facebook.com/{FB_PAGE_ID}/photos"
        payload = {
            'url': image_url,
            'caption': content,
            'access_token': FB_ACCESS_TOKEN
        }
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code == 200:
            print(f"[FB] Posted to Page")
        else:
            raise Exception(r.text)
    with_retry(_post, content, image_url)

# Post to Instagram (Error Retry)
def post_to_instagram(content, image_url):
    def _post():
        url = f"https://graph.facebook.com/{IG_USER_ID}/media"
        payload = {
            'image_url': image_url,
            'caption': content,
            'access_token': FB_ACCESS_TOKEN
        }
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code == 200:
            creation_id = r.json()['id']
            publish_url = f"https://graph.facebook.com/{IG_USER_ID}/media_publish"
            payload = {
                'creation_id': creation_id,
                'access_token': FB_ACCESS_TOKEN
            }
            r = requests.post(publish_url, data=payload, timeout=10)
            if r.status_code == 200:
                print(f"[IG] Posted to Instagram")
            else:
                raise Exception(r.text)
        else:
            raise Exception(r.text)
    with_retry(_post, content, image_url)

# Post via IFTTT (TikTok — Error Retry)
def post_via_ifttt(platform, content, image_url):
    def _post():
        url = f"https://maker.ifttt.com/trigger/{platform}_post/with/key/{IFTTT_KEY}"
        data = {"value1": content, "value2": image_url}
        r = requests.post(url, json=data, timeout=10)
        if r.status_code != 200:
            raise Exception(r.text)
        print(f"[{platform.upper()}] Sent via IFTTT")
    with_retry(_post, platform, content, image_url)

# YouTube Shorts Upload (Error Retry)
def upload_youtube_short(title, description, video_path):
    def _upload():
        if not os.path.exists('youtube_token.json'):
            return None
        with open('youtube_token.json') as f:
            creds = Credentials.from_authorized_user_info(json.load(f))
        youtube = build('youtube', 'v3', credentials=creds)
        body = {
            'snippet': {'title': title, 'description': description, 'tags': ['affiliate', 'sale'], 'categoryId': '22'},
            'status': {'privacyStatus': 'public'}
        }
        media = MediaFileUpload(video_path, chunksize=-1, resumable=True)
        request = youtube.videos().insert(part='snippet,status', body=body, media_body=media)
        response = request.execute()
        print(f"[YT] Uploaded: {response['id']}")
        return response['id']
    return with_retry(_upload, title, description, video_path)

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

# MAIN CAMPAIGN (500 SHORTS/DAY)
def run_daily_campaign():
    print(f"[BEAST] v7.5 Campaign started at {datetime.now()} — 500 Shorts/Day")
    
    offers = get_awin_offers() + get_rakuten_offers()
    if not offers:
        print("[BEAST] No offers found")
        return

    posts_today = 0
    for offer in offers[:500]:  # 500 Shorts/Day
        content = generate_post(offer)
        post_to_x(content)
        post_to_facebook(content, offer['image'])
        post_to_instagram(content, offer['image'])
        post_via_ifttt('tiktok', content, offer['image'])

        video_path = generate_short_video(offer)
        short_title = f"{offer['product']} Deal! #{posts_today + 1}"
        short_desc = content
        video_id = upload_youtube_short(short_title, short_desc, video_path)
        if video_id:
            conn, cur = get_db()
            cur.execute("INSERT INTO posts (platform, content, link, status) VALUES (%s, %s, %s, 'sent')", ('youtube', short_desc, video_id))
            conn.commit()
            conn.close()
        posts_today += 1
        time.sleep(5)  # 5s delay for 500/hour

    print(f"[BEAST] Campaign complete! {posts_today} posts/short sent")
    send_telegram(f"Beast Complete: {posts_today} posts/short live! $10M Mode ON")
