# worker.py - v7.0 AUTOPILOT ENGINE
import os
import requests
import openai
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import tweepy
import time
import json

# CONFIG
DB_URL = os.getenv('DATABASE_URL')
OPENAI_KEY = os.getenv('OPENAI_API_KEY')
openai.api_key = OPENAI_KEY

TWITTER_BEARER = os.getenv('TWITTER_BEARER_TOKEN')
client = tweepy.Client(bearer_token=TWITTER_BEARER)

IFTTT_KEY = os.getenv('IFTTT_KEY')

# DATABASE
def get_db():
    conn = psycopg2.connect(DB_URL)
    return conn, conn.cursor(cursor_factory=RealDictCursor)

# Awin Offers (REAL API)
def get_awin_offers():
    token = os.getenv('AWIN_API_TOKEN')
    publisher_id = os.getenv('AWIN_PUBLISHER_ID')
    url = f"https://productdata.awin.com/datafeed/download/apiv5/{publisher_id}/csv/"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            lines = r.text.splitlines()[1:6]
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
    except: pass
    return []

# Rakuten Offers
def get_rakuten_offers():
    # Add your Rakuten logic here later
    return [{'product': 'Gymshark Leggings', 'link': 'https://rakuten.link/abc123', 'image': 'https://i.imgur.com/xyz.jpg', 'commission': '12%'}]

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
        return f"70% OFF {offer['product']}! Shop now: {offer['link']}"

# Post to X
def post_to_x(content):
    try:
        client.create_tweet(text=content[:280])
        print(f"[X] Posted: {content[:50]}...")
    except Exception as e:
        print(f"[X] Failed: {e}")

# Post via IFTTT (IG, TikTok, etc.)
def post_via_ifttt(platform, content, image_url):
    url = f"https://maker.ifttt.com/trigger/{platform}_post/with/key/{IFTTT_KEY}"
    data = {"value1": content, "value2": image_url}
    try:
        requests.post(url, json=data)
        print(f"[{platform.upper()}] Sent via IFTTT")
    except: pass

# MAIN CAMPAIGN
def run_daily_campaign():
    print(f"[BEAST] Campaign started at {datetime.now()}")
    
    offers = get_awin_offers() + get_rakuten_offers()
    if not offers:
        print("[BEAST] No offers found")
        return

    for offer in offers[:10]:  # 10 posts
        content = generate_post(offer)
        post_to_x(content)
        post_via_ifttt('instagram', content, offer['image'])
        post_via_ifttt('tiktok', content, offer['image'])
        time.sleep(30)  # Avoid rate limits

    print("[BEAST] Campaign complete!")
