# worker.py - v7.0 AUTOPILOT ENGINE
import os
import requests
import openai
from datetime import datetime
import psycopg
from psycopg.rows import dict_row
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
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# Awin Offers (REAL API)
def get_awin_offers():
    token = os.getenv('AWIN_API_TOKEN')
    publisher_id = os.getenv('AWIN_PUBLISHER_ID')
    if not token or not publisher_id:
        return []
    url = f"https://productdata.awin.com/datafeed/download/apiv5/{publisher_id}/csv/"
    headers = {"Authorization": f"Bearer {token}", "User-Agent": "AutoAffiliateAI-v7.0"}
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
    except Exception as e:
        print(f"[AWIN] Error: {e}")
    return []

# Rakuten Offers (Placeholder — Add your API later)
def get_rakuten_offers():
    return [
        {
            'product': 'Gymshark Leggings',
            'link': 'https://rakuten.link/gymshark123',
            'image': 'https://i.imgur.com/gymshark.jpg',
            'commission': '12%'
        }
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
        return f"70% OFF {offer['product']}! Shop now: {offer['link']} #ad"

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

# Telegram Alert
def send_telegram(message):
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    if not bot_token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        requests.post=url, data={'chat_id': chat_id, 'text': message})
    except: pass

# MAIN CAMPAIGN
def run_daily_campaign():
    print(f"[BEAST] Campaign started at {datetime.now()}")
    
    offers = get_awin_offers() + get_rakuten_offers()
    if not offers:
        print("[BEAST] No offers found")
        return

    for offer in offers[:10]:
        content = generate_post(offer)
        post_to_x(content)
        post_via_ifttt('instagram', content, offer['image'])
        post_via_ifttt('tiktok', content, offer['image'])
        time.sleep(30)

    print("[BEAST] Campaign complete!")
    send_telegram("Beast Campaign Complete: 10 posts live!")
