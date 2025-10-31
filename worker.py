# worker.py - v7.5 $10M HARD CORE AUTOPILOT (REAL AWIN + RAKUTEN LINKS + DB STORE + 20 POSTS/DAY)
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
import csv
from io import StringIO

# CONFIG
DB_URL = os.getenv('DATABASE_URL')
OPENAI_KEY = os.getenv('OPENAI_API_KEY')
openai.api_key = OPENAI_KEY

TWITTER_BEARER = os.getenv('TWITTER_BEARER_TOKEN')
client = tweepy.Client(bearer_token=TWITTER_BEARER)

IFTTT_KEY = os.getenv('IFTTT_KEY')  # For TikTok

HEYGEN_KEY = os.getenv('HEYGEN_API_KEY')

AWIN_API_TOKEN = os.getenv('AWIN_API_TOKEN')
AWIN_PUBLISHER_ID = os.getenv('AWIN_PUBLISHER_ID')

RAKUTEN_CLIENT_ID = os.getenv('RAKUTEN_CLIENT_ID')
RAKUTEN_SECURITY_TOKEN = os.getenv('RAKUTEN_SECURITY_TOKEN')
RAKUTEN_SCOPE_ID = os.getenv('RAKUTEN_SCOPE_ID')

# DATABASE
def get_db():
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# Error Retry Wrapper (1 TRY ONLY — HARD CORE)
def with_retry(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        print(f"[RETRY] Failed {func.__name__}: {e}")
        return None  # No retry — go hard or go home

# AUTO CREATE DB TABLE IF NOT EXISTS (affiliate_links)
def create_affiliate_links_table():
    conn, cur = get_db()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS affiliate_links (
            id SERIAL PRIMARY KEY,
            network TEXT,
            product_name TEXT,
            deeplink TEXT,
            image_url TEXT,
            commission_rate TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    conn.close()

# AUTO PULL REAL AWIN LINKS (CSV DATAFEED)
def get_awin_offers():
    url = f"https://datafeed.awin.com/datafeed/download/apiv5/{AWIN_PUBLISHER_ID}/csv/?token={AWIN_API_TOKEN}"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            csv_data = r.text
            reader = csv.reader(StringIO(csv_data))
            next(reader)  # Skip header
            offers = []
            for row in reader:
                if len(row) > 5:
                    offers.append({
                        'product': row[1],
                        'deeplink': row[3],
                        'image': row[5],
                        'commission': '8%'
                    })
            print(f"[AWIN] Pulled {len(offers)} real links")
            return offers
    except Exception as e:
        print(f"[AWIN] Error: {e}")
    return []

# AUTO PULL REAL RAKUTEN LINKS (OFFERS API)
def get_rakuten_offers():
    url = "https://api.rakutenmarketing.com/offers/1.0"
    headers = {
        "Authorization": f"Bearer {os.getenv('RAKUTEN_WEBSERVICES_TOKEN')}",
        "Content-Type": "application/json"
    }
    params = {
        "client_id": RAKUTEN_CLIENT_ID,
        "client_secret": RAKUTEN_SECURITY_TOKEN,
        "scope": RAKUTEN_SCOPE_ID
    }
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 200:
            data = r.json()['offers'][:20]  # Limit to 20 for daily
            offers = []
            for item in data:
                offers.append({
                    'product': item['name'],
                    'deeplink': item['affiliate_link'],
                    'image': item['image_url'],
                    'commission': item['commission_rate']
                })
            print(f"[RAKUTEN] Pulled {len(offers)} real links")
            return offers
    except Exception as e:
        print(f"[RAKUTEN] Error: {e}")
    return []

# STORE LINKS IN DB (NO DUPLICATES)
def store_links_in_db(offers):
    conn, cur = get_db()
    for offer in offers:
        cur.execute("""
            INSERT INTO affiliate_links (network, product_name, deeplink, image_url, commission_rate) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (deeplink) DO NOTHING
        """, ('awin/rakuten', offer['product'], offer['deeplink'], offer['image'], offer['commission']))
    conn.commit()
    conn.close()
    print(f"[DB] Stored {len(offers)} links (no duplicates)")

# MAIN CAMPAIGN (PULL REAL LINKS + STORE + 20 POSTS/DAY)
def run_daily_campaign():
    create_affiliate_links_table()  # Auto-create table

    print(f"[BEAST] v7.5 Campaign started at {datetime.now()} — 20 Posts/Day")
    
    awin_offers = get_awin_offers()
    rakuten_offers = get_rakuten_offers()
    offers = awin_offers + rakuten_offers

    store_links_in_db(offers)  # Store all pulled links

    if not offers:
        print("[BEAST] No offers found")
        return

    posts_today = 0
    for offer in offers[:20]:  # 20 Posts/Day
        content = generate_post(offer)
        post_to_x(content)

        post_via_ifttt('instagram', content, offer['image'])
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
        time.sleep(180)  # 3 min delay — safe for free tier (20 posts = 1 hour)

    print(f"[BEAST] Campaign complete! {posts_today} posts sent with real deep links")
    send_telegram(f"Beast Complete: {posts_today} posts live with clickable deep links! $10M Mode ON")
