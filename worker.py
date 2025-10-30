# worker.py - v9.2 IMMEDIATE LOG + BULLETPROOF
import os
import sys
import time

# === PRINT BEFORE ANYTHING ELSE ===
print("\n" + "="*80)
print("    SLICKOFFICIALS AI v9.2 - BOT STARTED")
print("    TIME:", time.strftime("%Y-%m-%d %H:%M:%S"))
print("    PYTHON:", sys.version)
print("    CWD:", os.getcwd())
print("="*80)

# === FORCE FLUSH ===
sys.stdout.flush()

# === NOW IMPORT EVERYTHING SAFELY ===
try:
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
    print("[IMPORTS] ALL MODULES LOADED")
except Exception as e:
    print(f"[IMPORTS] FAILED: {e}")
    sys.exit(1)

# === ENV CHECK ===
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

print("[ENV] CHECKING KEYS...")
for key, val in required.items():
    status = "OK" if val else "MISSING"
    print(f"  → {key}: {status}")
sys.stdout.flush()

# === REST OF CODE (SAME AS v9.1) ===
openai_client = None
x_client = None
youtube = None
conn = None

def safe_connect_db():
    global conn
    print("[DB] Connecting...")
    try:
        conn = psycopg.connect(required['DATABASE_URL'], row_factory=dict_row, timeout=10)
        print("[DB] CONNECTED")
    except Exception as e:
        print(f"[DB] FAILED: {e}")

def safe_init_x():
    global x_client
    print("[X] Initializing...")
    try:
        x_client = tweepy.Client(
            consumer_key=required['TWITTER_API_KEY'],
            consumer_secret=required['TWITTER_API_SECRET'],
            access_token=required['TWITTER_ACCESS_TOKEN'],
            access_token_secret=required['TWITTER_ACCESS_SECRET'],
            bearer_token=required['TWITTER_BEARER_TOKEN']
        )
        print("[X] READY")
    except Exception as e:
        print(f"[X] FAILED: {e}")

def safe_init_youtube():
    global youtube
    if not required['YOUTUBE_TOKEN_JSON']:
        print("[YT] NO TOKEN")
        return
    print("[YT] Initializing...")
    try:
        creds = Credentials.from_authorized_user_info(json.loads(required['YOUTUBE_TOKEN_JSON']))
        youtube = build('youtube', 'v3', credentials=creds)
        print("[YT] READY")
    except Exception as e:
        print(f"[YT] FAILED: {e}")

if required['OPENAI_API_KEY']:
    try:
        openai_client = OpenAI(api_key=required['OPENAI_API_KEY'])
        print("[OPENAI] READY")
    except Exception as e:
        print(f"[OPENAI] FAILED: {e}")

# === MAIN LOOP ===
run_count = 0
while True:
    run_count += 1
    print(f"\n[RUN #{run_count}] {datetime.now().strftime('%H:%M:%S')}")

    safe_connect_db()
    safe_init_x()
    safe_init_youtube()

    link = "https://click.linksynergy.com/deeplink?id=SLICKO8&mid=36805&murl=..."
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT deeplink FROM affiliate_links ORDER BY RANDOM() LIMIT 1")
                row = cur.fetchone()
                if row: link = row['deeplink']
                print(f"[DB] LINK: {link[:60]}...")
        except Exception as e:
            print(f"[DB] ERROR: {e}")

    content = f"70% OFF! Shop: {link} #ad"
    if openai_client:
        try:
            resp = openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": f"Viral post. Link: {link}. Max 280. #ad"}],
                max_tokens=100
            )
            content = resp.choices[0].message.content.strip()[:280]
        except Exception as e:
            print(f"[OPENAI] ERROR: {e}")
    print(f"[CONTENT] {content}")

    if x_client:
        try:
            tweet = x_client.create_tweet(text=content)
            print(f"[X] POSTED: https://x.com/i/web/status/{tweet.data['id']}")
        except Exception as e:
            print(f"[X] ERROR: {e}")

    print("[SLEEP] 6 HOURS...")
    time.sleep(6 * 60 * 60)
