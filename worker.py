# worker.py - v12.1.1 — AUTO-DEEPLINK PULLER + 24 POSTS/DAY + $10M EMPIRE
import os
import sys          # ← FIXED: Added sys
import time
import json
import requests
import random
from datetime import datetime

# === LOGGING ===
os.environ['PYTHONUNBUFFERED'] = '1'
def log(msg):
    print(f"[MONEY] {datetime.now().strftime('%H:%M:%S')} | {msg}")
    sys.stdout.flush()

log("SLICKOFFICIALS v12.1.1 — AUTO-DEEPLINK PULLER + 24 POSTS/DAY STARTED")

# === IMPORTS ===
try:
    import psycopg
    from psycopg.rows import dict_row
    from openai import OpenAI
    import tweepy
    log("MODULES LOADED")
except Exception as e:
    log(f"IMPORT FAILED: {e}")
    sys.exit(1)

# === ENV ===
required = {k: os.getenv(k) for k in [
    'DATABASE_URL', 'OPENAI_API_KEY', 'TWITTER_API_KEY', 'TWITTER_API_SECRET',
    'TWITTER_ACCESS_TOKEN', 'TWITTER_ACCESS_SECRET', 'TWITTER_BEARER_TOKEN',
    'FB_ACCESS_TOKEN', 'IG_USER_ID', 'FB_PAGE_ID', 'IFTTT_KEY', 'YOUTUBE_TOKEN_JSON'
]}

# === CLIENTS ===
openai_client = OpenAI(api_key=required['OPENAI_API_KEY']) if required['OPENAI_API_KEY'] else None

# === AUTO-PULL DEEPLINKS FROM AWIN + RAKUTEN ===
def pull_deeplink():
    log("PULLING DEEPLINK FROM AWIN/RAKUTEN")

    # === AWIN API ===
    awin_token = os.getenv('AWIN_API_TOKEN')
    awin_publisher = os.getenv('AWIN_PUBLISHER_ID')
    if awin_token and awin_publisher:
        try:
            url = f"https://productdata.awin.com/datafeed/download/apiv5/{awin_publisher}/csv"
            headers = {"Authorization": f"Bearer {awin_token}"}
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                lines = r.text.splitlines()[1:10]
                for line in lines:
                    cols = line.split('|')
                    if len(cols) >= 6:
                        product = cols[1]
                        deeplink = cols[3]
                        log(f"AWIN LINK: {product} → {deeplink}")
                        return product, deeplink, 'awin'
        except Exception as e:
            log(f"AWIN ERROR: {e}")

    # === RAKUTEN API ===
    rakuten_id = os.getenv('RAKUTEN_ID')
    if rakuten_id:
        try:
            url = f"https://api.rakuten.co.uk/v1/affiliate/products?merchantId={rakuten_id}&limit=1"
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if data.get('products'):
                    product = data['products'][0]['name']
                    deeplink = data['products'][0]['link']
                    log(f"RAKUTEN LINK: {product} → {deeplink}")
                    return product, deeplink, 'rakuten'
        except Exception as e:
            log(f"RAKUTEN ERROR: {e}")

    # === FALLBACK: YOUR 11 LINKS (GUARANTEED) ===
    your_links = [
        ("Kila Custom Insoles", "https://tidd.ly/3J1KeV2", "awin"),
        ("Kapitalwise", "https://tidd.ly/43ibfu7", "awin"),
        ("Diamond Smile FR", "https://tidd.ly/4nanmAp", "awin"),
        ("Bell's Reines", "https://tidd.ly/3Jb6cEV", "awin"),
        ("Awin USD", "https://tidd.ly/46RRifY", "awin"),
        ("AliExpress P", "https://tidd.ly/3Jbg6GA", "awin"),
        ("NeckHammock", "https://tidd.ly/4qyhB2L", "awin"),
        ("Slimeafit Affiliate Program FR", "https://tidd.ly/3WbtvBv", "awin"),
        ("Timeshop24 DE", "https://tidd.ly/4nWuz8s", "awin"),
        ("Bonne et Filou", "https://tidd.ly/4hgNp7H", "awin"),
        ("Wondershare", "https://click.linksynergy.com/deeplink?id=iejQuC2lIug&mid=37160&murl=https%3A%2F%2Fwww.wondershare.com%2F", "rakuten")
    ]
    product, deeplink, network = random.choice(your_links)
    log(f"FALLBACK LINK: {product} → {deeplink} ({network})")
    return product, deeplink, network

# === MAIN LOOP — 24 POSTS/DAY ===
while True:
    log("RUN STARTED — AUTO DEEPLINK + POSTING")

    product, deeplink, network = pull_deeplink()

    content = f"70% OFF {product}! Shop now: {deeplink} #ad"
    if openai_client:
        try:
            resp = openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": f"Viral post for {product}. Include EXACT link: {deeplink}. Max 280. End with #ad"}],
                max_tokens=100
            )
            content = resp.choices[0].message.content.strip()
            if deeplink not in content:
                content = f"{content.split('#ad')[0].strip()} {deeplink} #ad"
            content = content[:280]
            log("AI CONTENT GENERATED")
        except Exception as e:
            log(f"OPENAI ERROR: {e}")
    log(f"POST: {content}")

    # === X POST ===
    try:
        x = tweepy.Client(
            consumer_key=required['TWITTER_API_KEY'],
            consumer_secret=required['TWITTER_API_SECRET'],
            access_token=required['TWITTER_ACCESS_TOKEN'],
            access_token_secret=required['TWITTER_ACCESS_SECRET'],
            bearer_token=required['TWITTER_BEARER_TOKEN']
        )
        tweet = x.create_tweet(text=content)
        log(f"X POSTED: https://x.com/i/web/status/{tweet.data['id']}")
    except Exception as e:
        log(f"X ERROR: {e}")

    # === INSTAGRAM POST ===
    if all([required['FB_ACCESS_TOKEN'], required['IG_USER_ID']]):
        img = "https://i.imgur.com/airmax270.jpg"
        try:
            r = requests.post(
                f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media",
                params={'image_url': img, 'caption': content, 'access_token': required['FB_ACCESS_TOKEN']},
                timeout=30
            )
            if r.status_code == 200:
                creation_id = r.json()['id']
                requests.post(
                    f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media_publish",
                    params={'creation_id': creation_id, 'access_token': required['FB_ACCESS_TOKEN']},
                    timeout=30
                )
                log("INSTAGRAM POSTED WITH DEEPLINK")
        except Exception as e:
            log(f"INSTAGRAM ERROR: {e}")

    # === FACEBOOK POST ===
    if required['FB_ACCESS_TOKEN'] and required['FB_PAGE_ID']:
        img = "https://i.imgur.com/airmax270.jpg"
        try:
            r = requests.post(
                f"https://graph.facebook.com/v20.0/{required['FB_PAGE_ID']}/photos",
                params={'url': img, 'caption': content, 'access_token': required['FB_ACCESS_TOKEN']},
                timeout=30
            )
            if r.status_code == 200:
                log("FACEBOOK POSTED WITH DEEPLINK")
        except Exception as e:
            log(f"FACEBOOK ERROR: {e}")

    # === TIKTOK ===
    if required['IFTTT_KEY']:
        try:
            requests.post(
                f"https://maker.ifttt.com/trigger/tiktok_post/with/key/{required['IFTTT_KEY']}",
                json={"value1": content, "value2": img},
                timeout=30
            )
            log("TIKTOK SENT WITH DEEPLINK")
        except Exception as e:
            log(f"TIKTOK ERROR: {e}")

    log("RUN COMPLETE — SLEEPING 1 HOUR (24 POSTS/DAY)")
    time.sleep(60 * 60)  # 1 HOUR
