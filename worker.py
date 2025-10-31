# worker.py - v11.1 YOUR 11 LINKS = AUTO MONEY
import os, sys, time, random, requests
from datetime import datetime
from openai import OpenAI
import tweepy

os.environ['PYTHONUNBUFFERED'] = '1'
def log(msg): print(f"[MONEY] {datetime.now().strftime('%H:%M:%S')} | {msg}"); sys.stdout.flush()

log("SLICKOFFICIALS v11.1 — 11 LINKS AUTO EMPIRE")

# === ENV ===
required = {k: os.getenv(k) for k in ['DATABASE_URL','OPENAI_API_KEY','TWITTER_API_KEY','TWITTER_API_SECRET','TWITTER_ACCESS_TOKEN','TWITTER_ACCESS_SECRET','TWITTER_BEARER_TOKEN','FB_ACCESS_TOKEN','IG_USER_ID','FB_PAGE_ID','IFTTT_KEY']}
openai_client = OpenAI(api_key=required['OPENAI_API_KEY']) if required['OPENAI_API_KEY'] else None

# === MAIN LOOP ===
while True:
    log("RUN STARTED — PULLING YOUR LINK")
    
    # === DB LINK ===
    product, deeplink = "Mystery Deal", "https://tidd.ly/3J1KeV2"
    try:
        import psycopg
        from psycopg.rows import dict_row
        conn = psycopg.connect(required['DATABASE_URL'], row_factory=dict_row)
        with conn.cursor() as cur:
            cur.execute("SELECT product_name, deeplink FROM affiliate_links WHERE active = TRUE ORDER BY RANDOM() LIMIT 1")
            row = cur.fetchone()
            if row: product, deeplink = row
        log(f"PRODUCT: {product}")
        log(f"PAID LINK: {deeplink}")
    except Exception as e: log(f"DB OFF: {e}")

    # === AI CONTENT ===
    content = f"70% OFF {product}! Shop now: {deeplink} #ad"
    if openai_client:
        try:
            resp = openai_client.chat.completions.create(model="gpt-3.5-turbo", messages=[{"role":"user","content":f"Make viral: {content}"}], max_tokens=100)
            content = resp.choices[0].message.content.strip()[:270] + f" {deeplink} #ad"
        except: pass
    log(f"POST: {content}")

    # === POST X ===
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
    except Exception as e: log(f"X ERROR: {e}")

    # === FB + IG + TIKTOK ===
    img = "https://i.imgur.com/airmax270.jpg"
    if all([required['FB_ACCESS_TOKEN'], required['IG_USER_ID'], required['FB_PAGE_ID']]):
        try:
            r = requests.post(f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media", data={'image_url':img,'caption':content,'access_token':required['FB_ACCESS_TOKEN']})
            if r.status_code == 200:
                requests.post(f"https://graph.facebook.com/v20.0/{required['IG_USER_ID']}/media_publish", data={'creation_id':r.json()['id'],'access_token':required['FB_ACCESS_TOKEN']})
                log("INSTAGRAM POSTED")
        except: pass
        try:
            requests.post(f"https://graph.facebook.com/v20.0/{required['FB_PAGE_ID']}/photos", data={'url':img,'caption':content,'access_token':required['FB_ACCESS_TOKEN']})
            log("FACEBOOK POSTED")
        except: pass
    if required['IFTTT_KEY']:
        try:
            requests.post(f"https://maker.ifttt.com/trigger/tiktok_post/with/key/{required['IFTTT_KEY']}", json={"value1":content,"value2":img})
            log("TIKTOK SENT")
        except: pass

    log("RUN DONE — SLEEPING 6 HOURS")
    time.sleep(6 * 60 * 60)
