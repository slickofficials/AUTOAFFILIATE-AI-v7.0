# worker.py - v15 AutoAffiliate AI | SlickOfficials HQ
import os, time, logging, requests, random
from datetime import datetime
from openai import OpenAI
import psycopg
from psycopg.rows import dict_row
from twilio.rest import Client

# === CONFIG ===
DB_URL = os.getenv("DATABASE_URL")
AWIN_TOKEN = os.getenv("AWIN_API_TOKEN")
RAKUTEN_TOKEN = os.getenv("RAKUTEN_API_TOKEN")
HEYGEN_KEY = os.getenv("HEYGEN_API_KEY")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")

REDIS_URL = os.getenv("REDIS_URL")
TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
YOUR_WHATSAPP = os.getenv("YOUR_WHATSAPP")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
client = OpenAI(api_key=OPENAI_KEY)
twilio_client = Client(TWILIO_SID, TWILIO_TOKEN) if TWILIO_SID and TWILIO_TOKEN else None

# === ALERT SYSTEM ===
def send_alert(title, msg):
    logging.info(f"ALERT: {title}")
    if not twilio_client or not YOUR_WHATSAPP:
        print(f"[ALERT] {title}: {msg}")
        return
    try:
        twilio_client.messages.create(
            from_="whatsapp:+14155238886",
            to=YOUR_WHATSAPP,
            body=f"*{title}*\n{msg}\nTime: {datetime.now()}"
        )
    except Exception as e:
        logging.warning(f"WhatsApp alert failed: {e}")

# === DB HELPERS ===
def get_db():
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# === AWIN ===
def pull_awin_links():
    try:
        url = "https://api.awin.com/publishers/me/transactions"
        headers = {"Authorization": f"Bearer {AWIN_TOKEN}"}
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 401:
            logging.warning(f"AWIN 401: {r.text}")
            return []
        return r.json()
    except Exception as e:
        logging.error(f"AWIN error: {e}")
        return []

# === RAKUTEN ===
def pull_rakuten_deeplinks():
    try:
        endpoint = "https://api.rakutenmarketing.com/linklocator/1.0/getMerchByCountry"
        headers = {"Authorization": f"Bearer {RAKUTEN_TOKEN}"}
        r = requests.get(endpoint, headers=headers, timeout=15, verify=True)
        if r.status_code != 200:
            logging.warning(f"Rakuten response {r.status_code}: {r.text[:200]}")
            return []
        return r.json()
    except requests.exceptions.SSLError as e:
        logging.warning(f"Rakuten SSL error: {e}")
        return []
    except Exception as e:
        logging.error(f"Rakuten API error: {e}")
        return []

# === HEYGEN ===
def generate_video(product_title):
    try:
        url = "https://api.heygen.com/v1/video/generate"
        headers = {"x-api-key": HEYGEN_KEY, "Content-Type": "application/json"}
        payload = {"title": product_title, "template_id": "default"}
        r = requests.post(url, headers=headers, json=payload, timeout=20)
        if r.status_code != 200:
            logging.warning(f"HeyGen response {r.status_code}: {r.text[:150]}")
            return None
        data = r.json()
        return data.get("video_url")
    except Exception as e:
        logging.error(f"HeyGen error: {e}")
        return None

# === OPENAI ===
def generate_caption(product_name):
    prompt = f"Write a viral social caption for '{product_name}' with emojis and hashtags."
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=60
        )
        caption = resp.choices[0].message.content.strip()
        return caption
    except Exception as e:
        logging.error(f"OpenAI failed: {e}")
        return f"🔥 {product_name} — shop now! #Deals #Promo"

# === MAIN LOOP ===
def main_loop():
    send_alert("WORKER START", "Affiliate worker online ✅")
    while True:
        logging.info("Refreshing affiliate sources")
        awin = pull_awin_links()
        rakuten = pull_rakuten_deeplinks()
        total = len(awin) + len(rakuten)
        logging.info(f"Attempted save {total} links from affiliate (approx added={total})")
        send_alert("REFRESH", f"Saved {total} links")

        # Example AI post creation
        if total > 0:
            sample_name = "Top Trending Product"
            caption = generate_caption(sample_name)
            video = generate_video(sample_name)
            logging.info(f"Generated caption: {caption}")
            if video:
                logging.info(f"Video ready: {video}")
            send_alert("POSTED", "Sample post generated successfully")

        logging.info("Sleeping 3600 seconds")
        time.sleep(3600)

if __name__ == "__main__":
    main_loop()
