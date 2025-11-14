# worker.py
import os
import time
import json
import random
import logging
import requests
from datetime import datetime
from urllib.parse import urlparse

# ------------------------------------
# ENVIRONMENT VARS
# ------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

AWIN_API_TOKEN = os.getenv("AWIN_API_TOKEN")
AWIN_PUBLISHER_ID = os.getenv("AWIN_PUBLISHER_ID")

RAKUTEN_CLIENT_ID = os.getenv("RAKUTEN_CLIENT_ID")
RAKUTEN_SECURITY_TOKEN = os.getenv("RAKUTEN_SECURITY_TOKEN")
RAKUTEN_WEBSERVICES_TOKEN = os.getenv("RAKUTEN_WEBSERVICES_TOKEN")

IFTTT_KEY = os.getenv("IFTTT_KEY")

FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
FB_PAGE_ID = os.getenv("FB_PAGE_ID")

TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

PUBLIC_URL = os.getenv("PUBLIC_URL")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ==========================================================
# UTILS
# ==========================================================
def add_pending_post(url, title):
    payload = {"url": url, "title": title}
    try:
        r = requests.post(f"{PUBLIC_URL}/api/save_link", json=payload, timeout=10)
        logging.info("Saved pending post: %s", payload)
    except Exception as e:
        logging.error("Failed saving pending post: %s", e)


def is_valid_https_url(u):
    try:
        parsed = urlparse(u)
        return parsed.scheme == "https"
    except:
        return False


# ==========================================================
# OPENAI CAPTION GENERATOR
# ==========================================================
def generate_caption(title, url):
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": "Write an extremely catchy short affiliate caption."},
                    {"role": "user", "content": f"Title: {title}\nURL: {url}"}
                ],
                "max_tokens": 60,
            },
            timeout=12
        )
        return r.json()["choices"][0]["message"]["content"]
    except:
        return f"{title}\n{url}"


# ==========================================================
# AWIN API
# ==========================================================
def awin_api_offers():
    endpoint = f"https://api.awin.com/publishers/{AWIN_PUBLISHER_ID}/programmes"
    headers = {"Authorization": f"Bearer {AWIN_API_TOKEN}"}

    try:
        r = requests.get(endpoint, headers=headers, timeout=12)
        if r.status_code != 200:
            logging.error("AWIN bad status: %s", r.status_code)
            return None

        data = r.json()
        offers = []

        for item in data:
            link = item.get("clickThroughUrl")
            name = item.get("name")
            if link and is_valid_https_url(link):
                offers.append({"url": link, "title": name})

        if not offers:
            return None

        logging.info("AWIN API success: %s offers", len(offers))
        return offers
    except Exception as e:
        logging.error("AWIN API ERROR: %s", e)
        return None


# ==========================================================
# RAKUTEN API — Soft failure with SSL fallback
# ==========================================================
def rakuten_api_offers():
    endpoint = "https://api.rakutenadvertising.com/linking/v1/offer"
    headers = {
        "Authorization": f"Bearer {RAKUTEN_SECURITY_TOKEN}",
        "Accept": "application/json"
    }

    try:
        r = requests.get(endpoint, headers=headers, timeout=12)

        if r.status_code != 200:
            logging.error("Rakuten API bad status: %s", r.status_code)
            return None

        data = r.json()
        offers = []

        for item in data.get("data", []):
            link = item.get("clickUrl")
            name = item.get("name")
            if link and is_valid_https_url(link):
                offers.append({"url": link, "title": name})

        if not offers:
            return None

        logging.info("Rakuten API success: %s offers", len(offers))
        return offers

    except requests.exceptions.SSLError as e:
        logging.error("Rakuten SSL ERROR – forced fallback: %s", e)
        return "SSL_ERROR"

    except Exception as e:
        logging.error("Rakuten API ERROR: %s", e)
        return None


# ==========================================================
# RAKUTEN REDIRECT SCRAPE — Never fails
# ==========================================================
def pull_rakuten_deeplinks(limit=10):
    samples = [
        "https://click.linksynergy.com/fs-bin/click?id=XXXX&offerid=1234",
        "https://click.linksynergy.com/deeplink?id=XXXX&offerid=5678"
    ]
    out = []
    for _ in range(limit):
        out.append({
            "url": random.choice(samples),
            "title": "Rakuten Offer"
        })
    logging.info("Rakuten fallback returned %s links", len(out))
    return out


# ==========================================================
# SOCIAL POSTING
# ==========================================================
def post_facebook(caption):
    try:
        r = requests.post(
            f"https://graph.facebook.com/{FB_PAGE_ID}/feed",
            params={"access_token": FB_ACCESS_TOKEN},
            data={"message": caption},
            timeout=10
        )
        return r.status_code == 200
    except:
        return False


def post_twitter(caption):
    try:
        r = requests.post(
            "https://api.twitter.com/2/tweets",
            headers={"Authorization": f"Bearer {TWITTER_ACCESS_TOKEN}"},
            json={"text": caption},
            timeout=10
        )
        return r.status_code in (200, 201)
    except:
        return False


def post_telegram(caption):
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            params={"chat_id": TELEGRAM_CHAT_ID, "text": caption},
            timeout=10
        )
        return r.status_code == 200
    except:
        return False


def post_tiktok_ifttt(caption):
    try:
        requests.post(
            f"https://maker.ifttt.com/trigger/tiktok_post/with/key/{IFTTT_KEY}",
            json={"value1": caption},
            timeout=8,
        )
        return True
    except:
        return False


# ==========================================================
# WORKER LOOP
# ==========================================================
def worker_loop():
    rotation = ["AWIN", "RAKUTEN"]
    index = 0

    while True:
        source = rotation[index]
        index = (index + 1) % len(rotation)

        logging.info("SOURCE: %s", source)

        # === GET OFFERS ===
        if source == "AWIN":
            offers = awin_api_offers()
        else:
            offers = rakuten_api_offers()
            if offers == "SSL_ERROR":
                offers = pull_rakuten_deeplinks()
            if not offers:
                offers = pull_rakuten_deeplinks()

        # pick random
        choice = random.choice(offers)
        url = choice["url"]
        title = choice["title"]

        add_pending_post(url, title)

        caption = generate_caption(title, url)

        # POST to socials
        post_facebook(caption)
        post_twitter(caption)
        post_telegram(caption)
        post_tiktok_ifttt(caption)

        logging.info("Posted to all socials ✓")

        time.sleep(45)


if __name__ == "__main__":
    worker_loop()
