# worker.py - v7.0 $100K SCALER
import os
import requests
import tweepy
import openai
from telegram import Bot
from datetime import datetime
import psycopg2
import googleapiclient.discovery
import googleapiclient.http
import json

COMPANY = "Slickofficials HQ | Amson Multi Global LTD"
IFTTT_WEBHOOK = "https://maker.ifttt.com/trigger/social_post/with/key/n6pmRHmZFiQK1uyu3pk3ovMaKS4NpoyoF_0VDO1V97j"

bot = Bot(token=os.getenv('TELEGRAM_BOT_TOKEN'))
chat_id = os.getenv('TELEGRAM_CHAT_ID')

def send_telegram(msg):
    try: bot.send_message(chat_id=chat_id, text=f"{COMPANY}\n{msg}")
    except: pass

def generate_content(offer):
    prompt = f"150-char viral post for {offer['name']}. Emojis, urgency, CTA. Include link. End with '{COMPANY}'"
    try:
        resp = openai.ChatCompletion.create(model="gpt-3.5-turbo", messages=[{"role": "user", "content": prompt}], max_tokens=100)
        return resp.choices[0].message.content.strip()
    except: return f"Get {offer['name']} NOW! {offer['link']} #SlickDeals\n{COMPANY}"

def post_to_x(content): ...  # (same)
def post_to_ig(content, link): ...  # (same)
def post_to_fb(content): ...  # (same)

def broadcast_ifttt(content):
    try:
        text = content.split("http")[0].strip()
        link = "http" + content.split("http")[1].split()[0] if "http" in content else ""
        payload = {"value1": text, "value2": link, "value3": COMPANY}
        requests.post(IFTTT_WEBHOOK, json=payload)
        send_telegram("TikTok + 10+ platforms LIVE")
    except: pass

def run_daily_campaign():
    offers = [...]  # 10 offers for $100k scale
    for offer in offers:
        content = generate_content(offer)
        post_to_x(content)
        post_to_ig(content, offer['link'])
        post_to_fb(content)
        broadcast_ifttt(content)
        generate_and_upload_video(offer)
    send_telegram(f"v7.0 $100K CAMPAIGN COMPLETE\n{COMPANY}")
