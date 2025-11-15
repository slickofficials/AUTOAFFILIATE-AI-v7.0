#!/usr/bin/env python3
import os, sys, time, logging, requests, random
from urllib.parse import urlencode, urljoin, quote_plus
import psycopg
from psycopg.rows import dict_row
try: import tweepy
except: tweepy=None

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s")
logger=logging.getLogger("worker")

DATABASE_URL=os.getenv("DATABASE_URL"); PUBLIC_URL=os.getenv("PUBLIC_URL","https://example.com")
RAKUTEN_CLIENT_ID=os.getenv("RAKUTEN_CLIENT_ID",""); RAKUTEN_SECRET=os.getenv("RAKUTEN_SECURITY_TOKEN",""); RAKUTEN_SID=os.getenv("RAKUTEN_MID","")
AWIN_PUBLISHER_ID=os.getenv("AWIN_PUBLISHER_ID",""); AWIN_API_TOKEN=os.getenv("AWIN_API_TOKEN","")
TWITTER_API_KEY=os.getenv("TWITTER_API_KEY",""); TWITTER_API_SECRET=os.getenv("TWITTER_API_SECRET",""); TWITTER_ACCESS_TOKEN=os.getenv("TWITTER_ACCESS_TOKEN",""); TWITTER_ACCESS_SECRET=os.getenv("TWITTER_ACCESS_SECRET","")
TELEGRAM_BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN",""); TELEGRAM_CHAT_ID=os.getenv("TELEGRAM_CHAT_ID","")
FB_ACCESS_TOKEN=os.getenv("FB_ACCESS_TOKEN",""); FB_PAGE_ID=os.getenv("FB_PAGE_ID",""); IG_USER_ID=os.getenv("IG_USER_ID","")
IFTTT_KEY=os.getenv("IFTTT_KEY",""); TWILIO_SID=os.getenv("TWILIO_SID",""); TWILIO_TOKEN=os.getenv("TWILIO_TOKEN",""); WHATSAPP_TO=os.getenv("WHATSAPP_TO",""); WHATSAPP_FROM=os.getenv("WHATSAPP_FROM","")

DEFAULT_INTERVAL=int(os.getenv("POST_INTERVAL_SECONDS","10800")); MAX_POSTS_PER_CYCLE=int(os.getenv("MAX_POSTS_PER_CYCLE","1")); SLEEP_ON_EMPTY=int(os.getenv("SLEEP_ON_EMPTY","300"))

def connect_db(): return psycopg.connect(DATABASE_URL,row_factory=dict_row)
conn=connect_db(); cur=conn.cursor()

def get_setting(k): cur.execute("SELECT value FROM public.settings WHERE key=%s",(k,)); r=cur.fetchone(); return r["value"] if r else None
def get_queued_posts(limit): cur.execute("SELECT * FROM public.posts WHERE status='queued' ORDER BY created_at ASC LIMIT %s",(limit,)); return cur.fetchall()
def mark_post_result(pid,status,err=None):
    if status=="posted": cur.execute("UPDATE public.posts SET status=%s,error_message=NULL,posted_at=NOW() WHERE id=%s",(status,pid))
    else: cur.execute("UPDATE public.posts SET status=%s,error_message=%s WHERE id=%s",(status,err,pid))
    conn.commit()
def save_deeplink(pid,link): cur.execute("UPDATE public.posts SET deeplink=%s WHERE id=%s",(link,pid)); conn.commit()

def build_utm_query(): return urlencode({"utm_source":get_setting("utm_source") or "social","utm_medium":get_setting("utm_medium") or "organic","utm_campaign":get_setting("utm_campaign") or "autopost"})
def build_base_link(slug): base=get_setting("base_url") or PUBLIC_URL; return f"{urljoin(base,'/p/'+quote_plus(slug))}?{build_utm_query()}"

def rakuten_affiliate_link(product_url):
    r=requests.post("https://api.rakutenmarketing.com/token",data={"grant_type":"client_credentials","client_id":RAKUTEN_CLIENT_ID,"client_secret":RAKUTEN_SECRET},timeout=20); r.raise_for_status()
    token=r.json()["access_token"]
    r2=requests.get("https://api.rakutenmarketing.com/linklocator/1.0/deeplink",headers={"Authorization":f"Bearer {token}"},params={"url":product_url,"sid":RAKUTEN_SID},timeout=20); r2.raise_for_status()
    return r2.json().get("link",product_url)

def awin_affiliate_link(product_url):
    r=requests.get("https://api.awin.com/linkbuilder",params={"publisherId":AWIN_PUBLISHER_ID,"accessToken":AWIN_API_TOKEN,"url":product_url},timeout=20); r.raise_for_status()
    return r.text.strip() or product_url

def build_deeplink(slug,product_url,source_hint):
    if product_url:
        if (source_hint or "").lower()=="rakuten": link=rakuten_affiliate_link(product_url)
        elif (source_hint or "").lower()=="awin": link=awin_affiliate_link(product_url)
        else: link=rakuten_affiliate_link(product_url) if random.choice([True,False
