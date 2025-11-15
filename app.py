#!/usr/bin/env python3
import os, sys, logging, subprocess, threading, psycopg
from psycopg.rows import dict_row
from flask import Flask, request, jsonify, redirect

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("app")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL: raise RuntimeError("DATABASE_URL not set")

def connect_db(): return psycopg.connect(DATABASE_URL,row_factory=dict_row)
conn = connect_db(); cur = conn.cursor()

app = Flask(__name__)
worker_lock = threading.Lock(); worker_process=None

def enqueue_post(title,slug,body,platform,url,source,image_url):
    cur.execute("""INSERT INTO public.posts (title,slug,body,platform,url,source,image_url,status)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,'queued') RETURNING id""",
                (title,slug,body or "",platform.lower(),url,source,image_url))
    pid=cur.fetchone()["id"]; conn.commit(); return pid

def log_click(pid,ip,ua):
    cur.execute("INSERT INTO public.clicks (post_id,ip,user_agent) VALUES (%s,%s,%s)",(pid,ip,ua or "")); conn.commit()

@app.get("/health")
def health(): return jsonify({"status":"ok"})

@app.post("/enqueue")
def enqueue():
    d=request.get_json(force=True)
    if not d.get("title") or not d.get("slug"): return jsonify({"error":"title and slug required"}),400
    if d.get("platform","").lower() not in ("twitter","telegram","facebook","instagram","tiktok","whatsapp"):
        return jsonify({"error":"invalid platform"}),400
    if d.get("platform","").lower()=="instagram" and not d.get("image_url"):
        return jsonify({"error":"instagram requires image_url"}),400
    pid=enqueue_post(d["title"],d["slug"],d.get("body"),d.get("platform"),d.get("url"),d.get("source"),d.get("image_url"))
    return jsonify({"queued":pid})

@app.get("/posts")
def posts():
    cur.execute("""SELECT p.*,COALESCE(c.click_count,0) AS click_count
                   FROM public.posts p
                   LEFT JOIN (SELECT post_id,COUNT(*) AS click_count FROM public.clicks GROUP BY post_id) c
                   ON p.id=c.post_id ORDER BY p.created_at DESC LIMIT 300""")
    return jsonify(cur.fetchall())

@app.get("/r/<slug>")
def redirect_slug(slug):
    cur.execute("SELECT * FROM public.posts WHERE slug=%s",(slug,)); post=cur.fetchone()
    if not post: return jsonify({"error":"post not found"}),404
    log_click(post["id"],request.remote_addr,request.headers.get("User-Agent",""))
    target=post.get("deeplink") or post.get("url") or os.getenv("PUBLIC_URL","https://example.com")
    return redirect(target,302)

@app.get("/analytics")
def analytics():
    cur.execute("SELECT COUNT(*) AS total_clicks FROM public.clicks"); total=cur.fetchone()["total_clicks"]
    cur.execute("SELECT p.platform,COUNT(c.id) AS clicks FROM public.posts p LEFT JOIN public.clicks c ON p.id=c.post_id GROUP BY p.platform"); per=cur.fetchall()
    return jsonify({"total_clicks":total,"clicks_per_platform":per})

@app.get("/metrics")
def metrics():
    cur.execute("SELECT status,COUNT(*) AS count FROM public.posts GROUP BY status"); status_counts={r["status"]:r["count"] for r in cur.fetchall()}
    return jsonify({"status_counts":status_counts})

@app.get("/worker/status")
def worker_status(): return jsonify({"running":bool(worker_process and worker_process.poll() is None)})

@app.post("/worker/start")
def worker_start():
    global worker_process
    with worker_lock:
        if worker_process and worker_process.poll() is None: return jsonify({"status":"already running"})
        worker_process=subprocess.Popen([sys.executable,"worker.py"]); return jsonify({"status":"started"})

@app.post("/worker/stop")
def worker_stop():
    global worker_process
    with worker_lock:
        if worker_process and worker_process.poll() is None:
            worker_process.terminate()
            try: worker_process.wait(timeout=10)
            except subprocess.TimeoutExpired: worker_process.kill()
            return jsonify({"status":"stopped"})
        return jsonify({"status":"not running"})

if __name__=="__main__": app.run(host="0.0.0.0",port=5000)
