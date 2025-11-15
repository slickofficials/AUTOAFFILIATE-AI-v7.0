import os
import json
import logging
import requests
from datetime import datetime, timezone
from flask import Flask, request, jsonify, render_template, redirect, abort
import psycopg
from psycopg.rows import dict_row

# ---------------------------
# Logging
# ---------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("app")

# ---------------------------
# Config / Env
# ---------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
APP_PUBLIC_URL = os.getenv("APP_PUBLIC_URL") or ""

# import worker module
import worker

# ---------------------------
# Database helpers
# ---------------------------
def get_db_conn():
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    return conn, conn.cursor()

# ---------------------------
# Flask Init
# ---------------------------
app = Flask(__name__, template_folder="templates")

# ---------------------------
# Redirect Handler
# /r/<id> → resolve affiliate URL + count click
# ---------------------------
@app.get("/r/<int:post_id>")
def redirect_click(post_id):
    try:
        conn, cur = get_db_conn()
        cur.execute("SELECT url FROM posts WHERE id=%s", (post_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            abort(404)

        target = row["url"]

        # log click
        ua = request.headers.get("User-Agent", "")
        ip = request.headers.get("X-Forwarded-For", request.remote_addr)
        cur.execute(
            "INSERT INTO clicks (post_id, ip, user_agent, created_at) VALUES (%s,%s,%s,now())",
            (post_id, ip, ua)
        )
        conn.commit()
        conn.close()

        return redirect(target, code=302)
    except Exception:
        logger.exception("/r/<id> failed")
        return redirect(target, code=302)

# ---------------------------
# Dashboard HTML
# ---------------------------
@app.get("/")
def dashboard():
    try:
        conn, cur = get_db_conn()

        # recent posts
        cur.execute(
            "SELECT id, url, source, status, created_at, posted_at FROM posts ORDER BY id DESC LIMIT 40"
        )
        posts = cur.fetchall()

        # worker state
        running = worker._worker_running

        # settings
        cur.execute("SELECT key, value FROM settings")
        settings_raw = cur.fetchall()
        settings = {row["key"]: row["value"] for row in settings_raw}

        # analytics count
        cur.execute("SELECT count(*) FROM posts WHERE status='sent'")
        sent_count = cur.fetchone()["count"]

        cur.execute("SELECT count(*) FROM posts WHERE status='pending'")
        pending_count = cur.fetchone()["count"]

        conn.close()

        return render_template(
            "dashboard.html",
            posts=posts,
            worker_running=running,
            settings=settings,
            sent_count=sent_count,
            pending_count=pending_count
        )
    except Exception:
        logger.exception("dashboard failed")
        return "Dashboard failed", 500

# ---------------------------
# Manual Add Link
# ---------------------------
@app.post("/api/add")
def api_add():
    data = request.json
    url = data.get("url")
    if not url:
        return jsonify({"error": "missing url"}), 400
    try:
        saved = worker.save_links_to_db([url], source="manual")
        return jsonify({"saved": saved})
    except Exception:
        logger.exception("/api/add error")
        return jsonify({"error": "internal"}), 500

# ---------------------------
# Force refresh affiliate sources
# ---------------------------
@app.post("/api/refresh")
def api_refresh():
    try:
        saved = worker.refresh_all_sources()
        return jsonify({"saved": saved})
    except Exception:
        logger.exception("/api/refresh error")
        return jsonify({"error": "internal"}), 500

# ---------------------------
# Worker controls
# ---------------------------
@app.post("/api/worker/start")
def api_worker_start():
    try:
        worker.start_worker_background()
        return jsonify({"running": True})
    except Exception:
        logger.exception("worker start failed")
        return jsonify({"error": "internal"}), 500

@app.post("/api/worker/stop")
def api_worker_stop():
    try:
        worker.stop_worker()
        return jsonify({"running": False})
    except Exception:
        logger.exception("worker stop failed")
        return jsonify({"error": "internal"}), 500

# ---------------------------
# Update settings
# ---------------------------
@app.post("/api/settings")
def api_settings():
    data = request.json or {}
    try:
        conn, cur = get_db_conn()
        for k, v in data.items():
            cur.execute(
                "INSERT INTO settings (key,value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
                (k, v)
            )
        conn.commit()
        conn.close()
        return jsonify({"updated": True})
    except Exception:
        logger.exception("settings update error")
        return jsonify({"error": "internal"}), 500

# ---------------------------
# Health Check
# ---------------------------
@app.get("/health")
def health():
    return {"status": "ok", "worker_running": worker._worker_running}

# ---------------------------
# Launch
# ---------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
