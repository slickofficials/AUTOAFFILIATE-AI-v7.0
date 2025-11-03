# app.py — AutoAffiliate Dashboard (v3, with debug toggle & worker integration)

import os
import threading
import logging
from datetime import datetime, timezone
from flask import Flask, render_template, request, redirect, url_for, jsonify
import psycopg
from psycopg.rows import dict_row
import worker  # import your worker.py in the same directory

app = Flask(__name__)

# Setup logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dashboard")

DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin123")
DEBUG_REDIRECTS = os.getenv("DEBUG_LOG_REDIRECTS", "False").lower() == "true"

def get_db_conn():
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    return conn, conn.cursor()

# ========== ROUTES ==========

@app.route("/")
def dashboard():
    """Main dashboard view"""
    conn, cur = get_db_conn()
    cur.execute("SELECT COUNT(*) FROM posts WHERE status='pending'")
    pending = cur.fetchone()["count"]

    cur.execute("SELECT COUNT(*) FROM posts WHERE status='sent'")
    sent = cur.fetchone()["count"]

    cur.execute("SELECT COUNT(*) FROM posts WHERE status='failed'")
    failed = cur.fetchone()["count"]

    cur.execute("SELECT * FROM posts ORDER BY created_at DESC LIMIT 10")
    recent = cur.fetchall()
    conn.close()

    last_pull = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return render_template(
        "dashboard.html",
        pending=pending,
        sent=sent,
        failed=failed,
        recent=recent,
        running=worker._worker_running,
        debug=DEBUG_REDIRECTS,
        last_pull=last_pull,
    )


@app.route("/start", methods=["POST"])
def start_worker():
    password = request.form.get("password")
    if password != ADMIN_PASS:
        return jsonify({"error": "Invalid password"}), 403
    threading.Thread(target=worker.start_worker_background, daemon=True).start()
    return redirect(url_for("dashboard"))


@app.route("/stop", methods=["POST"])
def stop_worker():
    password = request.form.get("password")
    if password != ADMIN_PASS:
        return jsonify({"error": "Invalid password"}), 403
    worker.stop_worker()
    return redirect(url_for("dashboard"))


@app.route("/refresh", methods=["POST"])
def manual_refresh():
    password = request.form.get("password")
    if password != ADMIN_PASS:
        return jsonify({"error": "Invalid password"}), 403
    count = worker.refresh_all_sources()
    return jsonify({"message": f"Pulled and saved {count} offers."})


@app.route("/toggle_debug", methods=["POST"])
def toggle_debug():
    global DEBUG_REDIRECTS
    password = request.form.get("password")
    if password != ADMIN_PASS:
        return jsonify({"error": "Invalid password"}), 403
    DEBUG_REDIRECTS = not DEBUG_REDIRECTS
    os.environ["DEBUG_LOG_REDIRECTS"] = str(DEBUG_REDIRECTS)
    return jsonify({"message": f"Debug mode set to {DEBUG_REDIRECTS}"})


@app.route("/logs")
def logs():
    """Read last 100 lines of log file if available"""
    log_file = "worker.log"
    if not os.path.exists(log_file):
        return "No logs yet"
    with open(log_file, "r", encoding="utf-8") as f:
        lines = f.readlines()[-100:]
    return "<pre>" + "".join(lines) + "</pre>"


@app.route("/status")
def status():
    """Return worker status (AJAX friendly)"""
    return jsonify({
        "running": worker._worker_running,
        "pending": get_count("pending"),
        "sent": get_count("sent"),
        "failed": get_count("failed"),
        "debug": DEBUG_REDIRECTS
    })


def get_count(status):
    conn, cur = get_db_conn()
    cur.execute("SELECT COUNT(*) FROM posts WHERE status=%s", (status,))
    count = cur.fetchone()["count"]
    conn.close()
    return count


# ========== MAIN ==========
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
