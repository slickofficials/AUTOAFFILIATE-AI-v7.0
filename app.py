# app.py — AutoAffiliate HQ (Flask + Dashboard + control APIs)
import os
import logging
from datetime import datetime, timezone, timedelta
from threading import Thread

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, abort
from flask_compress import Compress
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user

import psycopg
from psycopg.rows import dict_row

import worker

# ---------- config ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("app")

app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.getenv("SECRET_KEY", "slickofficials_hq_2025")
Compress(app)

COMPANY = os.getenv("COMPANY_NAME", "SlickOfficials HQ | Amson Multi Global LTD")
APP_PUBLIC_URL = os.getenv("APP_PUBLIC_URL") or os.getenv("PUBLIC_URL") or ""

# Auth
ALLOWED_EMAIL = os.getenv("ALLOWED_EMAIL", "admin@example.com").lower()
ADMIN_PASS = os.getenv("ADMIN_PASS", "12345")
failed_logins = {}
LOCKOUT_DURATION = timedelta(hours=int(os.getenv("LOCKOUT_HOURS", "24")))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "10"))

# DB helper
DB_URL = os.getenv("DATABASE_URL")
def get_db():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# Flask-Login
class User(UserMixin):
    def __init__(self, email): self.id = email

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

@login_manager.user_loader
def load_user(user_id):
    if user_id == ALLOWED_EMAIL:
        return User(user_id)
    return None

# security headers
@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Cache-Control"] = "no-store, must-revalidate"
    return response

# ---------- pages ----------
@app.route("/")
def welcome():
    if current_user and getattr(current_user, "id", None) == ALLOWED_EMAIL:
        return redirect(url_for("dashboard"))
    return render_template("welcome.html", company=COMPANY, title="Welcome")

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "GET":
        return render_template("login.html", company=COMPANY, title="Private Login")
    email = (request.form.get("email") or "").strip().lower()
    password = request.form.get("password","")
    if not email or not password:
        flash("Missing login fields."); return render_template("login.html", company=COMPANY)
    client_ip = request.remote_addr or "unknown"
    if client_ip not in failed_logins:
        failed_logins[client_ip] = {"count": 0, "locked_until": None}
    now = datetime.now(timezone.utc)
    if failed_logins[client_ip]["locked_until"] and failed_logins[client_ip]["locked_until"] > now:
        flash("Locked out. Try later."); return render_template("login.html", company=COMPANY)
    if email == ALLOWED_EMAIL and password == ADMIN_PASS:
        failed_logins.pop(client_ip, None)
        user = User(email); login_user(user)
        Thread(target=worker.refresh_all_sources, daemon=True).start()
        return redirect(url_for("dashboard"))
    failed_logins[client_ip]["count"] += 1
    if failed_logins[client_ip]["count"] >= MAX_ATTEMPTS:
        failed_logins[client_ip]["locked_until"] = now + LOCKOUT_DURATION
        flash("BANNED: 24hr lock.")
    else:
        left = MAX_ATTEMPTS - failed_logins[client_ip]["count"]
        flash(f"Invalid credentials. {left} attempts left.")
    return render_template("login.html", company=COMPANY)

@app.route("/logout")
@login_required
def logout():
    logout_user(); return redirect(url_for("welcome"))

@app.route("/dashboard")
@login_required
def dashboard():
    settings = {}
    try:
        conn, cur = get_db()
        cur.execute("SELECT key, value FROM settings")
        for r in cur.fetchall(): settings[r["key"]] = r["value"]
        conn.close()
    except Exception: logger.exception("dashboard failed")
    return render_template("dashboard.html", company=COMPANY, title="HQ Dashboard", public_url=APP_PUBLIC_URL, settings=settings)

# ---------- API endpoints ----------
@app.route("/api/stats")
@login_required
def api_stats():
    s = worker.get_stats()
    s["worker_running"] = getattr(worker, "_worker_running", False)
    s["post_interval_seconds"] = int(worker.POST_INTERVAL_SECONDS)
    return jsonify(s)

@app.route("/api/control", methods=["POST"])
@login_required
def api_control():
    action = (request.get_json() or {}).get("action","").lower()
    if action == "start":
        Thread(target=worker.start_worker_background, daemon=True).start(); return jsonify({"status":"worker_start_requested"})
    if action == "stop":
        worker.stop_worker(); return jsonify({"status":"worker_stop_requested"})
    if action == "refresh":
        Thread(target=worker.refresh_all_sources, daemon=True).start(); return jsonify({"status":"refresh_queued"})
    if action == "post_now":
        Thread(target=worker.post_next_pending, daemon=True).start(); return jsonify({"status":"post_queued"})
    return jsonify({"error":"unknown action"}), 400

@app.route("/api/interval", methods=["POST"])
@login_required
def api_interval():
    sec = int((request.get_json() or {}).get("interval",0))
    conn, cur = get_db()
    cur.execute("INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", ("post_interval_seconds", str(sec)))
    conn.commit(); conn.close()
    worker.POST_INTERVAL_SECONDS = sec
    return jsonify({"status":"ok","post_interval_seconds":sec})

@app.route("/api/enqueue", methods=["POST"])
@login_required
def api_enqueue():
    url = (request.get_json() or {}).get("url")
    if not url: return jsonify({"error":"url required"}), 400
    res = worker.enqueue_manual_link(url); return jsonify({"enqueued": True, "result": res})

@app.route("/api/recent_posts")
@login_required
def api_recent_posts():
    conn, cur = get_db()
    cur.execute("SELECT id, url, status, posted_at FROM posts ORDER BY created_at DESC LIMIT 20")
    rows = cur.fetchall(); conn.close()
    return jsonify({"recent_posts":[{"id":r["id"],"url":r["url"],"status":r["status"],"posted_at":r["posted_at"].isoformat() if r["posted_at"] else None} for r in rows]})

@app.route("/export/posts.csv")
@login_required
def export_posts_csv():
    conn, cur = get_db()
    cur.execute("SELECT id, url, source, status, created_at, posted_at FROM posts ORDER BY created_at DESC")
    rows = cur.fetchall(); conn.close()
    lines = ["id,url,source,status,created_at,posted_at"]
    for r in rows:
        lines.append(f"{r['id']},{r['url']},{r['source']},{r['status']},{r['created_at'].isoformat()},{r['posted_at'].isoformat() if r['posted_at'] else ''}")
    return "\n".join(lines), 200, {"Content-Type":"text/csv"}

@app.route("/api/health")
@login_required
def api_health(): return jsonify(worker.health_summary())

@app.route("/api/channel", methods=["POST"])
@login_required
def api_channel():
    data = request.get_json() or {}
    name = (data.get("name") or "").lower(); pause = bool(data.get("pause"))
    worker.pause_channel(name, pause)
    return jsonify({"status":"ok","channel":name,"paused":pause})

@app.route("/r/<int:post_id>")
def redirect_tracking(post_id):
    conn, cur = get_db()
    cur.execute("SELECT url FROM posts WHERE id=%s", (post_id,))
    row = cur.fetchone()
    if not row: conn.close(); abort(404)
    cur.execute("INSERT INTO clicks (post_id, ip, user_agent, created_at) VALUES (%s,%s,%s,%s)", (post_id, request.remote_addr or "unknown", request.headers.get("User-Agent",""), datetime.now(timezone.utc)))
    conn.commit(); conn.close()
    return redirect(row["url"], code=302)

@app.route("/health")
def health(): return jsonify({"ok": True, "ts": datetime.now(timezone.utc).isoformat()})

@app.errorhandler(404)
def not_found(e): return render_template("welcome
