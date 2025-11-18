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

# local worker module (make sure worker.py sits next to app.py)
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

# DB helper (used by admin UI)
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

# ---------- simple pages ----------
@app.route("/")
def welcome():
    # if logged in redirect to dashboard
    if current_user and getattr(current_user, "id", None) == ALLOWED_EMAIL:
        return redirect(url_for("dashboard"))
    return render_template("welcome.html", company=COMPANY, title="Welcome")

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "GET":
        return render_template("login.html", company=COMPANY, title="Private Login")
    email = (request.form.get("email") or request.form.get("username") or "").strip().lower()
    password = request.form.get("password","")
    if not email or not password:
        flash("Missing login fields.")
        return render_template("login.html", company=COMPANY, title="Private Login")
    client_ip = request.remote_addr or "unknown"
    if client_ip not in failed_logins:
        failed_logins[client_ip] = {"count": 0, "locked_until": None}
    now = datetime.now(timezone.utc)
    locked_until = failed_logins[client_ip]["locked_until"]
    if locked_until and locked_until > now:
        mins = int((locked_until - now).total_seconds() // 60)
        flash(f"Locked out. Try again in {mins} minutes.")
        return render_template("login.html", company=COMPANY, title="Private Login")
    if email == ALLOWED_EMAIL and password == ADMIN_PASS:
        if client_ip in failed_logins:
            del failed_logins[client_ip]
        user = User(email); login_user(user)
        logger.info("Login success: %s", email)
        # start a refresh in background for convenience
        Thread(target=worker.refresh_all_sources, daemon=True).start()
        return redirect(url_for("dashboard"))
    else:
        failed_logins[client_ip]["count"] += 1
        left = MAX_ATTEMPTS - failed_logins[client_ip]["count"]
        if failed_logins[client_ip]["count"] >= 3:
            logger.info("FAILED LOGIN attempt #%s for %s", failed_logins[client_ip]["count"], email)
        if left <= 0:
            failed_logins[client_ip]["locked_until"] = now + LOCKOUT_DURATION
            flash("BANNED: 24hr lock.")
        else:
            flash(f"Invalid credentials. {left} attempts left.")
        return render_template("login.html", company=COMPANY, title="Private Login")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    logger.info("User logged out")
    return redirect(url_for("welcome"))

# ---------- Dashboard (single-page) ----------
@app.route("/dashboard")
@login_required
def dashboard():
    # read some settings to show on UI
    settings = {}
    try:
        conn, cur = get_db()
        # settings table might contain keys
        cur.execute("SELECT key, value FROM settings")
        for r in cur.fetchall():
            settings[r["key"]] = r["value"]
        conn.close()
    except Exception:
        logger.exception("dashboard failed")
    return render_template("dashboard.html",
                           company=COMPANY, title="HQ Dashboard",
                           public_url=APP_PUBLIC_URL, settings=settings)

# ---------- API endpoints for dashboard ----------
@app.route("/api/stats")
@login_required
def api_stats():
    try:
        s = worker.get_stats()
        # augment with worker status
        s["worker_running"] = getattr(worker, "_worker_running", False)
        s["post_interval_seconds"] = int(worker.POST_INTERVAL_SECONDS) if getattr(worker, "POST_INTERVAL_SECONDS", None) else int(os.getenv("POST_INTERVAL_SECONDS", "10800"))
        return jsonify(s)
    except Exception:
        logger.exception("api_stats error")
        return jsonify({"error":"failed"}), 500

@app.route("/api/control", methods=["POST"])
@login_required
def api_control():
    data = request.get_json() or {}
    action = (data.get("action") or "").lower()
    if action == "start":
        Thread(target=worker.start_worker_background, daemon=True).start()
        return jsonify({"status":"worker_start_requested"}), 202
    if action == "stop":
        try:
            worker.stop_worker()
            return jsonify({"status":"worker_stop_requested"}), 200
        except Exception as e:
            logger.exception("stop failed")
            return jsonify({"error": str(e)}), 500
    if action == "refresh":
        Thread(target=worker.refresh_all_sources, daemon=True).start()
        return jsonify({"status":"refresh_queued"}), 202
    return jsonify({"error":"unknown action"}), 400

@app.route("/api/interval", methods=["POST"])
@login_required
def api_interval():
    data = request.get_json() or {}
    interval = data.get("interval")
    try:
        sec = int(interval)
        # persist to DB settings
        conn, cur = get_db()
        cur.execute("INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", ("post_interval_seconds", str(sec)))
        conn.commit(); conn.close()
        # update worker var (worker will pick next time it starts)
        worker.POST_INTERVAL_SECONDS = sec
        return jsonify({"status":"ok","post_interval_seconds":sec}), 200
    except Exception:
        logger.exception("set interval failed")
        return jsonify({"error":"invalid interval"}), 400

@app.route("/api/enqueue", methods=["POST"])
@login_required
def api_enqueue():
    data = request.get_json() or {}
    url = data.get("url")
    if not url:
        return jsonify({"error":"url required"}), 400
    try:
        res = worker.enqueue_manual_link(url)
        return jsonify({"enqueued": True, "result": res}), 202
    except Exception as e:
        logger.exception("enqueue failed")
        return jsonify({"error": str(e)}), 500

@app.route("/api/logs")
@login_required
def api_logs():
    # basic tail of postgres logs table if exists OR fallback to reading last worker.log file
    logs = []
    try:
        conn, cur = get_db()
        cur.execute("SELECT id, message, created_at FROM logs ORDER BY created_at DESC LIMIT 200")
        rows = cur.fetchall()
        for r in rows:
            logs.append({"id": r["id"], "message": r["message"], "created_at": r["created_at"].isoformat()})
        conn.close()
    except Exception:
        # if no logs table, return empty — real logging is in stdout (render logs)
        logger.debug("no logs table or failed to fetch")
    return jsonify({"logs": logs})

# convenience route for redirect tracking
@app.route("/r/<int:post_id>")
def redirect_tracking(post_id):
    try:
        conn, cur = get_db()
        cur.execute("SELECT url FROM posts WHERE id=%s", (post_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            abort(404)
        real = row["url"]
        # record click
        cur.execute("INSERT INTO clicks (post_id, ip, user_agent, created_at) VALUES (%s,%s,%s,%s)",
                    (post_id, request.remote_addr or "unknown", request.headers.get("User-Agent",""), datetime.now(timezone.utc)))
        conn.commit(); conn.close()
        return redirect(real, code=302)
    except Exception:
        logger.exception("redirect error")
        abort(500)

# health
@app.route("/health")
def health():
    return jsonify({"ok": True, "ts": datetime.now(timezone.utc).isoformat()}), 200

# error handlers
@app.errorhandler(404)
def not_found(e):
    return render_template("welcome.html", company=COMPANY), 404

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    logger.info("Starting app on port %s", port)
    app.run(host="0.0.0.0", port=port, debug=False)
