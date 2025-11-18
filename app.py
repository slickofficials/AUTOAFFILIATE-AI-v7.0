# app.py — AutoAffiliate HQ Full Production with Live Dashboard

import os
import logging
from datetime import datetime, timezone, timedelta
from threading import Thread

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_from_directory, abort
from flask_compress import Compress
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user
import psycopg
from psycopg.rows import dict_row

# ------------------------------
# Logging
# ------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("app")

# ------------------------------
# Flask setup
# ------------------------------
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.getenv("SECRET_KEY", "slickofficials_hq_2025")
Compress(app)

# ------------------------------
# App configuration
# ------------------------------
COMPANY = os.getenv("COMPANY_NAME", "SlickOfficials HQ | Amson Multi Global LTD")
CONTACT_EMAIL = os.getenv("CONTACT_EMAIL", "support@slickofficials.com")
APP_PUBLIC_URL = os.getenv("PUBLIC_URL") or os.getenv("APP_PUBLIC_URL") or ""

# Auth configuration
ALLOWED_EMAIL = os.getenv("ALLOWED_EMAIL", "admin@example.com")
ADMIN_PASS = os.getenv("ADMIN_PASS", "12345")
ALLOWED_IP = os.getenv("ALLOWED_IP", "").strip()
failed_logins = {}
LOCKOUT_DURATION = timedelta(hours=int(os.getenv("LOCKOUT_HOURS", "24")))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "10"))

# ------------------------------
# Database
# ------------------------------
DB_URL = os.getenv("DATABASE_URL")

def get_db():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg.connect(DB_URL, row_factory=dict_row)

def ensure_tables():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    id SERIAL PRIMARY KEY,
                    url TEXT UNIQUE NOT NULL,
                    source TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMPTZ DEFAULT now(),
                    posted_at TIMESTAMPTZ,
                    meta JSONB DEFAULT '{}'::jsonb
                );
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS clicks (
                    id SERIAL PRIMARY KEY,
                    post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
                    ip TEXT,
                    user_agent TEXT,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
                """)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
                """)
            conn.commit()
    except Exception:
        logger.exception("ensure_tables failed")

ensure_tables()

# ------------------------------
# Login
# ------------------------------
class User(UserMixin):
    def __init__(self, email):
        self.id = email

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

@login_manager.user_loader
def load_user(user_id):
    if user_id == ALLOWED_EMAIL:
        return User(user_id)
    return None

# ------------------------------
# Helpers
# ------------------------------
def send_alert_stub(title, body):
    logger.info("[ALERT] %s: %s", title, body)

def db_get_setting(key, fallback=None):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM settings WHERE key=%s", (key,))
                row = cur.fetchone()
        return row["value"] if row else fallback
    except Exception:
        logger.exception("db_get_setting failed")
        return fallback

def db_set_setting(key, value):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO settings(key,value)
                    VALUES(%s,%s)
                    ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                """, (key, str(value)))
            conn.commit()
        return True
    except Exception:
        logger.exception("db_set_setting failed")
        return False

@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Cache-Control"] = "no-store, must-revalidate"
    return response

# ------------------------------
# Static files
# ------------------------------
@app.route("/sitemap.xml")
def sitemap():
    return send_from_directory(".", "sitemap.xml")

@app.route("/robots.txt")
def robots():
    return send_from_directory(".", "robots.txt")

# ------------------------------
# Pages
# ------------------------------
@app.route("/")
def welcome():
    return render_template("welcome.html", company=COMPANY, title="Welcome")

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "GET":
        return render_template("login.html", company=COMPANY, title="Private Login")

    client_ip = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown")
    if ALLOWED_IP and client_ip != ALLOWED_IP:
        flash("Access restricted.")
        return render_template("login.html", company=COMPANY, title="Private Login")

    email = (request.form.get("email") or request.form.get("username") or "").strip().lower()
    password = request.form.get("password","")

    if not email or not password:
        flash("Missing login fields.")
        return render_template("login.html", company=COMPANY, title="Private Login")

    if client_ip not in failed_logins:
        failed_logins[client_ip] = {"count": 0, "locked_until": None}
    now = datetime.now(timezone.utc)
    locked_until = failed_logins[client_ip]["locked_until"]
    if locked_until and locked_until > now:
        mins = int((locked_until - now).total_seconds() // 60)
        flash(f"Locked out. Try again in {mins} minutes.")
        return render_template("login.html", company=COMPANY, title="Private Login")

    if email == ALLOWED_EMAIL and password == ADMIN_PASS:
        failed_logins[client_ip]["count"] = 0
        user = User(email)
        login_user(user)
        logger.info("Login success: %s", email)
        Thread(target=trigger_refresh_background, daemon=True).start()
        return redirect(url_for("dashboard"))
    else:
        failed_logins[client_ip]["count"] += 1
        left = MAX_ATTEMPTS - failed_logins[client_ip]["count"]
        if failed_logins[client_ip]["count"] >= 3:
            send_alert_stub("FAILED LOGIN", f"Attempt #{failed_logins[client_ip]['count']}\nEmail: {email}\nIP: {client_ip}")
        if left <= 0:
            failed_logins[client_ip]["locked_until"] = now + LOCKOUT_DURATION
            send_alert_stub("LOCKED OUT", "Too many failed login attempts")
            flash("BANNED: 24hr lock.")
        else:
            flash(f"Invalid credentials. {left} attempts left.")
        return render_template("login.html", company=COMPANY, title="Private Login")

@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html", company=COMPANY, title="HQ Dashboard", public_url=APP_PUBLIC_URL)

@app.route("/logout")
@login_required
def logout():
    logout_user()
    send_alert_stub("LOGOUT", "User logged out")
    return redirect(url_for("welcome"))

# ------------------------------
# Redirect + click logging
# ------------------------------
@app.route("/r/<int:post_id>")
def redirect_tracking(post_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT url FROM posts WHERE id=%s", (post_id,))
                row = cur.fetchone()
                if not row:
                    abort(404)
                real = row["url"]
                cur.execute(
                    "INSERT INTO clicks (post_id, ip, user_agent, created_at) VALUES (%s,%s,%s,%s)",
                    (post_id, request.headers.get("X-Forwarded-For", request.remote_addr or "unknown"),
                     request.headers.get("User-Agent",""), datetime.now(timezone.utc))
                )
            conn.commit()
        return redirect(real, code=302)
    except Exception as e:
        logger.exception("Redirect error: %s", e)
        abort(500)

# ------------------------------
# Analytics API
# ------------------------------
@app.route("/api/stats")
@login_required
def api_stats():
    from worker import get_worker_status
    stat = get_worker_status()
    return jsonify(stat)

# ------------------------------
# Admin actions
# ------------------------------
def trigger_refresh_background():
    try:
        from worker import refresh_all_sources
        saved = refresh_all_sources()
        logger.info("Manual refresh saved %s links", saved)
    except Exception:
        logger.exception("Manual refresh failed")

@app.route("/refresh", methods=["POST","GET"])
@login_required
def refresh_route():
    Thread(target=trigger_refresh_background, daemon=True).start()
    return jsonify({"status":"refresh_queued"}), 202

@app.route("/enqueue", methods=["POST"])
@login_required
def enqueue_route():
    data = request.get_json() or {}
    url = data.get("url")
    if not url:
        return jsonify({"error":"url required"}), 400
    try:
        from worker import enqueue_manual_link
        result = enqueue_manual_link(url)
        return jsonify({"enqueued": True, "result": result}), 202
    except Exception as e:
        logger.exception("enqueue failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/start", methods=["POST","GET"])
@login_required
def start_worker():
    from worker import start_worker_background
    Thread(target=start_worker_background, daemon=True).start()
    return jsonify({"status":"worker_start_requested"}), 202

@app.route("/stop", methods=["POST","GET"])
@login_required
def stop_worker_route():
    try:
        import worker
        worker.stop_worker()
        return jsonify({"status":"worker_stop_requested"}), 200
    except Exception as e:
        logger.exception("stop failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/api/interval", methods=["POST"])
@login_required
def api_interval():
    data = request.get_json() or {}
    interval_min = int(data.get("interval", 0))
    if interval_min <= 0:
        return jsonify({"error":"invalid interval"}), 400
    seconds = interval_min * 60
    ok = db_set_setting("post_interval_seconds", seconds)
    if ok:
        return jsonify({"status":"updated","post_interval_seconds":seconds})
    return jsonify({"error":"failed to save"}), 500

@app.route("/health")
def health():
    return jsonify({"ok": True, "ts": datetime.now(timezone.utc).isoformat()}), 200

@app.errorhandler(404)
def not_found(e):
    return render_template("welcome.html", company=COMPANY), 404

# ------------------------------
# Startup
# ------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    try:
        if not db_get_setting("post_interval_seconds"):
            db_set_setting("post_interval_seconds", str(3*3600))
    except Exception:
        pass
    logger.info("Starting app on port %s", port)
    app.run(host="0.0.0.0", port=port, debug=False)
