# app.py — AutoAffiliate HQ (Flask + Dashboard)
import os
import logging
from datetime import datetime, timezone, timedelta
from threading import Thread

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_from_directory, abort
from flask_compress import Compress
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user
import psycopg
from psycopg.rows import dict_row
import json

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("app")

app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.getenv("SECRET_KEY", "slickofficials_hq_2025")
Compress(app)

COMPANY = os.getenv("COMPANY_NAME", "SlickOfficials HQ | Amson Multi Global LTD")
CONTACT_EMAIL = os.getenv("CONTACT_EMAIL", "support@slickofficials.com")
APP_PUBLIC_URL = os.getenv("APP_PUBLIC_URL") or os.getenv("PUBLIC_URL") or ""

# Auth
ALLOWED_EMAIL = os.getenv("ALLOWED_EMAIL", "admin@example.com")
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

# Ensure tables exist (idempotent)
def ensure_tables():
    conn, cur = get_db()
    try:
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
        conn.rollback()
        logger.exception("ensure_tables failed")
    finally:
        conn.close()

ensure_tables()

def db_get_setting(key, fallback=None):
    try:
        conn, cur = get_db()
        cur.execute("SELECT value FROM settings WHERE key=%s", (key,))
        row = cur.fetchone()
        conn.close()
        return row["value"] if row else fallback
    except Exception:
        logger.exception("db_get_setting")
        return fallback

def db_set_setting(key, value):
    try:
        conn, cur = get_db()
        cur.execute("INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", (key, str(value)))
        conn.commit(); conn.close()
        return True
    except Exception:
        logger.exception("db_set_setting")
        return False

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

@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Cache-Control"] = "no-store, must-revalidate"
    return response

# Static files endpoints (optional)
@app.route("/sitemap.xml")
def sitemap():
    return send_from_directory(".", "sitemap.xml")

@app.route("/robots.txt")
def robots():
    return send_from_directory(".", "robots.txt")

# Pages
@app.route("/")
def welcome():
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
        # optional: trigger a manual pull
        Thread(target=trigger_refresh_background, daemon=True).start()
        return redirect(url_for("dashboard"))
    else:
        failed_logins[client_ip]["count"] += 1
        left = MAX_ATTEMPTS - failed_logins[client_ip]["count"]
        if failed_logins[client_ip]["count"] >= 3:
            logger.info("FAILED LOGIN: %s attempts for %s", failed_logins[client_ip]["count"], email)
        if left <= 0:
            failed_logins[client_ip]["locked_until"] = now + LOCKOUT_DURATION
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
    logger.info("LOGOUT")
    return redirect(url_for("welcome"))

# API used by worker to save links quickly (worker calls this)
@app.route("/api/save_link", methods=["POST"])
def api_save_link():
    data = request.get_json() or {}
    url = data.get("url")
    title = data.get("title") or ""
    if not url:
        return jsonify({"error":"url required"}), 400
    try:
        conn, cur = get_db()
        cur.execute("INSERT INTO posts (url, source, status, created_at, meta) VALUES (%s,%s,'pending',%s,%s) ON CONFLICT (url) DO NOTHING",
                    (url, "affiliate", datetime.now(timezone.utc), json.dumps({"title": title})))
        conn.commit(); conn.close()
        return jsonify({"saved": True}), 201
    except Exception as e:
        logger.exception("api_save_link failed")
        return jsonify({"error": str(e)}), 500

# Redirect tracking
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
        cur.execute("INSERT INTO clicks (post_id, ip, user_agent, created_at) VALUES (%s,%s,%s,%s)",
                    (post_id, request.remote_addr or "unknown", request.headers.get("User-Agent",""), datetime.now(timezone.utc)))
        conn.commit()
        conn.close()
        return redirect(real, code=302)
    except Exception as e:
        logger.exception("Redirect error: %s", e)
        abort(500)

# Stats for dashboard (called by dashboard JS)
@app.route("/api/stats")
@login_required
def api_stats():
    stat = {
        "total_links": 0, "pending":0, "sent":0, "failed":0,
        "last_post_time": None, "next_post_in_seconds": None, "clicks_total":0,
        "top_links": [], "recent_posts": [], "statuses": {}
    }
    try:
        conn, cur = get_db()
        cur.execute("SELECT COUNT(*) as c FROM posts")
        stat["total_links"] = cur.fetchone()["c"] or 0
        cur.execute("SELECT COUNT(*) as c FROM posts WHERE status='pending'")
        stat["pending"] = cur.fetchone()["c"] or 0
        cur.execute("SELECT COUNT(*) as c FROM posts WHERE status='sent'")
        stat["sent"] = cur.fetchone()["c"] or 0
        cur.execute("SELECT COUNT(*) as c FROM posts WHERE status='failed'")
        stat["failed"] = cur.fetchone()["c"] or 0
        cur.execute("SELECT posted_at FROM posts WHERE status='sent' ORDER BY posted_at DESC LIMIT 1")
        row = cur.fetchone()
        if row and row["posted_at"]:
            stat["last_post_time"] = row["posted_at"].astimezone(timezone.utc).isoformat()
        cur.execute("SELECT COUNT(*) as c FROM clicks")
        stat["clicks_total"] = cur.fetchone()["c"] or 0
        # top links
        cur.execute("""
            SELECT p.id, p.url, COALESCE(counts.c,0) AS clicks
            FROM posts p LEFT JOIN (
              SELECT post_id, count(*) as c FROM clicks GROUP BY post_id
            ) counts ON counts.post_id = p.id
            ORDER BY clicks DESC NULLS LAST LIMIT 6
        """)
        rows = cur.fetchall()
        stat["top_links"] = [{"id": r["id"], "url": r["url"], "clicks": int(r["clicks"])} for r in rows]
        # recent posts
        cur.execute("SELECT id, url, status, posted_at FROM posts ORDER BY created_at DESC LIMIT 10")
        rp = cur.fetchall()
        stat["recent_posts"] = [{"id":r["id"], "url":r["url"], "status":r["status"], "posted_at": r["posted_at"].astimezone(timezone.utc).isoformat() if r["posted_at"] else None} for r in rp]
        conn.close()
        # next_post computed from setting
        interval = int(db_get_setting("post_interval_seconds", fallback=str(3600)))
        if stat["last_post_time"]:
            last = datetime.fromisoformat(stat["last_post_time"])
            last_ts = last.replace(tzinfo=timezone.utc).timestamp()
            now_ts = datetime.now(timezone.utc).timestamp()
            elapsed = now_ts - last_ts
            next_in = max(0, interval - int(elapsed))
            stat["next_post_in_seconds"] = next_in
        else:
            stat["next_post_in_seconds"] = 0
        # extra fields for dashboard ease
        stat["conversions"] = 0
        stat["revenue"] = 0.0
        stat["chart_labels"] = []
        stat["chart_values"] = []
    except Exception as e:
        logger.exception("api_stats error: %s", e)
    return jsonify(stat)

# Admin actions: start/stop worker, update interval
@app.route("/api/control", methods=["POST"])
@login_required
def api_control():
    data = request.get_json() or {}
    action = data.get("action")
    if action == "start":
        try:
            from worker import start_worker_background
            Thread(target=start_worker_background, daemon=True).start()
            return jsonify({"status":"worker_start_requested"}), 202
        except Exception as e:
            logger.exception("start worker failed")
            return jsonify({"error": str(e)}), 500
    elif action == "stop":
        try:
            import worker
            worker.stop_worker()
            return jsonify({"status":"worker_stop_requested"}), 200
        except Exception as e:
            logger.exception("stop failed")
            return jsonify({"error": str(e)}), 500
    else:
        return jsonify({"error":"unknown action"}), 400

@app.route("/api/interval", methods=["POST"])
@login_required
def api_interval():
    data = request.get_json() or {}
    interval = int(data.get("interval", 3600))
    ok = db_set_setting("post_interval_seconds", str(interval))
    return jsonify({"ok": ok, "interval": interval})

# Manual refresh trigger (optional)
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

@app.route("/health")
def health():
    return jsonify({"ok": True, "ts": datetime.now(timezone.utc).isoformat()}), 200

@app.errorhandler(404)
def not_found(e):
    return render_template("welcome.html", company=COMPANY), 404

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    logger.info("Starting app on port %s", port)
    app.run(host="0.0.0.0", port=port, debug=False)
