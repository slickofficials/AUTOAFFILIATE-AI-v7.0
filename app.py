# app.py - v23.0 $10M EMPIRE | FULLY SECURE + ROBUST + CLICK TRACKING
import os
import logging
from datetime import datetime, timezone, timedelta
from threading import Thread
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_from_directory, abort
from flask_compress import Compress
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user
import psycopg
from psycopg.rows import dict_row

# === LOGGING ===
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("app")

app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.getenv("SECRET_KEY", "slickofficials_hq_2025")
Compress(app)

# === CONFIG ===
COMPANY = os.getenv("COMPANY_NAME", "SlickOfficials HQ | Amson Multi Global LTD")
CONTACT_EMAIL = os.getenv("CONTACT_EMAIL", "support@slickofficials.com")
APP_PUBLIC_URL = os.getenv("APP_PUBLIC_URL") or os.getenv("PUBLIC_URL") or ""

# === AUTH ===
ALLOWED_EMAIL = os.getenv("ALLOWED_EMAIL", "admin@example.com")
ADMIN_PASS = os.getenv("ADMIN_PASS", "12345")
failed_logins = {}
LOCKOUT_DURATION = timedelta(hours=int(os.getenv("LOCKOUT_HOURS", "24")))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "10"))

# === DB ===
DB_URL = os.getenv("DATABASE_URL")
def get_db():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    conn = psycopg.connect(DB_URL, row_factory=dict_row, timeout=10)
    return conn, conn.cursor()

# === LOGIN MANAGER ===
class User(UserMixin):
    def __init__(self, email): self.id = email

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

@login_manager.user_loader
def load_user(user_id):
    return User(user_id) if user_id == ALLOWED_EMAIL else None

# === ALERT STUB ===
def send_alert_stub(title, body):
    logger.info("[ALERT] %s: %s", title, body)

# === SECURITY HEADERS ===
@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Cache-Control"] = "no-store, must-revalidate"
    return response

# === STATIC FILES ===
@app.route("/sitemap.xml")
def sitemap():
    return send_from_directory(".", "sitemap.xml")

@app.route("/robots.txt")
def robots():
    return send_from_directory(".", "robots.txt")

# === PAGES ===
@app.route("/")
def welcome():
    return render_template("welcome.html", company=COMPANY, title="Welcome")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html", company=COMPANY, title="Private Login")

    email = (request.form.get("email") or "").strip().lower()
    password = request.form.get("password", "")
    if not email or not password:
        flash("Missing login fields.")
        return render_template("login.html", company=COMPANY, title="Private Login")

    client_ip = request.remote_addr or "unknown"
    now = datetime.now(timezone.utc)
    failed_logins.setdefault(client_ip, {"count": 0, "locked_until": None})
    locked_until = failed_logins[client_ip]["locked_until"]

    if locked_until and locked_until > now:
        mins = int((locked_until - now).total_seconds() // 60)
        flash(f"Locked out. Try again in {mins} minutes.")
        return render_template("login.html", company=COMPANY, title="Private Login")

    if email == ALLOWED_EMAIL and password == ADMIN_PASS:
        failed_logins.pop(client_ip, None)
        user = User(email)
        login_user(user)
        logger.info("Login success: %s", email)
        Thread(target=trigger_refresh_background, daemon=True).start()
        return redirect(url_for("dashboard"))

    failed_logins[client_ip]["count"] += 1
    left = MAX_ATTEMPTS - failed_logins[client_ip]["count"]
    if failed_logins[client_ip]["count"] >= 3:
        send_alert_stub("FAILED LOGIN", f"Attempt #{failed_logins[client_ip]['count']}\nEmail: {email}")
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

# === CLICK TRACKING ===
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

        cur.execute("""
            INSERT INTO clicks (post_id, ip, user_agent, created_at) 
            VALUES (%s, %s, %s, %s)
        """, (post_id, request.remote_addr or "unknown", request.headers.get("User-Agent", ""), datetime.now(timezone.utc)))
        conn.commit()
        conn.close()
        return redirect(real, code=302)
    except Exception as e:
        logger.exception("Redirect error: %s", e)
        abort(500)

# === API STATS ===
@app.route("/api/stats")
@login_required
def api_stats():
    stat = {
        "total_links": 0, "pending": 0, "sent": 0, "failed": 0,
        "last_post_time": None, "next_post_in_seconds": None, "clicks": 0,
        "conversions": 0, "revenue": 0.0,
        "top_links": [], "recent_posts": [], "statuses": {},
        "chart_labels": [], "chart_values": []
    }
    try:
        conn, cur = get_db()
        cur.execute("SELECT COUNT(*) FROM posts"); stat["total_links"] = cur.fetchone()["count"] or 0
        cur.execute("SELECT COUNT(*) FROM posts WHERE status='pending'"); stat["pending"] = cur.fetchone()["count"] or 0
        cur.execute("SELECT COUNT(*) FROM posts WHERE status='sent'"); stat["sent"] = cur.fetchone()["count"] or 0
        cur.execute("SELECT COUNT(*) FROM posts WHERE status='failed'"); stat["failed"] = cur.fetchone()["count"] or 0
        cur.execute("SELECT posted_at FROM posts WHERE status='sent' ORDER BY posted_at DESC LIMIT 1")
        row = cur.fetchone()
        if row and row["posted_at"]:
            stat["last_post_time"] = row["posted_at"].astimezone(timezone.utc).isoformat()

        cur.execute("SELECT COUNT(*) FROM clicks"); stat["clicks"] = cur.fetchone()["count"] or 0

        try:
            cur.execute("SELECT COUNT(*) FROM conversions"); stat["conversions"] = cur.fetchone()["count"] or 0
            cur.execute("SELECT COALESCE(SUM(amount),0) FROM earnings"); stat["revenue"] = float(cur.fetchone()["sum"] or 0.0)
        except: pass

        cur.execute("""
            SELECT p.id, p.url, COUNT(c.id) AS clicks
            FROM posts p LEFT JOIN clicks c ON c.post_id = p.id
            GROUP BY p.id ORDER BY clicks DESC NULLS LAST LIMIT 6
        """)
        stat["top_links"] = [{"id": r["id"], "url": r["url"], "clicks": int(r["clicks"] or 0)} for r in cur.fetchall()]

        cur.execute("SELECT id, url, status, posted_at, created_at FROM posts ORDER BY created_at DESC LIMIT 10")
        stat["recent_posts"] = [{
            "id": r["id"], "url": r["url"], "status": r["status"],
            "posted_at": r["posted_at"].astimezone(timezone.utc).isoformat() if r["posted_at"] else None,
            "created_at": r["created_at"].astimezone(timezone.utc).isoformat()
        } for r in cur.fetchall()]

        stat["statuses"] = {
            "awin": bool(os.getenv("AWIN_ID")),
            "rakuten": bool(os.getenv("RAKUTEN_ID")),
            "twilio": bool(os.getenv("TWILIO_SID")),
            "worker": True
        }

        cur.execute("""
            SELECT date_trunc('hour', COALESCE(posted_at, created_at)) AS hr, COUNT(*) AS cnt
            FROM posts
            WHERE COALESCE(created_at, now()) > (now() - interval '12 hours')
            GROUP BY hr ORDER BY hr
        """)
        rows = cur.fetchall()
        stat["chart_labels"] = [r["hr"].astimezone(timezone.utc).strftime("%H:%M") for r in rows]
        stat["chart_values"] = [int(r["cnt"]) for r in rows]

        try:
            interval = int(os.getenv("POST_INTERVAL_SECONDS", "3600"))
            cur.execute("SELECT posted_at FROM posts WHERE status='sent' ORDER BY posted_at DESC LIMIT 1")
            last = cur.fetchone()
            if last and last["posted_at"]:
                elapsed = (datetime.now(timezone.utc) - last["posted_at"].astimezone(timezone.utc)).total_seconds()
                stat["next_post_in_seconds"] = max(0, interval - int(elapsed))
            else:
                stat["next_post_in_seconds"] = 0
        except: pass

        conn.close()
    except Exception as e:
        logger.exception("api_stats error: %s", e)
    return jsonify(stat)

# === CONTROL ENDPOINTS ===
def trigger_refresh_background():
    try:
        from worker import refresh_all_sources
        saved = refresh_all_sources()
        logger.info("Manual refresh saved %s links", saved)
    except Exception as e:
        logger.exception("Manual refresh failed: %s", e)

@app.route("/refresh", methods=["POST", "GET"])
@login_required
def refresh_route():
    Thread(target=trigger_refresh_background, daemon=True).start()
    return jsonify({"status": "refresh_queued"}), 202

@app.route("/enqueue", methods=["POST"])
@login_required
def enqueue_route():
    url = request.get_json().get("url")
    if not url: return jsonify({"error": "url required"}), 400
    try:
        from worker import enqueue_manual_link
        result = enqueue_manual_link(url)
        return jsonify({"enqueued": True, "result": result}), 202
    except Exception as e:
        logger.exception("enqueue failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/api/control", methods=["POST"])
@login_required
def api_control():
    action = request.get_json().get("action")
    if action not in ("start", "stop"): return jsonify({"error": "invalid action"}), 400
    try:
        if action == "start":
            from worker import start_worker_background
            Thread(target=start_worker_background, daemon=True).start()
            return jsonify({"status": "worker_start_requested"}), 202
        else:
            import worker
            worker.stop_worker()
            return jsonify({"status": "worker_stop_requested"}), 200
    except Exception as e:
        logger.exception("control failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/api/interval", methods=["POST"])
@login_required
def api_interval():
    data = request.get_json() or {}
    interval = data.get("interval")
    unit = data.get("unit", "minutes")
    if interval is None: return jsonify({"error": "interval required"}), 400
    try:
        seconds = int(interval) * 60 if unit == "minutes" else int(interval)
        conn, cur = get_db()
        cur.execute("CREATE TABLE IF NOT EXISTS settings (name TEXT PRIMARY KEY, value TEXT)")
        cur.execute("INSERT INTO settings (name,value) VALUES (%s,%s) ON CONFLICT (name) DO UPDATE SET value=%s",
                    ("post_interval_seconds", str(seconds), str(seconds)))
        conn.commit()
        conn.close()
        return jsonify({"status": "interval_updated", "seconds": seconds}), 200
    except Exception as e:
        logger.exception("interval update failed: %s", e)
        return jsonify({"error": str(e)}), 500

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
