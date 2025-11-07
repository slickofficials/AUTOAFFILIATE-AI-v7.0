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

# Minimal alert helper (app keeps as log; worker sends real Twilio/Telegram)
def send_alert_stub(title, body):
    logger.info("[ALERT] %s: %s", title, body)

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

# Basic pages
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
        # start a one-off refresh in background for convenience
        Thread(target=trigger_refresh_background, daemon=True).start()
        return redirect(url_for("dashboard"))
    else:
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
    # templates/dashboard.html fetches /api/stats via JS to populate values.
    # supply public_url for redirect links if needed
    return render_template("dashboard.html", company=COMPANY, title="HQ Dashboard", public_url=APP_PUBLIC_URL)

@app.route("/logout")
@login_required
def logout():
    logout_user()
    send_alert_stub("LOGOUT", "User logged out")
    return redirect(url_for("welcome"))

# Redirect + click logging
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
        cur.execute(
            "INSERT INTO clicks (post_id, ip, user_agent, created_at) VALUES (%s,%s,%s,%s)",
            (post_id, request.remote_addr or "unknown", request.headers.get("User-Agent",""), datetime.now(timezone.utc))
        )
        conn.commit()
        conn.close()
        return redirect(real, code=302)
    except Exception as e:
        logger.exception("Redirect error: %s", e)
        abort(500)

# API for dashboard: returns both legacy and dashboard-HTML-expected fields
@app.route("/api/stats")
@login_required
def api_stats():
    stat = {
        "total_links": 0, "pending":0, "sent":0, "failed":0,
        "last_posted_at": None, "next_post_in_seconds": None, "clicks_total":0,
        # Legacy / dashboard fields below:
        "clicks": 0, "conversions": 0, "revenue": 0.0, "last_post_time": None,
        "chart_labels": [], "chart_values": [],
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
            stat["last_posted_at"] = row["posted_at"].astimezone(timezone.utc).isoformat()
            stat["last_post_time"] = stat["last_posted_at"]
        cur.execute("SELECT COUNT(*) as c FROM clicks")
        clicks_total = cur.fetchone()["c"] or 0
        stat["clicks_total"] = clicks_total
        stat["clicks"] = clicks_total  # for dashboard.html compatibility
        # conversions & revenue: if you have an earnings table, this will pick it up
        try:
            cur.execute("SELECT COUNT(*) as c FROM earnings")
            stat["conversions"] = cur.fetchone()["c"] or 0
            cur.execute("SELECT COALESCE(SUM(amount),0) as s FROM earnings")
            stat["revenue"] = float(cur.fetchone()["s"] or 0.0)
        except Exception:
            stat["conversions"] = 0
            stat["revenue"] = 0.0
        # top links by clicks (join)
        cur.execute("""
            SELECT p.id, p.url, COUNT(c.id) AS clicks
            FROM posts p LEFT JOIN clicks c ON c.post_id = p.id
            GROUP BY p.id ORDER BY clicks DESC NULLS LAST LIMIT 6
        """)
        rows = cur.fetchall()
        top = []
        for r in rows:
            top.append({"id": r["id"], "url": r["url"], "clicks": int(r["clicks"] or 0)})
        stat["top_links"] = top
        # recent posts
        cur.execute("SELECT id, url, status, posted_at FROM posts ORDER BY created_at DESC LIMIT 10")
        rp = cur.fetchall()
        recent = []
        for r in rp:
            recent.append({
                "id": r["id"], "url": r["url"], "status": r["status"],
                "posted_at": r["posted_at"].astimezone(timezone.utc).isoformat() if r["posted_at"] else None
            })
        stat["recent_posts"] = recent
        # statuses (simple health probes)
        stat["statuses"] = {
            "awin": bool(os.getenv("AWIN_PUBLISHER_ID")),
            "rakuten": bool(os.getenv("RAKUTEN_CLIENT_ID")),
            "openai": bool(os.getenv("OPENAI_API_KEY")),
            "heygen": bool(os.getenv("HEYGEN_API_KEY")),
            "twilio": bool(os.getenv("TWILIO_SID") and os.getenv("TWILIO_TOKEN"))
        }
        conn.close()

        # build a simple activity chart (posts-per-hour for last 12 hours)
        try:
            conn, cur = get_db()
            cur.execute("""
                SELECT date_trunc('hour', coalesce(posted_at, created_at)) as hr, count(*) as cnt
                FROM posts WHERE (posted_at IS NOT NULL OR created_at IS NOT NULL)
                AND created_at >= now() - interval '24 hours'
                GROUP BY hr ORDER BY hr ASC
            """)
            rows = cur.fetchall()
            labels = []
            values = []
            for r in rows:
                labels.append(r["hr"].astimezone(timezone.utc).strftime("%H:%M"))
                values.append(int(r["cnt"] or 0))
            stat["chart_labels"] = labels
            stat["chart_values"] = values
            conn.close()
        except Exception:
            stat["chart_labels"], stat["chart_values"] = [], []
        # next_post_in_seconds calculation (based on last_posted_at)
        interval = int(os.getenv("POST_INTERVAL_SECONDS", "3600"))
        if stat.get("last_posted_at"):
            last = datetime.fromisoformat(stat["last_posted_at"])
            last_ts = last.replace(tzinfo=timezone.utc).timestamp()
            now_ts = datetime.now(timezone.utc).timestamp()
            elapsed = now_ts - last_ts
            next_in = max(0, interval - int(elapsed))
            stat["next_post_in_seconds"] = next_in
        else:
            stat["next_post_in_seconds"] = 0
    except Exception as e:
        logger.exception("api_stats error: %s", e)
    return jsonify(stat)

# Admin API actions
def trigger_refresh_background():
    # import worker functions lazily to avoid circular imports on startup
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

# endpoint for dashboard buttons (start/stop) - used by dashboard's JS
@app.route("/api/control", methods=["POST"])
@login_required
def api_control():
    data = request.get_json() or {}
    action = data.get("action", "").lower()
    if action == "start":
        Thread(target=lambda: __import__("worker").start_worker_background(), daemon=True).start()
        return jsonify({"status":"started"}), 200
    if action == "stop":
        try:
            import worker
            worker.stop_worker()
            return jsonify({"status":"stopped"}), 200
        except Exception as e:
            logger.exception("stop failed: %s", e)
            return jsonify({"error": str(e)}), 500
    return jsonify({"error":"unknown action"}), 400

# interval update endpoint (dashboard control)
@app.route("/api/interval", methods=["POST"])
@login_required
def api_interval():
    data = request.get_json() or {}
    interval = int(data.get("interval") or 0)
    if interval <= 0:
        return jsonify({"error":"invalid interval"}), 400
    # set env var for new interval (effective for newly started worker only)
    os.environ["POST_INTERVAL_SECONDS"] = str(int(interval) * 60)  # incoming is minutes in UI
    return jsonify({"status":"ok","interval_seconds": os.environ["POST_INTERVAL_SECONDS"]}), 200

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
