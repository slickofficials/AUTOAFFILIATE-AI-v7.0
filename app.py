# app.py — AutoAffiliate Controller (final dashboard + controls)
import os
import logging
import math
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_from_directory, abort
from flask_compress import Compress
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user
from threading import Thread
from datetime import datetime, timedelta, timezone

# worker control functions (worker.py must be adjacent)
from worker import start_worker_background, refresh_all_sources, enqueue_manual_link, get_stats

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("app")

app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.getenv("SECRET_KEY", "slickofficials_hq_2025")
Compress(app)

COMPANY = os.getenv("COMPANY_NAME", "SlickOfficials HQ | Amson Multi Global LTD")
CONTACT_EMAIL = os.getenv("CONTACT_EMAIL", "support@slickofficials.com")

ALLOWED_EMAIL = os.getenv("ALLOWED_EMAIL", "admin@example.com")
ADMIN_PASS = os.getenv("ADMIN_PASS", "12345")
failed_logins = {}
LOCKOUT_DURATION = timedelta(hours=int(os.getenv("LOCKOUT_HOURS", "24")))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "10"))

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

@app.route("/sitemap.xml")
def sitemap(): return send_from_directory(".", "sitemap.xml")

@app.route("/robots.txt")
def robots(): return send_from_directory(".", "robots.txt")

@app.route("/")
def index():
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
        if client_ip in failed_logins: del failed_logins[client_ip]
        user = User(email); login_user(user)
        logger.info("Login success: %s", email)
        # start a background refresh when logging in
        Thread(target=refresh_all_sources, daemon=True).start()
        return redirect(url_for("dashboard"))
    else:
        failed_logins[client_ip]["count"] += 1
        left = MAX_ATTEMPTS - failed_logins[client_ip]["count"]
        if failed_logins[client_ip]["count"] >= 3:
            logger.warning("Failed login #%s for %s", failed_logins[client_ip]["count"], email)
        if left <= 0:
            failed_logins[client_ip]["locked_until"] = now + LOCKOUT_DURATION
            flash("BANNED: 24hr lock.")
        else:
            flash(f"Invalid credentials. {left} attempts left.")
        return render_template("login.html", company=COMPANY, title="Private Login")

@app.route("/dashboard")
@login_required
def dashboard():
    try:
        stats = get_stats()
        # compute next post in seconds
        next_post_in = stats.get("next_post_in_seconds", None)
        if next_post_in is not None:
            # show in human friendly hh:mm:ss
            secs = int(next_post_in)
            h = secs // 3600; m = (secs % 3600) // 60; s = secs % 60
            next_post_human = f"{h:02d}:{m:02d}:{s:02d}"
        else:
            next_post_human = "N/A"
        return render_template("dashboard.html",
                               company=COMPANY,
                               title="Dashboard",
                               stats=stats,
                               next_post_in=next_post_human)
    except Exception as e:
        logger.exception("Dashboard error: %s", e)
        return render_template("dashboard.html", company=COMPANY, title="Dashboard", stats={})

@app.route("/refresh", methods=["POST","GET"])
@login_required
def refresh():
    try:
        saved = refresh_all_sources()
        return jsonify({"status":"ok","saved": saved}), 200
    except Exception as e:
        logger.exception("Manual refresh failed: %s", e)
        return jsonify({"status":"error","error": str(e)}), 500

@app.route("/enqueue", methods=["POST"])
@login_required
def enqueue():
    data = request.get_json() or {}
    url = data.get("url")
    if not url:
        return jsonify({"error":"url required"}), 400
    try:
        result = enqueue_manual_link(url)
        return jsonify({"enqueued": True, "result": result}), 202
    except Exception as e:
        logger.exception("Enqueue failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/r/<int:post_id>")
def redirect_tracking(post_id):
    # Redirects to affiliate URL and logs a click (works for social links)
    try:
        import psycopg
        from psycopg.rows import dict_row
        DB = os.getenv("DATABASE_URL")
        conn = psycopg.connect(DB, row_factory=dict_row)
        cur = conn.cursor()
        cur.execute("SELECT url FROM posts WHERE id=%s", (post_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            abort(404)
        real = row["url"]
        # record click
        cur.execute("INSERT INTO clicks (post_id, ip, user_agent, created_at) VALUES (%s,%s,%s,%s)",
                    (post_id, request.remote_addr or "unknown", request.headers.get("User-Agent",""), datetime.now(timezone.utc)))
        conn.commit()
        conn.close()
        return redirect(real, code=302)
    except Exception as e:
        logger.exception("Redirect error: %s", e)
        abort(500)

@app.route("/health")
def health():
    return jsonify({"ok": True, "ts": datetime.now(timezone.utc).isoformat()}), 200

@app.route("/start", methods=["GET","POST"])
@login_required
def start():
    Thread(target=start_worker_background, daemon=True).start()
    logger.info("/start invoked - worker requested")
    return jsonify({"status":"worker_start_requested"}), 202

@app.errorhandler(404)
def not_found(e): return render_template("coming_soon.html"), 404

@app.route("/<path:path>")
def catch_all(path): return render_template("coming_soon.html")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    logger.info("Starting app on port %s", port)
    try:
        Thread(target=start_worker_background, daemon=True).start()
    except Exception:
        logger.exception("Auto worker start failed")
    app.run(host="0.0.0.0", port=port, debug=False)
