# app.py — AutoAffiliate HQ (Flask + Dashboard) — production-ready
import os
import logging
from datetime import datetime, timezone, timedelta
from threading import Thread

from flask import (
    Flask, render_template, request, redirect, url_for, flash,
    jsonify, send_from_directory, abort
)
from flask_compress import Compress
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from redis import Redis
from rq import Queue
from apscheduler.schedulers.background import BackgroundScheduler

# Worker import points (your worker.py must expose these)
# - refresh_all_sources()
# - enqueue_manual_link(url)
# - start_worker_background()
# - stop_worker()
# - get_stats()
import worker

# -------------------------
# Logging
# -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("app")

# -------------------------
# Flask
# -------------------------
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.getenv("SECRET_KEY", "slickofficials_hq_2025")
Compress(app)

# -------------------------
# App config
# -------------------------
COMPANY = os.getenv("COMPANY_NAME", "SlickOfficials HQ | Amson Multi Global LTD")
CONTACT_EMAIL = os.getenv("CONTACT_EMAIL", "support@slickofficials.com")
APP_PUBLIC_URL = os.getenv("PUBLIC_URL") or os.getenv("APP_PUBLIC_URL") or ""

# Auth config (single admin user)
ADMIN_USER = os.getenv("ADMIN_USER", os.getenv("ALLOWED_EMAIL", "admin@example.com"))
ADMIN_PASS = os.getenv("ADMIN_PASS", "12345")
ALLOWED_IP = os.getenv("ALLOWED_IP", "").strip()

# Lockout config (simple)
failed_logins = {}
LOCKOUT_DURATION = timedelta(hours=int(os.getenv("LOCKOUT_HOURS", "24")))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "10"))

# -------------------------
# DB (SQLAlchemy)
# -------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/autofiliate")
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)  # optional migration support

# Models
class Post(db.Model):
    __tablename__ = "posts"
    id = db.Column(db.Integer, primary_key=True)
    url = db.Column(db.Text, unique=True, nullable=False)
    source = db.Column(db.Text)
    status = db.Column(db.Text, default="pending")
    created_at = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    posted_at = db.Column(db.DateTime(timezone=True), nullable=True)
    meta = db.Column(db.JSON, default={})

class Click(db.Model):
    __tablename__ = "clicks"
    id = db.Column(db.Integer, primary_key=True)
    post_id = db.Column(db.Integer, db.ForeignKey("posts.id", ondelete="CASCADE"))
    ip = db.Column(db.Text)
    user_agent = db.Column(db.Text)
    created_at = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class Setting(db.Model):
    __tablename__ = "settings"
    key = db.Column(db.Text, primary_key=True)
    value = db.Column(db.Text)

# -------------------------
# Sanity check / ensure tables + columns
# -------------------------
with app.app_context():
    try:
        # Ensure the 'settings' table exists
        with db.engine.connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """)
        logger.info("Sanity check: 'settings' table ensured")

        # Optional: confirm other tables exist (posts, clicks)
        db.create_all()
        logger.info("Sanity check: other tables ensured via SQLAlchemy")
    except Exception:
        logger.exception("Sanity check failed")
        
# -------------------------
# Redis / RQ (for dashboard queueing)
# -------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_conn = Redis.from_url(REDIS_URL)
rq_queue = Queue(connection=redis_conn)

# -------------------------
# Scheduler (optional refresh cadence)
# -------------------------
scheduler = BackgroundScheduler()
scheduler.start()
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))

# schedule periodic refresh only if worker provides function
try:
    scheduler.add_job(worker.refresh_all_sources, "interval", minutes=PULL_INTERVAL_MINUTES, id="refresh_sources")
    logger.info("Scheduled refresh_all_sources every %s minutes", PULL_INTERVAL_MINUTES)
except Exception:
    logger.exception("scheduler add_job failed (maybe refresh_all_sources missing)")

# -------------------------
# Login manager
# -------------------------
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

class AdminUser(UserMixin):
    def __init__(self, email):
        self.id = email
        self.username = email

@login_manager.user_loader
def load_user(user_id):
    if user_id == ADMIN_USER:
        return AdminUser(user_id)
    return None

# -------------------------
# Settings helpers (SQLAlchemy)
# -------------------------
def db_get_setting(key, fallback=None):
    try:
        s = Setting.query.get(key)
        return s.value if s else fallback
    except Exception:
        logger.exception("db_get_setting")
        return fallback

def db_set_setting(key, value):
    try:
        s = Setting.query.get(key)
        if not s:
            s = Setting(key=key, value=str(value))
            db.session.add(s)
        else:
            s.value = str(value)
        db.session.commit()
        return True
    except Exception:
        db.session.rollback()
        logger.exception("db_set_setting")
        return False

# ensure default post interval
try:
    if not db_get_setting("post_interval_seconds"):
        db_set_setting("post_interval_seconds", str(3 * 3600))
except Exception:
    logger.exception("init post_interval")

# -------------------------
# Security headers
# -------------------------
@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Cache-Control"] = "no-store, must-revalidate"
    return response

# -------------------------
# Static helpers
# -------------------------
@app.route("/sitemap.xml")
def sitemap():
    return send_from_directory(".", "sitemap.xml")

@app.route("/robots.txt")
def robots():
    return send_from_directory(".", "robots.txt")

# -------------------------
# Pages
# -------------------------
@app.route("/")
def welcome():
    # if logged in go to dashboard
    try:
        if current_user.is_authenticated:
            return redirect(url_for("dashboard"))
    except Exception:
        pass
    return render_template("welcome.html", company=COMPANY, contact_email=CONTACT_EMAIL, title="Welcome")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html", company=COMPANY, title="Private Login")

    # optional IP allowlist
    client_ip = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown")
    if ALLOWED_IP and client_ip != ALLOWED_IP:
        flash("Access restricted.")
        return render_template("login.html", company=COMPANY, title="Private Login")

    username = (request.form.get("username") or request.form.get("email") or "").strip()
    password = request.form.get("password", "")

    if not username or not password:
        flash("Missing login fields.")
        return render_template("login.html", company=COMPANY, title="Private Login")

    # simple brute force tracking per IP
    if client_ip not in failed_logins:
        failed_logins[client_ip] = {"count": 0, "locked_until": None}
    now = datetime.now(timezone.utc)
    locked_until = failed_logins[client_ip]["locked_until"]
    if locked_until and locked_until > now:
        mins = int((locked_until - now).total_seconds() // 60)
        flash(f"Locked out. Try again in {mins} minutes.")
        return render_template("login.html", company=COMPANY, title="Private Login")

    if username == ADMIN_USER and password == ADMIN_PASS:
        # success
        failed_logins[client_ip]["count"] = 0
        user = AdminUser(ADMIN_USER)
        login_user(user)
        logger.info("Login success: %s (ip=%s)", username, client_ip)
        # start a refresh thread (non-blocking)
        Thread(target=worker.refresh_all_sources, daemon=True).start()
        return redirect(url_for("dashboard"))
    else:
        failed_logins[client_ip]["count"] += 1
        left = MAX_ATTEMPTS - failed_logins[client_ip]["count"]
        if failed_logins[client_ip]["count"] >= 3:
            logger.warning("FAILED LOGIN attempt #%s for %s from %s", failed_logins[client_ip]["count"], username, client_ip)
        if left <= 0:
            failed_logins[client_ip]["locked_until"] = now + LOCKOUT_DURATION
            flash("Too many failed attempts. Locked out.")
        else:
            flash(f"Invalid credentials. {left} attempts left.")
        return render_template("login.html", company=COMPANY, title="Private Login")

@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html", company=COMPANY, title="HQ Dashboard", public_url=APP_PUBLIC_URL, contact_email=CONTACT_EMAIL)

@app.route("/logout")
@login_required
def logout():
    logout_user()
    logger.info("User logged out")
    return redirect(url_for("welcome"))

# -------------------------
# Redirect tracking
# -------------------------
@app.route("/r/<int:post_id>")
def redirect_tracking(post_id):
    try:
        post = Post.query.get(post_id)
        if not post:
            abort(404)
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown")
        ua = request.headers.get("User-Agent", "")
        click = Click(post_id=post_id, ip=ip, user_agent=ua, created_at=datetime.now(timezone.utc))
        db.session.add(click)
        db.session.commit()
        return redirect(post.url, code=302)
    except Exception:
        logger.exception("redirect_tracking failed for %s", post_id)
        abort(500)

# -------------------------
# API / Admin actions
# -------------------------
@app.route("/api/stats")
@login_required
def api_stats():
    try:
        # prefer worker.get_stats if available
        if hasattr(worker, "get_stats"):
            return jsonify(worker.get_stats())
        # fallback derive from DB:
        total = Post.query.count()
        pending = Post.query.filter_by(status="pending").count()
        sent = Post.query.filter_by(status="sent").count() if hasattr(Post, "status") else 0
        clicks = Click.query.count()
        last_posted = Post.query.filter(Post.posted_at.isnot(None)).order_by(Post.posted_at.desc()).first()
        last_ts = last_posted.posted_at.astimezone(timezone.utc).isoformat() if last_posted and last_posted.posted_at else None
        return jsonify({
            "total_links": total,
            "pending": pending,
            "sent": sent,
            "clicks_total": clicks,
            "last_posted_at": last_ts
        })
    except Exception:
        logger.exception("api_stats failed")
        return jsonify({}), 500

@app.route("/enqueue", methods=["POST"])
@login_required
def enqueue_route():
    data = request.get_json() or {}
    url = data.get("url")
    source = data.get("source", "manual")
    if not url:
        return jsonify({"error": "url required"}), 400
    try:
        # call worker enqueue function (should return summary)
        if hasattr(worker, "enqueue_manual_link"):
            result = worker.enqueue_manual_link(url)
            return jsonify({"enqueued": True, "result": result}), 202
        else:
            # fallback: insert into DB directly
            p = Post(url=url, source=source, status="pending", created_at=datetime.now(timezone.utc))
            db.session.add(p)
            db.session.commit()
            return jsonify({"enqueued": True, "id": p.id}), 202
    except Exception as e:
        logger.exception("enqueue failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/refresh", methods=["POST", "GET"])
@login_required
def refresh_route():
    try:
        # trigger in background
        if hasattr(worker, "refresh_all_sources"):
            Thread(target=worker.refresh_all_sources, daemon=True).start()
            return jsonify({"status": "refresh_queued"}), 202
        return jsonify({"error": "worker.refresh_all_sources not available"}), 500
    except Exception as e:
        logger.exception("refresh_route failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/start", methods=["POST", "GET"])
@login_required
def start_worker():
    try:
        if hasattr(worker, "start_worker_background"):
            Thread(target=worker.start_worker_background, daemon=True).start()
            return jsonify({"status": "worker_start_requested"}), 202
        return jsonify({"error": "worker.start_worker_background not available"}), 500
    except Exception as e:
        logger.exception("start_worker failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/stop", methods=["POST", "GET"])
@login_required
def stop_worker_route():
    try:
        if hasattr(worker, "stop_worker"):
            worker.stop_worker()
            return jsonify({"status": "worker_stop_requested"}), 200
        return jsonify({"error": "worker.stop_worker not available"}), 500
    except Exception as e:
        logger.exception("stop_worker failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/api/interval", methods=["POST"])
@login_required
def api_interval():
    data = request.get_json() or {}
    interval_min = int(data.get("interval", 0))
    if interval_min <= 0:
        return jsonify({"error": "invalid interval"}), 400
    seconds = interval_min * 60
    ok = db_set_setting("post_interval_seconds", seconds)
    if ok:
        return jsonify({"status": "updated", "post_interval_seconds": seconds}), 200
    return jsonify({"error": "failed to save"}), 500

@app.route("/health")
def health():
    return jsonify({"ok": True, "ts": datetime.now(timezone.utc).isoformat()}), 200

@app.errorhandler(404)
def not_found(e):
    return render_template("welcome.html", company=COMPANY), 404

# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    logger.info("Starting app on port %s", port)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
