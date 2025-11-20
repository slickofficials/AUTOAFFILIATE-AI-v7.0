# app.py — AutoAffiliate HQ Dashboard
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

# Worker import points
import worker

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("app")

# Flask
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.getenv("SECRET_KEY", "slickofficials_hq_2025")
Compress(app)
# Config
COMPANY = os.getenv("COMPANY_NAME", "SlickOfficials HQ | Amson Multi Global LTD")
CONTACT_EMAIL = os.getenv("CONTACT_EMAIL", "support@slickofficials.com")
APP_PUBLIC_URL = os.getenv("PUBLIC_URL") or os.getenv("APP_PUBLIC_URL") or ""

ADMIN_USER = os.getenv("ADMIN_USER", os.getenv("ALLOWED_EMAIL", "admin@example.com"))
ADMIN_PASS = os.getenv("ADMIN_PASS", "12345")
ALLOWED_IP = os.getenv("ALLOWED_IP", "").strip()

failed_logins = {}
LOCKOUT_DURATION = timedelta(hours=int(os.getenv("LOCKOUT_HOURS", "24")))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "10"))

# Database (SQLAlchemy with psycopg3 driver)
DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")
db_uri = DATABASE_URL.replace("postgres://", "postgresql+psycopg://")
if db_uri.startswith("postgresql://"):
    db_uri = db_uri.replace("postgresql://", "postgresql+psycopg://", 1)

app.config["SQLALCHEMY_DATABASE_URI"] = db_uri
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)

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
    setting_key = db.Column(db.Text, primary_key=True)
    value = db.Column(db.Text)

class FailedLink(db.Model):
    __tablename__ = "failed_links"
    id = db.Column(db.Integer, primary_key=True)
    source = db.Column(db.String(50), nullable=False)
    attempted_url = db.Column(db.Text, nullable=False)
    reason = db.Column(db.Text)
    created_at = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
# Redis / RQ
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
try:
    redis_conn = Redis.from_url(REDIS_URL)
    rq_queue = Queue("autoaffiliate", connection=redis_conn)
except Exception:
    redis_conn, rq_queue = None, None
    logger.warning("Redis not available")

# Scheduler
scheduler = BackgroundScheduler()
scheduler.start()
PULL_INTERVAL_MINUTES = int(os.getenv("PULL_INTERVAL_MINUTES", "60"))
try:
    scheduler.add_job(worker.refresh_all_sources, "interval", minutes=PULL_INTERVAL_MINUTES, id="refresh_sources")
    logger.info("Scheduled refresh_all_sources every %s minutes", PULL_INTERVAL_MINUTES)
except Exception:
    logger.exception("scheduler add_job failed")

# Login manager
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
# Settings helpers
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
            s = Setting(setting_key=key, value=str(value))
            db.session.add(s)
        else:
            s.value = str(value)
        db.session.commit()
        return True
    except Exception:
        db.session.rollback()
        logger.exception("db_set_setting")
        return False

# Security headers
@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Cache-Control"] = "no-store, must-revalidate"
    return response

# Static helpers
@app.route("/sitemap.xml")
def sitemap():
    return send_from_directory(".", "sitemap.xml")

@app.route("/robots.txt")
def robots():
    return send_from_directory(".", "robots.txt")
# Auth routes
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html", company=COMPANY, title="Private Login")

    client_ip = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown")
    if ALLOWED_IP and client_ip != ALLOWED_IP:
        flash("Access restricted.")
        return render_template("login.html", company=COMPANY, title="Private Login")

    username = (request.form.get("username") or request.form.get("email") or "").strip()
    password = request.form.get("password", "")

    if username == ADMIN_USER and password == ADMIN_PASS:
        user = AdminUser(ADMIN_USER)
        login_user(user)
        Thread(target=worker.refresh_all_sources, daemon=True).start()
        return redirect(url_for("dashboard"))
    else:
        flash("Invalid credentials.")
        return render_template("login.html", company=COMPANY, title="Private Login")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("welcome"))

@app.route("/")
def welcome():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))
    return render_template("welcome.html", company=COMPANY, contact_email=CONTACT_EMAIL, title="Welcome")

@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html", company=COMPANY, title="HQ Dashboard", public_url=APP_PUBLIC_URL, contact_email=CONTACT_EMAIL)

# Redirect tracking
@app.route("/r/<int:post_id>")
def redirect_tracking(post_id):
    post = Post.query.get(post_id)
    if not post:
        abort(404)
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown")
    ua = request.headers.get("User-Agent", "")
    click = Click(post_id=post_id, ip=ip, user_agent=ua, created_at=datetime.now(timezone.utc))
    db.session.add(click)
    db.session.commit()
    return redirect(post.url, code=302)

# API routes
@app.route("/api/stats")
@login_required
def api_stats():
    try:
        if hasattr(worker, "get_stats"):
            return jsonify(worker.get_stats())
        total = Post.query.count()
        pending = Post.query.filter_by(status="pending").count()
        posted = Post.query.filter_by(status="posted").count()
        clicks = Click.query.count()
        last_posted = Post.query.filter(Post.posted_at.isnot(None)).order_by(Post.posted_at.desc()).first()
        last_ts = last_posted.posted_at.isoformat() if last_posted else None
        return jsonify({"total_links": total, "pending": pending, "posted": posted, "clicks_total": clicks
                })
    except Exception:
        logger.exception("api_stats failed")
        return jsonify({}), 500

@app.route("/api/posts")
@login_required
def api_posts():
    rows = db.session.execute(
        db.text("SELECT id, url, source, status, created_at, posted_at FROM posts ORDER BY id DESC LIMIT 200")
    ).mappings().all()
    return jsonify([dict(r) for r in rows])

@app.route("/api/failed_summary")
@login_required
def api_failed_summary():
    since = datetime.now(timezone.utc) - timedelta(days=1)
    rows = db.session.execute(
        db.text("SELECT source, COUNT(*) AS count FROM failed_links WHERE created_at >= :since GROUP BY source"),
        {"since": since}
    ).mappings().all()
    summary = {r["source"]: r["count"] for r in rows}
    for src in ("awin", "rakuten", "facebook", "twitter", "telegram", "ifttt"):
        summary.setdefault(src, 0)
    return jsonify({"date": datetime.now(timezone.utc).strftime("%Y-%m-%d"), "sources": summary})

@app.route("/api/worker/status")
@login_required
def api_worker_status():
    try:
        if hasattr(worker, "get_stats"):
            stats = worker.get_stats()
            running = stats.get("pending", 0) > 0 or stats.get("posted", 0) > 0
            return jsonify({"running": running})
        return jsonify({"running": False})
    except Exception:
        logger.exception("api_worker_status failed")
        return jsonify({"running": False}), 500

@app.route("/enqueue", methods=["POST"])
@login_required
def enqueue_route():
    data = request.get_json() or {}
    url = data.get("url")
    source = data.get("source", "manual")
    if not url:
        return jsonify({"error": "url required"}), 400
    try:
        if hasattr(worker, "enqueue_manual_link"):
            result = worker.enqueue_manual_link(url)
            return jsonify({"enqueued": True, "result": result}), 202
        p = Post(url=url, source=source, status="pending", created_at=datetime.now(timezone.utc))
        db.session.add(p)
        db.session.commit()
        return jsonify({"enqueued": True, "id": p.id}), 202
    except Exception as e:
        logger.exception("enqueue failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/refresh", methods=["POST"])
@login_required
def refresh_route():
    try:
        if hasattr(worker, "refresh_all_sources"):
            Thread(target=worker.refresh_all_sources, daemon=True).start()
            return jsonify({"status": "refresh_queued"}), 202
        return jsonify({"error": "worker.refresh_all_sources not available"}), 500
    except Exception as e:
        logger.exception("refresh_route failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/start", methods=["POST"])
@login_required
def start_worker_route():
    try:
        if hasattr(worker, "start_worker_background"):
            Thread(target=worker.start_worker_background, daemon=True).start()
            return jsonify({"status": "worker_start_requested"}), 202
        return jsonify({"error": "worker.start_worker_background not available"}), 500
    except Exception as e:
        logger.exception("start_worker failed: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/stop", methods=["POST"])
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
# Run
if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    logger.info("Starting app on port %s", port)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
