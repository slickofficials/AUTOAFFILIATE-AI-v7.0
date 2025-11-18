# app.py — Production-ready Flask with login, welcome, and dashboard
import os
from flask import Flask, render_template, redirect, url_for, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, login_user, login_required, logout_user, UserMixin, current_user
from flask_compress import Compress
from redis import Redis
from rq import Queue
from apscheduler.schedulers.background import BackgroundScheduler
import worker
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("app")

app = Flask(__name__, static_folder='.', static_url_path='')
app.secret_key = os.getenv("SECRET_KEY", "supersecret")
Compress(app)

# DB config
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///autofiliate.db")
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

# Login manager
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

# Redis queue
redis_conn = Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
queue = Queue(connection=redis_conn)

# Scheduler
scheduler = BackgroundScheduler()
scheduler.start()
scheduler.add_job(worker.refresh_all_sources, 'interval', minutes=int(os.getenv("PULL_INTERVAL_MINUTES", 60)))

# Models
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    password = db.Column(db.String(128), nullable=False)

db.create_all()

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# --- ROUTES ---

@app.route("/")
def index():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))
    return render_template("welcome.html")  # welcome page

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username") or request.form.get("email")
        password = request.form.get("password")
        user = User.query.filter_by(username=username, password=password).first()
        if user:
            login_user(user)
            return redirect(url_for("dashboard"))
        else:
            error = "Invalid credentials"
    return render_template("login.html", error=error)

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("index"))

@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html")  # your dashboard

# --- API endpoints for dashboard buttons ---

@app.route("/api/stats", methods=["GET"])
@login_required
def api_stats():
    return jsonify(worker.get_stats())

@app.route("/api/enqueue", methods=["POST"])
@login_required
def api_enqueue():
    data = request.json
    url = data.get("url")
    try:
        res = worker.enqueue_manual_link(url)
        return jsonify(res)
    except Exception as e:
        logger.exception("enqueue failed")
        return jsonify({"error": str(e)}), 500

@app.route("/api/refresh", methods=["POST"])
@login_required
def api_refresh():
    job = queue.enqueue(worker.refresh_all_sources)
    return jsonify({"status": "queued", "job_id": job.id})

@app.route("/api/worker/start", methods=["POST"])
@login_required
def api_worker_start():
    job = queue.enqueue(worker.start_worker_background)
    return jsonify({"status": "started", "job_id": job.id})

@app.route("/api/worker/stop", methods=["POST"])
@login_required
def api_worker_stop():
    worker.stop_worker()
    return jsonify({"status": "stop requested"})

@app.route("/api/settings", methods=["POST"])
@login_required
def api_settings():
    data = request.json
    key = data.get("key")
    value = data.get("value")
    ok = worker.db_set_setting(key, value)
    return jsonify({"updated": ok, "key": key, "value": value})

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
