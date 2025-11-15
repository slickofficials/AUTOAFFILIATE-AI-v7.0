from flask import Flask, render_template, jsonify, request
import threading
import logging
import worker  # your fixed worker.py

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

# ---------------------------
# Worker thread control
# ---------------------------
worker_thread = None

def start_worker_thread():
    global worker_thread
    if worker_thread and worker_thread.is_alive():
        return False
    worker_thread = threading.Thread(target=worker.start_worker_background, daemon=True)
    worker_thread.start()
    return True

def stop_worker_thread():
    worker.stop_worker()
    return True

# ---------------------------
# Routes
# ---------------------------
@app.route("/")
def dashboard():
    return render_template("dashboard.html")

@app.route("/api/status")
def api_status():
    return jsonify({
        "worker_running": worker._worker_running,
        "stop_requested": worker._stop_requested
    })

@app.route("/api/start_worker", methods=["POST"])
def api_start_worker():
    started = start_worker_thread()
    return jsonify({"started": started})

@app.route("/api/stop_worker", methods=["POST"])
def api_stop_worker():
    stopped = stop_worker_thread()
    return jsonify({"stopped": stopped})

@app.route("/api/refresh_sources", methods=["POST"])
def api_refresh_sources():
    count = worker.refresh_all_sources()
    return jsonify({"links_saved": count})

@app.route("/api/post_next", methods=["POST"])
def api_post_next():
    success = worker.post_next_pending()
    return jsonify({"success": success})

@app.route("/api/posts")
def api_posts():
    try:
        conn, cur = worker.get_db_conn()
        cur.execute("SELECT id, url, status, created_at, posted_at, meta FROM posts ORDER BY created_at DESC LIMIT 50")
        rows = cur.fetchall()
        conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/settings", methods=["POST"])
def api_settings():
    data = request.json or {}
    try:
        conn, cur = worker.get_db_conn()
        for k, v in data.items():
            cur.execute("INSERT INTO settings(key,value) VALUES (%s,%s) ON CONFLICT(key) DO UPDATE SET value=%s", (k,v,v))
        conn.commit(); conn.close()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
