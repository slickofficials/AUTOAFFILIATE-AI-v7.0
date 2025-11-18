# app.py — AutoAffiliate web app with login
import os
from flask import Flask, render_template, jsonify, redirect, request, session
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone

app = Flask(__name__)
DATABASE_URL = os.getenv("DATABASE_URL")
APP_PUBLIC_URL = os.getenv("APP_PUBLIC_URL") or os.getenv("PUBLIC_URL") or ""
SECRET_KEY = os.getenv("SECRET_KEY","changeme")
app.secret_key = SECRET_KEY

ADMIN_PASS = os.getenv("ADMIN_PASS")
ALLOWED_EMAIL = os.getenv("ALLOWED_EMAIL")

def db():
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    return conn, conn.cursor()

def ensure_tables():
    conn, cur = db()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS posts(
        id SERIAL PRIMARY KEY,
        url TEXT UNIQUE NOT NULL,
        source TEXT,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMPTZ DEFAULT now(),
        posted_at TIMESTAMPTZ,
        meta JSONB DEFAULT '{}'::jsonb
    );""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clicks(
        id SERIAL PRIMARY KEY,
        post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
        ip TEXT,
        user_agent TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
    );""")
    conn.commit(); conn.close()
ensure_tables()

# ---------- Routes ----------
@app.route("/")
def welcome():
    return render_template("welcome.html")

@app.route("/login", methods=["POST"])
def login():
    email = request.form.get("email")
    password = request.form.get("password")
    if email == ALLOWED_EMAIL and password == ADMIN_PASS:
        session["logged_in"] = True
        return redirect("/dashboard")
    return render_template("welcome.html", error="Invalid credentials")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

@app.route("/dashboard")
def dashboard():
    if not session.get("logged_in"):
        return redirect("/")
    return render_template("dashboard.html")

@app.route("/health")
def health():
    return jsonify({"ok": True, "app": "running"})

@app.route("/r/<int:post_id>")
def track_redirect(post_id):
    conn, cur = db()
    cur.execute("SELECT url FROM posts WHERE id=%s", (post_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return redirect("/")
    try:
        cur.execute("INSERT INTO clicks(post_id, ip, user_agent, created_at) VALUES (%s,%s,%s,%s)",
                    (post_id, request.remote_addr, request.headers.get("User-Agent"),
                     datetime.now(timezone.utc)))
        conn.commit()
    finally:
        conn.close()
    return redirect(row["url"])

@app.route("/api/stats")
def api_stats():
    conn, cur = db()
    cur.execute("SELECT COUNT(*) FROM posts"); total = cur.fetchone()["count"]
    cur.execute("SELECT COUNT(*) FROM posts WHERE status='pending'"); pending = cur.fetchone()["count"]
    cur.execute("SELECT COUNT(*) FROM posts WHERE status='sent'"); sent = cur.fetchone()["count"]
    cur.execute("SELECT COUNT(*) FROM clicks"); clicks = cur.fetchone()["count"]
    cur.execute("SELECT MAX(posted_at) FROM posts WHERE posted_at IS NOT NULL"); last = cur.fetchone()["max"]
    conn.close()
    return jsonify({
        "total": total,
        "pending": pending,
        "sent": sent,
        "clicks_total": clicks,
        "last_posted_at": last.isoformat() if last else None
    })

@app.errorhandler(404)
def not_found(e):
    return redirect("/")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
