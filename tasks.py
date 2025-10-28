# tasks.py - RQ-compatible job for trial auto-charge
import os
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import timedelta

# === CONFIG (re-read from environment) ===
DB_URL = os.getenv('DATABASE_URL')
PAYSTACK_KEY = os.getenv('PAYSTACK_SECRET_KEY')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# === DATABASE HELPER ===
def get_db():
    conn = psycopg.connect(DB_URL, row_factory=dict_row)
    return conn, conn.cursor()

# === TELEGRAM HELPER ===
def send_telegram(message):
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    if not bot_token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        requests.post(url, data={'chat_id': chat_id, 'text': message}, timeout=10)
    except:
        pass

# === MAIN RQ JOB: CHECK TRIALS ===
def check_trials():
    print("[TRIAL] Checking expired trials...")
    conn, cur = get_db()
    try:
        cur.execute("""
            SELECT * FROM saas_users
            WHERE status = 'trial'
              AND created_at < NOW() - INTERVAL '7 days'
        """)
        expired = cur.fetchall()

        charged = 0
        expired_count = 0

        for user in expired:
            user_id = user['id']
            email = user.get('email', 'unknown')

            # Skip if no Paystack customer code
            if not user.get('paystack_customer_code'):
                cur.execute("UPDATE saas_users SET status = 'expired' WHERE id = %s", (user_id,))
                expired_count += 1
                continue

            # Create Paystack subscription
            payload = {
                "customer": user['paystack_customer_code'],
                "plan": "PLN_monthly_150k"
            }
            headers = {"Authorization": f"Bearer {PAYSTACK_KEY}"}

            try:
                r = requests.post(
                    "https://api.paystack.co/subscription",
                    json=payload,
                    headers=headers,
                    timeout=30
                )
                if r.status_code == 200:
                    sub_code = r.json()['data']['subscription_code']
                    cur.execute(
                        """UPDATE saas_users
                           SET status = 'active',
                               paystack_subscription_code = %s
                           WHERE id = %s""",
                        (sub_code, user_id)
                    )
                    send_telegram(f"Auto-charged: {email} to ₦150k/mo")
                    charged += 1
                else:
                    print(f"[PAYSTACK] Failed for {email}: {r.text}")
                    cur.execute("UPDATE saas_users SET status = 'expired' WHERE id = %s", (user_id,))
                    expired_count += 1
            except Exception as e:
                print(f"[PAYSTACK] Exception for {email}: {e}")
                cur.execute("UPDATE saas_users SET status = 'expired' WHERE id = %s", (user_id,))
                expired_count += 1

        conn.commit()
        print(f"[TRIAL] Summary: {charged} charged, {expired_count} expired")
        send_telegram(f"Daily Trial Check: {charged} charged, {expired_count} expired")

    except Exception as e:
        print(f"[TRIAL] Database error: {e}")
        conn.rollback()
    finally:
        conn.close()

    # === RESCHEDULE FOR TOMORROW ===
    from redis import Redis
    from rq import Queue
    r = Redis.from_url(REDIS_URL)
    q = Queue(connection=r)
    q.enqueue_in(timedelta(days=1), check_trials)
