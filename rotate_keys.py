# rotate_keys.py — AUTO-ROTATE API KEYS (TWILIO, PAYSTACK, OPENAI, etc.)
import os
import secrets
import requests
from datetime import datetime
import json

# === GENERATE NEW SECRET_KEY ===
new_secret_key = secrets.token_hex(32)
print(f"NEW SECRET_KEY: {new_secret_key}")

# === TWILIO: REGENERATE AUTH TOKEN (IF NEEDED) ===
twilio_sid = os.getenv('TWILIO_SID')
if twilio_sid:
    # Manual: Go to Twilio Console → Regenerate Auth Token
    print("TWILIO: Manually regenerate auth token at console.twilio.com")

# === OPENAI: ROTATE API KEY (IF NEEDED) ===
openai_key = os.getenv('OPENAI_API_KEY')
if openai_key:
    # Create new key at platform.openai.com/account/api-keys
    print("OPENAI: Create new API key at platform.openai.com/account/api-keys")

# === PAYSTACK: REGENERATE SECRET KEY ===
paystack_key = os.getenv('PAYSTACK_SECRET_KEY')
if paystack_key:
    print("PAYSTACK: Regenerate at dashboard.paystack.com/#/settings/keys")

# === UPDATE RENDER ENV (MANUAL OR API) ===
print("=== UPDATE RENDER ENV ====")
print(f"SECRET_KEY = {new_secret_key}")
print("Deploy after updating.")

# === AUTO-UPDATE RENDER ENV VIA API (OPTIONAL) ===
render_api_key = os.getenv('RENDER_API_KEY')
if render_api_key:
    service_id = os.getenv('RENDER_SERVICE_ID')
    url = f"https://api.render.com/v1/services/{service_id}/env"
    headers = {"Authorization": f"Bearer {render_api_key}", "Content-Type": "application/json"}
    data = {"envVars": {"SECRET_KEY": new_secret_key}}
    r = requests.patch(url, headers=headers, json=data)
    if r.status_code == 200:
        print("RENDER ENV AUTO-UPDATED")
    else:
        print(f"RENDER UPDATE FAILED: {r.text}")

# === LOG & ALERT ===
log_msg = f"Key rotation: {datetime.now()} | New SECRET_KEY generated"
print(log_msg)
# Send to WhatsApp (using your existing function)
# send_alert("KEY ROTATED", log_msg)
