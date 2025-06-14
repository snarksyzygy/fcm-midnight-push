from flask import Flask, request
import firebase_admin
from firebase_admin import credentials, messaging
import os, json
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import requests, datetime
from dateutil import parser as dtparse     # ISO‑8601 parser
import logging, sys
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

from google.cloud import firestore

app = Flask(__name__)

# ------------------------------------------------------------
# Load service‑account key.
# Expect a one‑line base‑64 string in SA_B64 (preferred) or
# fallback to GOOGLE_APPLICATION_CREDENTIALS_JSON for older envs.
# ------------------------------------------------------------
import base64, tempfile

b64_key = os.getenv("SA_B64") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if not b64_key:
    raise RuntimeError("Missing SA_B64 or GOOGLE_APPLICATION_CREDENTIALS_JSON env‑var")

# Decode into a Python dict for firebase_admin
cred_info = json.loads(base64.b64decode(b64_key))
from google.oauth2 import service_account
sa_creds = service_account.Credentials.from_service_account_info(cred_info)
cred = credentials.Certificate(cred_info)
firebase_admin.initialize_app(cred)

# Initialize Firestore client using the same credentials
db = firestore.Client(credentials=sa_creds, project=cred_info["project_id"])

# Scheduler for per-device midnight pushes
scheduler = BackgroundScheduler()
scheduler.start()

# Run once daily at 00:05 UTC to cache the weekly feed and extract today's slice
def fetch_and_store_today():
    """
    Downloads ForexFactory's weekly JSON feed, extracts only today's rows,
    and stores them under /events/YYYY-MM-DD/{eventID} in Firestore.
    """
    URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    try:
        weekly = requests.get(URL, timeout=10).json()
        app.logger.info(f"Downloaded {len(weekly)} events from ForexFactory")

        today_date   = datetime.datetime.utcnow().date()      # e.g. 2025‑06‑14
        today_str    = today_date.isoformat()

        batch          = db.batch()
        writes_in_batch = 0
        written_total  = 0

        for ev in weekly:
            # ---------- keep only today ----------
            # Parse the ISO‑8601 timestamp but *keep the calendar day as published*
            # ForexFactory stamps each item with the local‑exchange date (usually ET).
            # Converting to UTC can push late‑evening events into the following day,
            # so we compare on the naïve calendar date only.
            try:
                ev_date = dtparse.isoparse(ev["date"]).date()   # YYYY‑MM‑DD
            except Exception:
                continue
            if ev_date != today_date:
                continue

            # ---------- build a stable document id ----------
            event_time = dtparse.isoparse(ev["date"]).strftime("%H:%M")
            key = f"{today_str}_{event_time}_{ev['country']}_{ev['title'].replace(' ', '_')}"

            # ---------- write to /eventCache/YYYY‑MM‑DD/items/{key} ----------
            doc_ref = (
                db.collection("eventCache")
                  .document(today_str)
                  .collection("items")
                  .document(key)
            )
            batch.set(doc_ref, ev, merge=True)
            writes_in_batch += 1
            written_total   += 1

            # Firestore limit: 500 writes per batch
            if writes_in_batch == 500:
                batch.commit()
                batch             = db.batch()
                writes_in_batch   = 0

        # commit any remainder
        if writes_in_batch:
            batch.commit()
            app.logger.info(f"Committed final batch with {writes_in_batch} writes")
        if written_total == 0:
            app.logger.warning("No events matched today's date filter ‑ check timezone logic.")

        app.logger.info(f"✅ Stored {written_total} events for {today_str} to Firestore.")
    except Exception as e:
        app.logger.error(f"❌ fetch_and_store_today failed: {e}")


# Add daily job to fetch and store today's events
scheduler.add_job(fetch_and_store_today,
                  trigger=CronTrigger(hour=0, minute=5, timezone="UTC"),
                  id="daily_calendar_fetch",
                  replace_existing=True)

@app.route("/health")
def health_check():
    return "✅ App is running!"


# Manual endpoint to trigger today's fetch
@app.route("/fetch-today-now")
def manual_fetch_today():
    fetch_and_store_today()
    return {"fetched": True}

@app.route("/register", methods=["POST"])
def register():
    data = request.get_json(force=True)
    token = data.get("token")
    tz = data.get("tz")
    if not token:
        return {"registered": False, "error": "Missing token"}, 400
    print(f"🔖 Registered device token: {token}, tz: {tz}")
    # Schedule a daily push at local midnight for this device
    try:
        # Remove any existing job for this token
        scheduler.remove_job(job_id=token)
    except Exception:
        pass
    # Schedule at 00:00 in the user's timezone
    trigger = CronTrigger(hour=0, minute=0, timezone=tz)
    scheduler.add_job(
        send_midnight_alert,
        trigger=trigger,
        args=[token],
        id=token,
        replace_existing=True
    )
    return {"registered": True, "received": data}

@app.route("/send-silent", methods=["POST"])
def send_silent():
    data = request.get_json(force=True)
    token = data.get("token", "")
    try:
        msg = messaging.Message(
            data={"action": "fetchNews"},
            token=token,
            android=messaging.AndroidConfig(priority="high"),
            apns=messaging.APNSConfig(
                headers={"apns-priority": "5"},
                payload=messaging.APNSPayload(aps=messaging.Aps(content_available=True))
            ),
        )
        msg_id = messaging.send(msg)
        return {"success": True, "message_id": msg_id}
    except Exception as e:
        app.logger.error(f"FCM send failed: {e}")
        return {"success": False, "error": str(e)}, 500

def send_midnight_alert(token: str):
    """
    Sends a remote push to the given FCM token with an alert-type payload
    at the device's local midnight.
    """
    try:
        message = messaging.Message(
            notification=messaging.Notification(
                title="Fetching Events…",
                body="Fetching today’s news events…"
            ),
            data={"action": "fetchNews"},
            token=token,
            apns=messaging.APNSConfig(
                headers={"apns-priority": "10"},  # high priority alert
                payload=messaging.APNSPayload(aps=messaging.Aps(alert={"title": "", "body": ""}))
            )
        )
        messaging.send(message)
        app.logger.info(f"🔔 Midnight alert sent to {token}")
    except Exception as e:
        app.logger.error(f"❌ Failed to send midnight alert to {token}: {e}")
