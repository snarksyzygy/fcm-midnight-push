import ijson
import io
from flask import Flask, request
import firebase_admin
from firebase_admin import credentials, messaging
from firebase_admin.messaging import ApsAlert
import os, json, time
import openai
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import requests, datetime
from dateutil import parser as dtparse     # ISO‑8601 parser
import logging, sys
from datetime import timezone, timedelta
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

# ---------- ChatGPT description cache ----------
openai.api_key = os.getenv("OPENAI_API_KEY")
DESC_DOC = db.document("descriptions/cache")

def load_description_cache() -> dict[str, str]:
    snap = DESC_DOC.get()
    return snap.to_dict().get("entries", {}) if snap.exists else {}

def save_description_batch(batch: dict[str, str]) -> None:
    if batch:
        DESC_DOC.set({"entries": batch}, merge=True)

def chatgpt_description(title: str, retries: int = 3) -> str:
    prompt = (f"Briefly (≤35 words) describe what “{title}” measures "
              "so a macro trader understands.")
    for i in range(retries):
        try:
            resp = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            backoff = 2 ** i
            app.logger.warning(f"GPT retry {i+1}/{retries} failed: {e}; sleeping {backoff}s")
            time.sleep(backoff)
    return "Description not available."

# Load cache once at start‑up
description_cache = load_description_cache()
# -----------------------------------------------

# Scheduler for per-device midnight pushes
scheduler = BackgroundScheduler()
scheduler.start()

# Run once daily at 00:05 UTC to cache the weekly feed and extract today's slice
def fetch_and_store_week():
    """
    Downloads the current‑week JSON and stores every event, grouped by calendar‑day, skipping anything already present.
    """
    from collections import defaultdict
    day_events: dict[str, list] = defaultdict(list)
    URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    try:
        # Retrieve the feed; requests automatically decompresses gzip when
        # stream=False (default), so we can parse the bytes directly.
        resp = requests.get(URL, timeout=10)
        resp.raise_for_status()

        # Feed the bytes into ijson using a BytesIO wrapper
        parser = ijson.items(io.BytesIO(resp.content), "item")
        count_downloaded = 0

        # Throttle Firestore writes to stay below per‑second quota
        MAX_WRITES = 200      # commit every 200 mutations
        SLEEP_SEC  = 0.4      # pause 400 ms between commits

        batch          = db.batch()
        writes_in_batch = 0
        written_total  = 0

        new_desc_batch = {}

        for ev in parser:
            count_downloaded += 1
            # ---------- keep all events ----------
            # Parse the ISO‑8601 timestamp but *keep the calendar day as published*
            # ForexFactory stamps each item with the local‑exchange date (usually ET).
            # Converting to UTC can push late‑evening events into the following day,
            # so we compare on the naïve calendar date only.
            try:
                ev_date = dtparse.isoparse(ev["date"]).date()   # YYYY‑MM‑DD
            except Exception:
                continue

            date_str   = ev_date.isoformat()                # e.g. 2025‑06‑14
            event_time = dtparse.isoparse(ev["date"]).strftime("%H:%M")
            # Sanitize the title so it is safe for a Firestore document ID
            safe_title = (
                ev["title"]
                .replace(" ", "_")
                .replace("/", "_")
                .replace("\\", "_")
            )
            # Accumulate events per Firestore document (one doc per day)
            # --- normalize fields so the app sees consistent data ---
            cleaned_ev = ev.copy()
            cleaned_ev["forecast"] = cleaned_ev.get("forecast") or "N/A"
            cleaned_ev["previous"] = cleaned_ev.get("previous") or "N/A"
            cleaned_ev["impact"]   = cleaned_ev.get("impact")   or "N/A"

            # Ensure detail exists – consult cache or GPT
            title = cleaned_ev.get("title", "")
            if title in description_cache:
                cleaned_ev["detail"] = description_cache[title]
            else:
                desc = chatgpt_description(title)
                cleaned_ev["detail"] = desc
                description_cache[title] = desc
                new_desc_batch[title] = desc

            cleaned_ev["time"] = event_time
            day_events[date_str].append(cleaned_ev)

        # Save any newly generated descriptions in one write
        save_description_batch(new_desc_batch)

        # Persist each day's events as a single document with an "events" array
        batch = db.batch()
        for date_key, ev_list in day_events.items():
            doc_ref = db.collection("eventCache").document(date_key)
            batch.set(doc_ref, {"events": ev_list}, merge=True)
        batch.commit()
        written_total = sum(len(v) for v in day_events.values())

        app.logger.info(f"Downloaded {count_downloaded} events from ForexFactory")
        app.logger.info(f"✅ Upserted {written_total} events across the week.")
    except Exception as e:
        app.logger.error(f"❌ fetch_and_store_week failed: {e}")


# Add daily job to fetch and store today's events
scheduler.add_job(fetch_and_store_week,
                  trigger=CronTrigger(hour=0, minute=5, timezone="UTC"),
                  id="weekly_calendar_refresh",
                  replace_existing=True)

@app.route("/health")
def health_check():
    return "✅ App is running!"

# Manual endpoint to trigger weekly fetch
@app.route("/fetch-week-now")
def manual_fetch_week():
    fetch_and_store_week()
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
    # tz is offset in minutes (e.g. -420). Convert to a tzinfo object for APScheduler.
    offset_tz = timezone(timedelta(minutes=tz))
    trigger = CronTrigger(hour=0, minute=0, timezone=offset_tz)
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
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        alert=ApsAlert(title="New Day!", body="Fetching today’s news events…"),
                        sound="default"
                    )
                )
            )
        )
        messaging.send(message)
        app.logger.info(f"🔔 Midnight alert sent to {token}")
    except Exception as e:
        app.logger.error(f"❌ Failed to send midnight alert to {token}: {e}")

@app.route("/events/<date_str>")
def events_for_day(date_str: str):
    """Return cached events for a given YYYY‑MM‑DD."""
    doc = db.collection("eventCache").document(date_str).get()
    if doc.exists and "events" in doc.to_dict():
        return {"events": doc.to_dict()["events"]}
    else:
        return {"events": []}
