from flask import Flask, request
import firebase_admin
from firebase_admin import credentials, messaging
import os, json

app = Flask(__name__)

# Load service‐account JSON from env var
json_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
cred = credentials.Certificate(json.loads(json_creds))
firebase_admin.initialize_app(cred)

@app.route("/health")
def health_check():
    return "✅ App is running!"

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
