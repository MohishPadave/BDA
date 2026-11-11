"""FastAPI bridge: Kafka (processed topics) -> WebSocket -> dashboard.
Run: uvicorn server:app --port 8000
"""
import asyncio, json, os, smtplib, threading, time
from email.message import EmailMessage
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from kafka import KafkaConsumer


if os.path.exists(".env"):                      # tiny .env loader (no extra dependency)
    for line in open(".env"):
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.strip().split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

SMTP_HOST, SMTP_PORT = os.getenv("SMTP_HOST", "smtp.gmail.com"), int(os.getenv("SMTP_PORT", "587"))
EMAIL_USER, EMAIL_PASS = os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO") or EMAIL_USER
BATCH_SECONDS = int(os.getenv("EMAIL_BATCH_SECONDS", "60"))
COOLDOWN = int(os.getenv("EMAIL_COOLDOWN_MIN", "10")) * 60
pending, last_sent, known_seen, lock = {}, {}, set(), threading.Lock()

def send_email(subject, body):
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, EMAIL_USER, EMAIL_TO
    msg.set_content(body)
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
        s.starttls(); s.login(EMAIL_USER, EMAIL_PASS); s.send_message(msg)

def queue_alert(d):
    """Critical faults and newly known-faulty sensors are collected and sent as ONE digest email."""
    if not EMAIL_USER:
        return
    new = d.get("known_faulty") and d["sensor_id"] not in known_seen
    if d.get("known_faulty"):
        known_seen.add(d["sensor_id"])
    if d.get("severity") != "critical" and not new:
        return
    with lock:
        if not new and time.time() - last_sent.get(d["sensor_id"], 0) < COOLDOWN:
            return
        pending[d["sensor_id"]] = dict(d, _new=new)

def mailer():
    while True:
        time.sleep(BATCH_SECONDS)
        with lock:
            batch = list(pending.values()); pending.clear()
            for e in batch:
                last_sent[e["sensor_id"]] = time.time()
        if not batch:
            continue
        lines = ["%s (%s): %s %s%s" % (e["sensor_id"], e["location"], e["value"], e["unit"],
                 "   <-- NEWLY MARKED KNOWN FAULTY (Bloom filter)" if e["_new"] else "") for e in batch[:30]]
        if len(batch) > 30:
            lines.append("...and %d more" % (len(batch) - 30))
        try:
            send_email("[Intel Lab Monitor] %d critical sensor fault(s)" % len(batch),
                       "Critical readings flagged by the Spark anomaly detector:\n\n" + "\n".join(lines))
            print("Email sent (%d faults)" % len(batch), flush=True)
        except Exception as ex:
            print("Email failed:", ex, flush=True)

app = FastAPI()
clients, loop = set(), None

async def broadcast(msg):
    for ws in list(clients):
        try:
            await ws.send_json(msg)
        except Exception:
            clients.discard(ws)

def consume():
    c = KafkaConsumer("sensor-processed", "sensor-agg", bootstrap_servers="localhost:9092",
                      auto_offset_reset="latest", value_deserializer=lambda m: json.loads(m.decode()))
    for m in c:
        data = m.value
        data["kind"] = "reading" if m.topic == "sensor-processed" else "agg"
        if data["kind"] == "reading":
            queue_alert(data)
        asyncio.run_coroutine_threadsafe(broadcast(data), loop)

@app.on_event("startup")
async def startup():
    global loop
    loop = asyncio.get_running_loop()
    threading.Thread(target=consume, daemon=True).start()
    threading.Thread(target=mailer, daemon=True).start()
    print("Email alerts: ON -> " + str(EMAIL_TO) if EMAIL_USER else "Email alerts: OFF (create a .env file to enable)", flush=True)

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)

@app.get("/")
def index():
    return FileResponse("dashboard.html")

@app.get("/api/motes")
def motes():
    out = []
    try:
        for line in open("data/mote_locs.txt"):
            q = line.split()
            if len(q) == 3:
                out.append({"id": int(float(q[0])), "x": float(q[1]), "y": float(q[2])})
    except Exception:
        pass
    return out

@app.get("/api/test-email")
def test_email():
    if not EMAIL_USER:
        return {"ok": False, "error": "EMAIL_USER / EMAIL_PASS not set in .env"}
    try:
        send_email("[Intel Lab Monitor] test email", "Email alerts are working.")
        return {"ok": True, "sent_to": EMAIL_TO}
    except Exception as ex:
        return {"ok": False, "error": str(ex)}