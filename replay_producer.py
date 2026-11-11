"""Replays the real Intel Berkeley Lab sensor data into Kafka as a live stream.

Original data: 54 motes, one reading every 31 s (Feb 28 - Apr 5, 2004).
--speed 31 means 1 second of wall-clock time = 1 original reading cycle (31 s of real time).
Usage: python replay_producer.py [--motes 1,2,3,4,5,6] [--speed 31] [--start-date 2004-03-01] [--loop]
"""
import argparse, json, time
import pandas as pd
from kafka import KafkaProducer

ap = argparse.ArgumentParser()
ap.add_argument("--file", default="data/data.txt.gz")
ap.add_argument("--motes", default="all", help="comma separated mote ids (1-54), or: all")
ap.add_argument("--speed", type=float, default=31.0, help="replay speed-up factor")
ap.add_argument("--start-date", default=None, help="e.g. 2004-03-01")
ap.add_argument("--loop", action="store_true", help="restart when data ends")
a = ap.parse_args()
motes = None if a.motes == "all" else {int(m) for m in a.motes.split(",")}

cols = ["date", "time", "epoch", "moteid", "temperature", "humidity", "light", "voltage"]
print("Loading dataset (takes ~20-40 seconds)...")
df = pd.read_csv(a.file, sep=r"\s+", names=cols, header=None, on_bad_lines="skip")
for c in cols[3:]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["moteid", "temperature", "humidity"])     # drop truncated rows
df = df[(df["moteid"] >= 1) & (df["moteid"] <= 54)]
if motes is not None:
    df = df[df["moteid"].isin(motes)]
df = df.copy()
df["t"] = pd.to_datetime(df["date"] + " " + df["time"], errors="coerce")
df = df.dropna(subset=["t"])
if a.start_date:
    df = df[df["t"] >= a.start_date]
df = df.sort_values("t")
df["bucket"] = ((df["t"] - df["t"].min()).dt.total_seconds() // 31).astype(int)
print(f"{len(df):,} rows from motes {sorted(motes) if motes else 'all'}, "
      f"{df['t'].min()} to {df['t'].max()}")

producer = KafkaProducer(bootstrap_servers="localhost:9092",
                         value_serializer=lambda v: json.dumps(v).encode())
METRICS = (("temperature", "T", "°C"), ("humidity", "H", "%"))

def replay():
    for _, g in df.groupby("bucket", sort=True):
        now = int(time.time() * 1000)               # re-stamp with current time for live windows
        for r in g.itertuples():
            m = int(r.moteid)
            for typ, tag, unit in METRICS:
                producer.send("sensor-readings", {
                    "sensor_id": f"MOTE{m:02d}-{tag}", "type": typ,
                    "location": f"Intel Lab, mote {m}", "unit": unit,
                    "value": round(float(getattr(r, typ)), 2), "ts": now,
                })
        producer.flush()
        time.sleep(31.0 / a.speed)

print("Streaming real sensor data to Kafka... Ctrl+C to stop")
while True:
    replay()
    if not a.loop:
        break
    print("Dataset finished, restarting from the beginning")