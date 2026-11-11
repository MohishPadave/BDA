"""Demo helper: injects TEST faults into the live stream so you can show the whole alert pipeline.
The readings go through the real Spark detector -> DGIM -> Bloom filter -> dashboard -> email.

Examples:
  python inject_faults.py                              # mote 3, temperature, 8 faults
  python inject_faults.py --motes 3,12,18 --metric both --count 8
"""
import argparse, json, time
from kafka import KafkaProducer

ap = argparse.ArgumentParser()
ap.add_argument("--motes", default="3", help="comma separated mote ids, e.g. 3,12,18")
ap.add_argument("--metric", default="temperature", choices=["temperature", "humidity", "both"])
ap.add_argument("--count", type=int, default=8, help="faulty readings per sensor (8 is enough to be marked known faulty)")
ap.add_argument("--interval", type=float, default=1.0, help="seconds between faulty readings")
a = ap.parse_args()

# the same impossible values the real dataset shows when a mote fails
FAULTS = {"temperature": ("T", "°C", 122.15), "humidity": ("H", "%", -3.92)}
kinds = ["temperature", "humidity"] if a.metric == "both" else [a.metric]
motes = [int(m) for m in a.motes.split(",")]

producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode())
print(f"Injecting {a.count} TEST faults into motes {motes} ({', '.join(kinds)})...")
for i in range(a.count):
    for m in motes:
        for kind in kinds:
            tag, unit, val = FAULTS[kind]
            producer.send("sensor-readings", {"sensor_id": f"MOTE{m:02d}-{tag}", "type": kind,
                          "location": f"Intel Lab, mote {m}", "unit": unit, "value": val,
                          "ts": int(time.time() * 1000)})
    producer.flush()
    print(f"  fault {i + 1}/{a.count} sent")
    time.sleep(a.interval)
print("Done. Watch the dashboard, then check your inbox (the digest email can take up to 60 seconds).")