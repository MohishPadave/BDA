"""IoT sensor simulator -> Kafka topic 'sensor-readings' (1 reading/sensor/second)."""
import json, math, random, time
from kafka import KafkaProducer

SENSORS = [  # id, type, location, unit, baseline, amplitude, noise, spike size
    ("TEMP-01", "temperature", "Server Room A", "°C", 24, 2.0, 0.3, 14),
    ("TEMP-02", "temperature", "Cold Storage", "°C", 4, 0.8, 0.15, 9),
    ("AQI-01", "pm25", "Factory Floor", "µg/m³", 38, 8.0, 2.0, 120),
    ("AQI-02", "pm25", "Office Block", "µg/m³", 18, 4.0, 1.2, 80),
    ("PWR-01", "power", "Main Feeder", "kW", 6.5, 1.5, 0.25, 6),
    ("HUM-01", "humidity", "Warehouse", "%", 52, 6.0, 1.0, 30),
]
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode(),
)
print("Streaming sensor data to Kafka... Ctrl+C to stop")
t = 0
while True:
    for sid, typ, loc, unit, base, amp, noise, spike in SENSORS:
        v = base + amp * math.sin(t / 30) + random.gauss(0, noise)
        if random.random() < 0.015:                      # inject an anomaly
            v += random.choice([-1, 1]) * spike * random.uniform(0.7, 1.2)
        producer.send("sensor-readings", {
            "sensor_id": sid, "type": typ, "location": loc, "unit": unit,
            "value": round(max(v, 0), 2), "ts": int(time.time() * 1000),
        })
    producer.flush()
    t += 1
    time.sleep(1)
