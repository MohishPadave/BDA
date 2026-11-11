# IoT Sensor Stream Analytics (Kafka + Spark Structured Streaming + Live Dashboard)

BDA mini project. Pipeline:

producer.py -> Kafka (sensor-readings) -> spark_job.py -> Kafka (sensor-processed, sensor-agg) -> server.py (WebSocket) -> dashboard.html

## Prerequisites
- Java 11 or 17  (check: `java -version`)
- Python 3.10+
- Docker Desktop (for Kafka)
- Windows users: run everything inside WSL2 (Ubuntu). Spark is unreliable on native Windows.

## Setup (once)
```bash
python3 -m venv venv
source venv/bin/activate          # Windows WSL: same. PowerShell: venv\Scripts\activate
pip install -r requirements.txt
```

## Run (4 terminals, in this order)
```bash
# 1. Kafka
docker compose up -d

# 2. Spark streaming job (first run downloads the Kafka connector)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_job.py
#    If spark-submit is not found, use:
#    python -m pyspark.find_spark_home  ->  or run:  venv/bin/spark-submit ...

# 3. Sensor simulator
python producer.py

# 4. Backend + dashboard
uvicorn server:app --port 8000
```
Open http://localhost:8000

Shortcut for Linux/macOS/WSL: `bash run_all.sh` (starts everything in the background, logs in ./logs). Stop with `bash stop_all.sh`.

## Using the real dataset (Intel Berkeley Research Lab)
54 real wireless sensors in a lab, 2.3 million readings, Feb 28 - Apr 5 2004 (one reading every 31 s).
Source: https://db.csail.mit.edu/labdata/labdata.html  (free to use, please acknowledge the authors:
Bodik, Hong, Guestrin, Madden, Paskin, Thibaux).

```bash
pip install -r requirements.txt     # adds pandas
python download_data.py             # downloads 34 MB to data/data.txt.gz
bash run_all.sh                     # automatically uses the real data if data/data.txt.gz exists
```
Or run the replay yourself: `python replay_producer.py --motes 1,2,3,4,5,6 --speed 31 --loop`
(speed 31 = one original 31-second reading cycle per second. Use --start-date 2004-03-01 to start later.)

If data/data.txt.gz is missing, run_all.sh falls back to the simulator (producer.py).

## Demo mode
If the backend is not running, opening dashboard.html directly shows simulated data so you can still demo the UI.

## Troubleshooting
- Spark version mismatch: the connector version (3.5.1) must match PySpark in requirements.txt.
- Checkpoint errors after editing spark_job.py: delete the `checkpoints/` folder.
- Dashboard says "Demo mode": the backend or Kafka is not running, or no data has reached `sensor-processed` yet.
- Port 9092 in use: stop other Kafka instances (`docker ps`).

## Files
- producer.py: simulates 6 IoT sensors (temperature, PM2.5, power, humidity) with injected anomalies
- spark_job.py: EWMA z-score + limit-based anomaly detection, 30 s sliding-window aggregates
- server.py: FastAPI bridge from Kafka to WebSocket, serves the dashboard
- dashboard.html: live monitoring UI (Chart.js)
