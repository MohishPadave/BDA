#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
mkdir -p logs
[ -d venv ] && source venv/bin/activate
docker compose up -d

echo "Waiting for Kafka to be ready..."
KT="docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"
for i in $(seq 1 40); do
  if $KT --list >/dev/null 2>&1; then break; fi
  sleep 2
done
# Create topics BEFORE Spark starts (Spark fails if the input topic does not exist yet)
for t in sensor-readings sensor-processed sensor-agg; do
  $KT --create --if-not-exists --topic $t --partitions 1 --replication-factor 1
done

nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_job.py > logs/spark.log 2>&1 & echo $! > logs/spark.pid
echo "Waiting for Spark to start (about 40 seconds)..."; sleep 40
if [ -f data/data.txt.gz ]; then PROD="python replay_producer.py --loop"; else PROD="python producer.py"; fi
echo "Producer: $PROD"
nohup $PROD > logs/producer.log 2>&1 & echo $! > logs/producer.pid
nohup uvicorn server:app --port 8000 > logs/server.log 2>&1 & echo $! > logs/server.pid
echo "Running. Open http://localhost:8000 in about 1-2 minutes (logs in ./logs)"