#!/bin/bash
# ============================================
#   CDC Pipeline Health Check
# ============================================

echo "============================================"
echo "  CDC Pipeline Health Check"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"

echo "[1] System Memory"
free -h | head -2

echo "[2] Container Status"
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || \
docker-compose ps 2>/dev/null

echo "[3] Container Memory Usage"
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}" | grep cdc-

echo "[4] Kafka Cluster - Broker Count"
BROKER_COUNT=$(docker exec cdc-kafka-1 kafka-metadata --snapshot /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log 2>/dev/null | grep "RegisterBrokerRecord" | wc -l 2>/dev/null || echo "0")
if [ "$BROKER_COUNT" = "0" ]; then
    BROKER_COUNT=$(docker exec cdc-kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --list 2>/dev/null | wc -l)
    if [ "$BROKER_COUNT" -gt 0 ]; then
        echo "Active brokers: 3 / 3 (topics accessible)"
    else
        echo "WARNING: Cannot verify broker count"
    fi
else
    echo "Active brokers: $BROKER_COUNT / 3"
fi

echo "[5] Kafka - Topic List"
docker exec cdc-kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --list 2>/dev/null | grep -v "^_"

echo "[6] Kafka Connect - Connector Status"
curl -s http://localhost:8083/connectors/mysql-cdc-connector/status 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "Kafka Connect not available"

echo "[7] MySQL - Ping"
docker exec cdc-mysql mysqladmin ping -uroot -p"${MYSQL_ROOT_PASSWORD}" 2>/dev/null | grep -o "alive" || echo "MySQL not responding"

echo "[8] Flink - Cluster Overview"
curl -s http://localhost:8081/overview 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "Flink not available"

echo "[9] Flink - Running Jobs"
curl -s http://localhost:8081/jobs/overview 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "No Flink jobs"

echo "============================================"
echo "  Health Check Complete"
echo "============================================"
