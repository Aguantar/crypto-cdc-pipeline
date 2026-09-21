#!/bin/bash
# ============================================
#   CDC Pipeline - Full Startup Script
#   docker compose up 후 실행
#   Connect 내부 토픽 수정 + Connector 등록 + Flink Job 제출
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo "  CDC Pipeline Startup"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"

# ---- 1. Kafka Broker 대기 ----
echo "[1/5] Kafka Broker 대기 중..."
for i in $(seq 1 30); do
    if docker exec cdc-kafka-1 kafka-broker-api-versions --bootstrap-server kafka-1:29092 > /dev/null 2>&1; then
        echo "  Kafka Broker 준비 완료"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  ERROR: Kafka Broker 시작 실패"
        exit 1
    fi
    sleep 2
done

# ---- 2. Connect 내부 토픽 확인/수정 ----
echo "[2/5] Connect 내부 토픽 확인..."

fix_topic() {
    local TOPIC=$1
    local PARTITIONS=$2

    RF=$(docker exec cdc-kafka-1 kafka-topics --bootstrap-server kafka-1:29092 \
        --describe --topic "$TOPIC" 2>/dev/null | grep "ReplicationFactor:" | grep -o "ReplicationFactor: [0-9]*" | awk '{print $2}')

    if [ "$RF" = "1" ]; then
        echo "  $TOPIC: RF=1 → 삭제 후 RF=3으로 재생성"
        docker stop cdc-kafka-connect > /dev/null 2>&1 || true
        docker exec cdc-kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --delete --topic "$TOPIC" 2>/dev/null
        sleep 2
        docker exec cdc-kafka-1 kafka-topics --bootstrap-server kafka-1:29092 \
            --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 3 --config cleanup.policy=compact
        return 1  # 수정됨
    elif [ -z "$RF" ]; then
        echo "  $TOPIC: 없음 → RF=3으로 생성"
        docker exec cdc-kafka-1 kafka-topics --bootstrap-server kafka-1:29092 \
            --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 3 --config cleanup.policy=compact
        return 1  # 생성됨
    else
        echo "  $TOPIC: RF=$RF ✓"
        return 0  # 정상
    fi
}

NEED_RESTART=0
fix_topic "_connect-configs" 1 || NEED_RESTART=1
fix_topic "_connect-offsets" 25 || NEED_RESTART=1
fix_topic "_connect-status" 5 || NEED_RESTART=1

if [ "$NEED_RESTART" -eq 1 ]; then
    echo "  Connect 재시작..."
    docker start cdc-kafka-connect > /dev/null 2>&1 || true
fi

# ---- 3. Kafka Connect 대기 ----
echo "[3/5] Kafka Connect 대기 중..."
for i in $(seq 1 30); do
    if curl -s http://localhost:8083/ > /dev/null 2>&1; then
        echo "  Kafka Connect 준비 완료"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  ERROR: Kafka Connect 시작 실패"
        exit 1
    fi
    sleep 3
done

# ---- 4. Debezium Connector 등록 ----
echo "[4/5] Debezium Connector 확인..."

CONNECTOR_STATUS=$(curl -s http://localhost:8083/connectors/mysql-cdc-connector/status 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "NOT_FOUND")

if [ "$CONNECTOR_STATUS" = "RUNNING" ]; then
    echo "  Connector 이미 실행 중 ✓"
else
    echo "  Connector 등록 중..."
    curl -s -X POST http://localhost:8083/connectors \
        -H "Content-Type: application/json" \
        -d @"$PROJECT_DIR/debezium/connector-config.json" > /dev/null

    sleep 10
    CONNECTOR_STATUS=$(curl -s http://localhost:8083/connectors/mysql-cdc-connector/status 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
    echo "  Connector 상태: $CONNECTOR_STATUS"
fi

# ---- 5. Flink Job 제출 ----
echo "[5/5] Flink Job 확인..."

FLINK_JOBS=$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "import sys,json; jobs=json.load(sys.stdin)['jobs']; print(len([j for j in jobs if j['state']=='RUNNING']))" 2>/dev/null || echo "0")

if [ "$FLINK_JOBS" != "0" ]; then
    echo "  Flink Job 이미 실행 중 ($FLINK_JOBS개) ✓"
else
    JAR_PATH="/opt/flink/usrlib/flink-cdc-job-1.0.0.jar"
    if docker exec cdc-flink-jobmanager test -f "$JAR_PATH" 2>/dev/null; then
        echo "  Flink Job 제출 중..."
        docker exec cdc-flink-jobmanager flink run -d "$JAR_PATH" 2>/dev/null
        sleep 5
        FLINK_JOBS=$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "import sys,json; jobs=json.load(sys.stdin)['jobs']; print(len([j for j in jobs if j['state']=='RUNNING']))" 2>/dev/null || echo "0")
        echo "  Flink Job 상태: ${FLINK_JOBS}개 실행 중"
    else
        echo "  JAR 파일 없음. 먼저 빌드하세요: bash scripts/build-flink-job.sh"
    fi
fi

echo ""
echo "============================================"
echo "  Startup Complete!"
echo "============================================"
