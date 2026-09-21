#!/bin/bash
# ============================================
#   Flink Job Build Script
#   Docker 멀티스테이지 빌드로 fat JAR 생성
#   호스트에 Java/Maven 설치 불필요
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FLINK_DIR="$SCRIPT_DIR/../flink"
TARGET_DIR="$FLINK_DIR/target"

echo "============================================"
echo "  Building Flink CDC Job"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"

# target 디렉토리 생성
mkdir -p "$TARGET_DIR"

echo "[1/3] Docker 멀티스테이지 빌드 시작..."
docker build -t flink-cdc-builder "$FLINK_DIR"

echo "[2/3] JAR 파일 추출..."
# 임시 컨테이너에서 JAR 복사
CONTAINER_ID=$(docker create flink-cdc-builder)
docker cp "$CONTAINER_ID:/output/flink-cdc-job-1.0.0.jar" "$TARGET_DIR/flink-cdc-job-1.0.0.jar"
docker rm "$CONTAINER_ID" > /dev/null

echo "[3/3] 빌드 이미지 정리..."
docker rmi flink-cdc-builder > /dev/null 2>&1 || true

JAR_SIZE=$(du -h "$TARGET_DIR/flink-cdc-job-1.0.0.jar" | cut -f1)
echo ""
echo "============================================"
echo "  Build Complete!"
echo "  JAR: flink/target/flink-cdc-job-1.0.0.jar"
echo "  Size: $JAR_SIZE"
echo "============================================"
