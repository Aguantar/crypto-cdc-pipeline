#!/usr/bin/env bash
# 토픽 정의를 코드로 (docs/29 §4·§6). 지금과 KRaft 컷오버 때 같은 스크립트를 돌린다. 존재하면 설정만 맞추고, 없으면 만든다.
# 왜 zstd: 800B JSON 을 그대로 쌓아 12.4M 건 9.97GB(docs/29 §1). 왜 7일 시간 보존: 재처리엔 "며칠치가 있다"는 약속이 필요한데 bytes 상한은 날짜를 보장 못 한다.
set -euo pipefail
K="docker exec cdc-kafka-1"; BS="--bootstrap-server kafka-1:29092"
declare -A TOPICS=(
  ["cdc.crypto_db.crypto_trades"]="partitions=3 compression.type=zstd retention.ms=604800000 retention.bytes=8589934592 min.insync.replicas=1"
  ["upbit.orderbook.v1"]="partitions=6 compression.type=producer retention.ms=86400000 retention.bytes=6442450944 min.insync.replicas=1"
  ["cdc.dlq.crypto_trades"]="partitions=1 compression.type=zstd retention.ms=2592000000 min.insync.replicas=1"
  # 2층 원장 (docs/28 B, 2026-09-19): 하루 수백 행 → 1 파티션이면 키 순서가 곧 토픽 순서. 30일 보존 = 테스트넷 리셋 주기(월 1회)보다 길게
  ["ledger.crypto_db.virtual_orders"]="partitions=1 compression.type=zstd retention.ms=2592000000 min.insync.replicas=1"
  ["ledger.crypto_db.virtual_fills"]="partitions=1 compression.type=zstd retention.ms=2592000000 min.insync.replicas=1"
  ["ledger.crypto_db.virtual_positions"]="partitions=1 compression.type=zstd retention.ms=2592000000 min.insync.replicas=1"
  ["ledger.crypto_db.binance_user_events"]="partitions=1 compression.type=zstd retention.ms=2592000000 min.insync.replicas=1"
  ["ledger.crypto_db.ledger_reconcile"]="partitions=1 compression.type=zstd retention.ms=2592000000 min.insync.replicas=1"
  ["ledger.crypto_db.cases"]="partitions=1 compression.type=zstd retention.ms=2592000000 min.insync.replicas=1"
  # Binance 체결 (docs/31 §3-2, 2026-09-20): 361 msg/s × ~200B ≈ 6GB/일 → 3일 시간 보존 + 바이트 상한. 6 파티션 = Flink 병렬 3 의 2배(키=symbol, 683 심볼 해시 분포)
  ["binance.trades.v1"]="partitions=6 compression.type=zstd retention.ms=259200000 retention.bytes=8589934592 min.insync.replicas=1"
  ["binance.dlq.trades"]="partitions=1 compression.type=zstd retention.ms=2592000000 min.insync.replicas=1"
  # Binance 호가 증분 + 주기 스냅샷 (docs/31 §3-3): 원문은 1일만(재구성 결과가 산출물), 키=symbol 로 스냅샷과 증분이 같은 파티션 순서
  ["binance.depth.v1"]="partitions=6 compression.type=zstd retention.ms=86400000 retention.bytes=8589934592 min.insync.replicas=1"
  ["binance.dlq.depth"]="partitions=1 compression.type=zstd retention.ms=2592000000 min.insync.replicas=1"
)
for t in "${!TOPICS[@]}"; do
  spec="${TOPICS[$t]}"; parts=$(echo "$spec" | grep -oE "partitions=[0-9]+" | cut -d= -f2); cfg=$(echo "$spec" | sed 's/partitions=[0-9]* //' | tr ' ' ',')
  if $K kafka-topics $BS --describe --topic "$t" >/dev/null 2>&1; then
    $K kafka-configs $BS --entity-type topics --entity-name "$t" --alter --add-config "$cfg" >/dev/null && echo "updated $t: $cfg"
  else
    args=(); for kv in ${cfg//,/ }; do args+=(--config "$kv"); done
    $K kafka-topics $BS --create --topic "$t" --partitions "$parts" --replication-factor 1 "${args[@]}" >/dev/null && echo "created $t ($parts p): $cfg"
  fi
done
for t in "${!TOPICS[@]}"; do echo -n "$t → "; $K kafka-configs $BS --entity-type topics --entity-name "$t" --describe 2>/dev/null | grep -oE "(compression.type|retention.ms|retention.bytes)=[^ ,]+" | tr '\n' ' '; echo; done
