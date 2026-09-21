#!/usr/bin/env bash
# A-5: ClickHouse crypto_trades 를 체결 시각 파티션·이벤트 시각 키·새 컬럼 3개로 재생성해 무정지 교환 (docs/28 A-5·A-7, 2026-09-19 실행 기록 있음). 단계: create | copy | verify | cutover
# 왜 일 단위 슬라이스·쿼리별 메모리 한도인가: 서버 한도 1.75GiB 에서 큰 INSERT SELECT/uniqExact 는 한 번에 넘긴다(docs/25 실측 1.84GiB, 09-19 verify 1.12GiB 초과).
set -euo pipefail
cd "$(dirname "$0")/../.."
CH(){ docker exec cdc-clickhouse clickhouse-client --max_memory_usage 1200000000 --max_bytes_before_external_group_by 500000000 --max_insert_threads 1 --max_threads 2 -q "$1"; }
say(){ echo "[$(date -u +%FT%TZ)] $*"; }
NEW=cdc_pipeline.crypto_trades_v2; CUR=cdc_pipeline.crypto_trades
LOG=/home/calme/kafka-reassign/a5-ch-$(date -u +%Y%m%d).log; mkdir -p /home/calme/kafka-reassign
COLS="op, trade_id, market, trade_price, trade_volume, trade_amount, ask_bid, upbit_timestamp, sequential_id, source_ts, cdc_ts, cdc_latency_ms, flink_ts, best_ask_price, best_ask_size, best_bid_price, best_bid_size, inserted_at"
case "${1:-status}" in
create)
  CH "CREATE TABLE IF NOT EXISTS $NEW (
    op LowCardinality(String), trade_id UInt64, market LowCardinality(String),
    trade_price Float64, trade_volume Float64, trade_amount Float64, ask_bid LowCardinality(String),
    upbit_timestamp Int64, sequential_id Int64, source_ts DateTime64(3), cdc_ts DateTime64(3), cdc_latency_ms Int64, flink_ts DateTime64(3),
    best_ask_price Nullable(Float64), best_ask_size Nullable(Float64), best_bid_price Nullable(Float64), best_bid_size Nullable(Float64),
    inserted_at DateTime64(3) DEFAULT now64(3),
    recv_ms Nullable(Int64) COMMENT 'producer WS 수신 epoch ms (창2부터 Flink 가 채움)',
    ingest_source LowCardinality(String) DEFAULT 'ws',
    stream_type LowCardinality(String) DEFAULT 'REALTIME'
  ) ENGINE = ReplacingMergeTree(flink_ts)
  PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(upbit_timestamp))
  ORDER BY (market, upbit_timestamp, sequential_id)
  TTL toDateTime(fromUnixTimestamp64Milli(upbit_timestamp)) + INTERVAL 365 DAY
  SETTINGS index_granularity = 8192"
  say "created: $(CH "SELECT engine FROM system.tables WHERE database='cdc_pipeline' AND name='crypto_trades_v2'")" | tee -a $LOG
  ;;
copy)
  say "copy: 이벤트 시각 일 단위 슬라이스 (원본 파티션은 binlog 월이라 프루닝 안 됨 → upbit_timestamp 범위로)" | tee -a $LOG
  T1=$(CH "SELECT toString(now64(3))"); echo "$T1" > /home/calme/kafka-reassign/a5-t1; say "  T1=$T1" | tee -a $LOG
  for d in $(CH "SELECT arrayStringConcat(arrayMap(x -> toString(x), groupArray(d)), ' ') FROM (SELECT DISTINCT toDate(fromUnixTimestamp64Milli(upbit_timestamp)) AS d FROM $CUR ORDER BY d)"); do
    s=$(date +%s); LO=$(date -u -d "$d" +%s)000; HI=$(date -u -d "$d +1 day" +%s)000
    CH "INSERT INTO $NEW ($COLS) SELECT $COLS FROM $CUR WHERE upbit_timestamp >= $LO AND upbit_timestamp < $HI AND flink_ts < '$T1'"
    echo -n "."; [ $(( $(date +%s) - s )) -gt 20 ] && say "  $d $(( $(date +%s) - s ))s" | tee -a $LOG
  done; echo
  say "  copy done. rows(parts) $(CH "SELECT sum(rows) FROM system.parts WHERE table='crypto_trades_v2' AND active")" | tee -a $LOG
  ;;
verify)
  say "verify: 이벤트 월별 원본 count vs 새 테이블 count(복사 완전성, 정확) + 일별 uniqExact(market, sequential_id) 원본 vs 새 FINAL(중복 정리, 정확·일 단위라 메모리 안전)" | tee -a $LOG
  T1=$(cat /home/calme/kafka-reassign/a5-t1)
  CH "SELECT m, src_cnt, dst_cnt, src_cnt = dst_cnt AS ok FROM (SELECT toYYYYMM(fromUnixTimestamp64Milli(upbit_timestamp)) AS m, count() AS src_cnt FROM $CUR WHERE flink_ts < '$T1' GROUP BY m) AS a INNER JOIN (SELECT toYYYYMM(fromUnixTimestamp64Milli(upbit_timestamp)) AS m, count() AS dst_cnt FROM $NEW WHERE flink_ts < '$T1' GROUP BY m) AS b USING m ORDER BY m FORMAT TSV" | tee -a $LOG
  bad=0; n=0
  for d in $(CH "SELECT arrayStringConcat(arrayMap(x -> toString(x), groupArray(d)), ' ') FROM (SELECT DISTINCT toDate(fromUnixTimestamp64Milli(upbit_timestamp)) AS d FROM $NEW ORDER BY d)"); do
    LO=$(date -u -d "$d" +%s)000; HI=$(date -u -d "$d +1 day" +%s)000
    s=$(CH "SELECT uniqExact(market, sequential_id) FROM $CUR WHERE upbit_timestamp >= $LO AND upbit_timestamp < $HI AND flink_ts < '$T1'")
    f=$(CH "SELECT count() FROM $NEW FINAL WHERE upbit_timestamp >= $LO AND upbit_timestamp < $HI AND flink_ts < '$T1'")
    n=$((n+1)); [ "$s" = "$f" ] || { bad=$((bad+1)); say "  MISMATCH $d src_uniq=$s dst_final=$f" | tee -a $LOG; }
  done
  say "  일별 uniq 대조: $n일 중 불일치 $bad" | tee -a $LOG
  ;;
cutover)
  say "cutover: MV detach → 차이분 → EXCHANGE → 잔여 → MV attach → 검증" | tee -a $LOG
  T1=$(cat /home/calme/kafka-reassign/a5-t1)
  CH "DETACH TABLE cdc_pipeline.mv_latency_stats"
  T2=$(CH "SELECT toString(now64(3))"); say "  T2=$T2" | tee -a $LOG
  CH "INSERT INTO $NEW ($COLS) SELECT $COLS FROM $CUR WHERE flink_ts >= '$T1' AND flink_ts < '$T2'"
  CH "EXCHANGE TABLES $CUR AND $NEW"
  say "  exchanged: crypto_trades partition key = $(CH "SELECT partition_key FROM system.tables WHERE database='cdc_pipeline' AND name='crypto_trades'")" | tee -a $LOG
  CH "INSERT INTO $CUR ($COLS) SELECT $COLS FROM $NEW WHERE flink_ts >= '$T2'"
  say "  residual $(CH "SELECT count() FROM $NEW WHERE flink_ts >= '$T2'") rows" | tee -a $LOG
  CH "ATTACH TABLE cdc_pipeline.mv_latency_stats"
  sleep 25
  say "  flink 적재 60s: $(CH "SELECT count() FROM $CUR WHERE flink_ts >= now() - INTERVAL 60 SECOND"), MV max minute: $(CH "SELECT max(minute) FROM cdc_pipeline.mv_latency_stats"), 파티션: $(CH "SELECT arrayStringConcat(groupArray(concat(partition, ':', toString(r))), ' ') FROM (SELECT partition, sum(rows) r FROM system.parts WHERE table='crypto_trades' AND active GROUP BY partition ORDER BY partition)")" | tee -a $LOG
  say "  프루닝 확인(오늘 1시간 조회가 읽는 파트 수): $(CH "EXPLAIN indexes=1 SELECT count() FROM $CUR WHERE upbit_timestamp >= toUnixTimestamp(now() - INTERVAL 1 HOUR)*1000" | grep -iE "Parts:|Granules" | head -2 | tr '\n' ' ')" | tee -a $LOG
  say "  옛 테이블(crypto_trades_v2 이름)은 7일 뒤 DROP. 롤백 = EXCHANGE 되돌리기" | tee -a $LOG
  ;;
status) CH "SELECT name, engine, partition_key, sorting_key, formatReadableSize(total_bytes) FROM system.tables WHERE database='cdc_pipeline' AND name LIKE 'crypto_trades%' FORMAT TSV";;
esac
