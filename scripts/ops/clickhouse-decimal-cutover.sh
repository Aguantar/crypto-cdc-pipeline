#!/usr/bin/env bash
# docs/34 #5: 금액·수량 Float64 → Decimal 무정지 재생성. 단계: create | copy | verify | cutover | rollback
#
# 왜: 저장 층의 숫자 타입은 하류에 주는 계약이다. 원천(MySQL DECIMAL(20,8), Debezium decimal.handling.mode=string, Binance JSON 문자열)엔
#     정밀도가 있었는데 Flink 가 double 로 받아 버렸다 - 우리가 버린 것이라 DE 가 고칠 일.
# 스케일 규약: price·volume = 8 (원천과 동일, Binance exchangeInfo 실측 최대 8), amount = price×volume 이라 정확히 16.
#             Decimal(38,16) 의 정수부 22자리 vs 전체 기간 합계 15자리 → 여유 7자리(09-20 실측 115.75조 KRW).
# 금액 재계산: MySQL trade_amount 는 DECIMAL(20,4) 라 먼지 체결이 0(7일 42,642행). 과거분도 price×volume 으로 다시 계산해
#             "amount = 정의" 가 이력 전체에서 성립하게 한다. 한계: 과거 값은 Float64 를 거쳤으므로 double 의 15~16 유효숫자 안에서만 복원된다(관측된 모든 행이 그 범위).
# 절차가 EXCHANGE 인 이유: 117M 행 ALTER MODIFY 는 한 번에 걸리는 뮤테이션이라 중간 검증이 불가능. 복사 → 검증 → 교환은 어제까지 두 번 검증된 경로(docs/25·28).
set -euo pipefail
cd "$(dirname "$0")/../.."
CH(){ docker exec cdc-clickhouse clickhouse-client --max_memory_usage 1200000000 --max_bytes_before_external_group_by 500000000 --max_insert_threads 1 --max_threads 2 -q "$1"; }
say(){ echo "[$(date -u +%FT%TZ)] $*" | tee -a "$LOG"; }
NEW=cdc_pipeline.crypto_trades_dec; CUR=cdc_pipeline.crypto_trades
LOG=/home/calme/kafka-reassign/decimal-$(date -u +%Y%m%d).log; mkdir -p /home/calme/kafka-reassign
T1F=/home/calme/kafka-reassign/decimal-t1
# 복사 대상 컬럼: 금액 3개만 변환, 나머지는 그대로
# Float64 → Decimal 은 반드시 toString 을 거친다(09-20 실측, 이 스크립트의 핵심):
#   CAST/toDecimal64/accurateCast 는 모두 버림이라 0.00518(Float64 = 0.00517999999…) 이 0.00517999 가 된다.
#   KRW-PEPE 한 행에서 금액이 42.86 KRW 어긋났고(차이 = volume × 1e-8), 월별 금액합이 2e-8 만큼 작게 나와 잡혔다 - 행수만 봤으면 못 봤다.
#   toString 은 그 double 로 되돌아가는 가장 짧은 십진 표기를 주므로 원본 DECIMAL(20,8) 을 그대로 복원한다(MySQL 원본과 대조 확인: 4285662548.26254840).
#   지수 표기(1e-8, 3.2e-7)도 toDecimal128 이 그대로 파싱한다.
DEC='toDecimal128(toString(%s), 8)'
SRC_COLS="op, trade_id, market,
  toDecimal128(toString(trade_price), 8), toDecimal128(toString(trade_volume), 8),
  toDecimal128(toString(trade_price), 8) * toDecimal128(toString(trade_volume), 8),
  ask_bid, upbit_timestamp, sequential_id, source_ts, cdc_ts, cdc_latency_ms, flink_ts,
  toDecimal128OrNull(toString(best_ask_price), 8), toDecimal128OrNull(toString(best_ask_size), 8),
  toDecimal128OrNull(toString(best_bid_price), 8), toDecimal128OrNull(toString(best_bid_size), 8),
  inserted_at, recv_ms, ingest_source, stream_type"
DST_COLS="op, trade_id, market, trade_price, trade_volume, trade_amount, ask_bid, upbit_timestamp, sequential_id, source_ts, cdc_ts, cdc_latency_ms, flink_ts, best_ask_price, best_ask_size, best_bid_price, best_bid_size, inserted_at, recv_ms, ingest_source, stream_type"

case "${1:-status}" in
create)
  CH "CREATE TABLE IF NOT EXISTS $NEW (
    op LowCardinality(String), trade_id UInt64, market LowCardinality(String),
    trade_price Decimal(20, 8) COMMENT '원천 MySQL DECIMAL(20,8) 과 같은 스케일',
    trade_volume Decimal(20, 8),
    trade_amount Decimal(38, 16) COMMENT 'price × volume 의 정확한 곱(스케일 8+8). MySQL 의 DECIMAL(20,4) 값은 쓰지 않는다',
    ask_bid LowCardinality(String), upbit_timestamp Int64, sequential_id Int64,
    source_ts DateTime64(3), cdc_ts DateTime64(3), cdc_latency_ms Int64, flink_ts DateTime64(3),
    best_ask_price Nullable(Decimal(20, 8)), best_ask_size Nullable(Decimal(20, 8)),
    best_bid_price Nullable(Decimal(20, 8)), best_bid_size Nullable(Decimal(20, 8)),
    inserted_at DateTime64(3) DEFAULT now64(3),
    recv_ms Nullable(Int64), ingest_source LowCardinality(String) DEFAULT 'ws', stream_type LowCardinality(String) DEFAULT 'REALTIME'
  ) ENGINE = ReplacingMergeTree(flink_ts)
  PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(upbit_timestamp))
  ORDER BY (market, upbit_timestamp, sequential_id)
  TTL toDateTime(fromUnixTimestamp64Milli(upbit_timestamp)) + INTERVAL 365 DAY
  SETTINGS index_granularity = 8192"
  say "created $NEW"
  ;;
copy)
  T1=$(CH "SELECT toString(now64(3))"); echo "$T1" > "$T1F"; say "copy 시작 T1=$T1 (이 시각 이전 행만)"
  for d in $(CH "SELECT arrayStringConcat(arrayMap(x -> toString(x), groupArray(d)), ' ') FROM (SELECT DISTINCT toDate(fromUnixTimestamp64Milli(upbit_timestamp)) AS d FROM $CUR ORDER BY d)"); do
    s=$(date +%s); LO=$(date -u -d "$d" +%s)000; HI=$(date -u -d "$d +1 day" +%s)000
    CH "INSERT INTO $NEW ($DST_COLS) SELECT $SRC_COLS FROM $CUR WHERE upbit_timestamp >= $LO AND upbit_timestamp < $HI AND flink_ts < '$T1'"
    echo -n "."; [ $(( $(date +%s) - s )) -gt 20 ] && say "  $d $(( $(date +%s) - s ))s"
  done; echo
  say "copy 완료: $(CH "SELECT sum(rows) FROM system.parts WHERE table='crypto_trades_dec' AND active") rows"
  ;;
verify)
  T1=$(cat "$T1F"); say "verify (T1=$T1)"
  say "월별 행수 / 수량합 상대오차 / 금액합:"
  CH "SELECT m, src_rows, dst_rows, src_rows = dst_rows AS rows_ok,
        round(abs(src_vol - dst_vol) / greatest(abs(src_vol), 1e-9), 12) AS vol_rel_diff,
        round(dst_amt / greatest(src_amt, 1e-9), 9) AS amt_ratio
      FROM (SELECT toYYYYMM(fromUnixTimestamp64Milli(upbit_timestamp)) AS m, count() AS src_rows, sum(trade_volume) AS src_vol, sum(trade_amount) AS src_amt
            FROM $CUR WHERE flink_ts < '$T1' GROUP BY m) AS a
      INNER JOIN (SELECT toYYYYMM(fromUnixTimestamp64Milli(upbit_timestamp)) AS m, count() AS dst_rows, toFloat64(sum(trade_volume)) AS dst_vol, toFloat64(sum(trade_amount)) AS dst_amt
            FROM $NEW WHERE flink_ts < '$T1' GROUP BY m) AS b USING m ORDER BY m FORMAT TSVWithNames" | tee -a "$LOG"
  say "먼지 체결 복원: 옛 amount=0 이면서 새 amount>0 = $(CH "SELECT count() FROM $NEW WHERE trade_amount > 0 AND trade_volume > 0 AND toFloat64(trade_amount) < 0.00005")"
  say "스케일 확인: $(CH "SELECT toString(any(trade_price)) || ' / ' || toString(any(trade_amount)) FROM $NEW WHERE upbit_timestamp > toUnixTimestamp(now()-INTERVAL 1 DAY)*1000")"
  ;;
cutover)
  # 전제: Flink CDC 잡이 정지된 상태(세이브포인트). 정지 중이라 잔여분 복사가 필요 없다.
  T1=$(cat "$T1F"); say "cutover: MV detach → 차이분 → EXCHANGE → MV attach"
  RUNNING=$(curl -s -m 5 localhost:8081/jobs/overview | python3 -c "import sys,json; print(sum(1 for j in json.load(sys.stdin)['jobs'] if j['name']=='CDC Realtime Pipeline' and j['state']=='RUNNING'))")
  [ "$RUNNING" = "0" ] || { say "중단: CDC 잡이 아직 RUNNING - 세이브포인트 정지 후 실행"; exit 1; }
  CH "DETACH TABLE cdc_pipeline.mv_latency_stats"
  T2=$(CH "SELECT toString(now64(3))"); say "  T2=$T2"
  CH "INSERT INTO $NEW ($DST_COLS) SELECT $SRC_COLS FROM $CUR WHERE flink_ts >= '$T1'"
  say "  차이분 $(CH "SELECT count() FROM $CUR WHERE flink_ts >= '$T1'") rows"
  CH "EXCHANGE TABLES $CUR AND $NEW"
  CH "ATTACH TABLE cdc_pipeline.mv_latency_stats"
  say "  교환 완료: trade_price 타입 = $(CH "SELECT type FROM system.columns WHERE database='cdc_pipeline' AND table='crypto_trades' AND name='trade_price'")"
  say "  옛 Float64 표는 crypto_trades_dec 이름으로 남는다(EXCHANGE 라 이름이 맞바뀐다 - 헷갈리므로 검증 뒤 *_float_bak 으로 rename). 롤백 = rollback 단계. 7일 뒤 DROP"
  ;;
binance-prepare)
  # Binance 체결(6M 행, 30일 TTL)도 같은 규칙(toString 경유). quote_qty 는 price×qty 로 다시 계산한다 - 옛 값은 Flink 가 double 로 곱한 것.
  CH "CREATE TABLE IF NOT EXISTS cdc_pipeline.binance_trades_dec (
    symbol LowCardinality(String), trade_id UInt64,
    price Decimal(20, 8), qty Decimal(20, 8), quote_qty Decimal(38, 16),
    is_buyer_maker UInt8, trade_ms Int64, event_ms Int64, recv_ms Int64,
    flink_ts DateTime64(3), inserted_at DateTime64(3) DEFAULT now64(3)
  ) ENGINE = ReplacingMergeTree(recv_ms)
  PARTITION BY toDate(fromUnixTimestamp64Milli(trade_ms))
  ORDER BY (symbol, trade_ms, trade_id)
  TTL toDateTime(fromUnixTimestamp64Milli(trade_ms)) + INTERVAL 30 DAY
  SETTINGS index_granularity = 8192"
  T1B=$(CH "SELECT toString(now64(3))"); echo "$T1B" > /home/calme/kafka-reassign/decimal-t1b; say "binance T1=$T1B"
  CH "INSERT INTO cdc_pipeline.binance_trades_dec (symbol, trade_id, price, qty, quote_qty, is_buyer_maker, trade_ms, event_ms, recv_ms, flink_ts)
      SELECT symbol, trade_id, toDecimal128(toString(price), 8), toDecimal128(toString(qty), 8),
             toDecimal128(toString(price), 8) * toDecimal128(toString(qty), 8),
             is_buyer_maker, trade_ms, event_ms, recv_ms, flink_ts
      FROM cdc_pipeline.binance_trades WHERE flink_ts < '$T1B'"
  say "binance 복사: $(CH "SELECT count() FROM cdc_pipeline.binance_trades_dec") / 원본 $(CH "SELECT count() FROM cdc_pipeline.binance_trades WHERE flink_ts < '$T1B'")"
  ;;
binance-cutover)
  T1B=$(cat /home/calme/kafka-reassign/decimal-t1b)
  RUNNING=$(curl -s -m 5 localhost:8081/jobs/overview | python3 -c "import sys,json; print(sum(1 for j in json.load(sys.stdin)['jobs'] if j['name']=='Binance Trade Pipeline' and j['state']=='RUNNING'))")
  [ "$RUNNING" = "0" ] || { say "중단: Binance 잡이 아직 RUNNING"; exit 1; }
  CH "INSERT INTO cdc_pipeline.binance_trades_dec (symbol, trade_id, price, qty, quote_qty, is_buyer_maker, trade_ms, event_ms, recv_ms, flink_ts)
      SELECT symbol, trade_id, toDecimal128(toString(price), 8), toDecimal128(toString(qty), 8),
             toDecimal128(toString(price), 8) * toDecimal128(toString(qty), 8),
             is_buyer_maker, trade_ms, event_ms, recv_ms, flink_ts
      FROM cdc_pipeline.binance_trades WHERE flink_ts >= '$T1B'"
  say "  binance 차이분 $(CH "SELECT count() FROM cdc_pipeline.binance_trades WHERE flink_ts >= '$T1B'") rows"
  CH "EXCHANGE TABLES cdc_pipeline.binance_trades AND cdc_pipeline.binance_trades_dec"
  say "binance 교환 완료: price 타입 = $(CH "SELECT type FROM system.columns WHERE database='cdc_pipeline' AND table='binance_trades' AND name='price'")"
  ;;
rollback)
  # 교환 직후엔 $NEW(crypto_trades_dec) 가 옛 Float64 표다. rename 을 이미 했다면 crypto_trades_float_bak 으로 바꿔 실행한다.
  CH "EXCHANGE TABLES $CUR AND $NEW"; say "롤백: 되돌림. trade_price 타입 = $(CH "SELECT type FROM system.columns WHERE database='cdc_pipeline' AND table='crypto_trades' AND name='trade_price'")"
  ;;
status) CH "SELECT table, name, type FROM system.columns WHERE database='cdc_pipeline' AND table LIKE 'crypto_trades%' AND name IN ('trade_price','trade_volume','trade_amount') ORDER BY table, position FORMAT TSV";;
esac
