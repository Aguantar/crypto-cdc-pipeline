#!/usr/bin/env bash
# docs/34 #6: 특정 구간을 같은 코드로 다시 흘려 ClickHouse 를 재생성한다.
#   사용: scripts/ops/reprocess-day.sh '2026-09-19 10:00:00' '2026-09-19 11:00:00'   (UTC)
#
# 왜 Kafka 재소비인가 (설계 판단):
#   ① 같은 Flink 잡·같은 파서를 쓰므로 변환 로직이 하나로 유지된다. MySQL 원장에서 SQL 로 다시 만들면 파서의 두 번째 구현이 생겨
#      시간이 지나면 드리프트한다(오늘 Decimal 전환 같은 변경이 한쪽에만 반영되는 사고).
#   ② 중복 걱정이 없다. ClickHouse ReplacingMergeTree 가 (market, upbit_timestamp, sequential_id) 로 접고, 재처리분은 flink_ts 가 더 커서 이긴다 → 멱등.
#   ③ 슬롯은 부하 실험용 lab TaskManager(profile lab, 슬롯 2)를 잠깐 띄워 쓴다. 프로덕션 잡의 슬롯을 뺏지 않는다.
# Kafka 구간은 '도착 시각'이고 우리가 원하는 창은 '체결 시각'이다 (09-20 실측):
#   1시간 창을 그대로 읽었더니 29행이 빠졌다 - 전부 10:59:59 에 체결됐는데 producer 배치가 경계를 넘겨 11:00:00.46 에 binlog 에 찍힌 행.
#   그래서 Kafka 는 [시작−여유, 끝+여유] 로 읽고(기본 10분, REPROCESS_MARGIN_MIN), 커버리지를 직접 잰다:
#   "체결 시각이 창 안인데 flink_ts 가 잡 시작보다 이른 행" = 재처리가 못 덮은 행. 0 이어야 통과.
#   여유 밖에서 온 행(백필·gap-fill 로 몇 시간 뒤 도착)이 있으면 이 숫자가 0 이 아니고, 여유를 늘려 다시 돌리면 된다.
# 한계(반드시 알고 쓸 것):
#   - Kafka 체결 토픽 보존 7일 밖은 이 경로로 못 한다 → 그때는 MySQL 원장(7일 파티션) 또는 백업/Parquet(§보조 경로).
#   - 재처리한 구간은 flink_ts 가 "다시 적재한 시각"으로 바뀐다 → 그 구간의 e2e 지연 지표는 더 이상 원래 값이 아니다(대조·정합성 지표는 영향 없음).
#   - 섀도 알럿은 끈다(MARKET_ALERTS_ENABLED=false). 켜면 같은 전이가 두 번 생겨 동등성 판정이 오염된다.
set -euo pipefail
cd "$(dirname "$0")/../.."
if [ "${1:-}" = "paths" ]; then cat <<'PATHS'
재처리 경로 결정표 (docs/34 #6) - "무엇을, 얼마나 오래된 것을 되살리나"
┌ Upbit 체결 ─────────────────────────────────────────────────────────────────────
│ 7일 안  : 이 스크립트(Kafka 재소비 → 같은 Flink 잡). 변환 로직이 하나라 드리프트 없음. 멱등.
│ 7일 밖  : **소스에서 되살릴 수 없다.** Kafka 7일·MySQL 파티션 7일·거래소 REST(trades/ticks) 7일이 전부 같은 경계.
│           남는 선택지는 ① ClickHouse 백업 복원(값 그대로, /backups full_+incr_) ② 거래소 일봉·시간봉 수준의 집계만 재구성.
│ 유실 메우기(특정 체결이 없음): scripts/observe/backfill_trades.py --from --to  (거래소 REST 대조 후 MySQL INSERT → CDC 가 하류로)
├ Binance 체결 ───────────────────────────────────────────────────────────────────
│ 3일 안  : binance.trades.v1 재소비. 잡을 group.id 바꿔 띄운다(BinanceTradeJob 은 아직 bounded 파라미터 없음 → 필요 시 추가).
│ 3일 밖  : 거래소 REST aggTrades(가중치 4/1000건)로 재수집하거나 포기. 시세라 원장 의무 없음.
├ 호가 ───────────────────────────────────────────────────────────────────────────
│ 7일 안  : orderbook_raw 에 그대로 있음.
│ 120일 안: /backups/parquet/orderbook_raw_<날짜>.parquet 복원 (09-20 검증: 09-17 아카이브 16,333,731행 = 당시 표와 동일)
│           docker exec cdc-clickhouse sh -c 'clickhouse-client -q "INSERT INTO cdc_pipeline.orderbook_raw FORMAT Parquet" < /backups/parquet/orderbook_raw_2026-09-17.parquet'
│           (읽기만 확인: docker exec cdc-clickhouse clickhouse-local -q "SELECT count() FROM file('<경로>', Parquet)")
│ 365일   : orderbook_1m(파생)은 남아 있으므로 분 단위 지표는 복구 불필요.
└ 원장(2층)/케이스 ───────────────────────────────────────────────────────────────
  MySQL 에 그대로 있고 ledger 토픽 30일 → 커넥터 스냅샷 재실행 또는 토픽 재소비.
PATHS
exit 0; fi
FROM_TS="${1:-}"; TO_TS="${2:-}"
[ -n "$FROM_TS" ] && [ -n "$TO_TS" ] || { echo "사용: $0 '<UTC from>' '<UTC to>'   |   $0 paths"; exit 2; }
LOG=/home/calme/kafka-reassign/reprocess-$(date -u +%Y%m%dT%H%M).log; mkdir -p /home/calme/kafka-reassign
CH(){ docker exec cdc-clickhouse clickhouse-client --max_memory_usage 1200000000 --max_threads 2 -q "$1"; }
say(){ echo "[$(date -u +%FT%TZ)] $*" | tee -a "$LOG"; }
START_MS=$(( $(date -u -d "$FROM_TS" +%s) * 1000 )); END_MS=$(( $(date -u -d "$TO_TS" +%s) * 1000 ))
MARGIN_MIN=${REPROCESS_MARGIN_MIN:-10}; MARGIN_MS=$(( MARGIN_MIN * 60000 ))
READ_FROM_MS=$(( START_MS - MARGIN_MS )); READ_TO_MS=$(( END_MS + MARGIN_MS ))
say "재처리 구간(체결 시각) $FROM_TS ~ $TO_TS (UTC) = $START_MS ~ $END_MS"
say "Kafka 읽기 구간(도착 시각, 여유 ${MARGIN_MIN}분) = $READ_FROM_MS ~ $READ_TO_MS"

# 0) 보존 창 안인가
RET_MS=$(docker exec cdc-kafka-1 kafka-configs --bootstrap-server kafka-1:29092 --entity-type topics --entity-name cdc.crypto_db.crypto_trades --describe 2>/dev/null | grep -oE "retention.ms=[0-9]+" | head -1 | cut -d= -f2)
OLDEST_MS=$(( $(date -u +%s) * 1000 - ${RET_MS:-604800000} ))
[ "$READ_FROM_MS" -ge "$OLDEST_MS" ] || { say "중단: 구간 시작이 Kafka 보존(${RET_MS}ms) 밖 - 보조 경로(MySQL 원장/Parquet)를 쓸 것"; exit 1; }

# 1) 재처리 전 상태 (멱등성 판정 기준)
snap(){ CH "SELECT count() AS final_rows, toString(sum(trade_amount)) AS amt, toString(sum(trade_volume)) AS vol, uniqExact(market, sequential_id) AS uniq_keys
  FROM cdc_pipeline.crypto_trades FINAL WHERE upbit_timestamp >= $START_MS AND upbit_timestamp < $END_MS FORMAT TSV"; }
BEFORE=$(snap); say "전(FINAL 행수/금액합/수량합/고유키): $BEFORE"
RAW_BEFORE=$(CH "SELECT count() FROM cdc_pipeline.crypto_trades WHERE upbit_timestamp >= $START_MS AND upbit_timestamp < $END_MS"); say "전(raw 행수, 머지 전 중복 포함): $RAW_BEFORE"

# 2) 슬롯 확보 + MV 분리(지연 통계 이중 집계 방지)
say "lab TaskManager 기동(슬롯 +2)"; docker compose --profile lab up -d flink-taskmanager-lab >/dev/null 2>&1
for i in $(seq 1 30); do T=$(curl -s -m 5 localhost:8081/overview | python3 -c "import sys,json; print(json.load(sys.stdin)['slots-available'])" 2>/dev/null || echo 0); [ "${T:-0}" -ge 2 ] && break; sleep 2; done
say "  여유 슬롯 $T"
CH "DETACH TABLE cdc_pipeline.mv_latency_stats"; say "  mv_latency_stats detach"

# 3) 재처리 잡 (bounded → 스스로 끝난다)
GROUP="reprocess-$(date -u +%Y%m%d%H%M%S)"; JOB_START=$(CH "SELECT toString(now64(3))")
say "잡 제출 (group=$GROUP, 알럿 off)"
JID=$(docker exec -e CDC_START_TS_MS="$READ_FROM_MS" -e CDC_END_TS_MS="$READ_TO_MS" -e CDC_GROUP_ID="$GROUP" \
      -e MARKET_ALERTS_ENABLED=false -e JOB_NAME="Reprocess $FROM_TS" \
      cdc-flink-jobmanager flink run -d /opt/flink/usrlib/flink-cdc-job-1.0.0.jar 2>&1 | grep -oE "JobID [a-f0-9]+" | awk '{print $2}')
say "  JobID $JID"
for i in $(seq 1 180); do
  ST=$(curl -s -m 5 "localhost:8081/jobs/$JID" | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])" 2>/dev/null || echo UNKNOWN)
  [ "$ST" = "FINISHED" ] && { say "  잡 완료 ($((i*5))초)"; break; }
  [ "$ST" = "FAILED" ] && { say "  잡 실패 - 로그 확인"; break; }
  sleep 5
done
say "  최종 상태 $ST"

# 4) 정리
CH "ATTACH TABLE cdc_pipeline.mv_latency_stats"; say "mv attach"
docker compose --profile lab stop flink-taskmanager-lab >/dev/null 2>&1; say "lab TaskManager 정지"

# 5) 검증 - 멱등이면 FINAL 의 행수·금액합·수량합이 하나도 안 바뀐다
AFTER=$(snap); RAW_AFTER=$(CH "SELECT count() FROM cdc_pipeline.crypto_trades WHERE upbit_timestamp >= $START_MS AND upbit_timestamp < $END_MS")
say "후(FINAL): $AFTER"
say "후(raw): $RAW_AFTER  (raw 가 늘고 FINAL 이 그대로면 RMT 가 접은 것)"
# 커버리지: 체결 시각이 창 안인데 이번 재처리를 못 받은 행 (0 이어야 한다)
NOT_COVERED=$(CH "SELECT count() FROM cdc_pipeline.crypto_trades FINAL WHERE upbit_timestamp >= $START_MS AND upbit_timestamp < $END_MS AND flink_ts < '$JOB_START'")
say "커버리지: 재처리를 못 받은 행 $NOT_COVERED (0 이어야 통과 - 아니면 REPROCESS_MARGIN_MIN 을 늘려 재실행)"
if [ "$BEFORE" = "$AFTER" ] && [ "$NOT_COVERED" = "0" ]; then say "판정: **통과** - 전 구간이 다시 흘렀고(커버리지 0) FINAL 은 완전히 동일(멱등)"
elif [ "$NOT_COVERED" != "0" ]; then say "판정: **커버리지 부족** - 여유를 늘려 재실행"
else say "판정: FINAL 에 차이 있음 - 위 두 줄 비교(유실을 메운 재처리라면 행수가 늘어야 정상)"; fi
say "done"
