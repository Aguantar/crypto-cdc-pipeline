#!/usr/bin/env bash
# CDC Flink 잡 재배포 (savepoint → 새 JAR → 복원) + 창2 검증 (docs/29 창2, 2026-09-19).
# 배운 것(09-19): 연산자 메트릭 id 는 공백이 _ 로 바뀐다(CDC_Event_Parser.parseFailures); docker exec 로 stdin 을 넘기려면 -i 가 필요하다(없으면 producer 가 아무것도 안 보내고 조용히 끝난다)
# 단계: deploy | verify | inject(깨진 메시지 1건을 체결 토픽에 넣어 DLQ·카운터 확인. 소비자는 CDC 잡뿐이라 안전)
set -euo pipefail
cd "$(dirname "$0")/../.."
FL(){ docker exec cdc-flink-jobmanager flink "$@"; }
CH(){ docker exec cdc-clickhouse clickhouse-client -q "$1"; }
K="docker exec cdc-kafka-1"; BS="--bootstrap-server kafka-1:29092"
say(){ echo "[$(date -u +%FT%TZ)] $*"; }
JOB="CDC Realtime Pipeline"
jid(){ curl -s localhost:8081/jobs/overview | python3 -c "import sys,json; print(next((j['jid'] for j in json.load(sys.stdin)['jobs'] if j['name']=='$JOB' and j['state']=='RUNNING'), ''))"; }
dlq_end(){ $K kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka-1:29092 --topic cdc.dlq.crypto_trades 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'; }
metric(){ local j=$1 v; v=$(curl -s localhost:8081/jobs/$j | python3 -c "import sys,json; print(next(x['id'] for x in json.load(sys.stdin)['vertices'] if x['name'].startswith('Source')))"); curl -s -G "localhost:8081/jobs/$j/vertices/$v/subtasks/metrics" --data-urlencode "get=CDC_Event_Parser.$2" --data-urlencode "agg=sum" | python3 -c "import sys,json; m=json.load(sys.stdin); print(m[0]['sum'] if m else 'n/a')"; }
case "${1:-verify}" in
deploy)
  J=$(jid); [ -n "$J" ] || { say "prod job not running"; exit 1; }
  PRE_MAX=$(CH "SELECT max(trade_id) FROM cdc_pipeline.crypto_trades"); say "pre: job $J, max trade_id $PRE_MAX, dlq end $(dlq_end)"
  SP=$(FL stop --savepointPath /opt/flink/savepoints "$J" | grep -oE "savepoint-[a-f0-9]+-[a-f0-9]+" | tail -1); say "savepoint: $SP"
  # 2026-09-20 함정: 여기서 이미지 `cdc-flink-build` 에서 JAR 을 꺼냈는데, 정상 빌드 경로인
  # scripts/build-flink-job.sh 는 `flink-cdc-builder` 라는 다른 이름으로 빌드하고 끝나면 그 이미지를 지운다.
  # 그래서 방금 빌드한 JAR 을 옛 이미지의 JAR 로 덮어쓰고도 "배포 성공"이라고 보고했다(규칙 승격이 반영 안 됨).
  # → 재배포는 스스로 빌드한다. 무엇이 배포되는지 한 곳에서만 정해져야 한다.
  say "JAR 빌드 (scripts/build-flink-job.sh)"
  scripts/build-flink-job.sh 2>&1 | grep -E "Tests run:.*Failures|BUILD|Size:" | sed 's/^/   /'
  say "jar: $(ls -la flink/target/flink-cdc-job-1.0.0.jar | awk '{print $5, $6, $7, $8}')"
  # 배포 전 가드: JAR 이 어떤 소스보다도 새것인가. 아니면 빌드가 실패했거나 옛 파일을 보고 있는 것이다.
  NEWEST_SRC=$(find flink/src -type f -newer flink/target/flink-cdc-job-1.0.0.jar | head -1)
  [ -z "$NEWEST_SRC" ] || { say "중단: JAR 이 소스보다 오래됐다 ($NEWEST_SRC)"; exit 1; }
  # --allowNonRestoredState: 폐기한 5분 집계 창 상태와 자동 uid 였던 소스·싱크 상태는 버린다. 소스는 커밋된 그룹 오프셋에서 재개(OffsetsInitializer.committedOffsets)
  FL run -d -s "/opt/flink/savepoints/$SP" --allowNonRestoredState /opt/flink/usrlib/flink-cdc-job-1.0.0.jar | grep -i "JobID\|submitted" | sed 's/^/  /'
  sleep 45; J2=$(jid); say "new job: ${J2:-NOT RUNNING}"
  say "trade_id 연속성: pre max $PRE_MAX → 새 잡 첫 행 $(CH "SELECT min(trade_id) FROM cdc_pipeline.crypto_trades WHERE flink_ts >= now() - INTERVAL 60 SECOND AND trade_id > $PRE_MAX - 100000") (pre+1 이하면 무손실)"
  ;;
verify)
  J=$(jid); say "job $J RUNNING; 60s rows $(CH "SELECT count() FROM cdc_pipeline.crypto_trades WHERE flink_ts >= now() - INTERVAL 60 SECOND")"
  say "새 컬럼(최근 60s): $(CH "SELECT ingest_source, stream_type, countIf(recv_ms IS NOT NULL) AS with_recv, count() FROM cdc_pipeline.crypto_trades WHERE flink_ts >= now() - INTERVAL 60 SECOND GROUP BY 1,2 FORMAT TSV" | tr '\t' '/' | tr '\n' ' ')"
  say "metrics: parseFailures=$(metric $J parseFailures) skipped=$(metric $J skipped) ; dlq end $(dlq_end)"
  say "5분 집계 테이블 마지막 창: $(CH "SELECT max(window_start) FROM cdc_pipeline.trade_aggregations") (더 안 늘어야)"
  say "MarketAlertDetector 상태 복원: minutesEvaluated 등은 ALERT 로그/market_alerts 로 확인 - 최근 전이 $(CH "SELECT count() FROM cdc_pipeline.market_alerts WHERE detected_at >= now() - INTERVAL 10 MINUTE")건"
  ;;
inject)
  J=$(jid); B=$(dlq_end); F=$(metric $J parseFailures); say "before: dlq end $B, parseFailures $F"
  printf '%s\n' '{"payload":{"op":"c","after":{"trade_id":1,"market":"KRW-TEST",' | docker exec -i cdc-kafka-1 kafka-console-producer $BS --topic cdc.crypto_db.crypto_trades 2>&1 | sed 's/^/  producer: /' || true
  say "injected 1 broken message (truncated JSON) to cdc.crypto_db.crypto_trades"
  sleep 80
  say "after: dlq end $(dlq_end) (기대 $((B+1))), parseFailures $(metric $J parseFailures) (기대 +1; REST 메트릭은 수집 주기 지연 있음, docs/20)"
  say "DLQ 원문: $($K kafka-console-consumer $BS --topic cdc.dlq.crypto_trades --from-beginning --max-messages 1 --timeout-ms 10000 2>/dev/null | cut -c1-200)"
  say "적재 계속: 60s rows $(CH "SELECT count() FROM cdc_pipeline.crypto_trades WHERE flink_ts >= now() - INTERVAL 60 SECOND"), KRW-TEST 행 $(CH "SELECT count() FROM cdc_pipeline.crypto_trades WHERE market='KRW-TEST'") (0 이어야)"
  ;;
esac
