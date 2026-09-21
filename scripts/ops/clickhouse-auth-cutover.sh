#!/usr/bin/env bash
# ClickHouse 접근 통제 컷오버 런북 (2026-09-17, docs/21).
# 한 번의 정지 창에서: 3 Flink 잡 savepoint 정지 → 변경된 서비스 재생성(clickhouse 비밀번호·flink env·grafana·airflow·producer)
# → ClickHouse 준비 대기 → 3 잡을 savepoint 에서 재제출 → 검증.
# 왜 Flink 를 먼저 세우나: ClickHouse 재시작 중 JDBC 싱크가 실패→재시도하면 at-least-once 중복이 생긴다(docs/07). savepoint 정지가 이를 막는다.
# 왜 한 창에 묶나: ClickHouse 재시작과 Flink 컨테이너 재생성(env 변경) 각각이 정지 창을 요구한다. 두 번 세우면 손실 창도 두 번.
set -euo pipefail
cd "$(dirname "$0")/../.."
source .env
FL="docker exec cdc-flink-jobmanager"
CH() { docker exec cdc-clickhouse clickhouse-client "$@"; }
say() { echo "[$(date -u +%H:%M:%S)] $*"; }

say "0. 사전 상태"
PRE_MAX=$(CH -q "SELECT max(trade_id) FROM cdc_pipeline.crypto_trades"); say "   pre-stop max trade_id=$PRE_MAX"
curl -s localhost:8081/jobs/overview | python3 -c "import sys,json; [print('   ', j['jid'], j['name'], j['state']) for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING']"

say "1. 3 잡 savepoint 정지"
declare -A SP
for pair in "CDC Realtime Pipeline:cdc" "Orderbook Pipeline:ob" "Circuit Connect Stream Processing:circuit"; do
  name="${pair%%:*}"; key="${pair##*:}"
  jid=$(curl -s localhost:8081/jobs/overview | python3 -c "import sys,json; print([j['jid'] for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING' and j['name']=='$name'][0])")
  sp=$($FL flink stop --savepointPath /opt/flink/savepoints "$jid" 2>&1 | grep -o "savepoint-[a-z0-9-]*" | tail -1)
  SP[$key]="$sp"; say "   $name → $sp"
done

say "2. 변경된 서비스 재생성 (clickhouse 비밀번호·클라이언트 설정, flink env, grafana, airflow, producer)"
docker compose up -d clickhouse flink-jobmanager flink-taskmanager grafana airflow-webserver airflow-scheduler upbit-producer 2>&1 | grep -E "Recreat|Start|Creat" | sed 's/^/   /'

say "3. ClickHouse 준비 대기 (인증 동작 확인)"
for i in $(seq 1 40); do
  if curl -s "http://localhost:8123/?user=pipeline&password=${CLICKHOUSE_PIPELINE_PASSWORD}&query=SELECT%201" 2>/dev/null | grep -q '^1$'; then break; fi; sleep 3
done
code=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:8123/?query=SELECT%201"); say "   무인증 default 접속 → HTTP $code (516/401 이어야)"
CH -q "SELECT 'docker-exec client (config.xml 인증) OK'" | sed 's/^/   /'

say "4. Flink JobManager 준비 대기"
for i in $(seq 1 40); do curl -s localhost:8081/overview >/dev/null 2>&1 && break; sleep 3; done
$FL ls /opt/flink/savepoints/${SP[cdc]} >/dev/null && say "   savepoints 볼륨 유지 확인"

say "5. 잡 재제출 (savepoint 복원)"
$FL flink run -d -s "file:/opt/flink/savepoints/${SP[cdc]}" /opt/flink/usrlib/flink-cdc-job-1.0.0.jar 2>&1 | grep -o "JobID [a-f0-9]*" | sed 's/^/   CDC: /'
$FL flink run -d -s "file:/opt/flink/savepoints/${SP[ob]}" -c com.cdc.pipeline.orderbook.OrderbookJob /opt/flink/usrlib/flink-cdc-job-1.0.0.jar 2>&1 | grep -o "JobID [a-f0-9]*" | sed 's/^/   Orderbook: /'
# circuit 은 타 프로젝트 잡: 자기 DB(circuit_connect) URL 을 제출 시 env 로 준다 (JM 기본 env 는 cdc_pipeline 을 가리키므로)
docker exec -e "CLICKHOUSE_URL=jdbc:clickhouse://clickhouse:8123/circuit_connect?user=${CLICKHOUSE_PIPELINE_USER}&password=${CLICKHOUSE_PIPELINE_PASSWORD}" cdc-flink-jobmanager \
  flink run -d -s "file:/opt/flink/savepoints/${SP[circuit]}" /opt/flink/usrlib/circuit-connect-flink-1.0.0.jar 2>&1 | grep -o "JobID [a-f0-9]*" | sed 's/^/   Circuit: /'

say "6. 검증"
sleep 25
curl -s localhost:8081/jobs/overview | python3 -c "import sys,json; [print('   ', j['name'], j['state']) for j in json.load(sys.stdin)['jobs'] if j['state']!='FINISHED']"
CH -q "SELECT min(trade_id) AS first_after, count() AS rows_after FROM cdc_pipeline.crypto_trades WHERE trade_id > $PRE_MAX FORMAT TSV" | sed "s/^/   연속성(pre max $PRE_MAX): first_after,rows_after = /"
CH -q "SELECT user, count() FROM system.query_log WHERE event_time > now() - INTERVAL 2 MINUTE AND type='QueryFinish' GROUP BY user ORDER BY 2 DESC FORMAT TSV" | sed 's/^/   최근 2분 사용자별 쿼리: /'
docker logs cdc-upbit-producer 2>&1 | grep -E "gap-fill 완료|ERROR" | tail -2 | sed 's/^/   producer: /'
say "done"
