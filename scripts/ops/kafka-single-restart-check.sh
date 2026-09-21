#!/usr/bin/env bash
# 1브로커 재기동 실측 + compose 정리 마무리 (docs/24 §4 단계 5·6). kafka-1 을 새 정적 설정으로 재생성하고(= 전체 정지 수십 초),
# 그 사이 각 경로가 docs/23 §7-1 과 같은 방식으로 버티는지 기록한다. 브로커 2·3 컨테이너는 제거(볼륨은 롤백 대비 7일 보관).
# 실행: 사용자 터미널에서 - cd ~/cdc-realtime-pipeline && scripts/ops/kafka-single-restart-check.sh 2>&1 | tee ~/kafka-reassign/restart-$(date -u +%Y%m%dT%H%M).log
set -u; cd "$(dirname "$0")/../.."
CH(){ docker exec cdc-clickhouse clickhouse-client -q "$1" 2>&1 | tr '\t' '/'; }
MY(){ docker exec cdc-mysql mysql -uroot -p"$(grep '^MYSQL_ROOT_PASSWORD=' .env | cut -d= -f2-)" -N -e "$1" 2>/dev/null | tr '\t' '/'; }
CSTATE(){ docker exec cdc-kafka-connect curl -s -m 5 localhost:8083/connectors/mysql-cdc-connector/status 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state']+'/'+','.join(t['state'] for t in d['tasks']))" 2>/dev/null || echo unreachable; }
COLL(){ docker logs cdc-orderbook-collector --since 40s 2>&1 | grep STATS | tail -1 | grep -oE "deliv_err=[0-9]+ buf_err=[0-9]+ queue=[0-9]+"; }
say(){ echo "[$(date -u +%FT%TZ)] $*"; }
say "pre: connect=$(CSTATE) collector=$(COLL) ch max trade_id=$(CH 'SELECT max(trade_id) FROM cdc_pipeline.crypto_trades')"
say "브로커 2·3 컨테이너 제거 (볼륨 보존)"; docker rm cdc-kafka-2 cdc-kafka-3 >/dev/null 2>&1 && say "  removed" || say "  이미 없음"
STOP_T=$(date -u +%s); say "kafka-1 재생성 (새 정적 설정: minISR 1, default RF 1, offsets RF 1)"
docker compose up -d kafka-1 2>&1 | grep -E "Recreat|Start|Creat" | sed 's/^/  /'
for i in $(seq 1 60); do docker exec cdc-kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --list >/dev/null 2>&1 && break; sleep 2; done
UP_T=$(date -u +%s); say "브로커 응답까지 $((UP_T-STOP_T))초"
for i in $(seq 1 8); do say "  +$((i*15))s connect=$(CSTATE) collector=$(COLL) under_repl=$(docker exec cdc-kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --describe --under-replicated-partitions 2>/dev/null | grep -c Partition:)"; sleep 15; done
LO=$(date -u -d @$STOP_T +'%Y-%m-%d %H:%M:%S'); HI=$(date -u -d @$((UP_T+30)) +'%Y-%m-%d %H:%M:%S')
# 2026-09-18: 따라붙기 비교는 count() 가 아니라 uniqExact - 재전송 중복(239)으로 MySQL 과 안 맞아 상한까지 돌았다. (그리고 실행 중인 스크립트를 편집하지 말 것)
for i in $(seq 1 40); do m=$(MY "SELECT count(*) FROM crypto_db.crypto_trades WHERE created_at >= '$LO' AND created_at < '$HI'"); c=$(CH "SELECT uniqExact(trade_id) FROM cdc_pipeline.crypto_trades WHERE source_ts >= '$LO' AND source_ts < '$HI'"); say "  catch-up +$((i*15))s mysql=$m ch=$c connect=$(CSTATE)"; [ "$m" = "$c" ] && [ "$m" != "0" ] && break; sleep 15; done
say "post: collector=$(COLL) orderbook per-minute: $(CH "SELECT groupArray((m, c)) FROM (SELECT toString(toStartOfMinute(recv_ts)) AS m, count() AS c FROM cdc_pipeline.orderbook_raw WHERE recv_ts >= toDateTime('$LO') - INTERVAL 2 MINUTE AND recv_ts < now() GROUP BY m ORDER BY m)" | head -c 500)"
say "post: 정적 설정 확인: $(docker exec cdc-kafka-1 sh -c 'grep -E "^(min.insync.replicas|default.replication.factor|offsets.topic.replication.factor)=" /etc/kafka/kafka.properties' | tr '\n' ' ')"
say "post: flink=$(curl -s localhost:8081/jobs/overview | python3 -c "import sys,json; print(';'.join(j['name'][:12]+'='+j['state'] for j in json.load(sys.stdin)['jobs'] if j['state']!='FINISHED'))")"
say DONE
