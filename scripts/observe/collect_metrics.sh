#!/bin/bash
# ============================================================
#  7일 관찰용 파이프라인 지표 스냅샷 (5분 간격, crontab)
#  - 읽기 전용: docker logs/stats, Flink REST, Kafka CLI, ClickHouse readonly_user, MySQL COUNT
#  - 출력: $OUT_DIR/metrics_5m.csv (헤더 1회), 실패 항목은 빈 값
#  - 등록: */5 * * * * /home/calme/cdc-realtime-pipeline/scripts/observe/collect_metrics.sh
#  근거: docs/worklog.md 결정 09-09 04:28 (7일 무변경 관찰, cron 수집)
# ============================================================
set -u
OUT_DIR="${OUT_DIR:-/home/calme/pipeline-observation}"
CSV="$OUT_DIR/metrics_5m.csv"
ENV_FILE=/home/calme/cdc-realtime-pipeline/.env
mkdir -p "$OUT_DIR"
TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
# 2026-09-20 (docs/32): 컨테이너 CPU 는 여기서 먼저 찍는다 - 아래 Kafka CLI(docker exec JVM) 가 돌고 난 뒤 재면 cdc-kafka-1 CPU 가 133% 로 부풀어 보인다(docs/23 의 147% 착시와 같은 원인, 02:55 실측)
STATS_EARLY=$(docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' 2>/dev/null)

# --- ClickHouse (readonly_user, HTTP) ---
CH_USER=$(grep '^CLICKHOUSE_READONLY_USER=' $ENV_FILE | cut -d= -f2-)
CH_PASS=$(grep '^CLICKHOUSE_READONLY_PASSWORD=' $ENV_FILE | cut -d= -f2-)
chq() { curl -s --max-time 60 "http://localhost:8123/?user=$CH_USER&password=$CH_PASS&max_memory_usage=500000000&max_threads=2" --data-binary "$1" 2>/dev/null | tr '\t' ',' | tr -d '\n'; }

# 체결: 최근 5분 행수·마켓수·ingest lag(source_ts−upbit_ts) p50/p95/max·cdc_latency·flink lag
TRADE=$(chq "SELECT count(), uniqExact(market), round(quantile(0.5)(toUnixTimestamp64Milli(source_ts)-upbit_timestamp)/1000,2), round(quantile(0.95)(toUnixTimestamp64Milli(source_ts)-upbit_timestamp)/1000,2), round(max(toUnixTimestamp64Milli(source_ts)-upbit_timestamp)/1000,2), round(avg(cdc_latency_ms),1), round(quantile(0.95)(toUnixTimestamp64Milli(flink_ts)-toUnixTimestamp64Milli(source_ts))/1000,2), countIf(best_ask_price IS NULL) FROM cdc_pipeline.crypto_trades WHERE source_ts >= now() - INTERVAL 5 MINUTE FORMAT TSV")
# 호가: 최근 5분 행수·마켓수·recv lag p50/p95·e2e p50/p95/max
OB=$(chq "SELECT count(), uniqExact(market), round(quantile(0.5)(toUnixTimestamp64Milli(recv_ts)-toUnixTimestamp64Milli(ts))), round(quantile(0.95)(toUnixTimestamp64Milli(recv_ts)-toUnixTimestamp64Milli(ts))), round(quantile(0.5)(toUnixTimestamp64Milli(flink_ts)-toUnixTimestamp64Milli(ts))), round(quantile(0.95)(toUnixTimestamp64Milli(flink_ts)-toUnixTimestamp64Milli(ts))), round(max(toUnixTimestamp64Milli(flink_ts)-toUnixTimestamp64Milli(ts))) FROM cdc_pipeline.orderbook_raw WHERE ts >= now() - INTERVAL 5 MINUTE FORMAT TSV")
OB1M=$(chq "SELECT count(), uniqExact(market) FROM cdc_pipeline.orderbook_1m WHERE window_start >= now() - INTERVAL 5 MINUTE FORMAT TSV")
# 2026-09-20 (docs/34 #7): v1 규칙 폐기(09-17) 후 이 네 열은 계속 0 이었다 → v2 등급 전이로 교체.
# 열 의미 변경: alerts5m = 전이 수, alerts_large = 승급(level>prev), alerts_spike = 강등(level<prev), alerts_surge = 미사용(0).
# 09-20 이전 CSV 행의 같은 열은 v1 규칙 건수다(daily_digest 의 H10 임계는 "알림이 너무 많다" 라는 뜻이 같아 그대로 쓴다).
ALERTS=$(chq "SELECT count(), countIf(level > prev_level), countIf(level < prev_level), 0 FROM cdc_pipeline.market_alerts WHERE detected_at >= now() - INTERVAL 5 MINUTE FORMAT TSV")
# system.* 은 readonly_user 권한 밖 → 컨테이너 내부 clickhouse-client(조회 전용)로
CHMEM=$(docker exec cdc-clickhouse clickhouse-client --max_threads=1 -q "SELECT value FROM system.metrics WHERE metric='MemoryTracking'" 2>/dev/null | tr -d '\n')
CHPARTS=$(docker exec cdc-clickhouse clickhouse-client --max_threads=1 -q "SELECT countIf(table='crypto_trades'), countIf(table='orderbook_raw'), sum(bytes_on_disk) FROM system.parts WHERE active AND database='cdc_pipeline'" 2>/dev/null | tr '\t' ',' | tr -d '\n')
# 어떤 값이든 콤마/개행이 섞이면 CSV가 깨지므로 방어
CHMEM=${CHMEM//,/}; TRADE=${TRADE//$'\n'/}; OB=${OB//$'\n'/}

# --- producer / collector 마지막 STATS ---
P=$(docker logs cdc-upbit-producer --tail 40 2>&1 | grep '\[STATS\]' | tail -1)
P_RECV=$(echo "$P" | sed -n 's/.*received=\([0-9]*\).*/\1/p'); P_INS=$(echo "$P" | sed -n 's/.*inserted=\([0-9]*\).*/\1/p'); P_DUP=$(echo "$P" | sed -n 's/.*duplicates=\([0-9]*\).*/\1/p'); P_ERR=$(echo "$P" | sed -n 's/.*errors=\([0-9]*\).*/\1/p'); P_BUF=$(echo "$P" | sed -n 's/.*buffer=\([0-9]*\).*/\1/p')
P_WARN=$(docker logs cdc-upbit-producer --since 5m 2>&1 | grep -c -E 'WARNING|ERROR')
C=$(docker logs cdc-orderbook-collector --tail 40 2>&1 | grep '\[STATS\]' | tail -1)
C_RECV=$(echo "$C" | sed -n 's/.*recv=\([0-9]*\).*/\1/p'); C_DERR=$(echo "$C" | sed -n 's/.*deliv_err=\([0-9]*\).*/\1/p'); C_Q=$(echo "$C" | sed -n 's/.*queue=\([0-9]*\).*/\1/p'); C_RATE=$(echo "$C" | sed -n 's/.*rate=\([0-9.]*\).*/\1/p'); C_P95=$(echo "$C" | sed -n 's/.*lag_p95=\([0-9]*\).*/\1/p'); C_RECON=$(echo "$C" | sed -n 's/.*reconnects=\([0-9]*\).*/\1/p')

# --- Flink ---
FL=$(curl -s --max-time 10 localhost:8081/jobs/overview | python3 -c '
import json,sys,urllib.request
d=json.load(sys.stdin); out=[]
for name in ("CDC Realtime Pipeline","Orderbook Pipeline","Circuit Connect Stream Processing"):
    js=[j for j in d["jobs"] if j["name"]==name and j["state"]=="RUNNING"]
    if not js: out+=["0","","","",""]; continue
    j=js[0]
    try:
        c=json.load(urllib.request.urlopen("http://localhost:8081/jobs/%s/checkpoints"%j["jid"],timeout=10))
        h=c["history"][0] if c["history"] else {}
        out+=["1",str(c["counts"]["completed"]),str(c["counts"]["failed"]),str(h.get("state_size","")),str(h.get("end_to_end_duration",""))]
    except Exception: out+=["1","","","",""]
print(",".join(out))' 2>/dev/null)
TMID=$(curl -s --max-time 10 localhost:8081/taskmanagers | python3 -c 'import json,sys;print(json.load(sys.stdin)["taskmanagers"][0]["id"])' 2>/dev/null)
TMM=$(curl -s --max-time 10 "localhost:8081/taskmanagers/$TMID/metrics?get=Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Metaspace.Used" | python3 -c 'import json,sys;d={m["id"]:m["value"] for m in json.load(sys.stdin)};print(d.get("Status.JVM.Memory.Heap.Used",""),d.get("Status.JVM.Memory.Metaspace.Used",""),sep=",")' 2>/dev/null)

# --- Kafka ---
K_TRADE=$(docker exec cdc-kafka-1 kafka-get-offsets --bootstrap-server kafka-1:29092 --topic cdc.crypto_db.crypto_trades 2>/dev/null | awk -F: '{s+=$3} END {print s}')
K_OB=$(docker exec cdc-kafka-1 kafka-get-offsets --bootstrap-server kafka-1:29092 --topic upbit.orderbook.v1 2>/dev/null | awk -F: '{s+=$3} END {print s}')
K_LAG_CDC=$(docker exec cdc-kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --describe --group flink-cdc-consumer 2>/dev/null | grep crypto_trades | awk '{s+=$6} END {print s}')
K_LAG_OB=$(docker exec cdc-kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --describe --group flink-orderbook-consumer 2>/dev/null | grep orderbook | awk '{s+=$6} END {print s}')
K_DISK=$(docker exec cdc-kafka-1 sh -c 'du -sb /var/lib/kafka/data 2>/dev/null | cut -f1')

# --- MySQL ---
MY=$(docker exec cdc-mysql sh -c 'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -N -e "SELECT COUNT(*), MAX(trade_id) FROM crypto_db.crypto_trades" 2>/dev/null' | tr '\t' ',')

# --- 컨테이너/호스트 ---
# 2026-09-21: 전에는 docker stats 에 컨테이너 이름을 나열하고 출력 순서대로 잘라 썼다.
# 09-17 브로커 축소로 cdc-kafka-2·3 이 사라지자 출력 줄이 줄어 CSV 가 87칸 -> 70칸으로 어긋났고,
# 09-18 11:00 부터 load1/load5/load15·호스트 메모리가 통째로 비어 있었다(09-21 발견).
# 하필 그 지표가 Binance 심볼 확대 판단에 쓰려던 것이었다. 순서가 아니라 이름으로 찾고,
# 없으면 빈 칸을 채워 칸 수를 고정한다 - 컨테이너가 사라져도 열이 밀리지 않는다.
DS=$(for n in cdc-clickhouse cdc-flink-taskmanager cdc-kafka-1 cdc-kafka-2 cdc-kafka-3 cdc-mysql cdc-upbit-producer cdc-orderbook-collector cdc-kafka-connect; do
  echo "$STATS_EARLY" | awk -v n="$n" '$1==n {gsub("%","",$2); print $3","$2; f=1} END{if(!f) print ","}'
done | paste -sd, -)
LOAD=$(cut -d' ' -f1-3 /proc/loadavg | tr ' ' ',')
MEM=$(free -m | awk 'NR==2{printf "%s,%s,", $3, $7} NR==3{printf "%s", $3}')
DF=$(df -B1 / | awk 'NR==2{print $3}')

HEADER="ts,tr_rows5m,tr_markets,tr_lag_p50_s,tr_lag_p95_s,tr_lag_max_s,tr_cdc_lat_ms,tr_flink_lag_p95_s,tr_best_null,ob_rows5m,ob_markets,ob_recv_p50_ms,ob_recv_p95_ms,ob_e2e_p50_ms,ob_e2e_p95_ms,ob_e2e_max_ms,ob1m_rows5m,ob1m_markets,alerts5m,alerts_large,alerts_spike,alerts_surge,ch_mem_bytes,ch_parts_trades,ch_parts_ob,ch_bytes_cdc_pipeline,p_received,p_inserted,p_dups,p_errors,p_buffer,p_warn5m,c_recv,c_deliv_err,c_queue,c_rate,c_lag_p95_ms,c_reconnects,fl_cdc_run,fl_cdc_cp_ok,fl_cdc_cp_fail,fl_cdc_state,fl_cdc_e2e,fl_ob_run,fl_ob_cp_ok,fl_ob_cp_fail,fl_ob_state,fl_ob_e2e,fl_cc_run,fl_cc_cp_ok,fl_cc_cp_fail,fl_cc_state,fl_cc_e2e,tm_heap_used,tm_metaspace_used,k_trade_endoffset,k_ob_endoffset,k_lag_cdc,k_lag_ob,k_disk_b1_bytes,my_rows,my_max_id,ch_mem,ch_cpu,tm_mem,tm_cpu,k1_mem,k1_cpu,k2_mem,k2_cpu,k3_mem,k3_cpu,my_mem,my_cpu,p_mem,p_cpu,c_mem,c_cpu,conn_mem,conn_cpu,load1,load5,load15,host_used_mb,host_avail_mb,swap_used_mb,df_used_bytes"
[ -f "$CSV" ] || echo "$HEADER" > "$CSV"
echo "$TS,$TRADE,$OB,$OB1M,$ALERTS,$CHMEM,$CHPARTS,$P_RECV,$P_INS,$P_DUP,$P_ERR,$P_BUF,$P_WARN,$C_RECV,$C_DERR,$C_Q,$C_RATE,$C_P95,$C_RECON,$FL,$TMM,$K_TRADE,$K_OB,$K_LAG_CDC,$K_LAG_OB,$K_DISK,$MY,$DS,$LOAD,$MEM,$DF" >> "$CSV"

# --- 2026-09-20 (docs/32): 호스트·컨테이너 자원 + 유입률을 ClickHouse ops_metrics_5m 에 한 행 (Prometheus 대체). 실패해도 CSV 수집에는 영향 없음 ---
PU=$(grep '^CLICKHOUSE_PIPELINE_USER=' $ENV_FILE | cut -d= -f2-); PP=$(grep '^CLICKHOUSE_PIPELINE_PASSWORD=' $ENV_FILE | cut -d= -f2-)
L1=$(cut -d' ' -f1 /proc/loadavg); L5=$(cut -d' ' -f2 /proc/loadavg)
read -r MU MA SU <<<"$(free -m | awk 'NR==2{u=$3; a=$7} NR==3{s=$3} END{print u, a, s}')"
read -r DU DF <<<"$(df -m / | awk 'NR==2{print $3, $4}')"
STATS="$STATS_EARLY"
cpu_of(){ echo "$STATS" | awk -v n="$1" '$1==n {gsub("%","",$2); print $2+0; f=1} END{if(!f)print 0}'; }
mem_of(){ echo "$STATS" | awk -v n="$1" '$1==n {v=$3; if (v ~ /GiB/) {gsub("GiB","",v); print int(v*1024)} else {gsub("MiB","",v); print int(v)}; f=1} END{if(!f)print 0}'; }
CPU_COLL=$(echo "$(cpu_of cdc-upbit-producer) + $(cpu_of cdc-orderbook-collector) + $(cpu_of cdc-binance-collector) + $(cpu_of cdc-binance-depth-collector)" | bc 2>/dev/null || echo 0)
chw() { curl -s --max-time 30 "http://localhost:8123/?user=$PU&password=$PP&max_memory_usage=500000000&max_threads=2" --data-binary "$1" 2>/dev/null; }
UP5=$(chq "SELECT count() FROM cdc_pipeline.crypto_trades WHERE flink_ts >= now() - INTERVAL 5 MINUTE FORMAT TSV")
# 2026-09-21: `< 600000` 을 붙였다. 없으면 재소비·백필로 들어온 19.5시간짜리 행이 p95 에 섞여
# 322 표본 중 2건이 70,338초로 찍혔고, load 구간별 SLO 를 보려는 순간 그 2건이 평균을 416초로 만들었다.
# 운영 지연을 보는 지표에 과거 적재를 섞지 않는다(같은 함정을 docs/18 에서 이미 한 번 겪었다).
UPP95=$(chq "SELECT round(quantile(0.95)(toUnixTimestamp64Milli(flink_ts)-upbit_timestamp)/1000,2) FROM cdc_pipeline.crypto_trades WHERE flink_ts >= now() - INTERVAL 5 MINUTE AND toUnixTimestamp64Milli(flink_ts)-upbit_timestamp < 600000 FORMAT TSV")
BN5=$(chq "SELECT count() FROM cdc_pipeline.binance_trades WHERE flink_ts >= now() - INTERVAL 5 MINUTE FORMAT TSV")
BNP95=$(chq "SELECT round(quantile(0.95)(toUnixTimestamp64Milli(flink_ts)-trade_ms)/1000,2) FROM cdc_pipeline.binance_trades WHERE flink_ts >= now() - INTERVAL 5 MINUTE AND toUnixTimestamp64Milli(flink_ts)-trade_ms < 600000 FORMAT TSV")
OB5=$(chq "SELECT count() FROM cdc_pipeline.orderbook_raw WHERE flink_ts >= now() - INTERVAL 5 MINUTE FORMAT TSV")
FJ=$(curl -s --max-time 10 localhost:8081/jobs/overview | python3 -c "import sys,json; print(sum(1 for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING'))" 2>/dev/null || echo 0)
FB=$(for j in $(curl -s --max-time 10 localhost:8081/jobs/overview | python3 -c "import sys,json; print(' '.join(j['jid'] for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING'))" 2>/dev/null); do v=$(curl -s --max-time 10 localhost:8081/jobs/$j | python3 -c "import sys,json; print(next(x['id'] for x in json.load(sys.stdin)['vertices'] if x['name'].startswith('Source')))" 2>/dev/null); curl -s --max-time 10 -G "localhost:8081/jobs/$j/vertices/$v/subtasks/metrics" --data-urlencode "get=busyTimeMsPerSecond" --data-urlencode "agg=max" | python3 -c "import sys,json; m=json.load(sys.stdin); print(m[0]['max'] if m else 0)" 2>/dev/null; done | sort -n | tail -1)
chw "INSERT INTO cdc_pipeline.ops_metrics_5m FORMAT CSV
$(date -u +%Y-%m-%d\ %H:%M:%S),${L1:-0},${L5:-0},${MU:-0},${MA:-0},${SU:-0},${DU:-0},${DF:-0},$(cpu_of cdc-kafka-1),$(cpu_of cdc-flink-taskmanager),$(cpu_of cdc-clickhouse),$(cpu_of cdc-mysql),${CPU_COLL:-0},$(mem_of cdc-kafka-1),$(mem_of cdc-flink-taskmanager),$(mem_of cdc-clickhouse),$(mem_of cdc-mysql),$(mem_of cdc-airflow-scheduler),${UP5:-0},${BN5:-0},${OB5:-0},${UPP95:-0},${BNP95:-0},${FJ:-0},${FB:-0}"

# --- 2026-09-25 (docs/48 §7): 수집기 STATS 의 lag·queue 를 collector_stats_5m 에. 급등 때 수집기가 뒤처지는 것은
# deliv_err 로 안 보이고 lag_p95 로만 보인다(09-23 14:13 lag_p95 12,830ms 뒤 연결 끊김). 없는 필드는 0.
TS_NOW=$(date -u +%Y-%m-%d\ %H:%M:%S); ROWS=""
for c in cdc-binance-collector cdc-binance-depth-collector cdc-orderbook-collector; do
  S=$(docker logs "$c" --tail 60 2>&1 | grep '\[STATS\]' | tail -1)
  f(){ echo "$S" | sed -n "s/.*[[:space:]]$1=\([0-9]*\).*/\1/p" | head -1; }
  [ -n "$S" ] && ROWS="$ROWS$TS_NOW,${c#cdc-},$(f recv | sed 's/^$/0/'),$(f produced | sed 's/^$/0/'),$(f deliv_err | sed 's/^$/0/'),$(f buf_err | sed 's/^$/0/'),$(f queue | sed 's/^$/0/'),$(f conns | sed 's/^$/0/'),$(f reconnects | sed 's/^$/0/'),$(f lag_p50 | sed 's/^$/0/'),$(f lag_p95 | sed 's/^$/0/')
"
done
[ -n "$ROWS" ] && chw "INSERT INTO cdc_pipeline.collector_stats_5m FORMAT CSV
$ROWS"
