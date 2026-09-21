#!/bin/bash
# 2026-09-20: 관리자 비밀번호가 하드코딩돼 있었다(`-u admin:<값>`). 공개 저장소에 올라간 값이고,
# 심지어 옛 값이라 이 cron 은 매분 403 으로 실패하고 있었다 - 주석이 하나도 안 찍히고 있었다는 뜻이다.
# 비밀값을 코드에 두면 바뀔 때 같이 안 바뀌고, 조용히 죽는다. .env 에서 읽는다.
set -u
ENV_FILE="${ENV_FILE:-/home/calme/cdc-realtime-pipeline/.env}"
GRAFANA_PASSWORD=$(grep -m1 '^GRAFANA_PASSWORD=' "$ENV_FILE" | cut -d= -f2-)
[ -n "$GRAFANA_PASSWORD" ] || { echo "GRAFANA_PASSWORD 없음 ($ENV_FILE)"; exit 1; }
# 2026-09-20 (docs/34 #7): v1 규칙(anomaly_alerts)은 09-17 에 폐기됐는데 이 cron 이 매분 빈 결과를 돌고 있었다 → v2 등급 전이(market_alerts)로 교체.
docker exec cdc-clickhouse clickhouse-client --query "
SELECT toUnixTimestamp(detected_at)*1000 as ts, alert_type, market,
       concat(toString(prev_level), '->', toString(level), ' (', toString(round(value, 1)), '%, ', rule_version, ')') AS message
FROM cdc_pipeline.market_alerts
WHERE detected_at >= now() - INTERVAL 1 MINUTE
ORDER BY detected_at FORMAT JSONEachRow
" 2>/dev/null | while read -r line; do
  ts=$(echo "$line" | python3 -c "import sys,json; print(json.load(sys.stdin)['ts'])")
  text=$(echo "$line" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['alert_type']} | {d['market']}: {d['message']}\")")
  tags=$(echo "$line" | python3 -c "import sys,json; print(json.load(sys.stdin)['alert_type'])")
  
  curl -s -X POST http://localhost:3000/api/annotations \
    -H "Content-Type: application/json" \
    -u "admin:$GRAFANA_PASSWORD" \
    -d "{\"dashboardId\": 3, \"panelId\": 12, \"time\": $ts, \"text\": \"$text\", \"tags\": [\"$tags\"]}" \
    -o /dev/null -w "%{http_code} $text\n" | grep -v '^200 ' || true
done
