#!/usr/bin/env bash
# 미니PC 생존 신호를 Oracle 로 push 한다. 호스트 cron 5분 주기.
#
# 감시자를 pull 로 두면 미니PC 가 통째로 죽을 때 질의가 실패해 판정에 도달하지 못한다.
# 실제로 그 구조에서 2,007회 연속 에러가 났는데 알림은 0건이었다. push 면 파일이 안 갱신된
# 것 자체가 신호다. 보내는 쪽을 Airflow 가 아니라 호스트 cron 에 둔 것도 같은 이유고,
# ClickHouse 질의가 실패해도 heartbeat 는 보낸다(멈추면 컨테이너 장애가 호스트 사망으로 보인다).
# 8123 이 127.0.0.1 인 것은 하드닝 결과이고, 그래서 포트를 열지 않고 방향을 뒤집었다.
set -uo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

REPO=/home/calme/cdc-realtime-pipeline
REMOTE=${HB_REMOTE:-ubuntu@10.88.0.1}
KEY=${HB_KEY:-/home/calme/.ssh/oci_key}
RDIR=${HB_RDIR:-/home/ubuntu/cdc-watchdog}
MODE=${1:-push}          # push | --stdout (전송 없이 payload 만 출력, 테스트용)

CU=$(grep '^CLICKHOUSE_READONLY_USER='     "$REPO/.env" | cut -d= -f2-)
CP=$(grep '^CLICKHOUSE_READONLY_PASSWORD=' "$REPO/.env" | cut -d= -f2-)

# 창은 cron 주기와 같은 5분. 여기 숫자는 생존 지표이지 정확한 집계가 아니다(RMT 를 FINAL 없이
# 세므로 머지 전 중복이 섞인다). 감시자는 0 이냐 아니냐만 보므로 그대로 두고, 대조·리포트에는 쓰지 않는다.
# 파티션 조건(ts, upbit_timestamp)은 프루닝용이다. 없으면 전체 스캔이라 5분마다 1.5초가 걸렸고,
# 거래가 3배로 뛴 구간에서 10초 제한을 넘겨 거짓 degraded 가 났다. 붙인 뒤 0.03초.
SQL="SELECT
  (SELECT count() FROM cdc_pipeline.crypto_trades
     WHERE source_ts >= now() - INTERVAL 5 MINUTE
       AND upbit_timestamp >= toUnixTimestamp64Milli(now64()) - 3600000) AS recent_trades_5m,
  (SELECT count() FROM cdc_pipeline.orderbook_raw
     WHERE recv_ts >= now() - INTERVAL 5 MINUTE
       AND ts >= now() - INTERVAL 1 HOUR)                                 AS recent_orderbook_5m,
  (SELECT count() FROM cdc_pipeline.crypto_trades) AS total_trades
FORMAT JSON"

# HTTP 상태를 같이 잡는다: 과거에 "옛 비밀번호로 403 나던 cron"(docs/42)을 겪었는데
# 알림에 파싱 오류만 뜨면 원인을 못 찾는다. 403 이라고 말해주는 알림이어야 사람이 바로 움직인다
CH_OUT=$(curl -s -m 10 -u "$CU:$CP" -w '\n%{http_code}' 'http://127.0.0.1:8123/' --data-binary "$SQL" 2>/dev/null)
CH_RC=$?
CH_CODE=$(printf '%s' "$CH_OUT" | tail -n1)
CH_BODY=$(printf '%s' "$CH_OUT" | sed '$d')

PAYLOAD=$(CH_BODY="$CH_BODY" CH_RC="$CH_RC" CH_CODE="$CH_CODE" python3 - <<'PY'
import json, os, socket, time
body, rc, code = os.environ["CH_BODY"], os.environ["CH_RC"], os.environ.get("CH_CODE", "")
now = time.time()
out = {
    "schema": 1,
    "host": socket.gethostname(),
    "sent_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
    "sent_at_epoch": int(now),
    "clickhouse": "fail",
    "verdict": "degraded",          # ClickHouse 를 못 읽으면 살아있다는 사실만 보낸다
    "detail": "",
}
try:
    row = json.loads(body)["data"][0]
    out.update({
        "clickhouse": "ok",
        "recent_trades_5m":    int(row["recent_trades_5m"]),
        "recent_orderbook_5m": int(row["recent_orderbook_5m"]),
        "total_trades":        int(row["total_trades"]),
    })
    # stale = 호스트는 살아있는데 적재가 멎음. down(=heartbeat 부재)과 다른 상태다
    out["verdict"] = "stale" if out["recent_trades_5m"] == 0 else "ok"
except Exception as e:
    hint = {"403": "인증 실패 (자격증명이 바뀌었는지 확인)", "000": "접속 불가 (컨테이너가 떠 있는지 확인)",
            "500": "ClickHouse 내부 오류", "": "응답 없음"}.get(code, f"HTTP {code}")
    out["detail"] = f"HTTP {code or '없음'} - {hint} (curl rc={rc}): {type(e).__name__}"
print(json.dumps(out, ensure_ascii=False))
PY
)

[ "$MODE" = "--stdout" ] && { echo "$PAYLOAD"; exit 0; }

# 원자적 쓰기: 감시자가 쓰는 도중의 반쪽 파일을 읽으면 파싱 실패 = 오탐이 된다
if printf '%s\n' "$PAYLOAD" | ssh -i "$KEY" -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
     "$REMOTE" "mkdir -p $RDIR && cat > $RDIR/heartbeat.json.tmp && mv $RDIR/heartbeat.json.tmp $RDIR/heartbeat.json" 2>/dev/null; then
  echo "$(date -u +%FT%TZ) sent $(echo "$PAYLOAD" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["verdict"], "ch="+d["clickhouse"], "trades5m="+str(d.get("recent_trades_5m","-")))')"
else
  # 여기 실패는 "Oracle 이 안 보이거나 터널이 끊김". 미니PC 가 할 수 있는 건 남기는 것뿐이다
  echo "$(date -u +%FT%TZ) PUSH FAILED (oracle unreachable or tunnel down)"
  exit 1
fi
