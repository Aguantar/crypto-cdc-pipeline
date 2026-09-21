#!/bin/bash
# 관찰 기간 일일 원장 대조 (읽기 전용, 백필 없음). 전날 24시간(UTC) 287마켓 × 시간봉 vs ClickHouse.
# 등록: 35 6 * * * (06:20 일일 요약 뒤, 다른 REST 작업과 겹치지 않게 직렬)
# 출력: ~/pipeline-observation/reconcile_<날짜>_<시>Z.csv + reconcile_daily.log 요약 한 줄 + worklog 자동 한 줄
set -u
cd /home/calme/pipeline-observation || exit 1
OUT=$(python3 /home/calme/cdc-realtime-pipeline/scripts/observe/reconcile_trades.py --hours 24 2>/dev/null | grep '^{' | tail -1)
SUM=$(python3 - "$OUT" <<'PY'
import json,sys,csv
d=json.loads(sys.argv[1]); rows=[r for r in csv.DictReader(open(d['out'])) if r['ratio_pct'] and float(r['candle_vol'])>0]
byh={}
for r in rows: a=byh.setdefault(r['hour_utc'],[0,0]); a[0]+=float(r['ch_vol']); a[1]+=float(r['candle_vol'])
worst=min((round(100*a/b,2),h) for h,(a,b) in byh.items()) if byh else (None,None)
low=[r for r in rows if float(r['ratio_pct'])<99]
print(f"weighted {d['volume_weighted_ratio_pct']}% | cells {len(rows)} | <99% {len(low)} | <95% {d['cells_below_95']} | worst hour {worst[1]} {worst[0]}% | fetch_fail {d['candle_fetch_fail']} | {d['out']}")
PY
)
echo "$(date -u +%FT%TZ) $SUM" >> /home/calme/pipeline-observation/reconcile_daily.log
echo "- $(date -u +'%m-%d %H:%M') UTC 일일 원장 대조(자동, 백필 없음): $SUM" >> /home/calme/cdc-realtime-pipeline/docs/worklog.md
