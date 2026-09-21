#!/usr/bin/env python3
"""6개월 PRICE_FLUCTUATIONS 에피소드의 지정 시각에 |24h 변동률|을 업비트 1분봉으로 재계산해 임계값을 독립 검증.
(market, UTC day, level)당 첫 에피소드 1건만 사용 → 자기상관 제거. 콜당 1분봉 최대 200개, 에피소드당 2콜(지정 시각, 24h 전)."""
import json, subprocess, time, urllib.request, urllib.parse, sys
from collections import defaultdict
def get(u):
    for i in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers={'Accept': 'application/json'}), timeout=15) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429: time.sleep(1.5 * (i + 1)); continue
            return None  # 404/400: 상장폐지 등 → 건너뜀
        except Exception: time.sleep(1)
    return None
OUT = '/home/calme/pipeline-observation/price_threshold_verify.json'
def report():
    d = json.load(open(OUT)); by = defaultdict(list)
    for o in d: by[o['level']].append(abs(o['chg']))
    for lvl in sorted(by):
        v = sorted(by[lvl]); n = len(v)
        print(f"{lvl} n={n} min={v[0]*100:.1f}% p10={v[int(n*.1)]*100:.1f}% p50={v[n//2]*100:.1f}% max={v[-1]*100:.1f}% below45={sum(1 for x in v if x<0.45)} 45-55={sum(1 for x in v if 0.45<=x<0.55)} 95-105={sum(1 for x in v if 0.95<=x<1.05)} ge195={sum(1 for x in v if x>=1.95)}")
    bm = defaultdict(list)
    for o in d:
        if o['level'] == 'LEVEL_1': bm[o['t'][:7]].append(abs(o['chg']))
    for m in sorted(bm):
        v = sorted(bm[m]); n = len(v); print(f"LEVEL_1 {m} n={n} p10={v[int(n*.1)]*100:.1f}% p50={v[n//2]*100:.1f}% in45-55={sum(1 for x in v if .45<=x<.55)} below45={sum(1 for x in v if x<.45)}")
    for tag, f in [('pre_0518', lambda t: t < '2026-05-18'), ('post_0518', lambda t: t >= '2026-05-18')]:
        v = sorted(abs(o['chg']) for o in d if o['level'] == 'LEVEL_1' and f(o['t'])); n = len(v)
        print(f"LEVEL_1 {tag} n={n} p10={v[int(n*.1)]*100:.1f}% p50={v[n//2]*100:.1f}% below45={sum(1 for x in v if x<.45)}")
    for o in d:
        if (o['level'] == 'LEVEL_1' and abs(o['chg']) < .45) or (o['level'] == 'LEVEL_2' and abs(o['chg']) < .9): print('outlier', o['level'], o['market'], o['t'], f"{o['chg']*100:+.1f}%")
if '--report' in sys.argv:
    report(); sys.exit()
rows = subprocess.run(['docker', 'exec', 'cdc-clickhouse', 'clickhouse-client', '-d', 'cdc_pipeline', '-q',
    "SELECT market, warning_level, min(trigger_time_utc) FROM upbit_market_event_records FINAL WHERE event_type='PRICE_FLUCTUATIONS' GROUP BY market, warning_level, toDate(trigger_time_utc) ORDER BY 3 FORMAT TSV"],
    capture_output=True, text=True, check=True).stdout.strip().split('\n')
print('episodes to verify', len(rows), file=sys.stderr)
def close_at(market, ts):  # ts 'YYYY-MM-DD HH:MM:SS' UTC → 그 시각 이전 마지막 1분봉 종가
    d = get(f'https://api.upbit.com/v1/candles/minutes/1?market={market}&to={ts.replace(' ', 'T')}Z&count=1')
    time.sleep(0.12)
    return d[0]['trade_price'] if d else None
out = []
from datetime import datetime, timedelta
for r in rows:
    market, lvl, t = r.split('\t')
    t0 = datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
    p_now = close_at(market, (t0 + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S'))
    p_24h = close_at(market, (t0 - timedelta(hours=24) + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S'))
    if p_now and p_24h:
        out.append({'market': market, 'level': lvl, 't': t, 'chg': p_now / p_24h - 1})
json.dump(out, open(OUT, 'w'))
report()
