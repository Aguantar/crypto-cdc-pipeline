#!/usr/bin/env python3
"""업비트 시장경보 플래그 폴러 (라벨 수집). cron 1분.
GET /v1/market/all?is_details=true 1회 → 이전 스냅샷과 비교 → 바뀐 (market, flag)만 ClickHouse에 적재.
스냅샷 파일이 없으면(최초) 또는 --snapshot 이면 현재 지정 상태 전체를 kind='snapshot'으로 적재.
쓰기는 docker exec clickhouse-client (collect_metrics.sh 와 동일 경로). 실패 시 스냅샷 파일을 갱신하지 않아 다음 분에 재시도.
"""
import json, os, subprocess, sys, time, urllib.request
from datetime import datetime, timezone

URL = 'https://api.upbit.com/v1/market/all?is_details=true'
STATE = os.path.expanduser('~/pipeline-observation/market_events_state.json')
FLAGS = ['PRICE_FLUCTUATIONS', 'TRADING_VOLUME_SOARING', 'DEPOSIT_AMOUNT_SOARING',
         'GLOBAL_PRICE_DIFFERENCES', 'CONCENTRATION_OF_SMALL_ACCOUNTS']

def fetch():
    req = urllib.request.Request(URL, headers={'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=10) as r:
        data = json.load(r)
    cur = {}
    for m in data:
        if not m['market'].startswith('KRW-') or not m.get('market_event'):
            continue
        ev = m['market_event']
        cur[m['market']] = {'WARNING': bool(ev.get('warning'))} | {f: bool(ev.get('caution', {}).get(f)) for f in FLAGS}
    return cur

def insert(rows):
    if not rows:
        return
    payload = '\n'.join(json.dumps(r, ensure_ascii=False) for r in rows)
    subprocess.run(['docker', 'exec', '-i', 'cdc-clickhouse', 'clickhouse-client', '-q',
                    'INSERT INTO cdc_pipeline.upbit_market_events FORMAT JSONEachRow'],
                   input=payload.encode(), check=True, timeout=30)

def main():
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
    cur = fetch()
    if len(cur) < 200:
        sys.exit(f'suspicious market count {len(cur)}; skip')
    prev = json.load(open(STATE)) if os.path.exists(STATE) else None
    rows = []
    if prev is None or '--snapshot' in sys.argv:
        for mk, fl in cur.items():
            for f, v in fl.items():
                if v:
                    rows.append({'observed_at': now, 'market': mk, 'flag': f, 'state': 1, 'kind': 'snapshot'})
    if prev is not None:
        for mk, fl in cur.items():
            for f, v in fl.items():
                pv = prev.get(mk, {}).get(f, False)
                if v != pv:
                    rows.append({'observed_at': now, 'market': mk, 'flag': f, 'state': int(v), 'kind': 'transition'})
    insert(rows)
    tmp = STATE + '.tmp'
    json.dump(cur, open(tmp, 'w'))
    # 2026-09-26 (docs/46 §1 확장): 이 표도 전이만 적재하므로 09-25 01:50 에 "market_events 49분 정지" 오경보가 났다(그 시간 cron 은 매분 돌았다).
    subprocess.run(['docker', 'exec', '-i', 'cdc-clickhouse', 'clickhouse-client', '-q', 'INSERT INTO cdc_pipeline.cron_heartbeats FORMAT JSONEachRow'],
                   input=json.dumps({'job': 'market_events', 'ts': now, 'detail': f'markets={len(cur)} rows={len(rows)}'}), text=True, capture_output=True)
    os.replace(tmp, STATE)
    print(f'{now} markets={len(cur)} rows={len(rows)}')

if __name__ == '__main__':
    main()
