#!/usr/bin/env python3
"""업비트 시장경보 지정/해제 이력 동기화.
사용: fetch_market_event_records.py [--since 'YYYY-MM-DD HH:MM:SS'] [--until ...]
기본: 최근 2일(증분, cron 시간당 1회). --since 를 과거로 주면 백필. 30일 창·200건 페이지로 나눠 호출, 0.3초 간격.
ReplacingMergeTree 라 재수집해도 중복 없음(동일 키·최신 fetched_at 유지).
"""
import json, subprocess, sys, time, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

BASE = 'https://crix-api-cdn.upbit.com/v1/crix/market-event-records'
H = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) Chrome/128.0', 'Origin': 'https://upbit.com',
     'Referer': 'https://upbit.com/service_center/market_warning_history'}
FMT = '%Y-%m-%d %H:%M:%S'

def get(**p):
    req = urllib.request.Request(BASE + '?' + urllib.parse.urlencode(p), headers=H)
    for i in range(5):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.load(r)
        except Exception as e:  # noqa
            time.sleep(2 * (i + 1))
    raise SystemExit(f'fetch failed: {p}')

def rows_between(start, end):
    page, out = 1, []
    while True:
        d = get(quoteCurrencyCode='KRW', start=start.strftime(FMT), end=end.strftime(FMT), page=page, size=200)
        for r in d['list']:
            out.append({'market': r['code'].replace('CRIX.UPBIT.', ''), 'event_type': r['eventType'],
                        'warning_level': r['warningLevel'], 'trigger_type': r['triggerType'],
                        'trigger_time_utc': r['triggerTimeUtc'], 'expiration_time_utc': r.get('expirationTimeUtc')})
        if page >= d['totalPage']:
            return out, d['totalCount']
        page += 1
        time.sleep(0.3)

def main():
    args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
    until = datetime.strptime(args['--until'], FMT) if '--until' in args else datetime.now(timezone.utc).replace(tzinfo=None)
    since = datetime.strptime(args['--since'], FMT) if '--since' in args else until - timedelta(days=2)
    total = 0
    end = until
    while end > since:
        start = max(since, end - timedelta(days=30))
        rows, tc = rows_between(start, end)
        if len(rows) != tc:
            print(f'WARN {start}~{end}: listed {len(rows)} != totalCount {tc}', file=sys.stderr)
        if rows:
            payload = '\n'.join(json.dumps(r) for r in rows)
            subprocess.run(['docker', 'exec', '-i', 'cdc-clickhouse', 'clickhouse-client', '-q',
                            'INSERT INTO cdc_pipeline.upbit_market_event_records FORMAT JSONEachRow'],
                           input=payload.encode(), check=True, timeout=60)
        print(f'{start:%Y-%m-%d} ~ {end:%Y-%m-%d}: {len(rows)} rows')
        total += len(rows)
        end = start
        time.sleep(0.3)
    print(f'done total={total}')

if __name__ == '__main__':
    main()
