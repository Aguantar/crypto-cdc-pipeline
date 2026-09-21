#!/usr/bin/env python3
"""업비트 REST 일봉(최대 200일)을 KRW 전 마켓에 대해 받아 ClickHouse upbit_daily_candles 에 적재.
용도: 시장경보(거래량 급등 등) 6개월 백테스트 피처. 287콜, 초당 5콜 이하. 재실행 시 ReplacingMergeTree 로 중복 없음."""
import json, subprocess, time, urllib.request
H = {'Accept': 'application/json'}
def get(u):
    for i in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers=H), timeout=15) as r:
                return json.load(r)
        except Exception:
            time.sleep(2 * (i + 1))
    raise SystemExit('fail ' + u)
mk = [m['market'] for m in get('https://api.upbit.com/v1/market/all?is_details=false') if m['market'].startswith('KRW-')]
rows = []
for i, m in enumerate(mk):
    for c in get(f'https://api.upbit.com/v1/candles/days?market={m}&count=200'):
        rows.append({'market': m, 'day': c['candle_date_time_utc'][:10], 'open': c['opening_price'], 'high': c['high_price'],
                     'low': c['low_price'], 'close': c['trade_price'], 'amount': c['candle_acc_trade_price'], 'volume': c['candle_acc_trade_volume']})
    time.sleep(0.25)
subprocess.run(['docker', 'exec', '-i', 'cdc-clickhouse', 'clickhouse-client', '--multiquery', '-q', '''
CREATE TABLE IF NOT EXISTS cdc_pipeline.upbit_daily_candles (market LowCardinality(String), day Date, open Float64, high Float64, low Float64, close Float64, amount Float64, volume Float64, fetched_at DateTime DEFAULT now())
ENGINE = ReplacingMergeTree(fetched_at) ORDER BY (market, day)'''], check=True)
subprocess.run(['docker', 'exec', '-i', 'cdc-clickhouse', 'clickhouse-client', '-q', 'INSERT INTO cdc_pipeline.upbit_daily_candles FORMAT JSONEachRow'],
               input='\n'.join(json.dumps(r) for r in rows).encode(), check=True)
print(f'markets={len(mk)} rows={len(rows)}')
