#!/usr/bin/env python3
"""
유실 대조 - 업비트 시간봉(거래소 진실값) vs ClickHouse 체결(체결시각 기준) 마켓×시간 거래량 비율.
읽기 전용. REST /v1/candles/minutes/60 (마켓당 1회, 초당 8회 이하 페이싱), ClickHouse readonly_user.
출력: ~/pipeline-observation/reconcile_<UTC일자>.csv (market, hour_utc, ch_vol, candle_vol, ratio_pct, ch_n)
요약: 마켓별 최소/평균 비율, 95% 미만 (market, hour) 목록.
사용: python3 reconcile_trades.py [--hours 12]
"""
import argparse, csv, json, os, sys, time, urllib.parse, urllib.request
from datetime import datetime, timezone, timedelta

ENV='/home/calme/cdc-realtime-pipeline/.env'
OUT_DIR=os.path.expanduser('~/pipeline-observation')

def ch(sql, u, p):
    q=urllib.parse.urlencode({'query':sql,'user':u,'password':p,'max_memory_usage':'500000000','max_threads':'2'})
    return urllib.request.urlopen('http://localhost:8123/?'+q, timeout=300).read().decode()

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--hours', type=int, default=12); a=ap.parse_args()
    env=dict(l.strip().split('=',1) for l in open(ENV) if '=' in l and not l.startswith('#'))
    u,p=env['CLICKHOUSE_READONLY_USER'],env['CLICKHOUSE_READONLY_PASSWORD']
    now=datetime.now(timezone.utc).replace(minute=0,second=0,microsecond=0)
    start=now-timedelta(hours=a.hours)   # 완결된 시간만: [start, now)
    # ClickHouse: 체결시각 기준 마켓×시간 거래량
    sql=f"""SELECT market, toStartOfHour(fromUnixTimestamp64Milli(upbit_timestamp)) h, sum(trade_volume) v, count() n
            FROM cdc_pipeline.crypto_trades
            WHERE upbit_timestamp >= {int(start.timestamp()*1000)} AND upbit_timestamp < {int(now.timestamp()*1000)}
            GROUP BY market, h FORMAT JSONEachRow"""
    chd={}
    for line in ch(sql,u,p).splitlines():
        r=json.loads(line); chd[(r['market'], r['h'][:13].replace(' ','T'))]=(float(r['v']), int(r['n']))
    markets=sorted({m for m,_ in chd})
    # 업비트 시간봉
    to=now.strftime('%Y-%m-%dT%H:%M:%SZ'); rows=[]; miss=0
    for i,m in enumerate(markets):
        url=f"https://api.upbit.com/v1/candles/minutes/60?market={m}&to={to}&count={a.hours}"
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={'Accept':'application/json'}), timeout=15) as r:
                cs=json.load(r)
        except Exception as e:
            miss+=1; print('candle fetch fail', m, e, file=sys.stderr); time.sleep(1); continue
        for c in cs:
            h=c['candle_date_time_utc'][:13]; cv=float(c['candle_acc_trade_volume'])
            if h < start.strftime('%Y-%m-%dT%H'): continue   # 거래 없는 시간은 캔들이 생략되어 count가 창 밖까지 확장됨
            v,n=chd.get((m,h),(0.0,0))
            ratio=100*v/cv if cv>0 else (100.0 if v==0 else None)
            rows.append((m,h,v,cv,ratio,n))
        time.sleep(0.13)   # ~7.5 req/s < 10/s
    day=now.strftime('%Y-%m-%d'); out=os.path.join(OUT_DIR,f'reconcile_{day}_{now:%H}Z.csv')
    with open(out,'w',newline='') as f:
        w=csv.writer(f); w.writerow(['market','hour_utc','ch_vol','candle_vol','ratio_pct','ch_n']); w.writerows(rows)
    # 요약
    valid=[r for r in rows if r[4] is not None and r[3]>0]
    per_m={}
    for r in valid: per_m.setdefault(r[0],[]).append(r[4])
    low=[r for r in valid if r[4]<95]; high=[r for r in valid if r[4]>105]
    tot_ch=sum(r[2] for r in valid); tot_c=sum(r[3] for r in valid)
    print(json.dumps({'window_utc':[start.isoformat(),now.isoformat()],'markets_ch':len(markets),'candle_fetch_fail':miss,
        'cells':len(valid),'cells_below_95':len(low),'cells_above_105':len(high),
        'markets_min_ratio_below_95':sorted(m for m,v in per_m.items() if min(v)<95),
        'volume_weighted_ratio_pct':round(100*tot_ch/tot_c,2) if tot_c else None,'out':out}, ensure_ascii=False))
    for r in sorted(low, key=lambda x:x[4])[:15]: print(f'  LOW {r[0]} {r[1]} ch={r[2]:.4f} candle={r[3]:.4f} ratio={r[4]:.1f}% n={r[5]}')

if __name__=='__main__': main()
