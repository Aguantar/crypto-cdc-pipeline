#!/usr/bin/env python3
"""
체결 백필 - 업비트 REST /v1/trades/ticks(원장) vs MySQL (market, sequential_id) 대조 후 누락 체결을 MySQL에 INSERT (CDC 경유로 ClickHouse까지 전파).
- --dry-run: 누락 집계만(INSERT 없음). 결과 CSV: out/backfill/<run>.csv (market, hour_utc, rest_n, mysql_n, missing_n, inserted_n)
- 대상: --from/--to UTC ISO. REST는 daysAgo 0~7 (오늘=0). 초당 7요청 이하.
- INSERT: 마켓 단위로 시간순 일괄(이상탐지 interleave 최소화), 배치 500, best_* NULL, INSERT IGNORE(새 유니크 (market, sequential_id, upbit_timestamp)), ingest_source='backfill' (2026-09-19)
- 조회: MySQL 은 (market, upbit_timestamp) 창으로 읽는다 → 체결 시각 일 파티션 프루닝 + idx_market_ts (2026-09-19 A-1 뒤). 보존 7일 = REST daysAgo 한도 7 과 같아 그보다 오래된 날은 건너뛴다
"""
import argparse, csv, json, os, sys, time, urllib.parse, urllib.request, subprocess
from datetime import datetime, timezone, timedelta
from decimal import Decimal

def rest_ticks(market, day_ago, to_hms, lo_ms, hi_ms):
    out=[]; cursor=None
    for _ in range(2000):
        q={'market':market,'to':to_hms,'count':500,'daysAgo':day_ago}
        if cursor: q['cursor']=cursor
        for attempt in range(5):
            try:
                with urllib.request.urlopen(urllib.request.Request('https://api.upbit.com/v1/trades/ticks?'+urllib.parse.urlencode(q),headers={'Accept':'application/json'}),timeout=20) as r:
                    d=json.load(r); break
            except urllib.error.HTTPError as e:
                if e.code==429: time.sleep(1.5); continue
                raise
        if not d: break
        stop=False
        for t in d:
            if t['timestamp']<lo_ms: stop=True; break
            if t['timestamp']<hi_ms: out.append(t)
        if stop or len(d)<500: break
        cursor=d[-1]['sequential_id']; time.sleep(0.14)
    time.sleep(0.14); return out

def record_repair(lo_ms, hi_ms, markets_n, rest_rows, inserted, elapsed_s, note=''):
    """수리 계보(창 단위) → ClickHouse ingest_repairs (docs/19 #14). producer gap-fill 과 같은 테이블."""
    from datetime import datetime, timezone
    fmt=lambda ms: datetime.fromtimestamp(ms/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    row={'repaired_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), 'reason':'manual', 'window_start':fmt(lo_ms), 'window_end':fmt(hi_ms),
         'markets':markets_n, 'rest_rows':rest_rows, 'inserted_rows':inserted, 'elapsed_s':round(elapsed_s,1), 'note':note}
    try:
        subprocess.run(['docker','exec','-i','cdc-clickhouse','clickhouse-client','-q','INSERT INTO cdc_pipeline.ingest_repairs FORMAT JSONEachRow'],
                       input=json.dumps(row).encode(), check=True, timeout=30)
    except Exception as e:
        print(f"수리 계보 기록 실패(무시): {e}", file=sys.stderr)

def mysql(sql):
    r=subprocess.run(['docker','exec','-i','cdc-mysql','sh','-c','mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -N crypto_db'],input=sql,capture_output=True,text=True,timeout=600)
    if r.returncode!=0: raise RuntimeError(r.stderr[:500])
    return r.stdout

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--from',dest='t_from',required=True); ap.add_argument('--to',dest='t_to',required=True)
    ap.add_argument('--markets',help='콤마 구분(기본: MySQL에 있는 전 마켓)'); ap.add_argument('--dry-run',action='store_true'); ap.add_argument('--out',default='out/backfill'); a=ap.parse_args()
    lo=datetime.fromisoformat(a.t_from.replace('Z','+00:00')); hi=datetime.fromisoformat(a.t_to.replace('Z','+00:00'))
    lo_ms=int(lo.timestamp()*1000); hi_ms=int(hi.timestamp()*1000)
    today=datetime.now(timezone.utc).date()
    markets=[m for m in a.markets.split(',')] if a.markets else mysql(f"SELECT DISTINCT market FROM crypto_trades WHERE upbit_timestamp>={lo_ms} AND upbit_timestamp<{hi_ms};").split()
    os.makedirs(a.out,exist_ok=True); run=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'); csvp=os.path.join(a.out,f'backfill_{run}{"_dry" if a.dry_run else ""}.csv')
    w=csv.writer(open(csvp,'w',newline='')); w.writerow(['market','day_utc','rest_n','mysql_n','missing_n','inserted_n'])
    tot_rest=tot_my=tot_miss=tot_ins=0; t0=time.time()
    for i,m in enumerate(markets):
        # 일 단위로 REST 조회(daysAgo는 UTC 일자 기준)
        day=lo.date()
        while day<=hi.date():
            d_lo=max(lo_ms,int(datetime(day.year,day.month,day.day,tzinfo=timezone.utc).timestamp()*1000)); d_hi=min(hi_ms,int((datetime(day.year,day.month,day.day,tzinfo=timezone.utc)+timedelta(days=1)).timestamp()*1000))
            if d_lo>=d_hi: day+=timedelta(days=1); continue
            days_ago=(today-day).days
            if days_ago>7: day+=timedelta(days=1); continue
            to_hms=datetime.fromtimestamp(d_hi/1000,tz=timezone.utc).strftime('%H:%M:%S') if d_hi<int((datetime(day.year,day.month,day.day,tzinfo=timezone.utc)+timedelta(days=1)).timestamp()*1000) else '23:59:59'
            T=rest_ticks(m,days_ago,to_hms,d_lo,d_hi)
            have=set(int(x) for x in mysql(f"SELECT sequential_id FROM crypto_trades WHERE market='{m}' AND upbit_timestamp>={d_lo} AND upbit_timestamp<{d_hi};").split())
            miss=[t for t in T if t['sequential_id'] not in have]
            miss.sort(key=lambda t:(t['timestamp'],t['sequential_id'])); ins=0
            if miss and not a.dry_run:
                for k in range(0,len(miss),500):
                    vals=[]
                    for t in miss[k:k+500]:
                        p=Decimal(str(t['trade_price'])); v=Decimal(str(t['trade_volume'])); amt=(p*v).quantize(Decimal('0.0001'))
                        vals.append(f"('{m}',{p},{v},{amt},'{t['ask_bid']}',{t['timestamp']},{t['sequential_id']},NULL,NULL,NULL,NULL,NULL,'backfill','REALTIME')")
                    out=mysql("INSERT IGNORE INTO crypto_trades (market,trade_price,trade_volume,trade_amount,ask_bid,upbit_timestamp,sequential_id,best_ask_price,best_ask_size,best_bid_price,best_bid_size,recv_ms,ingest_source,stream_type) VALUES "+",".join(vals)+"; SELECT ROW_COUNT();")
                    ins+=int(out.strip().split()[-1]); time.sleep(0.5)   # ≤1,000 rows/s → 하류(2초 배치)에 부담 없음
            w.writerow([m,day.isoformat(),len(T),len(have),len(miss),ins]); tot_rest+=len(T); tot_my+=len(have); tot_miss+=len(miss); tot_ins+=ins
            day+=timedelta(days=1)
        if (i+1)%20==0: print(f"  {i+1}/{len(markets)} markets, rest {tot_rest} mysql {tot_my} missing {tot_miss} inserted {tot_ins} ({time.time()-t0:.0f}s)", file=sys.stderr, flush=True)
    if not a.dry_run:
        record_repair(lo_ms, hi_ms, len(markets), tot_rest, tot_ins, time.time()-t0, note=f'manual backfill {csvp}')
    print(json.dumps({'from':a.t_from,'to':a.t_to,'markets':len(markets),'rest':tot_rest,'mysql':tot_my,'missing':tot_miss,'inserted':tot_ins,'missing_pct':round(100*tot_miss/tot_rest,2) if tot_rest else None,'dry_run':a.dry_run,'csv':csvp,'elapsed_s':round(time.time()-t0)}))

if __name__=='__main__': main()
