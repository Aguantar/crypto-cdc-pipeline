#!/usr/bin/env python3
"""
7일 관찰 일일 요약 - metrics_5m.csv 최근 24h를 docs/12-observation-plan.md 의 가설 임계값과 대조해 마크다운 요약을 남긴다.
- 입력: ~/pipeline-observation/metrics_5m.csv (collect_metrics.sh, 5분 간격)
- 출력: ~/pipeline-observation/digest_YYYY-MM-DD.md, 그리고 docs/worklog.md 에 한 줄 요약 append
- 등록: 20 6 * * * python3 /home/calme/cdc-realtime-pipeline/scripts/observe/daily_digest.py
읽기 전용. 파이프라인에 접근하지 않고 CSV만 읽는다.
"""
import csv, os, statistics, sys
from datetime import datetime, timezone, timedelta

CSV = os.path.expanduser('~/pipeline-observation/metrics_5m.csv')
OUT_DIR = os.path.expanduser('~/pipeline-observation')
WORKLOG = '/home/calme/cdc-realtime-pipeline/docs/worklog.md'

def f(v):
    try: return float(v)
    except Exception: return None

def pct(xs, p):
    xs = sorted(x for x in xs if x is not None)
    return xs[min(len(xs)-1, int(len(xs)*p))] if xs else None

def main():
    now = datetime.now(timezone.utc); since = now - timedelta(hours=24)
    rows = [r for r in csv.DictReader(open(CSV)) if datetime.fromisoformat(r['ts'].replace('Z','+00:00')) >= since]
    if not rows: sys.exit('no rows in last 24h')
    kst = lambda r: (datetime.fromisoformat(r['ts'].replace('Z','+00:00')) + timedelta(hours=9)).strftime('%H')
    # 시간대(KST)별 체결 lag p95 최대, 호가 e2e p95 최대, producer buffer 최대
    by_h = {}
    for r in rows:
        h = kst(r); d = by_h.setdefault(h, {'lag95': [], 'ob95': [], 'buf': [], 'rows': []})
        d['lag95'].append(f(r['tr_lag_p95_s'])); d['ob95'].append(f(r['ob_e2e_p95_ms'])); d['buf'].append(f(r['p_buffer'])); d['rows'].append(f(r['tr_rows5m']))
    def mx(xs): xs = [x for x in xs if x is not None]; return max(xs) if xs else None
    last = rows[-1]
    checks = []
    # H1 체결 lag p95 ≥ 5s 3연속
    streak = 0; h1 = 0
    for r in rows:
        v = f(r['tr_lag_p95_s']); streak = streak + 1 if (v is not None and v >= 5) else 0; h1 = max(h1, streak)
    checks.append(('H1 체결 lag p95≥5s 3연속', h1 >= 3, f'최대 연속 {h1}, 24h p95 max {mx([f(r["tr_lag_p95_s"]) for r in rows])}s'))
    bufmax = mx([f(r['p_buffer']) for r in rows]); warn = sum(int(f(r['p_warn5m']) or 0) for r in rows)
    checks.append(('H2 producer buffer>1000 또는 WARNING', (bufmax or 0) > 1000 or warn > 0, f'buffer max {bufmax}, WARNING {warn}'))
    streak = 0; h3 = 0
    for r in rows:
        v = f(r['ob_e2e_p95_ms']); streak = streak + 1 if (v is not None and v >= 3000) else 0; h3 = max(h3, streak)
    checks.append(('H3 호가 e2e p95≥3s 3연속', h3 >= 3, f'최대 연속 {h3}, 24h p95 max {mx([f(r["ob_e2e_p95_ms"]) for r in rows])}ms'))
    recon = f(last['c_reconnects']); derr = f(last['c_deliv_err'])
    checks.append(('H4 수집기 재연결/발행실패', (derr or 0) > 0, f'reconnects 누적 {recon}, deliv_err {derr}'))
    cpf = sum(int(f(r[k]) or 0) for r in [last] for k in ('fl_cdc_cp_fail', 'fl_ob_cp_fail', 'fl_cc_cp_fail'))
    checks.append(('H7 체크포인트 실패', cpf > 0, f'실패 누적 cdc/ob/cc = {last["fl_cdc_cp_fail"]}/{last["fl_ob_cp_fail"]}/{last["fl_cc_cp_fail"]}, state {last["fl_cdc_state"]}/{last["fl_ob_state"]}'))
    chm = [f(r['ch_mem_bytes']) for r in rows]; chmax = mx(chm)
    checks.append(('H8 ClickHouse 메모리>1.4GiB', (chmax or 0) > 1.4 * 1024**3, f'max {round((chmax or 0)/1024**3, 2)} GiB, parts trades/ob {last["ch_parts_trades"]}/{last["ch_parts_ob"]}'))
    kd = f(last['k_disk_b1_bytes'])
    checks.append(('H9 Kafka 브로커1 로그>20GB', (kd or 0) > 20e9, f'{round((kd or 0)/1e9, 1)} GB'))
    alerts = sum(int(f(r['alerts5m']) or 0) for r in rows)
    checks.append(('H10 알림>2000/일', alerts > 2000, f'24h 알림 {alerts}건'))
    my0 = f(rows[0]['my_rows']); my1 = f(last['my_rows'])
    checks.append(('H11 MySQL 행수 증가', (my1 or 0) - (my0 or 0) > 500000, f'{int(my0 or 0):,} → {int(my1 or 0):,}'))
    runs = sum(1 for r in rows if r['fl_cdc_run'] == '1' and r['fl_ob_run'] == '1' and r['fl_cc_run'] == '1')
    day = now.strftime('%Y-%m-%d')
    lines = [f'# 관찰 일일 요약 {day} (UTC {now:%H:%M}, 최근 24h, 샘플 {len(rows)}개, 3잡 RUNNING 샘플 {runs}/{len(rows)})', '',
             '| 가설 | 위반 | 근거 |', '|---|---|---|']
    lines += [f'| {n} | {"**위반**" if bad else "정상"} | {ev} |' for n, bad, ev in checks]
    lines += ['', '## KST 시간대별 (24h)', '', '| 시(KST) | 체결 lag p95 max(s) | 호가 e2e p95 max(ms) | buffer max | 체결 5분 행수 평균 |', '|---|---|---|---|---|']
    for h in sorted(by_h):
        d = by_h[h]; rr = [x for x in d['rows'] if x is not None]
        lines.append(f'| {h} | {mx(d["lag95"])} | {mx(d["ob95"])} | {mx(d["buf"])} | {round(statistics.mean(rr)) if rr else ""} |')
    lines += ['', f'호스트: used {last["host_used_mb"]}MB, swap {last["swap_used_mb"]}MB, df {round(f(last["df_used_bytes"])/1e9,1)}GB, load1 {last["load1"]}. ClickHouse {last["ch_mem"]}, TM {last["tm_mem"]}.']
    out = os.path.join(OUT_DIR, f'digest_{day}.md'); open(out, 'w').write('\n'.join(lines) + '\n')
    viol = [n for n, bad, _ in checks if bad]
    with open(WORKLOG, 'a') as w:
        w.write(f'- {now:%m-%d %H:%M} UTC 관찰 일일 요약(자동): 샘플 {len(rows)}, 위반 {viol if viol else "없음"}, 체결 lag p95 max {mx([f(r["tr_lag_p95_s"]) for r in rows])}s, 호가 e2e p95 max {mx([f(r["ob_e2e_p95_ms"]) for r in rows])}ms, buffer max {bufmax}, 알림 {alerts}, ClickHouse max {round((chmax or 0)/1024**3,2)}GiB → {out}\n')
    print(out); print('\n'.join(lines[:14]))

if __name__ == '__main__': main()
