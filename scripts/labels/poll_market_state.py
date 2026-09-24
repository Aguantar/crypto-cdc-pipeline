#!/usr/bin/env python3
"""업비트 마켓 거래 상태 폴러 (10분 cron). clickhouse/market_state.sql 참고.

왜 이 스크립트가 있나: 상장폐지·거래정지를 유실과 구분하기 위해서다. 폐지된 마켓은 거래소 목록에서
사라지고 우리 대조 분모가 줄어드는데, 기록이 없으면 "어제는 289개, 오늘은 287개 - 유실인가?"에 답할 수 없다.

왜 웹소켓인가: market_state·delisting_date·is_trading_suspended 는 REST 에 없다(2026-09-20 실측,
/v1/market/all?isDetails=true 와 /v1/ticker, /v1/ticker/all 전부 확인). 웹소켓 ticker 에만 있다.
구독하면 코드마다 SNAPSHOT 이 한 번 오므로 체결이 없는 마켓도 상태를 준다.

웹소켓 클라이언트는 scripts/lib/minws.py (표준 라이브러리 직접 구현). 호스트에 websockets 가 없고
apt 설치는 sudo 가 필요해서, 쓰는 기능 셋(접속·전송·수신) 때문에 의존성을 늘리지 않았다.
컨테이너의 websockets 결과와 289 마켓 전부 대조해 동일함을 확인했다(2026-09-20).

쓰기: docker exec clickhouse-client (poll_market_events.py·collect_metrics.sh 와 같은 경로).
실패하면 상태 파일을 갱신하지 않아 다음 실행에서 다시 시도한다 - 전이를 놓치지 않기 위해서다.
"""
import json, os, subprocess, sys, urllib.request
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
from minws import MinWS

STATE = os.path.expanduser('~/pipeline-observation/market_state_state.json')
MARKETS_URL = 'https://api.upbit.com/v1/market/all?isDetails=true'
WS_URL = 'wss://api.upbit.com/websocket/v1'
RECV_TIMEOUT_S = 12


def list_krw_markets():
    req = urllib.request.Request(MARKETS_URL, headers={'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.load(r)
    return [m['market'] for m in data if m['market'].startswith('KRW-')]


def snapshot(codes):
    """구독 직후 코드마다 오는 SNAPSHOT 한 건씩을 모은다. 전부 모이거나 타임아웃이면 끝낸다."""
    got = {}
    with MinWS(WS_URL, timeout=RECV_TIMEOUT_S) as ws:
        ws.send(json.dumps([{'ticket': 'market-state'},
                            {'type': 'ticker', 'codes': codes},
                            {'format': 'DEFAULT'}]))
        try:
            while len(got) < len(codes):
                m = ws.recv_json()
                got.setdefault(m['code'], m)
        except Exception:
            pass   # 타임아웃·조기 종료: 받은 만큼만 쓴다(부분 스냅샷도 전이 판정에 쓸 수 있다)
    return got


def to_row(m):
    d = m.get('delisting_date')
    return {
        'market_state': m.get('market_state') or 'UNKNOWN',
        'is_trading_suspended': 1 if m.get('is_trading_suspended') else 0,
        # 거래소는 delisting_date 를 {year, month, day} 객체로 준다 (문자열 아님)
        'delisting_date': f"{d['year']:04d}-{d['month']:02d}-{d['day']:02d}" if isinstance(d, dict) else None,
    }


def insert(rows, table='cdc_pipeline.upbit_market_state_events'):
    if not rows:
        return
    payload = '\n'.join(json.dumps(r, ensure_ascii=False) for r in rows)
    p = subprocess.run(
        ['docker', 'exec', '-i', 'cdc-clickhouse', 'clickhouse-client', '-q',
         f'INSERT INTO {table} FORMAT JSONEachRow'],
        input=payload, text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip()[:300])


def heartbeat(now, detail):
    # 2026-09-24 (docs/46): 이 표는 전이만 적재하므로 "변화 없음" 과 "cron 죽음" 이 같은 모양이다. 폴링이 성공할 때마다
    # 생존 신호를 따로 남긴다. quality_alerts 의 Cron Freshness 가 이 표의 max(ts) 를 본다.
    insert([{'job': 'market_state', 'ts': now, 'detail': detail}], table='cdc_pipeline.cron_heartbeats')


def main():
    force_snapshot = '--snapshot' in sys.argv
    # --dry-run: 적재도 상태 파일 갱신도 하지 않고 만들어질 행만 출력한다.
    # 전이·폐지 경로를 프로덕션 표를 건드리지 않고 시험하려고 둔다(2026-09-20).
    dry = '--dry-run' in sys.argv
    state_path = next((a.split('=', 1)[1] for a in sys.argv if a.startswith('--state=')), STATE)
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None).isoformat(sep=' ')
    prev = {}
    if os.path.exists(state_path) and not force_snapshot:
        try:
            prev = json.load(open(state_path))
        except Exception:
            prev = {}

    codes = list_krw_markets()
    got = snapshot(codes)
    if not got:
        print(f'{now} 응답 0건 - 적재하지 않음(상태 파일 유지)')
        return 1
    cur = {c: to_row(m) for c, m in got.items()}

    rows, kind = [], 'transition' if prev else 'snapshot'
    for market, v in cur.items():
        if prev.get(market) != v:
            rows.append({'observed_at': now, 'market': market, 'kind': kind if prev else 'snapshot', **v})
    # 목록에서 사라진 마켓: 폐지가 실제로 일어난 순간이다. 이 한 줄이 "유실 아님"의 근거가 된다.
    for market in set(prev) - set(cur):
        rows.append({'observed_at': now, 'market': market, 'market_state': 'DELISTED',
                     'is_trading_suspended': 1, 'delisting_date': prev[market].get('delisting_date'),
                     'kind': 'gone'})

    if dry:
        for r in rows:
            print('  DRY', json.dumps(r, ensure_ascii=False))
        print(f'{now} 마켓 {len(cur)} / 만들어질 행 {len(rows)} (적재·상태파일 갱신 없음)')
        return 0
    insert(rows)
    json.dump(cur, open(state_path, 'w'))   # 적재 성공 뒤에만 갱신
    heartbeat(now, f'markets={len(cur)} rows={len(rows)} kind={kind}')
    changed = ', '.join(f"{r['market']}={r['market_state']}" for r in rows[:5])
    print(f'{now} 마켓 {len(cur)} / 적재 {len(rows)} ({kind}) {changed}{" ..." if len(rows) > 5 else ""}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
