"""
virtual-trader - 2층 원장 생성기 (docs/28 B, 2026-09-19)
=========================================================
Binance Spot **Testnet**(가상 자금) 에 규칙대로 주문을 내고, 유저 데이터 스트림(executionReport)이 주는
주문 상태 전이를 MySQL 원장(virtual_orders 거울 / virtual_fills 보관소 / binance_user_events 원문)에 반영한다.
상태를 바꾸는 주체는 거래소 매칭 엔진이다. 우리가 만든 것은 아래 규칙뿐이고, 규칙은 결정적이며 이 파일이 문서다.

규칙 (CYCLE_SEC 마다 심볼별로 한 번, 심볼당 일 상한 MAX_ORDERS_PER_SYMBOL_PER_DAY):
  A. maker-probe : 최우선 매수호가 - MAKER_TICKS 틱에 LIMIT GTC 매수(체결 안 되게 조금 아래). CANCEL_AFTER_SEC 뒤에도 열려 있으면
                   수량을 절반으로 **amend(keepPriority, orderId 유지)** → 다시 CANCEL_AFTER_SEC 뒤 취소.  → NEW · REPLACED · CANCELED 전이
  B. taker-ioc   : 최우선 매도호가에 LIMIT IOC 매수(즉시 체결, 남으면 만료).                                  → NEW · TRADE(FILLED 또는 PARTIALLY_FILLED→EXPIRED)
  C. unwind      : 누적 순매수 수량이 있으면 최우선 매수호가에 LIMIT IOC 매도 → 잔고가 한쪽으로 쏠리지 않게.    → TRADE
  주문 금액은 ORDER_NOTIONAL_USDT(기본 20 USDT) 이고 거래소 필터(tickSize·stepSize·minNotional)로 양자화한다.

리셋 감지: 테스트넷은 약 월 1회 예고 없이 전체 리셋(주문 전부 삭제, 키 보존). 우리 원장의 열린 주문이 거래소에 "존재하지 않음"(-2013)이고
  FILLED 였던 최근 주문도 없으면 리셋으로 판정 → reset_epoch+1, 이전 세대 주문·체결을 **물리 DELETE**(CDC op=d 가 ClickHouse 로 흘러 is_deleted).

인증: Ed25519 (Binance 권장). 서명 = 정렬한 params 를 k=v&… 로 이어 개인키로 서명, base64. session.logon 뒤엔 apiKey·signature 생략.
"""
import asyncio, base64, json, logging, os, signal, sys, time, uuid, urllib.request, urllib.parse
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger('virtual-trader')

WS_API_URL = os.getenv('BINANCE_WS_API_URL', 'wss://ws-api.testnet.binance.vision/ws-api/v3')
REST_URL = os.getenv('BINANCE_REST_URL', 'https://testnet.binance.vision')
API_KEY = os.getenv('BINANCE_API_KEY', '')
PRIVATE_KEY_PATH = os.getenv('BINANCE_ED25519_PRIVATE_KEY_PATH', '/run/secrets/binance_ed25519.pem')
SYMBOLS = [s for s in os.getenv('SYMBOLS', 'BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT').split(',') if s]
# 2026-09-20: 300초·심볼당 60건이면 하루치를 100분 만에 다 쓰고 22시간을 쉬었다.
# 이 원장은 '합성 거래 카나리아'다 - 양이 아니라 CDC 경로가 24시간 내내 신호를 내는 것이 목적이고,
# 매시 도는 3자 대조가 매번 새 데이터를 봐야 한다. 그래서 주기를 늘려 하루에 고르게 편다.
#   900초 × 24시간 = 96 사이클, 심볼당 3건 → 288건/일, 매시 4 사이클(빈 시간 없음).
# 상한 288 은 임의 숫자가 아니라 '15분 주기로 24시간'의 결과다. 거래소 한도 대비:
#   주문 1,440건/일 = 일 한도 160,000 의 0.9%, 사이클당 25 요청 = 10초 50건 한도 안(주문 사이 1초 대기).
CYCLE_SEC = float(os.getenv('CYCLE_SEC', '900'))
CANCEL_AFTER_SEC = float(os.getenv('CANCEL_AFTER_SEC', '90'))
ORDER_NOTIONAL_USDT = Decimal(os.getenv('ORDER_NOTIONAL_USDT', '20'))
MAKER_TICKS = int(os.getenv('MAKER_TICKS', '5'))
MAX_ORDERS_PER_SYMBOL_PER_DAY = int(os.getenv('MAX_ORDERS_PER_SYMBOL_PER_DAY', '288'))   # = 3건 × (86400/CYCLE_SEC)
STATS_INTERVAL = int(os.getenv('STATS_INTERVAL_SEC', '30'))
MYSQL = dict(host=os.getenv('MYSQL_HOST', 'cdc-mysql'), port=int(os.getenv('MYSQL_PORT', '3306')), user=os.getenv('MYSQL_USER', 'ledger'),
             password=os.getenv('MYSQL_PASSWORD', ''), database=os.getenv('MYSQL_DATABASE', 'crypto_db'), autocommit=True)


# ---------------------------------------------------------------- 순수 함수 (테스트 대상)
def sign_payload(params: dict) -> str:
    """Binance 규칙: signature 를 제외한 params 를 이름순 정렬해 k=v&k=v (URL 인코딩 없음)."""
    return '&'.join(f"{k}={params[k]}" for k in sorted(params) if k != 'signature')


def ed25519_sign(private_key, payload: str) -> str:
    return base64.b64encode(private_key.sign(payload.encode('utf-8'))).decode('ascii')


def load_private_key(path: str):
    from cryptography.hazmat.primitives import serialization
    with open(path, 'rb') as f:
        return serialization.load_pem_private_key(f.read(), password=None)


@dataclass
class SymbolFilters:
    tick_size: Decimal
    step_size: Decimal
    min_notional: Decimal
    amend_allowed: bool = True

    @staticmethod
    def from_exchange_info(sym: dict) -> 'SymbolFilters':
        f = {x['filterType']: x for x in sym['filters']}
        return SymbolFilters(tick_size=Decimal(f['PRICE_FILTER']['tickSize']), step_size=Decimal(f['LOT_SIZE']['stepSize']),
                             min_notional=Decimal(f.get('NOTIONAL', f.get('MIN_NOTIONAL', {})).get('minNotional', '5')),
                             amend_allowed=bool(sym.get('amendAllowed', False)))


def quantize(value: Decimal, step: Decimal) -> Decimal:
    """거래소 필터 단위로 내림. Decimal 그대로 문자열화해야 1e-05 같은 지수 표기가 안 나간다."""
    q = (value / step).to_integral_value(rounding=ROUND_DOWN) * step
    return q.quantize(step) if step < 1 else q


def fmt(d: Decimal) -> str:
    s = format(d, 'f')
    return s.rstrip('0').rstrip('.') if '.' in s else s


def plan_orders(symbol: str, bid: Decimal, ask: Decimal, f: SymbolFilters, net_qty: Decimal, notional: Decimal, maker_ticks: int) -> list:
    """한 사이클의 주문 계획 (규칙 A·B·C). 반환: [{'strategy','side','type','tif','price','qty'}]. 네트워크·상태 없음 → 테스트 가능."""
    plans = []
    qty = quantize(notional / ask, f.step_size)
    if qty <= 0 or qty * ask < f.min_notional:
        return plans
    maker_px = quantize(bid - f.tick_size * maker_ticks, f.tick_size)
    if maker_px > 0 and qty * maker_px >= f.min_notional:
        plans.append({'strategy': 'maker-probe', 'side': 'BUY', 'type': 'LIMIT', 'tif': 'GTC', 'price': maker_px, 'qty': qty})
    plans.append({'strategy': 'taker-ioc', 'side': 'BUY', 'type': 'LIMIT', 'tif': 'IOC', 'price': ask, 'qty': qty})
    if net_qty > 0:
        sell = quantize(net_qty, f.step_size)
        if sell > 0 and sell * bid >= f.min_notional:
            plans.append({'strategy': 'unwind', 'side': 'SELL', 'type': 'LIMIT', 'tif': 'IOC', 'price': bid, 'qty': sell})
    return plans


def order_row_from_report(ev: dict, strategy: str) -> dict:
    """executionReport → virtual_orders 행 값. 누적 필드(z, Z)는 거래소가 주는 값을 그대로 쓴다(우리 계산 아님)."""
    return {'order_id': int(ev['i']), 'symbol': ev['s'], 'client_order_id': (ev.get('C') or ev['c']),
            'side': ev['S'], 'order_type': ev['o'], 'time_in_force': ev.get('f'), 'price': ev['p'], 'orig_qty': ev['q'],
            'executed_qty': ev['z'], 'cum_quote_qty': ev['Z'], 'status': ev['X'], 'last_exec_type': ev['x'], 'reject_reason': ev.get('r'),
            'strategy': strategy, 'created_ms': int(ev['O']), 'updated_ms': int(ev['E']), 'last_exec_id': int(ev['I'])}


def fill_row_from_report(ev: dict, strategy: str) -> dict | None:
    if ev.get('x') != 'TRADE' or int(ev.get('t', -1)) < 0:
        return None
    return {'symbol': ev['s'], 'fill_id': int(ev['t']), 'order_id': int(ev['i']), 'side': ev['S'], 'price': ev['L'], 'qty': ev['l'], 'quote_qty': ev['Y'],
            'commission': ev.get('n') or '0', 'commission_asset': ev.get('N'), 'is_maker': 1 if ev.get('m') else 0, 'filled_ms': int(ev['T']),
            'exec_id': int(ev['I']), 'strategy': strategy}


STRATEGY_CODES = {'mkr': 'maker-probe', 'ioc': 'taker-ioc', 'unw': 'unwind'}
STRATEGY_CODE = {v: k for k, v in STRATEGY_CODES.items()}


def client_order_id(strategy: str, symbol: str, n: int, sec: int) -> str:
    """테스트넷 실측 규칙(09-19) ^[a-zA-Z0-9-_]{1,36}$ - 문서의 '.' ':' '/' 는 거부됐다 → '_' 구분: <code>_<yyyymmdd>_<symbol>_<n>_<sec%100000> (≤ 30자)."""
    return f"{STRATEGY_CODE[strategy]}_{time.strftime('%Y%m%d', time.gmtime(sec))}_{symbol}_{n}_{sec % 100000}"


def strategy_of(cid: str) -> str:
    return STRATEGY_CODES.get(cid.split('_')[0], 'unknown') if cid else 'unknown'


def compare_ledger(ex_orders: list, ex_trades: list, my: dict) -> dict:
    """거래소(REST allOrders/myTrades) vs 우리 원장 집계 → 대조 행. 상태별 수·수량 합이 전부 같아야 mismatch=0."""
    open_st = {'NEW', 'PARTIALLY_FILLED', 'PENDING_NEW'}
    ex = {'orders': len(ex_orders), 'filled': sum(1 for o in ex_orders if o['status'] == 'FILLED'),
          'canceled': sum(1 for o in ex_orders if o['status'] in ('CANCELED', 'EXPIRED', 'EXPIRED_IN_MATCH', 'REJECTED')),
          'open': sum(1 for o in ex_orders if o['status'] in open_st),
          'exec_qty': sum(Decimal(o['executedQty']) for o in ex_orders), 'trades': len(ex_trades), 'trade_qty': sum(Decimal(t['qty']) for t in ex_trades)}
    diffs = [k for k in ex if Decimal(str(ex[k])) != Decimal(str(my[k]))]
    return {'ex': ex, 'my': my, 'mismatch': 1 if diffs else 0, 'detail': ','.join(f"{k}:ex={ex[k]}/my={my[k]}" for k in diffs)[:512] or None}


def dedup_key(ev: dict) -> str:
    return f"exec:{ev['I']}" if ev.get('e') == 'executionReport' else f"{ev.get('e')}:{ev.get('E')}"


# ---------------------------------------------------------------- MySQL 원장
UPSERT_ORDER = """INSERT INTO virtual_orders (order_id,symbol,client_order_id,side,order_type,time_in_force,price,orig_qty,executed_qty,cum_quote_qty,status,last_exec_type,reject_reason,strategy,fill_count,created_ms,updated_ms,last_exec_id,version,reset_epoch)
VALUES (%(order_id)s,%(symbol)s,%(client_order_id)s,%(side)s,%(order_type)s,%(time_in_force)s,%(price)s,%(orig_qty)s,%(executed_qty)s,%(cum_quote_qty)s,%(status)s,%(last_exec_type)s,%(reject_reason)s,%(strategy)s,%(fill_inc)s,%(created_ms)s,%(updated_ms)s,%(last_exec_id)s,1,%(epoch)s)
ON DUPLICATE KEY UPDATE
  executed_qty=IF(VALUES(last_exec_id)>last_exec_id, VALUES(executed_qty), executed_qty),
  cum_quote_qty=IF(VALUES(last_exec_id)>last_exec_id, VALUES(cum_quote_qty), cum_quote_qty),
  status=IF(VALUES(last_exec_id)>last_exec_id, VALUES(status), status),
  last_exec_type=IF(VALUES(last_exec_id)>last_exec_id, VALUES(last_exec_type), last_exec_type),
  reject_reason=IF(VALUES(last_exec_id)>last_exec_id, VALUES(reject_reason), reject_reason),
  orig_qty=IF(VALUES(last_exec_id)>last_exec_id, VALUES(orig_qty), orig_qty),
  fill_count=fill_count+IF(VALUES(last_exec_id)>last_exec_id, VALUES(fill_count), 0),
  updated_ms=IF(VALUES(last_exec_id)>last_exec_id, VALUES(updated_ms), updated_ms),
  version=version+IF(VALUES(last_exec_id)>last_exec_id, 1, 0),
  last_exec_id=GREATEST(last_exec_id, VALUES(last_exec_id))"""
# 왜 last_exec_id 비교인가: at-least-once 재수신·순서 뒤바뀜에서 옛 이벤트가 새 상태를 덮지 않게. version 은 실제로 바뀔 때만 +1 (ClickHouse RMT 버전).
INSERT_FILL = """INSERT IGNORE INTO virtual_fills (symbol,fill_id,order_id,side,price,qty,quote_qty,commission,commission_asset,is_maker,filled_ms,exec_id,strategy)
VALUES (%(symbol)s,%(fill_id)s,%(order_id)s,%(side)s,%(price)s,%(qty)s,%(quote_qty)s,%(commission)s,%(commission_asset)s,%(is_maker)s,%(filled_ms)s,%(exec_id)s,%(strategy)s)"""
INSERT_EVENT = """INSERT IGNORE INTO binance_user_events (event_type,dedup_key,event_ms,symbol,order_id,exec_type,order_status,raw,recv_ms)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""


class Ledger:
    def __init__(self):
        import mysql.connector
        self._m = mysql.connector
        self.conn = None
        self.epoch = 0

    def connect(self):
        self.conn = self._m.connect(**MYSQL)
        cur = self.conn.cursor(); cur.execute("SELECT COALESCE(MAX(reset_epoch),0) FROM virtual_orders"); self.epoch = int(cur.fetchone()[0]); cur.close()

    def _cur(self):
        try:
            self.conn.ping(reconnect=True, attempts=3, delay=1)
        except Exception:
            self.connect()
        return self.conn.cursor()

    def record_event(self, ev: dict, recv_ms: int) -> bool:
        cur = self._cur()
        cur.execute(INSERT_EVENT, (ev.get('e'), dedup_key(ev), int(ev.get('E', recv_ms)), ev.get('s'), int(ev['i']) if 'i' in ev else None, ev.get('x'), ev.get('X'), json.dumps(ev, separators=(',', ':')), recv_ms))
        new = cur.rowcount == 1; cur.close(); return new

    def apply_report(self, ev: dict) -> dict:
        strategy = strategy_of(ev.get('C') or ev.get('c', ''))   # 취소 이벤트는 c 가 취소 요청 id, C 가 원 주문 id
        row = order_row_from_report(ev, strategy); fill = fill_row_from_report(ev, strategy)
        row['fill_inc'] = 1 if fill else 0; row['epoch'] = self.epoch
        cur = self._cur(); cur.execute(UPSERT_ORDER, row); n = cur.rowcount
        if fill: cur.execute(INSERT_FILL, fill)
        cur.close(); return {'order_rows': n, 'fill': bool(fill)}

    def open_orders(self):
        cur = self._cur(); cur.execute("SELECT order_id, symbol, client_order_id, orig_qty, created_ms FROM virtual_orders WHERE status IN ('NEW','PARTIALLY_FILLED') AND reset_epoch=%s", (self.epoch,)); r = cur.fetchall(); cur.close(); return r

    def last_filled(self, symbol):
        cur = self._cur(); cur.execute("SELECT order_id FROM virtual_orders WHERE symbol=%s AND status='FILLED' AND reset_epoch=%s ORDER BY updated_ms DESC LIMIT 1", (symbol, self.epoch)); r = cur.fetchone(); cur.close(); return r[0] if r else None

    def orders_today(self, symbol, day_start_ms):
        cur = self._cur(); cur.execute("SELECT count(*) FROM virtual_orders WHERE symbol=%s AND created_ms>=%s", (symbol, day_start_ms)); n = cur.fetchone()[0]; cur.close(); return int(n)

    def net_qty(self, symbol) -> Decimal:
        cur = self._cur(); cur.execute("SELECT COALESCE(SUM(IF(side='BUY',qty,-qty)),0) FROM virtual_fills WHERE symbol=%s", (symbol,)); v = cur.fetchone()[0]; cur.close(); return Decimal(str(v))

    def snapshot_positions(self, balances: list, snapshot_ms: int):
        day = time.strftime('%Y-%m-%d', time.gmtime(snapshot_ms / 1000)); cur = self._cur()
        for b in balances:
            if Decimal(b['free']) == 0 and Decimal(b['locked']) == 0: continue
            cur.execute("INSERT INTO virtual_positions (as_of_day,asset,free,locked,snapshot_ms,reset_epoch) VALUES (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE free=VALUES(free), locked=VALUES(locked), snapshot_ms=VALUES(snapshot_ms), reset_epoch=VALUES(reset_epoch)",
                        (day, b['asset'], b['free'], b['locked'], snapshot_ms, self.epoch))
        cur.close()

    def my_summary(self, symbol: str, day_start_ms: int) -> dict:
        cur = self._cur()
        cur.execute("""SELECT count(*), SUM(status='FILLED'), SUM(status IN ('CANCELED','EXPIRED','EXPIRED_IN_MATCH','REJECTED')), SUM(status IN ('NEW','PARTIALLY_FILLED','PENDING_NEW')), COALESCE(SUM(executed_qty),0)
                       FROM virtual_orders WHERE symbol=%s AND created_ms>=%s AND reset_epoch=%s""", (symbol, day_start_ms, self.epoch)); o = cur.fetchone()
        cur.execute("SELECT count(*), COALESCE(SUM(qty),0) FROM virtual_fills WHERE symbol=%s AND filled_ms>=%s", (symbol, day_start_ms)); f = cur.fetchone(); cur.close()
        return {'orders': int(o[0]), 'filled': int(o[1] or 0), 'canceled': int(o[2] or 0), 'open': int(o[3] or 0), 'exec_qty': Decimal(str(o[4])), 'trades': int(f[0]), 'trade_qty': Decimal(str(f[1]))}

    def record_reconcile(self, symbol: str, day: str, r: dict, now_ms: int):
        ex, my = r['ex'], r['my']; cur = self._cur()
        cur.execute("""INSERT INTO ledger_reconcile (reconciled_ms,as_of_day,symbol,ex_orders,ex_filled,ex_canceled,ex_open,ex_exec_qty,ex_trades,ex_trade_qty,my_orders,my_filled,my_canceled,my_open,my_exec_qty,my_trades,my_trade_qty,mismatch,detail)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (now_ms, day, symbol, ex['orders'], ex['filled'], ex['canceled'], ex['open'], str(ex['exec_qty']), ex['trades'], str(ex['trade_qty']),
                     my['orders'], my['filled'], my['canceled'], my['open'], str(my['exec_qty']), my['trades'], str(my['trade_qty']), r['mismatch'], r['detail']))
        cur.close()

    def is_open(self, order_id: int) -> bool:
        cur = self._cur(); cur.execute("SELECT status IN ('NEW','PARTIALLY_FILLED') FROM virtual_orders WHERE order_id=%s", (order_id,)); r = cur.fetchone(); cur.close(); return bool(r and r[0])

    def open_maker_orders(self):
        cur = self._cur(); cur.execute("SELECT order_id, symbol, orig_qty, price FROM virtual_orders WHERE strategy='maker-probe' AND status IN ('NEW','PARTIALLY_FILLED')"); r = cur.fetchall(); cur.close(); return r

    def apply_reset(self, note: str, recv_ms: int) -> dict:
        """리셋 판정 뒤: 이전 세대 주문·체결 물리 삭제(CDC 가 op=d 로 실어 나른다), 세대 +1, 원문 로그에 남김."""
        cur = self._cur()
        cur.execute("DELETE FROM virtual_fills WHERE order_id IN (SELECT order_id FROM (SELECT order_id FROM virtual_orders WHERE reset_epoch=%s) t)", (self.epoch,)); nf = cur.rowcount
        cur.execute("DELETE FROM virtual_orders WHERE reset_epoch=%s", (self.epoch,)); no = cur.rowcount
        self.epoch += 1
        cur.execute(INSERT_EVENT, ('RESET_DETECTED', f"reset:{recv_ms}", recv_ms, None, None, None, None, json.dumps({'note': note, 'deleted_orders': no, 'deleted_fills': nf, 'new_epoch': self.epoch}), recv_ms))
        cur.close(); return {'deleted_orders': no, 'deleted_fills': nf, 'epoch': self.epoch}


# ---------------------------------------------------------------- Binance 클라이언트
def rest_get(path: str, params: dict | None = None, signed: bool = False, key=None) -> dict | list:
    """서명 요청은 '서명한 문자열 = 보낸 쿼리 문자열' 이어야 한다(첫 실측 400: 정렬해 서명하고 삽입 순서로 보냈다). 정렬 순서로 만들어 그대로 보낸다."""
    params = dict(params or {})
    if signed:
        params['timestamp'] = int(time.time() * 1000); params['recvWindow'] = 10000
        qs = sign_payload(params)
        qs += '&signature=' + urllib.parse.quote(ed25519_sign(key, qs), safe='')
    else:
        qs = urllib.parse.urlencode(params)
    req = urllib.request.Request(f"{REST_URL}{path}" + ('?' + qs if qs else ''), headers={'X-MBX-APIKEY': API_KEY} if signed else {})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code} {e.read().decode()[:200]}") from None


class WsApi:
    """WebSocket API: 요청/응답(id 매칭) + 유저 데이터 이벤트(subscriptionId 래퍼)를 한 연결에서."""
    def __init__(self, key, on_event):
        self.key = key; self.on_event = on_event; self.ws = None; self.pending = {}; self.reconnects = 0

    async def connect(self):
        import websockets
        self.ws = await websockets.connect(WS_API_URL, ping_interval=20, ping_timeout=20, max_size=2 ** 22)
        asyncio.create_task(self._reader())
        r = await self.call('session.logon', {'apiKey': API_KEY, 'timestamp': int(time.time() * 1000)}, sign=True)
        log.info(f"logon: status={r.get('status')}")   # 키 조각도 로그에 남기지 않는다
        r = await self.call('userDataStream.subscribe', {})
        log.info(f"userDataStream.subscribe: status={r.get('status')} result={r.get('result')}")

    async def _reader(self):
        try:
            async for raw in self.ws:
                m = json.loads(raw)
                if 'event' in m:
                    await self.on_event(m['event'], int(time.time() * 1000))
                elif 'id' in m and m['id'] in self.pending:
                    self.pending.pop(m['id']).set_result(m)
                elif m.get('e'):
                    await self.on_event(m, int(time.time() * 1000))
        except Exception as e:
            log.warning(f"ws reader ended: {type(e).__name__}: {e}")
            for f in self.pending.values():
                if not f.done(): f.set_exception(RuntimeError('ws closed'))
            self.pending.clear(); self.ws = None

    async def call(self, method: str, params: dict, sign: bool = False, timeout: float = 15) -> dict:
        if self.ws is None: raise RuntimeError('ws not connected')
        params = dict(params)
        if sign:
            params['signature'] = ed25519_sign(self.key, sign_payload(params))
        rid = str(uuid.uuid4()); fut = asyncio.get_event_loop().create_future(); self.pending[rid] = fut
        await self.ws.send(json.dumps({'id': rid, 'method': method, 'params': params} if params else {'id': rid, 'method': method}))
        return await asyncio.wait_for(fut, timeout)

    async def signed(self, method: str, params: dict) -> dict:
        p = dict(params); p['timestamp'] = int(time.time() * 1000); p['recvWindow'] = 10000
        r = await self.call(method, p)
        if r.get('status') != 200:
            raise RuntimeError(f"{method} {r.get('status')} {r.get('error')}")
        return r['result']


@dataclass
class Stats:
    events: int = 0; dup_events: int = 0; orders: int = 0; amends: int = 0; cancels: int = 0; fills: int = 0; errors: int = 0; started: float = field(default_factory=time.time)


class Trader:
    def __init__(self):
        self.key = load_private_key(PRIVATE_KEY_PATH); self.ledger = Ledger(); self.api = WsApi(self.key, self.on_event)
        self.filters: dict[str, SymbolFilters] = {}; self.stats = Stats(); self.stop = asyncio.Event()
        self._capped_logged: set[str] = set()   # 일 상한 로그를 심볼당 하루 한 번만 (UTC 자정에 비운다)

    async def on_event(self, ev: dict, recv_ms: int):
        try:
            self.stats.events += 1
            if not self.ledger.record_event(ev, recv_ms):
                self.stats.dup_events += 1; return
            if ev.get('e') == 'executionReport':
                r = self.ledger.apply_report(ev)
                if r['fill']: self.stats.fills += 1
                if ev.get('x') == 'REPLACED': self.stats.amends += 0
                log.info(f"exec {ev['s']} {ev['c']} x={ev['x']} X={ev['X']} z={ev['z']}/{ev['q']} i={ev['i']} I={ev['I']}")
        except Exception as e:
            self.stats.errors += 1; log.error(f"event apply failed: {type(e).__name__}: {e} ev={str(ev)[:200]}")

    def load_filters(self):
        info = rest_get('/api/v3/exchangeInfo', {'symbols': json.dumps(SYMBOLS, separators=(',', ':'))})
        for s in info['symbols']:
            self.filters[s['symbol']] = SymbolFilters.from_exchange_info(s)
        log.info(f"filters: { {k: (fmt(v.tick_size), fmt(v.step_size), fmt(v.min_notional), v.amend_allowed) for k, v in self.filters.items()} }")

    async def place(self, symbol: str, p: dict, n: int) -> dict | None:
        cid = client_order_id(p['strategy'], symbol, n, int(time.time()))
        params = {'symbol': symbol, 'side': p['side'], 'type': p['type'], 'timeInForce': p['tif'], 'price': fmt(p['price']), 'quantity': fmt(p['qty']), 'newClientOrderId': cid, 'newOrderRespType': 'ACK'}
        try:
            r = await self.api.signed('order.place', params); self.stats.orders += 1
            log.info(f"placed {symbol} {p['strategy']} {p['side']} {p['type']}/{p['tif']} {fmt(p['qty'])}@{fmt(p['price'])} → orderId {r.get('orderId')}")
            return r
        except Exception as e:
            self.stats.errors += 1; log.warning(f"order.place failed {symbol} {p['strategy']}: {e}"); return None

    async def amend_then_cancel(self, symbol: str, order_id: int, qty: Decimal, price: Decimal):
        await asyncio.sleep(CANCEL_AFTER_SEC)
        if not self.ledger.is_open(order_id):
            return   # 그 사이 체결·취소됨(재기동 인수 주문에서 -2038 'Unknown order' 가 났던 원인) - 거래소에 없는 주문을 건드리지 않는다
        f = self.filters[symbol]; half = quantize(qty / 2, f.step_size)
        # 절반이 minNotional(가격×수량) 아래면 거래소가 -2038 'Filter failure: NOTIONAL' 로 거부(실측 09-19) → amend 건너뛰고 취소만
        if f.amend_allowed and half > 0 and half * price >= f.min_notional:
            try:
                await self.api.signed('order.amend.keepPriority', {'symbol': symbol, 'orderId': order_id, 'newQty': fmt(half)}); self.stats.amends += 1
                log.info(f"amended {symbol} orderId {order_id} qty → {fmt(half)} (keepPriority)")
            except Exception as e:
                self.stats.errors += 1; log.warning(f"amend failed {symbol} {order_id}: {e}")
            await asyncio.sleep(CANCEL_AFTER_SEC)
        if not self.ledger.is_open(order_id):
            return
        try:
            await self.api.signed('order.cancel', {'symbol': symbol, 'orderId': order_id}); self.stats.cancels += 1
            log.info(f"canceled {symbol} orderId {order_id}")
        except Exception as e:
            self.stats.errors += 1; log.warning(f"cancel failed {symbol} {order_id}: {e}")

    async def cycle(self):
        day_start_ms = int(time.time() // 86400 * 86400 * 1000)
        if getattr(self, '_capped_day', None) != day_start_ms:
            self._capped_day = day_start_ms; self._capped_logged.clear()
        for symbol in SYMBOLS:
            if symbol not in self.filters: continue
            n = self.ledger.orders_today(symbol, day_start_ms)
            if n >= MAX_ORDERS_PER_SYMBOL_PER_DAY:
                # 2026-09-20: 상한에 닿으면 STATS 카운터가 하루 내내 멈춘 채로 있어 '조용한 실패'처럼 보인다.
                # 실제로 한 번 그렇게 보고 4시간짜리 정지로 오해해 조사했다. 멈춘 이유를 로그에 남긴다.
                if symbol not in self._capped_logged:
                    self._capped_logged.add(symbol)
                    log.info(f"{symbol} 일 상한 도달 {n}/{MAX_ORDERS_PER_SYMBOL_PER_DAY} - 다음 UTC 자정까지 신규 주문 없음(정상)")
                continue
            try:
                bt = rest_get('/api/v3/ticker/bookTicker', {'symbol': symbol}); bid = Decimal(bt['bidPrice']); ask = Decimal(bt['askPrice'])
            except Exception as e:
                self.stats.errors += 1; log.warning(f"bookTicker failed {symbol}: {e}"); continue
            for p in plan_orders(symbol, bid, ask, self.filters[symbol], self.ledger.net_qty(symbol), ORDER_NOTIONAL_USDT, MAKER_TICKS):
                n += 1
                r = await self.place(symbol, p, n)
                if r and p['strategy'] == 'maker-probe':
                    asyncio.create_task(self.amend_then_cancel(symbol, int(r['orderId']), p['qty'], p['price']))
                await asyncio.sleep(1)

    async def check_reset(self):
        """열린 주문이 거래소에 없고(-2013) 최근 FILLED 주문도 없으면 리셋. 둘 다 없어야 한다 - 하나면 단순 취소·만료일 수 있다."""
        opened = self.ledger.open_orders()
        if not opened: return
        oid, symbol = opened[0][0], opened[0][1]
        try:
            await self.api.signed('order.status', {'symbol': symbol, 'orderId': int(oid)}); return
        except Exception as e:
            if '-2013' not in str(e): return
        last = self.ledger.last_filled(symbol)
        if last is None: return
        try:
            await self.api.signed('order.status', {'symbol': symbol, 'orderId': int(last)}); return
        except Exception as e:
            if '-2013' not in str(e): return
        r = self.ledger.apply_reset(f"open {oid} and filled {last} missing on exchange", int(time.time() * 1000))
        log.warning(f"TESTNET RESET detected → {r}")

    async def reconcile(self):
        """시간당: 거래소 REST(allOrders·myTrades, 당일) vs 우리 원장 → ledger_reconcile. 정답은 거래소."""
        day_start_ms = int(time.time() // 86400 * 86400 * 1000); day = time.strftime('%Y-%m-%d', time.gmtime()); now_ms = int(time.time() * 1000); bad = 0
        for symbol in SYMBOLS:
            try:
                ex_orders = rest_get('/api/v3/allOrders', {'symbol': symbol, 'startTime': day_start_ms, 'limit': 1000}, signed=True, key=self.key)
                ex_trades = rest_get('/api/v3/myTrades', {'symbol': symbol, 'startTime': day_start_ms, 'limit': 1000}, signed=True, key=self.key)
                r = compare_ledger(ex_orders, ex_trades, self.ledger.my_summary(symbol, day_start_ms)); self.ledger.record_reconcile(symbol, day, r, now_ms); bad += r['mismatch']
                if r['mismatch']: log.warning(f"reconcile MISMATCH {symbol}: {r['detail']}")
            except Exception as e:
                self.stats.errors += 1; log.warning(f"reconcile failed {symbol}: {e}")
        log.info(f"reconcile done: {len(SYMBOLS)} symbols, mismatch {bad}")

    async def adopt_open_orders(self):
        """재기동 시 메모리의 amend→cancel 예약이 사라지므로, 원장의 열린 maker 주문을 다시 예약한다 (안 하면 영원히 NEW)."""
        for oid, symbol, qty, price in self.ledger.open_maker_orders():
            log.info(f"adopt open maker order {symbol} {oid} → amend/cancel 예약"); asyncio.create_task(self.amend_then_cancel(symbol, int(oid), Decimal(str(qty)), Decimal(str(price))))

    async def snapshot(self):
        try:
            acct = await self.api.signed('account.status', {'omitZeroBalances': True}); self.ledger.snapshot_positions(acct['balances'], int(time.time() * 1000))
            log.info(f"positions snapshot: {len(acct['balances'])} assets")
        except Exception as e:
            self.stats.errors += 1; log.warning(f"account snapshot failed: {e}")

    async def run(self):
        self.ledger.connect(); self.load_filters()
        last_stats = time.time(); last_cycle = 0.0; last_snapshot = 0.0; last_reset_check = 0.0; last_reconcile = 0.0; adopted = False
        while not self.stop.is_set():
            if self.api.ws is None:
                try:
                    await self.api.connect(); self.api.reconnects += 1
                except Exception as e:
                    self.stats.errors += 1; log.warning(f"connect failed: {e}"); await asyncio.sleep(10); continue
            if not adopted: await self.adopt_open_orders(); adopted = True
            now = time.time()
            if now - last_snapshot >= 3600: await self.snapshot(); last_snapshot = now
            if now - last_reconcile >= 3600 and now - self.stats.started >= 120: await self.reconcile(); last_reconcile = now
            if now - last_reset_check >= 600: await self.check_reset(); last_reset_check = now
            if now - last_cycle >= CYCLE_SEC: await self.cycle(); last_cycle = now
            if now - last_stats >= STATS_INTERVAL:
                s = self.stats; log.info(f"[STATS] events={s.events} dup={s.dup_events} orders={s.orders} amends={s.amends} cancels={s.cancels} fills={s.fills} errors={s.errors} epoch={self.ledger.epoch} ws_reconnects={self.api.reconnects} uptime={int(now - s.started)}s"); last_stats = now
            await asyncio.sleep(1)


def main():
    if not API_KEY:
        log.error('BINANCE_API_KEY 가 비어 있음 - 테스트넷 키 발급 후 .env 에 넣는다 (docs/28 B-4)'); sys.exit(2)
    t = Trader(); loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, t.stop.set)
    loop.run_until_complete(t.run())


if __name__ == '__main__':
    main()
