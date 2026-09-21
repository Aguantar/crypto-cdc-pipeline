"""네트워크·DB 없이 도는 부분만: 서명 규칙, 필터 양자화, 주문 계획, 이벤트→행 매핑, 중복 키."""
import sys, os, base64
from decimal import Decimal
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.environ.setdefault('BINANCE_API_KEY', 'x')
import trader as T


def test_sign_payload_sorted_without_signature():
    p = {'timestamp': 1, 'symbol': 'BTCUSDT', 'apiKey': 'k', 'signature': 'zzz', 'side': 'BUY'}
    assert T.sign_payload(p) == 'apiKey=k&side=BUY&symbol=BTCUSDT&timestamp=1'


def test_ed25519_signature_is_base64_and_verifies():
    from cryptography.hazmat.primitives.asymmetric import ed25519
    k = ed25519.Ed25519PrivateKey.generate()
    sig = T.ed25519_sign(k, 'apiKey=k&timestamp=1')
    k.public_key().verify(base64.b64decode(sig), b'apiKey=k&timestamp=1')


def _filters(tick='0.01', step='0.00001', min_notional='5'):
    return T.SymbolFilters(tick_size=Decimal(tick), step_size=Decimal(step), min_notional=Decimal(min_notional), amend_allowed=True)


def test_quantize_rounds_down_to_step_and_formats_without_exponent():
    assert T.fmt(T.quantize(Decimal('0.000246913'), Decimal('0.00001'))) == '0.00024'
    assert T.fmt(T.quantize(Decimal('81045.917'), Decimal('0.01'))) == '81045.91'
    assert T.fmt(T.quantize(Decimal('123.7'), Decimal('1'))) == '123'


def test_plan_orders_rules_a_b_c():
    f = _filters()
    plans = T.plan_orders('BTCUSDT', Decimal('81045.90'), Decimal('81045.91'), f, net_qty=Decimal('0.0003'), notional=Decimal('20'), maker_ticks=5)
    by = {p['strategy']: p for p in plans}
    assert set(by) == {'maker-probe', 'taker-ioc', 'unwind'}
    assert by['maker-probe']['price'] == Decimal('81045.85') and by['maker-probe']['tif'] == 'GTC'
    assert by['taker-ioc']['price'] == Decimal('81045.91') and by['taker-ioc']['tif'] == 'IOC'
    assert by['unwind']['side'] == 'SELL' and by['unwind']['qty'] == Decimal('0.00030')
    assert all(p['qty'] * p['price'] >= f.min_notional for p in plans)


def test_plan_orders_skips_when_below_min_notional():
    assert T.plan_orders('X', Decimal('100'), Decimal('101'), _filters(step='1', min_notional='500'), Decimal('0'), Decimal('20'), 5) == []


def _report(**kw):
    ev = {'e': 'executionReport', 'E': 1789800000100, 's': 'BTCUSDT', 'c': 'ioc_20260919_BTCUSDT_3_12', 'S': 'BUY', 'o': 'LIMIT', 'f': 'IOC',
          'q': '0.00024', 'p': '81045.91', 'x': 'NEW', 'X': 'NEW', 'r': 'NONE', 'i': 555, 'l': '0', 'z': '0', 'L': '0', 'n': '0', 'N': None,
          'T': 1789800000099, 't': -1, 'I': 9001, 'w': True, 'm': False, 'O': 1789800000000, 'Z': '0', 'Y': '0', 'Q': '0'}
    ev.update(kw); return ev


def test_order_row_and_fill_row_mapping():
    row = T.order_row_from_report(_report(), 'taker-ioc')
    assert row['order_id'] == 555 and row['status'] == 'NEW' and row['created_ms'] == 1789800000000 and row['updated_ms'] == 1789800000100 and row['last_exec_id'] == 9001
    assert T.fill_row_from_report(_report(), 'taker-ioc') is None
    tr = _report(x='TRADE', X='FILLED', l='0.00024', z='0.00024', L='81045.91', Y='19.45', Z='19.45', t=777, I=9002, n='0.00001', N='BNB', m=False, E=1789800000200)
    fill = T.fill_row_from_report(tr, 'taker-ioc')
    assert fill['fill_id'] == 777 and fill['qty'] == '0.00024' and fill['is_maker'] == 0 and fill['exec_id'] == 9002
    assert T.order_row_from_report(tr, 'taker-ioc')['executed_qty'] == '0.00024'


def test_cancel_report_keeps_original_client_id_and_dedup_keys():
    c = _report(x='CANCELED', X='CANCELED', c='cancel-req-id', C='mkr_20260919_BTCUSDT_1_5', I=9010)
    assert T.order_row_from_report(c, 'maker-probe')['client_order_id'] == 'mkr_20260919_BTCUSDT_1_5'
    assert T.strategy_of('mkr_20260919_BTCUSDT_1_5') == 'maker-probe'
    cid = T.client_order_id('unwind', 'BTCUSDT', 12, 1789800000)
    import re
    assert cid.startswith('unw_20260919_BTCUSDT_12_') and re.fullmatch(r'[a-zA-Z0-9-_]{1,36}', cid) and T.strategy_of(cid) == 'unwind'
    assert T.dedup_key(c) == 'exec:9010'
    assert T.dedup_key({'e': 'outboundAccountPosition', 'E': 5}) == 'outboundAccountPosition:5'


def test_compare_ledger_flags_only_real_differences():
    ex_orders = [{'status': 'FILLED', 'executedQty': '0.00024'}, {'status': 'CANCELED', 'executedQty': '0'}, {'status': 'NEW', 'executedQty': '0'}]
    ex_trades = [{'qty': '0.00024'}]
    my = {'orders': 3, 'filled': 1, 'canceled': 1, 'open': 1, 'exec_qty': Decimal('0.00024000'), 'trades': 1, 'trade_qty': Decimal('0.00024')}
    r = T.compare_ledger(ex_orders, ex_trades, my)
    assert r['mismatch'] == 0 and r['detail'] is None
    my2 = dict(my, trades=0, trade_qty=Decimal('0'))
    r2 = T.compare_ledger(ex_orders, ex_trades, my2)
    assert r2['mismatch'] == 1 and 'trades:ex=1/my=0' in r2['detail'] and 'trade_qty' in r2['detail']
