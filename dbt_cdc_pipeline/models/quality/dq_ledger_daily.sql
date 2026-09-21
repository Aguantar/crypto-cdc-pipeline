{{ config(order_by='(day_utc, symbol)') }}
-- 2층 원장 3자 대조 (docs/28 B-5): 거래소(REST) = MySQL = ClickHouse FINAL.
-- 거래소·MySQL 수는 생성기가 시간당 기록한 ledger_reconcile 의 하루 마지막 행, ClickHouse 수는 여기서 FINAL 로 센다.
-- 셋이 다 같아야 CDC 가 변경을 하나도 놓치지 않은 것. 하나라도 다르면 어느 구간이 틀렸는지 열 이름이 말해 준다(ex_/my_/ch_).
--
-- 2026-09-20 정정: 열린 날은 항상 불일치로 나왔다. MySQL·거래소 수는 생성기가 매시 남긴 스냅샷인데
-- ClickHouse 수는 dbt 실행 시점의 현재 값이라, 두 값이 서로 다른 시각을 재고 있었다.
-- 실제로 09-20 07:07 실행에서 5심볼 전부 ch 가 my 보다 정확히 9씩 많았다(그 사이에 낸 주문). 데이터는 멀쩡했다.
-- → ClickHouse 쪽을 그 (날짜, 심볼)의 마지막 대조 시각까지로 자른다. 같은 순간을 재야 비교가 성립한다.
--
-- 자르는 기준이 열마다 다르다(이걸 한 번에 못 맞춰 2건이 남았었다):
--   주문 수  → created_ms  ≤ 기준시각 (MySQL 의 my_orders 도 '생성'을 센다)
--   상태 집계 → updated_ms ≤ 기준시각. 기준시각 전에 생겼지만 그 뒤에 체결된 주문은 스냅샷엔 '미체결'인데
--              ClickHouse 엔 현재 상태(FILLED)로 있다. 실제로 BTC·SOL 에서 정확히 그 1건씩이 남았다.
--   체결 수  → filled_ms   ≤ 기준시각
WITH last_rec AS (
    SELECT as_of_day AS day_utc, symbol, argMax(ex_orders, reconciled_ms) AS ex_orders, argMax(ex_filled, reconciled_ms) AS ex_filled,
           argMax(ex_canceled, reconciled_ms) AS ex_canceled, argMax(ex_open, reconciled_ms) AS ex_open, argMax(ex_exec_qty, reconciled_ms) AS ex_exec_qty,
           argMax(ex_trades, reconciled_ms) AS ex_trades, argMax(ex_trade_qty, reconciled_ms) AS ex_trade_qty,
           argMax(my_orders, reconciled_ms) AS my_orders, argMax(my_filled, reconciled_ms) AS my_filled, argMax(my_exec_qty, reconciled_ms) AS my_exec_qty,
           argMax(my_trades, reconciled_ms) AS my_trades, argMax(my_trade_qty, reconciled_ms) AS my_trade_qty,
           argMax(mismatch, reconciled_ms) AS ex_my_mismatch, max(reconciled_ms) AS last_reconciled_ms
    FROM {{ source('raw', 'ledger_reconcile') }} FINAL
    GROUP BY as_of_day, symbol
),
bounds AS (   -- (날짜, 심볼)별 '언제까지를 비교 대상으로 볼 것인가'
    SELECT day_utc, symbol, last_reconciled_ms AS cutoff_ms FROM last_rec
),
ch_orders AS (
    SELECT b.day_utc AS day_utc, b.symbol AS symbol, count() AS ch_orders,
           countIf(v.status = 'FILLED' AND v.updated_ms <= b.cutoff_ms) AS ch_filled,
           countIf(v.status IN ('CANCELED', 'EXPIRED', 'EXPIRED_IN_MATCH', 'REJECTED') AND v.updated_ms <= b.cutoff_ms) AS ch_canceled,
           sum(if(v.updated_ms <= b.cutoff_ms, v.executed_qty, toDecimal64(0, 8))) AS ch_exec_qty
    FROM {{ source('raw', 'virtual_orders') }} AS v FINAL
    INNER JOIN bounds AS b ON b.symbol = v.symbol AND b.day_utc = toDate(fromUnixTimestamp64Milli(v.created_ms))
    WHERE v.created_ms <= b.cutoff_ms
    GROUP BY day_utc, symbol
),
ch_fills AS (
    SELECT b.day_utc AS day_utc, b.symbol AS symbol, count() AS ch_trades, sum(f.qty) AS ch_trade_qty
    FROM {{ source('raw', 'virtual_fills') }} AS f FINAL
    INNER JOIN bounds AS b ON b.symbol = f.symbol AND b.day_utc = toDate(fromUnixTimestamp64Milli(f.filled_ms))
    WHERE f.filled_ms <= b.cutoff_ms
    GROUP BY day_utc, symbol
)
SELECT
    r.day_utc AS day_utc, r.symbol AS symbol,
    r.ex_orders AS ex_orders, r.my_orders AS my_orders, o.ch_orders AS ch_orders,
    r.ex_filled AS ex_filled, r.my_filled AS my_filled, o.ch_filled AS ch_filled,
    r.ex_exec_qty AS ex_exec_qty, r.my_exec_qty AS my_exec_qty, o.ch_exec_qty AS ch_exec_qty,
    r.ex_trades AS ex_trades, r.my_trades AS my_trades, f.ch_trades AS ch_trades,
    r.ex_trade_qty AS ex_trade_qty, r.my_trade_qty AS my_trade_qty, f.ch_trade_qty AS ch_trade_qty,
    r.ex_my_mismatch AS ex_my_mismatch,
    toUInt8(r.my_orders != o.ch_orders OR r.my_filled != o.ch_filled OR r.my_exec_qty != o.ch_exec_qty OR r.my_trades != f.ch_trades OR r.my_trade_qty != f.ch_trade_qty) AS my_ch_mismatch,
    fromUnixTimestamp64Milli(r.last_reconciled_ms) AS reconciled_at
FROM last_rec AS r
LEFT JOIN ch_orders AS o ON o.day_utc = r.day_utc AND o.symbol = r.symbol
LEFT JOIN ch_fills AS f ON f.day_utc = r.day_utc AND f.symbol = r.symbol
