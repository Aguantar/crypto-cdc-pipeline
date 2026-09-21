{% macro generate_ohlcv(time_expr, group_cols) %}
    argMin(trade_price, trade_time_kst) AS open,
    max(trade_price) AS high,
    min(trade_price) AS low,
    argMax(trade_price, trade_time_kst) AS close,
    sum(trade_volume) AS volume,
    sum(trade_amount) AS amount,
    count(*) AS trade_count,
    countIf(ask_bid = 'BID') AS bid_count,
    countIf(ask_bid = 'ASK') AS ask_count,
    if(sum(trade_volume) > 0,
       sum(trade_amount) / sum(trade_volume),
       0
    ) AS vwap
{% endmacro %}
