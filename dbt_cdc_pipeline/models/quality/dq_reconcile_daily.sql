{{ config(order_by='day_utc') }}
-- 시간 규약(docs/34 #3): day_utc = UTC 하루(거래소 정답과 같은 기준). KST 하루는 day_kst 로만 부른다.
-- 파이프라인 품질 층 (docs/22): 일별 원장 대조 요약. 유실 판정의 단일 근거(int_reconcile_hourly)를 하루 단위로 접는다.
-- 셀 단위 최소값을 함께 두는 이유: 가중 비율은 99.3% 인데 한 마켓·한 시간이 50% 인 경우(09-15 TIA)를 가중치가 가린다.
SELECT
    toDate(hour_utc)                                        AS day_utc,
    round(100 * sum(ch_vol) / sum(candle_vol), 3)           AS weighted_pct,
    count()                                                 AS cells,
    countIf(ratio_pct < 99)                                 AS cells_below_99,
    countIf(ch_n = 0)                                       AS cells_no_rows,
    uniqExactIf(market, ch_n = 0)                           AS markets_no_rows,
    round(min(ratio_pct), 2)                                AS min_cell_pct,
    argMin(concat(market, ' ', toString(hour_utc)), ratio_pct) AS worst_cell,
    max(reconciled_at)                                      AS reconciled_at
FROM {{ ref('int_reconcile_hourly') }}
WHERE candle_vol > 0
GROUP BY day_utc
