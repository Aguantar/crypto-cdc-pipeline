#!/usr/bin/env bash
# docs/16 이상탐지 기준의 모든 결론을 재생산하는 쿼리 모음. 처음부터 끝까지 실행해 출력이 문서와 일치하는지 확인한다.
# 입력 테이블: crypto_trades(287마켓 체결, 09-09~), upbit_market_event_records(거래소 경보 이력), upbit_daily_candles(일봉 200일)
# 실행: scripts/analysis/rule_basis_check.sh > ~/pipeline-observation/rule_basis_check_$(date +%Y%m%d).txt
set -u
CH="docker exec cdc-clickhouse clickhouse-client -d cdc_pipeline --max_memory_usage=700000000 --max_threads=2"
q() { echo; echo "### $1"; echo "$2" | sed 's/^/    /'; echo; $CH -q "$2 FORMAT PrettyCompactNoEscapes" 2>&1; }

q "Q0 라벨 개요: 유형별 에피소드 수·현재 진행 중·일평균·지속시간" "
SELECT event_type, count() episodes, countIf(trigger_type='TRIGGER') active_now,
       round(count()/dateDiff('day', min(trigger_time_utc), max(trigger_time_utc)),1) per_day,
       round(quantile(0.5)(dateDiff('minute', trigger_time_utc, expiration_time_utc))) dur_p50_min
FROM upbit_market_event_records FINAL GROUP BY event_type ORDER BY episodes DESC"

q "Q1 가격: 관찰 주간 PRICE_FLUCTUATIONS 지정 시각의 |분 종가 / 24h 전 분 종가 - 1| (ASOF 조인, 우리 체결 데이터)" "
WITH ep AS (SELECT market, warning_level, trigger_time_utc t, trigger_time_utc - INTERVAL 24 HOUR t24
            FROM upbit_market_event_records FINAL
            WHERE event_type='PRICE_FLUCTUATIONS' AND trigger_time_utc >= '2026-09-10 06:10:00' AND trigger_time_utc < '2026-09-16 06:10:00'),
     px AS (SELECT market, toStartOfMinute(fromUnixTimestamp64Milli(upbit_timestamp)) m, argMax(trade_price, upbit_timestamp) p
            FROM crypto_trades WHERE source_ts >= '2026-09-09 00:00:00' AND market IN (SELECT market FROM ep) GROUP BY market, m),
     j1 AS (SELECT ep.market, ep.warning_level, ep.t, ep.t24, px.p p_now FROM ep ASOF JOIN px ON ep.market = px.market AND px.m <= ep.t),
     j  AS (SELECT j1.*, px.p p_24h FROM j1 ASOF JOIN px ON j1.market = px.market AND px.m <= j1.t24)
SELECT warning_level, count() n, round(min(abs(p_now/p_24h-1))*100,1) min_pct, round(quantile(0.1)(abs(p_now/p_24h-1))*100,1) p10,
       round(quantile(0.5)(abs(p_now/p_24h-1))*100,1) p50, round(max(abs(p_now/p_24h-1))*100,1) max_pct, countIf(p_now>p_24h) up, countIf(p_now<p_24h) down
FROM j WHERE p_now>0 AND p_24h>0 GROUP BY warning_level ORDER BY warning_level"

q "Q2 가격: 같은 지표의 5% 구간 히스토그램" "
WITH ep AS (SELECT market, warning_level, trigger_time_utc t, trigger_time_utc - INTERVAL 24 HOUR t24
            FROM upbit_market_event_records FINAL
            WHERE event_type='PRICE_FLUCTUATIONS' AND trigger_time_utc >= '2026-09-10 06:10:00' AND trigger_time_utc < '2026-09-16 06:10:00'),
     px AS (SELECT market, toStartOfMinute(fromUnixTimestamp64Milli(upbit_timestamp)) m, argMax(trade_price, upbit_timestamp) p
            FROM crypto_trades WHERE source_ts >= '2026-09-09 00:00:00' AND market IN (SELECT market FROM ep) GROUP BY market, m),
     j1 AS (SELECT ep.market, ep.warning_level, ep.t, ep.t24, px.p p_now FROM ep ASOF JOIN px ON ep.market = px.market AND px.m <= ep.t),
     j  AS (SELECT j1.*, px.p p_24h FROM j1 ASOF JOIN px ON j1.market = px.market AND px.m <= j1.t24)
SELECT warning_level, floor(abs(p_now/p_24h-1)*100/5)*5 bin_pct, count() c FROM j WHERE p_now>0 AND p_24h>0 GROUP BY 1,2 ORDER BY 1,2"

q "Q3 가격 역검증(정밀도): 287마켓 전체에서 |24h 변동| >= 50% 인 분이 거래소 에피소드(±2분) 안에 있는가" "
WITH px AS (SELECT market, toStartOfMinute(fromUnixTimestamp64Milli(upbit_timestamp)) m, argMax(trade_price, upbit_timestamp) p
            FROM crypto_trades WHERE source_ts >= '2026-09-09 00:00:00' AND source_ts < '2026-09-16 06:10:00' GROUP BY market, m),
     chg AS (SELECT a.market, a.m FROM px a JOIN px b ON a.market=b.market AND b.m = a.m - INTERVAL 24 HOUR
             WHERE a.m >= '2026-09-10 06:10:00' AND a.m < '2026-09-16 06:10:00' AND abs(a.p/b.p-1) >= 0.5),
     ep AS (SELECT market, trigger_time_utc - INTERVAL 2 MINUTE t0, ifNull(expiration_time_utc, now()) + INTERVAL 2 MINUTE t1
            FROM upbit_market_event_records FINAL WHERE event_type='PRICE_FLUCTUATIONS' AND trigger_time_utc >= '2026-09-09'),
     x AS (SELECT chg.market, chg.m, max(ep.t0 <= chg.m AND chg.m <= ep.t1) inside FROM chg LEFT JOIN ep ON chg.market = ep.market GROUP BY chg.market, chg.m)
SELECT count() minutes_ge50, countIf(inside) inside_ep, countIf(NOT inside) outside, uniqExactIf(market, NOT inside) outside_markets FROM x"

q "Q4 가격 재현율: 에피소드 안의 모든 분 중 지표가 46%/50%/96% 이상인 비율" "
WITH px AS (SELECT market, toStartOfMinute(fromUnixTimestamp64Milli(upbit_timestamp)) m, argMax(trade_price, upbit_timestamp) p
            FROM crypto_trades WHERE source_ts >= '2026-09-09 00:00:00' AND source_ts < '2026-09-16 06:10:00'
              AND market IN (SELECT market FROM upbit_market_event_records WHERE event_type='PRICE_FLUCTUATIONS' AND trigger_time_utc >= '2026-09-10') GROUP BY market, m),
     chg AS (SELECT a.market, a.m, abs(a.p/b.p-1) r FROM px a JOIN px b ON a.market=b.market AND b.m = a.m - INTERVAL 24 HOUR
             WHERE a.m >= '2026-09-10 06:10:00' AND a.m < '2026-09-16 06:10:00'),
     ep AS (SELECT market, warning_level, trigger_time_utc t0, ifNull(expiration_time_utc, now()) t1 FROM upbit_market_event_records FINAL
            WHERE event_type='PRICE_FLUCTUATIONS' AND trigger_time_utc >= '2026-09-10 06:10:00' AND trigger_time_utc < '2026-09-16 06:10:00')
SELECT warning_level, count() ep_minutes, round(countIf(r>=0.46)/count(),3) ge46, round(countIf(r>=0.50)/count(),3) ge50, round(countIf(r>=0.96)/count(),3) ge96, round(min(r)*100,1) min_pct
FROM ep JOIN chg USING market WHERE chg.m >= ep.t0 AND chg.m <= ep.t1 GROUP BY warning_level"

q "Q5 표본 독립성: 에피소드 수 vs 마켓 수 vs 마켓-일 수 (관찰 주간 / 6개월)" "
SELECT 'obs_week' w, count() episodes, uniqExact(market) markets, uniqExact((market, toDate(trigger_time_utc))) market_days FROM upbit_market_event_records FINAL
WHERE event_type='PRICE_FLUCTUATIONS' AND trigger_time_utc >= '2026-09-10 06:10:00' AND trigger_time_utc < '2026-09-16 06:10:00'
UNION ALL SELECT '6_months', count(), uniqExact(market), uniqExact((market, toDate(trigger_time_utc))) FROM upbit_market_event_records FINAL WHERE event_type='PRICE_FLUCTUATIONS'"

q "Q6 거래량: 일봉 거래대금 / 직전 7일(30일) 평균, 라벨 = 다음날 01:00 UTC 지정. 임계 스윕 (6개월)" "
WITH c AS (SELECT market, day, amount,
        avg(amount) OVER (PARTITION BY market ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) a7,
        avg(amount) OVER (PARTITION BY market ORDER BY day ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) a30,
        count() OVER (PARTITION BY market ORDER BY day ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) nb
      FROM upbit_daily_candles FINAL WHERE day >= '2026-03-01'),
 lab AS (SELECT market, toDate(trigger_time_utc) - 1 day FROM upbit_market_event_records FINAL WHERE event_type='TRADING_VOLUME_SOARING' AND toHour(trigger_time_utc)=1 GROUP BY 1,2),
 j AS (SELECT c.*, lab.day != toDate('1970-01-01') pos FROM c LEFT JOIN lab USING (market, day) WHERE nb>=30 AND day >= '2026-03-24' AND day < '2026-09-16')
SELECT k, t, tp, fp, fn, round(tp/(tp+fp),2) precision, round(tp/(tp+fn),2) recall FROM (
 SELECT 'r7' k, t, countIf(amount/a7>=t AND pos) tp, countIf(amount/a7>=t AND NOT pos) fp, countIf(amount/a7<t AND pos) fn FROM j ARRAY JOIN [3,4,5,8] AS t GROUP BY t
 UNION ALL SELECT 'r30', t, countIf(amount/a30>=t AND pos), countIf(amount/a30>=t AND NOT pos), countIf(amount/a30<t AND pos) FROM j ARRAY JOIN [3,4,5,8] AS t GROUP BY t) ORDER BY k, t"

q "Q7 거래량: 7일 평균 임계 × 절대 하한(원) 조합" "
WITH c AS (SELECT market, day, amount, avg(amount) OVER (PARTITION BY market ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) a7,
        count() OVER (PARTITION BY market ORDER BY day ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) nb FROM upbit_daily_candles FINAL WHERE day >= '2026-03-01'),
 lab AS (SELECT market, toDate(trigger_time_utc) - 1 day FROM upbit_market_event_records FINAL WHERE event_type='TRADING_VOLUME_SOARING' AND toHour(trigger_time_utc)=1 GROUP BY 1,2),
 j AS (SELECT c.*, lab.day != toDate('1970-01-01') pos FROM c LEFT JOIN lab USING (market, day) WHERE nb>=30 AND day >= '2026-03-24' AND day < '2026-09-16')
SELECT tf.1 t, tf.2 fl, tp, fp, fn, round(tp/(tp+fp),2) prec, round(tp/(tp+fn),2) rec FROM (
  SELECT tf, countIf(amount/a7>=tf.1 AND amount>=tf.2 AND pos) tp, countIf(amount/a7>=tf.1 AND amount>=tf.2 AND NOT pos) fp, countIf(NOT(amount/a7>=tf.1 AND amount>=tf.2) AND pos) fn
  FROM j ARRAY JOIN [(4,0.),(4,5e8),(4,1e9),(5,0.),(5,1e9)] AS tf GROUP BY tf) ORDER BY t, fl"

q "Q8 거래량 감사: 라벨 날짜를 -2/-1/0/+1일로 어긋나게 붙였을 때. 맞는 정렬(+1)에서만 성능이 나와야 한다" "
WITH c AS (SELECT market, day, amount, avg(amount) OVER (PARTITION BY market ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) a7,
        count() OVER (PARTITION BY market ORDER BY day ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) nb FROM upbit_daily_candles FINAL WHERE day >= '2026-03-01'),
 lab AS (SELECT market, toDate(trigger_time_utc) tday FROM upbit_market_event_records FINAL WHERE event_type='TRADING_VOLUME_SOARING' AND toHour(trigger_time_utc)=1 GROUP BY 1,2)
SELECT sh, countIf(pos) positives, countIf(amount/a7>=4 AND pos) tp, countIf(amount/a7>=4 AND NOT pos) fp,
       round(countIf(amount/a7>=4 AND pos)/countIf(amount/a7>=4),2) prec, round(countIf(amount/a7>=4 AND pos)/countIf(pos),2) rec
FROM (SELECT c.*, sh, lab.tday != toDate('1970-01-01') AS pos FROM c ARRAY JOIN [-2,-1,0,1] AS sh LEFT JOIN lab ON c.market=lab.market AND c.day + sh = lab.tday
      WHERE nb>=30 AND day >= '2026-03-24' AND day < '2026-09-16') GROUP BY sh ORDER BY sh"

q "Q9 거래량 감사: 기준 변형 (수량/7일 평균, 금액/7일 중앙값), 임계 4" "
WITH c AS (SELECT market, day, amount, volume, avg(amount) OVER (PARTITION BY market ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) a7,
        avg(volume) OVER (PARTITION BY market ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) v7,
        quantileExact(0.5)(amount) OVER (PARTITION BY market ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) med7,
        count() OVER (PARTITION BY market ORDER BY day ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) nb FROM upbit_daily_candles FINAL WHERE day >= '2026-03-01'),
 lab AS (SELECT market, toDate(trigger_time_utc) - 1 day FROM upbit_market_event_records FINAL WHERE event_type='TRADING_VOLUME_SOARING' AND toHour(trigger_time_utc)=1 GROUP BY 1,2),
 j AS (SELECT c.*, lab.day != toDate('1970-01-01') AS pos FROM c LEFT JOIN lab USING (market, day) WHERE nb>=30 AND day >= '2026-03-24' AND day < '2026-09-16')
SELECT k, tp, fp, fn, round(tp/(tp+fp),2) prec, round(tp/(tp+fn),2) rec FROM (
 SELECT 'amount/avg7' k, countIf(amount/a7>=4 AND pos) tp, countIf(amount/a7>=4 AND NOT pos) fp, countIf(amount/a7<4 AND pos) fn FROM j
 UNION ALL SELECT 'volume/avg7', countIf(volume/v7>=4 AND pos), countIf(volume/v7>=4 AND NOT pos), countIf(volume/v7<4 AND pos) FROM j
 UNION ALL SELECT 'amount/median7', countIf(amount/med7>=4 AND pos), countIf(amount/med7>=4 AND NOT pos), countIf(amount/med7<4 AND pos) FROM j) ORDER BY k"

echo; echo "### Q10 가격 독립 검증(6개월, 업비트 1분봉): scripts/analysis/verify_price_threshold.py --report (저장된 결과 재집계)"
python3 "$(dirname "$0")/verify_price_threshold.py" --report
