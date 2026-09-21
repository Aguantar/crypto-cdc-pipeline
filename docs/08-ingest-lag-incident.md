# 08. 적재 지연 사고 분석 - 2026-08-19 ~ 08-30 (유실 의심 → 지연으로 판정)

- 분석일: 2026-09-09 (UTC 09-08 23:18 ~ 23:48, KST 09-09 08:18 ~ 08:48)
- 발단: 사전 검증(09-09 아침)에서 producer 재연결 직후인 08-29를 경계로 ClickHouse 일별 적재 건수가 690K → 364K로 급감한 것을 발견. "재연결 후 부분 구독 실패 = 유실" 가능성 제기.
- 방법: 읽기 전용. 업비트 REST 일봉(`/v1/candles/days`, UTC 일 경계)을 외부 기준으로 삼아 ClickHouse `cdc_pipeline.crypto_trades`와 코인·일 단위로 대조. 쿼리는 `readonly_user`, `max_memory_usage=600MB, max_threads=2` 캡.
- 산출물: `~/cdc-orderbook-probe/out/candles/` (days_5coins.json, ch_daily_5coins.json, ch_daily_by_upbit_ts.json, ratio_table.json, hourly_lag.json, queue_depth.json)

## 1. 판정: 유실 아님, 적재 지연

### 1-1. 1차 대조(적재 시각 기준) - 오판을 유발하는 지표

`source_ts`(MySQL binlog 시각 = INSERT 시각)로 일자를 나누면 08-19~08-31 구간의 적재량/시장량 비율이 41%~322%로 요동한다. 동일 일자에 5개 코인이 같은 방향으로 움직이는 것이 특징(예: 08-22 전 코인 41~63%, 08-29 전 코인 291~324%).

| 날짜(UTC) | BTC | ETH | XRP | SOL | DOGE |
|---|---|---|---|---|---|
| 08-18 | 99.6% | 99.6% | 99.8% | 99.6% | 99.6% |
| 08-22 | 58.8% | 62.8% | 41.2% | 51.7% | 52.4% |
| 08-26 | 179.4% | 109.6% | 110.2% | 229.0% | 160.5% |
| 08-29 | 297.8% | 321.9% | 292.6% | 290.6% | 298.0% |
| 09-01 | 99.6% | 99.5% | 99.7% | 99.5% | 99.8% |

(비율 = ClickHouse `sum(trade_volume)` ÷ 업비트 일봉 `candle_acc_trade_volume`. 전체 표는 ratio_table.json)

### 1-2. 2차 대조(체결 시각 기준) - 유실 없음

`upbit_timestamp`(거래소 체결 시각)로 일자를 다시 나누면 **08-18 ~ 09-06 전 기간, 5개 코인 모두 97.5% ~ 99.9%** 로 일치한다. 즉 체결은 전부 수집됐고, MySQL에 늦게 들어갔을 뿐이다.

| 날짜(UTC) | BTC | ETH | XRP | SOL | DOGE |
|---|---|---|---|---|---|
| 08-19 | 99.2% | 99.1% | 99.1% | 98.9% | 98.3% |
| 08-22 | 98.8% | 98.2% | 99.1% | 98.3% | 97.9% |
| 08-26 | 99.4% | 99.2% | 99.5% | 99.5% | 99.2% |
| 08-29 | 99.8% | 99.6% | 99.7% | 99.8% | 99.7% |
| 09-04 | 99.0% | 98.8% | 99.3% | 99.0% | 98.7% |

잔여 0.1~2.5%는 정상 기간(09-01~09-06)에도 동일하게 존재하는 기저 차이(재연결 공백 ≈5초/회, 일봉 집계 방식 차이 등)로, 이번 사고와 무관.

## 2. 지연의 크기와 시계열

지연 = `source_ts − upbit_timestamp`. 시간별 p50/max (UTC, 6시간 간격 발췌. 전체는 hourly_lag.json):

| 시각(UTC) | 체결 msg/s | 지연 p50 | 지연 max |
|---|---|---|---|
| 08-19 12:00 | 3.22 | 5 s | 87 s |
| 08-19 18:00 | 2.59 | 3,565 s | 4,320 s |
| 08-21 12:00 | 11.01 | 36,771 s | 37,160 s |
| 08-22 06:00 | 22.42 | 102,354 s | 105,387 s |
| **08-22 15:00** | 7.24 | **132,933 s (36.9 h)** | **133,154 s (37.0 h)** |
| 08-25 00:00 | 14.03 | 107,249 s | 108,910 s |
| 08-28 00:00 | 8.83 | 59,428 s | 59,700 s |
| 08-29 18:00 | 1.70 | 6,645 s | 7,292 s |
| 08-30 00:00 | 4.42 | 10 s | 71 s |
| 08-31 00:00 | 9.47 | 7,265 s | 7,540 s |

큐 깊이(시각 T에 이미 체결됐으나 아직 적재되지 않은 행 수 = `upbit_ts ≤ T AND source_ts > T`):

| T (UTC) | 미적재 행 |
|---|---|
| 08-19 12:00 | 2 |
| 08-19 18:00 | 32,326 |
| 08-21 12:00 | 300,259 |
| 08-22 06:00 | 831,110 |
| **08-22 15:00** | **1,082,811** |
| 08-25 00:00 | 853,080 |
| 08-28 00:00 | 473,041 |
| 08-29 12:00 | 162,352 |
| 08-30 00:00 | 3 |
| 08-31 03:00 | 54,360 |

- 시작: 08-19 12:00~18:00 UTC 사이(KST 08-19 21시~08-20 03시). 종료: 08-30 00:00 UTC. 지속 약 10.5일.
- 08-31 03:00 UTC에 54K 규모로 재발 후 당일 해소. 9월에도 시간당 7~12 msg/s 구간에서 p50 60~1,475초(09-02 02:00 UTC), max 8,236초(09-04)의 소규모 지연이 반복됨.

## 3. 원인: producer의 INSERT 처리량 상한

### 3-1. 코드상 상한

`producer/producer.py`(변경 전 원본 `~/cdc-orderbook-probe/backup/producer.py.orig`):

- `BATCH_SIZE = 20`, `BATCH_INTERVAL_SEC = 2.0` (compose 환경변수도 동일)
- `MySQLWriter.flush()`는 호출 1회에 버퍼에서 **최대 BATCH_SIZE(20)건만** 꺼내 `executemany`.
- `subscribe_upbit()` 루프는 `now - last_flush >= BATCH_INTERVAL_SEC`일 때만 `flush()`를 **1회** 호출.
- → 이론상 최대 20건 / 2초 = **10 rows/s**. 초과분은 `deque`에 무한 적체(상한 없음).

### 3-2. 실측 상한 (포화 서명)

적재 시각 기준 일별 INSERT 건수 (= producer 처리량):

| 날짜(UTC) | 적재 행 | rows/s |
|---|---|---|
| 08-18 | 223,171 | 2.58 |
| 08-19 | 423,010 | 4.90 |
| 08-20 | 689,445 | 7.98 |
| 08-21 | 740,885 | 8.58 |
| 08-22 | 744,689 | 8.62 |
| 08-23 ~ 08-28 | 682,806 ~ 707,853 | 7.90 ~ 8.19 |
| 08-29 | 600,628 | 6.95 |
| 08-30 | 313,192 | 3.62 |

배수 구간(08-23 00:00 ~ 08-29 12:00 UTC) 시간당 처리량: min 7.27 / p50 **7.95** / max 9.08 rows/s, 변동계수 **0.047**. 정상 구간(09-05~09-07)은 min 1.0 / p50 3.1 / max 7.52, 변동계수 0.428. 입력이 어떻든 출력이 ~8 rows/s에 평평하게 붙는 것은 처리량 상한 포화의 전형이며, 이론값 10/s와의 차이는 루프·MySQL 왕복 오버헤드로 해석된다(별도 실측은 하지 않음).

### 3-3. 트리거: 시장 급증

체결 시각 기준 시간당 유입: 08-22 05:00 UTC **261,895건(72.75 msg/s)**, 08-22 10:00 31.9/s, 08-21 08~09시 26~27/s. 08-18~09-07 사이 시간당 평균 10 msg/s를 넘은 시간대는 66시간. 유입이 상한(≈8/s)을 넘는 동안 큐가 쌓이고, 유입이 상한 아래로 떨어져야만 (상한 − 유입) 속도로 배수된다. 08-22에 108만 건이 쌓인 뒤 8일간 net 1.7 rows/s로 배수된 것이 "08-20~08-29 열흘간 ~700K/일"의 정체다. 08-29 06:45 UTC 재연결은 배수 중 발생한 별개 이벤트로 인과관계 없음.

## 4. 왜 감지되지 않았나

| 감시 수단 | 측정 대상 | 이번 사고에서의 동작 |
|---|---|---|
| Airflow `health_check` (10분) `check_clickhouse_ingest` / `check_producer_activity` | `source_ts >= now()-10분/1분` 행 수 | 큐를 배수하는 동안 source_ts는 계속 현재 시각이므로 **정상 통과** |
| Grafana "Avg CDC Latency" / "CDC Latency" 패널 | `cdc_latency_ms` = Debezium `ts_ms − source.ts_ms` (binlog→Kafka, 실측 6~13 ms) | producer 앞단 지연을 **측정 범위에 포함하지 않음** |
| Flink `flink_ts − source_ts` | Kafka→Flink (실측 1.8 s 고정) | 동일하게 앞단 미포함 |
| producer `[STATS]` 로그 `received − inserted − duplicates` | 버퍼 잔량 | 값은 존재했으나 알림·수집 대상이 아니었음. **재기동(09-08 23:19 UTC)으로 이전 컨테이너 로그가 소실되어 사후 확인 불가** |

`upbit_timestamp`를 기준으로 하는 지연 지표는 어디에도 없었다(`grep upbit_timestamp` 결과: 대시보드·DAG·마트 모델 0건, stg_trades에만 컬럼 통과).

## 5. 영향

1. **데이터 정확성**: `source_ts` 기준으로 일자를 나누는 모든 산출물(dbt `int_ohlcv_daily`, `mart_daily_summary`, daily_pipeline 품질 게이트, Grafana 일별 패널)은 08-19~08-31 구간에서 체결이 다른 날짜로 귀속됨. 파티션 키도 `toYYYYMM(source_ts)`. 유실은 없으므로 `upbit_timestamp` 기준 재집계로 복구 가능.
2. **실시간성**: 5분 윈도우 집계·이상탐지는 최대 37시간 지난 체결을 "실시간"으로 처리했음(processing-time 윈도우).
3. **자원**: 108만 건 deque는 producer 컨테이너 메모리 제한(256 MiB, swap 허용 512 MiB)을 넘었을 가능성이 크나 당시 지표가 없어 미확인.
4. 재발 조건: 시간당 평균 유입이 8 rows/s를 넘는 시간대(KST 09시, 22~24시)마다 분 단위 지연이 생기고 있음(2절). 전 코인 확장(체결 10.9 msg/s 평균) 시 **현 producer로는 상시 포화**.

## 6. 후속 조치 (2차 지시 결정 항목)

- **A. producer 처리량 상한 제거**: flush 시 버퍼가 빌 때까지 반복(또는 BATCH_SIZE 상향+주기 단축), 버퍼 상한과 초과 시 경고. 변경 범위가 "신규 필드 추가"를 넘어서므로 1차 지시서 범위 밖 → 2차 결정 필요.
- **B. 지연 지표 추가**: `source_ts − upbit_timestamp`(ingest lag)를 Grafana 패널과 `health_check`(예: p50 > 60 s 알림)에 추가. ClickHouse에 이미 두 컬럼이 있어 쿼리만으로 가능.
- **C. producer 버퍼 잔량 노출**: STATS 로그의 `received − inserted − duplicates`를 Prometheus/StatsD로 내보내기.
- **D. 08-19~08-31 마트 재계산**: `upbit_timestamp` 기준 재집계(dedup 감사 Phase 3-B의 2월 백필과 묶어 진행 가능).
- **E. 로그 보존 절차**: 컨테이너 재생성 전 `docker logs` 파일 백업을 표준 절차에 추가(이번 분석에서 로그 소실).

## 7. 교훈 (기록용)

1. "유실"과 "지연"은 외부 진실값(거래소 체결시각·일봉)과 대조해야 구분된다. 적재시각 기준 지표만 보면 둘 다 "건수 급감"으로 보인다.
2. 지연 지표는 소스 이벤트 시각 기준이어야 한다. binlog→Debezium 3ms는 파이프라인 앞단(producer 큐)의 37시간을 보지 못했다.
3. 고정 배치(20행/2초)는 유입 변동(시간대 2.6배, 버스트 72 msg/s)에 상한이 된다. 큐 깊이를 지표로 내고, flush는 버퍼 소진까지.
4. 컨테이너를 재생성하기 전에 로그를 백업한다. 이번 분석에서 producer의 8월 로그를 잃어 백로그를 ClickHouse로 재구성해야 했다.

## 부록: 확인 명령

- 업비트 일봉: `GET https://api.upbit.com/v1/candles/days?market=KRW-BTC&to=2026-09-07T00:00:00Z&count=20` (5코인, 0.3초 간격, 응답 헤더 `remaining-req: group=candles; sec=9`)
- 체결 시각 기준 일별: `SELECT market, toDate(fromUnixTimestamp64Milli(upbit_timestamp)) d, count(), sum(trade_volume) ... GROUP BY market, d`
- 지연 시계열: `SELECT toStartOfHour(fromUnixTimestamp64Milli(upbit_timestamp)) h, quantile(0.5)(toUnixTimestamp64Milli(source_ts)-upbit_timestamp), max(...) ... GROUP BY h`
- 큐 깊이: `SELECT count() FROM crypto_trades WHERE source_ts > T AND source_ts < T + INTERVAL 3 DAY AND upbit_timestamp <= toUnixTimestamp64Milli(T)`
- 처리량 포화: `SELECT toStartOfHour(source_ts) h, count() ... WHERE source_ts BETWEEN '2026-08-23' AND '2026-08-29 12:00'` 의 min/p50/max/stddev
