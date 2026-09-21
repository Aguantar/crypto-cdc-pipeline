# 36. 메트릭 정의 - 이 파이프라인의 숫자가 무엇을 뜻하나 (2026-09-20)

> 왜: docs/33 평가에서 "VWAP·spread_bp·imbalance·volume_over_depth15 의 정의가 각 SQL 주석에만 흩어져 있다"가 약점으로 나왔다.
> 소비자(분석가·면접관·미래의 나)가 한 곳에서 **정의와 단위와 함정**을 볼 수 있어야 한다.
> 타입 규약(docs/34 #5): **금액·수량은 Decimal, 비율·파생지표는 Float64.** 아래 "비율" 은 전부 Float64 다.

## 1. 시세 - 체결
| 이름 | 정의 | 단위 | 어디 | 함정 |
|---|---|---|---|---|
| `trade_amount` | `trade_price × trade_volume` (BigDecimal, 스케일 16) | KRW | `crypto_trades` | MySQL 의 `trade_amount`(DECIMAL(20,4))가 아니다. 그 값은 먼지 체결에서 0 이 된다(docs/34 #5) |
| `vwap` | `sum(amount) / sum(volume)` | KRW | `int_ohlcv_*`·마트 | 분모 0 이면 NULL→0. Decimal 로 나누면 예외가 나므로 Float64 로 캐스팅한다 |
| OHLC | `open=argMin(price, ts)`, `high=max`, `low=min`, `close=argMax(price, ts)` | KRW | `int_ohlcv_1h/daily` | **이벤트 시각(체결 시각)** 기준. 1h/daily 는 KST(`hour_kst`·`day_kst`), 품질·대조는 UTC(`day_utc`) |
| `daily_range_pct` | `(high − low) / low × 100` | % | `int_ohlcv_daily` | - |
| `taker_side` | Upbit `ask_bid` 그대로 / Binance 는 `is_buyer_maker` 를 뒤집은 값 | BID·ASK | `stg_*` | 두 거래소의 원문 의미가 **반대**다. 통일한 열은 이것뿐(docs/34 #4) |

## 2. 시세 - 호가 (Flink `OrderbookAggregator`, 분 단위 평균)
| 이름 | 정의 | 단위 | 함정 |
|---|---|---|---|
| `mid` | `(bestAsk + bestBid) / 2` | KRW | `mid_open/close/min/max` 는 분 안의 첫·마지막·최소·최대 |
| `spread` | `bestAsk − bestBid` | KRW | |
| `spread_bp` | `spread / mid × 10,000` | bp(0.01%) | `spread_bp_avg` 는 스냅샷 평균, `spread_bp_max` 는 분 최대 |
| `imbalance_N` | `(bidTopN − askTopN) / (bidTopN + askTopN)`, N ∈ {1,5,15} | −1~+1 | 양수 = 매수 우위. N 은 **호가 단 수**이지 금액이 아니다 |
| `ask_depth15` / `bid_depth15` | 상위 15단 잔량 합 | 수량 | 금액이 아니라 수량. 코인마다 단위가 다르다 |
| `volume_over_depth15` | 그 분 체결 수량 ÷ (ask_depth15_avg + bid_depth15_avg) | 배수 | **1 이면 "그 분에 호가장 15단 전부가 체결된 셈"**. EURC 되튐의 원인 지표(docs/27) |
| `price_range_bp` | `(high − low) / close × 10,000` | bp | |
| `close_vs_mid_bp` | `(체결 종가 − mid_close) / mid_close × 10,000` | bp | 체결과 호가의 어긋남 |
| `recv_lag_ms` | `수신 시각 − 거래소 호가 시각` | ms | |

## 3. 교차 거래소
| 이름 | 정의 | 단위 | 함정 |
|---|---|---|---|
| `premium_pct` | `Upbit KRW ÷ (Binance USDT × USDT/KRW) − 1`, ×100 | % | 조건: 같은 UTC 시간, 두 거래소 모두 체결 ≥10. 환율은 **Upbit KRW-USDT 시간 종가**(외부 FX 아님). 얇은 마켓에서 ±30% 극단값이 나오는데 **숨기지 않는다** - 유동성 필터는 소비자 몫 |
| `on_both_venues` | `dim_coins` 에서 두 거래소에 모두 상장 | 0/1 | 매핑은 거래소가 준 base/quote + 사람이 검토한 별칭(`coin_alias`). 문자열 치환 아님 |

## 4. 품질·정합성
| 이름 | 정의 | 목표 | 어디 |
|---|---|---|---|
| `weighted_pct` (Upbit 대조) | `sum(우리 거래량) / sum(거래소 시간봉 거래량) × 100`, 하루 가중 | ≥ 99.9% | `dq_reconcile_daily` |
| `weighted_pct` (Binance 대조) | `sum(우리 행수) / sum(거래소 1h 캔들 체결 수) × 100` | ≥ 99.9% | `dq_binance_reconcile_daily` |
| `min_cell_pct` | (마켓 × 시간) 셀 중 최저 비율 | ≥ 99% | 가중 평균이 가리는 한 마켓·한 시간을 잡는다(09-15 TIA 사례) |
| `lag_p50/p95_s` | `MySQL 적재 시각 − 거래소 체결 시각` | p50 < 60s | `dq_ingest_daily`. **수리 행(>60s, 백필)은 제외**하고 잰다 |
| `gap_windows` / `est_lost_snapshots` | 호가 유입이 3초 이상 끊긴 창 수와 추정 유실 | 0 | `dq_orderbook_gaps_daily` |
| `parity_ok` | SQL 재계산 전이 = Flink 전이 = 매칭 | 1 | `dq_alert_parity_daily`. 전이 10건 이상인 날만 유의미 |
| `ex_my_mismatch` / `my_ch_mismatch` | 거래소≠MySQL / MySQL≠ClickHouse | 0 | `dq_ledger_daily` (3자 대조) |

## 5. 규칙 평가 (거래소 시장경보가 정답)
| 이름 | 정의 | 함정 |
|---|---|---|
| `precision` | `matched / ours` - 우리가 낸 것 중 거래소도 지정한 비율 | |
| `recall` | `matched / exchange` - 거래소 지정 중 우리도 낸 비율 | **전이 기준**이라 우리가 이미 높은 등급을 유지 중이면 새 전이가 없어 낮게 나온다 |
| `state_recall` | 거래소 지정 ±10분에 **우리 등급 > 0 이었던** 비율 | 상태 기준. ASOF 최신 상태라 분 단위로 오르내리는 마켓에서 낮게 나온다(docs/22 §4-5 해석) |
| `lead_median_s` | `거래소 지정 시각 − 우리 전이 시각` 중앙값 | 음수 = 우리가 먼저 |
| `ratio` (VOLUME_24H) | `전일 거래대금 / 직전 7일 평균` | 임계 4배 + 하한 10억 (6개월 라벨로 역산, docs/16) |

## 6. 운영
| 이름 | 정의 | 목표 | 어디 |
|---|---|---|---|
| e2e p95 | `flink_ts − 거래소 체결 시각` 의 95분위 | Upbit < 6s / Binance < 5s | `ops_metrics_5m`·health_check |
| 시각 6개 | 거래소 체결 → 수신(`recv_ms`) → MySQL(`source_ts`) → Debezium(`cdc_ts`) → Kafka(`kafka_ts_ms`) → Flink(`flink_ts`) | - | 구간별 지연을 행 단위로 가른다(docs/28 A-8) |
| `busyTimeMsPerSecond` | Flink 서브태스크가 실제로 일한 ms/s | < 500 | 1,000 = 포화. 유실 전에 울리는 선행 지표(docs/23) |
| `parseFailures` | 파싱 실패 누적(원문은 DLQ 토픽) | 증가 0 | 카운터는 재시작에 0 이 되므로 **증가분**으로 판정 |

## 7. 이 문서가 답하는 질문
- "VWAP 이 원장 금액과 왜 다르죠?" → VWAP 은 비율이라 Float64, 금액 합계는 Decimal. 합계끼리는 원천과 마트가 소수 20자리까지 같다(docs/34 §5).
- "recall 이 0.93 인데 놓친 건가요?" → 전이 기준이라 그렇다. 상태 기준은 `state_recall` 을 같이 본다.
- "premium_pct 에 +38% 가 있는데요?" → 얇은 마켓이다. 체결 10건 조건만 걸었고 유동성 필터는 조회에서 건다.
