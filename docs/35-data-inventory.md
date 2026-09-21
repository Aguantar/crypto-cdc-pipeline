# 35. 데이터 인벤토리 - 무엇이 있고, 누가 쓰고, 언제 사라지나 (2026-09-20)

> 왜: docs/33 평가에서 "이 표는 뭐죠?" 에 답할 수 없는 산출물이 6종 나왔다. 바꿀 때 옛것을 지우는 절차가 없었던 탓이다.
> 규칙: 표를 만들면 여기에 한 줄을 추가한다. **소유자·보존·소비자·재생성 경로**가 없으면 만들지 않는다.

## 1. 층별 표 (ClickHouse `cdc_pipeline`, 2026-09-20 기준 54표 + MV/Kafka 13)
| 층 | 표 | 크기 | 보존 | 소비자 | 재생성 |
|---|---|---|---|---|---|
| 원천 시세 | `crypto_trades` | 4.16 GiB / 117.2M | TTL 365일(체결 시각) | dbt 전 층·대조·마트 | Kafka 7일 재처리(docs/34 #6) |
| 원천 시세 | `orderbook_raw` | 4.76 GiB / 132.5M | TTL 7일 | `orderbook_1m`·마트 | Parquet 120일 복원 |
| 원천 시세 | `binance_trades` | 145 MiB / 6.9M | TTL 30일 | 교차 신호·대조 | 토픽 3일 |
| 원천 시세 | `binance_orderbook_raw` | 20.6 MiB | TTL 7일 | `binance_orderbook_1m` | 재구성 잡 |
| 파생 | `orderbook_1m`·`binance_orderbook_1m` | 372 MiB·265 KiB | TTL 365일 | 마트·신호 | 원본에서 재집계 |
| 원장(2층) | `virtual_orders`·`virtual_fills`·`virtual_positions`·`binance_user_events`·`cases`·`ledger_reconcile` | < 1 MiB | 무제한(이벤트 365일) | 3자 대조·케이스 | MySQL + ledger 토픽 30일 |
| 참조 | `upbit_market_master`·`upbit_market_events`·`upbit_market_event_records`·`upbit_hourly_candles`·`upbit_daily_candles`·`binance_symbols`·`binance_hourly_candles` | < 3 MiB | 60~90일/무제한 | dim·대조·규칙 평가 | 거래소 REST 재수집 |
| 차원 | `dim_markets`·`dim_coins`·`dim_venues`·`coin_alias`·`dim_market_flag_scd` | < 200 KiB | 매 실행 재생성 | 마트·신호 | dbt |
| 중간 | `int_*` 6표 | < 6 MiB | 매 실행 재생성 | 마트·품질 | dbt |
| 마트 | `mart_trade_orderbook_1m`·`mart_daily_summary` | 367 MiB·353 KiB | 증분/재생성 | 리포트·분석 | dbt(일 단위 vars) |
| 품질 | `dq_*` 8표 | < 5 KiB | 매 실행 재생성 | 품질 판정·다이제스트 | dbt |
| 신호 | `sig_kimchi_premium_hourly`·`sig_cross_venue_flag_overlap` | < 40 KiB | 매 실행 재생성 | 분석 | dbt |
| 운영 | `ops_metrics_5m`(TTL 180일)·`alert_events`(365일)·`ops_digest` | < 15 KiB | TTL | 주간 다이제스트 | cron·DAG |
| 규칙 | `market_alerts` | 9 KiB | 무제한 | 동등성·규칙 평가 | Flink 섀도 |
| 수리 | `ingest_repairs` | 2 KiB | 무제한 | 수리 계보 dq | - |

## 2. 한시 보관 - **삭제 예정일이 있는 표**
| 표 | 크기 | 왜 있나 | 삭제 예정 |
|---|---|---|---|
| `crypto_trades_rmt` | 3.74 GiB | 09-18 ReplacingMergeTree 전환의 롤백본(docs/25) | **2026-09-25** |
| `crypto_trades_v2` | 3.85 GiB | 09-19 체결 시각 파티션 전환의 롤백본(docs/28 A-7) | **2026-09-26** |
| `crypto_trades_float_bak` | 3.95 GiB | 09-20 Decimal 전환의 롤백본(docs/34 §5) | **2026-09-27** |
| `binance_trades_float_bak` | 129 MiB | 같음 | **2026-09-27** |
| MySQL `crypto_trades_old` | 2.3 GB | 09-19 파티션 테이블 교체의 롤백본(docs/28 A-6) | **2026-09-26** |
| `anomaly_alerts` | 2.3 MiB | v1 규칙 이력(09-17 적재 중단). 규칙 교체의 증거 | TTL 365일 → **2027-09 자동** |
합계 15.8 GiB. 디스크 31% 사용(311 GB 여유)라 예정일까지 두는 것이 롤백 가치보다 싸다.

## 3. 2026-09-20 삭제한 것 (docs/34 #7)
| 표 | 크기 | 왜 지웠나 |
|---|---|---|
| `trade_aggregations` | 47.2 MiB | Flink 5분 **처리 시간** 집계. 09-19 폐기(정지 뒤 따라붙는 행이 '지금' 창을 왜곡, docs/29 §7). 이벤트 시각 집계(`int_ohlcv_1h`)가 대체하고 원본에서 언제든 재계산된다 |
| `mart_alert_rate`·`mart_volume_spike` | 933 KiB | v1 규칙 시절 마트. **dbt 모델이 이미 삭제돼 고아**였고 09-16/17 이후 갱신 없음 |
| `coin_metadata` | 902 B | seed 로 만들어졌으나 **참조 0**. `dim_coins`(docs/34 #4)가 그 자리를 대신한다 |
| `load_test_*` 4표 | 0 B | 부하 실험 격리용(docs/23). 실험 종료, `clickhouse/load_test.sql` 로 언제든 재생성 |

## 4. 죽어 있던 **참조**도 함께 정리 (표보다 위험한 것)
| 대상 | 상태 | 조치 |
|---|---|---|
| `scripts/sync-annotations.sh` (cron 매분) | v1 `anomaly_alerts` 를 조회 - 09-17 이후 **매분 빈 결과** | `market_alerts`(v2 전이)로 교체 |
| `scripts/observe/collect_metrics.sh` alerts 4열 | 같은 이유로 09-17 이후 계속 0 | `market_alerts` 로 교체(열 의미 변경을 스크립트 주석·worklog 에 명시) |
| Grafana `cdc-pipeline.json` 주석 쿼리 | `anomaly_alerts` 조회 | `market_alerts` 로 교체 |
| **n8n 알림 워크플로 3개** | `CDC_Pipeline_-_Anomaly_Health_Monitor`·`FDS_SLA_Monitor`·`FDS_Fraud_Alert` 가 **전부 비활성**. 앞의 것은 죽은 `anomaly_alerts` 를 보고, 뒤 둘은 타 프로젝트용/스텁 | **되살리지 않는다.** 알림은 Airflow 로 일원화(10분 헬스 12체크 + 매시 품질 판정 + 주간 다이제스트). 정의는 `n8n/workflows/*.json` 으로 내보내 저장소에 보관(비밀값 마스킹) |
문서 정정: README·docs 가 "n8n 매분 실시간 알림"이라고 말해 왔으나 **09-17 이후 사실이 아니다.**

## 5. 표를 만들 때 지킬 것
1. 이 문서에 한 줄(소유·보존·소비자·재생성)을 먼저 적는다.
2. 보존은 TTL 로 **표에 박는다**. "나중에 지우자"는 안 지켜진다(오늘 6종이 증거).
3. 로직을 바꾸면 옛 산출물의 **소비자부터** 옮기고, 표는 그다음에 지운다. 죽은 표보다 죽은 참조가 위험하다(매분 도는 cron 이 그랬다).
