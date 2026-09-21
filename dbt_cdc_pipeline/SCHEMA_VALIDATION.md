# 스키마 검증 결과: 인수인계서 vs 실제 ClickHouse

## 실제 데이터베이스: `cdc_pipeline` (인수인계서에는 미기재)

## 테이블별 실제 컬럼 구조

### 1. crypto_trades (인수인계서: `trades`)
| 실제 컬럼 | 타입 | 인수인계서 참조명 | 불일치 |
|-----------|------|-------------------|--------|
| op | String | - | - |
| trade_id | UInt64 | trade_id | ✅ 일치 |
| market | String | symbol | ❌ `symbol` → `market` |
| trade_price | Float64 | price | ❌ `price` → `trade_price` |
| trade_volume | Float64 | volume | ❌ `volume` → `trade_volume` |
| trade_amount | Float64 | - | 인수인계서 누락 (유용한 필드) |
| ask_bid | String | - | 인수인계서 누락 |
| upbit_timestamp | Int64 | timestamp | ❌ 타입도 다름 (DateTime이 아닌 Unix ms Int64) |
| sequential_id | Int64 | - | 인수인계서 누락 |
| source_ts | DateTime64(3) | - | - |
| cdc_ts | DateTime64(3) | - | - |
| cdc_latency_ms | Int64 | - | - |
| flink_ts | DateTime64(3) | - | - |
| inserted_at | DateTime64(3) | - | - |

### 2. trade_aggregations (Flink 5분 윈도우 집계 - 이미 존재)
| 실제 컬럼 | 타입 |
|-----------|------|
| market | String |
| window_start | DateTime64(3) |
| window_end | DateTime64(3) |
| trade_count | UInt64 |
| bid_count | UInt64 |
| ask_count | UInt64 |
| total_amount | Float64 |
| total_volume | Float64 |
| avg_price | Float64 |
| min_price | Float64 |
| max_price | Float64 |
| vwap | Float64 |
| inserted_at | DateTime64(3) |

### 3. anomaly_alerts
| 실제 컬럼 | 타입 | 인수인계서 참조명 | 불일치 |
|-----------|------|-------------------|--------|
| alert_type | String | rule_type | ❌ `rule_type` → `alert_type` |
| market | String | symbol | ❌ `symbol` → `market` |
| trade_id | UInt64 | - | - |
| message | String | - | - |
| value | Float64 | - | - |
| threshold | Float64 | - | - |
| detected_at | DateTime64(3) | alert_time | ❌ `alert_time` → `detected_at` |
| inserted_at | DateTime64(3) | - | - |

### 4. mv_latency_stats (Materialized View - 기존 유지)
| 실제 컬럼 | 타입 |
|-----------|------|
| minute | DateTime |
| avg_latency | AggregateFunction(avg, Int64) |
| max_latency | AggregateFunction(max, Int64) |
| min_latency | AggregateFunction(min, Int64) |
| event_count | AggregateFunction(count) |

## 핵심 불일치 요약

| # | 인수인계서 | 실제 | 영향 범위 |
|---|-----------|------|----------|
| 1 | source: `raw.trades` | `cdc_pipeline.crypto_trades` | 모든 모델 |
| 2 | `symbol` | `market` | stg, int, mart 전체 |
| 3 | `price` | `trade_price` | stg, int 전체 |
| 4 | `volume` | `trade_volume` | stg, int 전체 |
| 5 | `timestamp` (DateTime) | `upbit_timestamp` (Int64, Unix ms) | stg 타임스탬프 변환 로직 |
| 6 | `alert_time` | `detected_at` | mart_alert_rate |
| 7 | `rule_type` | `alert_type` | mart_alert_rate |

## DBT ↔ Flink 역할 분리 (MV 중복 방지)

| 기능 | Flink (실시간, 기존 유지) | DBT (배치, 새로 추가) |
|------|--------------------------|----------------------|
| 5분 윈도우 집계 | `trade_aggregations` 테이블 | 사용하지 않음 (Flink 담당) |
| 1시간봉 OHLCV | ❌ 없음 | `int_ohlcv_1h` ✅ DBT 담당 |
| 일봉 OHLCV | ❌ 없음 | `int_ohlcv_daily` ✅ DBT 담당 |
| 거래량 급등 탐지 | `anomaly_alerts` (실시간, 틱 단위) | `mart_volume_spike` (배치, 시간봉 단위) |
| 일별 서머리 | ❌ 없음 | `mart_daily_summary` ✅ DBT 담당 |
| CDC 레이턴시 통계 | `mv_latency_stats` (분 단위 MV) | 사용하지 않음 (MV 담당) |
