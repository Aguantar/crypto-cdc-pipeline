# 50. Flink 잡 하나를 ClickHouse Kafka 엔진 + MV 로 옮겼다 - 2026-09-26 ~ 09-28

- 배경: README "왜 Flink 인가" 의 지금 답은 "이 규모엔 과했다" 다. 그 답이 의견이 아니라 측정이 되게 하려고, 잡 하나를 옆에 세워 병행하고 전후를 쟀다.
- 대상: `Binance Trade Pipeline`. 네 잡 중 유일하게 상태가 0 인 잡(JSON 파싱 → JDBC 싱크). 초당 평균 441, 최대 1,558 로 가장 무거운 경로라 측정이 의미 있다.
  Upbit CDC 잡은 파싱과 분 종가 이상탐지가 한 잡이라 파싱만 뗄 수 없고, 호가장 재구성은 키별 상태라 마지막까지 남는다.
- 결론: 값이 같고 파트·머지는 절반 이하, 대가는 적재 지연 +0.7초와 기준선 메모리 +40 MiB. 09-28 01:41 UTC 컷오버, 표 기준 정지 0초.

## 1. 성공 기준 (먼저 정했다)

| 항목 | 기준 |
|---|---|
| 값 | 24시간 병행 뒤 심볼 × 시간 셀 대조: 행 수·`sum(qty)`·`sum(quote_qty)` 불일치 0 |
| 중복 | FINAL 대비 0 |
| 지연 | e2e p95 가 Flink 경로(2.83초)와 같은 자릿수 |
| 자원 | ClickHouse 기준선 메모리 증가 100 MiB 이내 |
| 파트 | 줄어야 한다 (Flink JDBC 배치 1,000행 / 3초 → 하루 70,724 파트) |
| 급등 | 초당 5,000 넘는 구간이 오면 그 구간을 따로 대조 |

하나라도 실패하면 빼지 않고 이유를 적기로 했다.

## 2. 구성

```sql
CREATE TABLE binance_trades_queue (raw String) ENGINE = Kafka
SETTINGS kafka_topic_list = 'binance.trades.v1', kafka_group_name = 'clickhouse-binance-trades',
         kafka_format = 'JSONAsString', kafka_num_consumers = 2, kafka_flush_interval_ms = 3000;

CREATE MATERIALIZED VIEW mv_binance_trades TO binance_trades_mv AS   -- 그림자 표: 살아 있는 표와 DDL 동일
SELECT JSONExtractString(raw, 's') AS symbol, JSONExtractUInt(raw, 't') AS trade_id,
       toDecimal128(JSONExtractString(raw, 'p'), 8) AS price, toDecimal128(JSONExtractString(raw, 'q'), 8) AS qty,
       price * qty AS quote_qty, ... , now64(3) AS flink_ts
FROM binance_trades_queue
WHERE symbol != '' AND trade_id > 0 AND trade_ms > 0 AND price >= 0 AND qty >= 0;
```

- 컨슈머 그룹은 Flink 와 다르게 두어 둘 다 전체를 읽는다. 표를 만들기 전에 그룹 오프셋을 latest 로 만들어 뒀다. 안 그러면 Kafka 엔진이 earliest 부터(보존 3일치 약 1.1억 건) 되감아 읽어 측정이 오염되고 서버가 흔들린다.
- 문자열 → Decimal 은 직접 변환한다. Float 경유는 버림이 생긴다(docs/34 #5 의 교훈). `quote_qty` 는 Decimal(20,8) × Decimal(20,8) = Decimal(38,16) 으로 파서와 같은 스케일.
- 파서의 검증 실패(DLQ)는 같은 큐에 MV 를 하나 더 붙여 원문을 남긴다. Kafka 엔진 표 하나에 MV 여러 개가 붙는다.
- flush 3초는 옛 JDBC 싱크의 배치 간격과 맞춘 값이다. 기본 7.5초면 지연이 4초 더 는다.

## 3. 47시간 병행 결과 (09-26 01:50 ~ 09-28 01:07 UTC)

| 항목 | Flink 잡 | Kafka 엔진 + MV | 판정 |
|---|---|---|---|
| 셀 대조 (심볼 × 시간, FINAL) | 기준 | 24,262 셀, 행 수·수량 합 불일치 **0**. 49,305,314 = 49,305,314 | 통과 |
| 값 (피크 10분, 초당 약 1,000) | 기준 | 469,353건의 price·qty·quote_qty·is_buyer_maker·event_ms·recv_ms 전부 일치, 한쪽에만 있는 키 0 | 통과 |
| 중복 (일별 count vs FINAL) | 0 | 0 | 통과 |
| 적재 지연 p95 (p50) | 2.90초 (1.34) | 3.56초 (1.76) | 통과 |
| 파트 생성 (48h) | 125,176 | 51,494 (−59%) | 통과 |
| 머지 (48h) | 40,402회, 0.54 코어시간 | 11,326회, 0.17 코어시간 (−72%) | 통과 |
| 기준선 메모리 (`metric_log` median) | 769 MiB | 807 MiB (+38) | 통과 |
| 소비 정지·예외 (5분 관찰 581줄) | | 그림자 60초 0행 0회, DLQ 0, 컨슈머 lag 0, 예외 0 | 통과 |
| 급등 | | 병행 중 최대 1,038/s. 오지 않았다 | 미측정 |

첫 2분 창에서 그림자에만 141건이 있어 유실을 의심했다. 10분 창으로 넓히니 양방향(32 / 117)이 됐고, 창이 닫히고 6분 뒤에 다시 세니 137,507 = 137,507 로 0 이었다.
두 경로의 적재 지연이 달라서 생기는 차이이고, 대조는 창 끝 + 5분 뒤에 세야 한다는 규칙이 여기서 나왔다.

## 4. 컷오버 (09-28 01:41 UTC)

| 시각 | 단계 |
|---|---|
| 01:41:00 | 살아 있는 표로 쓰는 MV 를 하나 더 만든다. 이 순간부터 Flink 와 MV 가 같은 표에 쓴다 |
| 01:41:21 | Flink 잡 취소. 상태 0 이라 세이브포인트 불필요. 컨슈머 그룹은 롤백용으로 남긴다 |
| 01:42:31 | 최근 60초 26,755행 / 410 심볼. 겹친 90초 구간 raw 33,649 = FINAL 33,649 (중복은 ReplacingMergeTree 가 접었다) |
| 02:00 | 고정 창 01:43 ~ 01:55: 살아 있는 표 raw = FINAL = 그림자 = 490,285. 한쪽에만 있는 키 0. 지연 p95 3.67초. DLQ·예외 0. 헬스체크 정상 |

표 기준 정지 0초. 롤백은 MV DROP 과 잡 재제출(`flink run -d -c ...BinanceTradeJob`)이고 겹친 구간은 RMT 가 접는다. 시작 스크립트는 CDC 잡만 올리므로 재시작해도 옛 잡이 저절로 다시 뜨지 않는다.

컷오버 뒤 24시간(MV 단독): 09-28 하루 42,051,802행, 중복 0, 거래소 대조 99.995%(Flink 때와 같은 수준). 파트 26,101 / 머지 5,554 (Flink 때 70,724 / 18,744).
기준선 메모리는 median 847 MiB 로 Flink 단독 때(769)보다 **78 MiB** 높다. 병행 중 잰 +38 의 두 배이고, 대가는 이 값으로 적는다.

## 5. 남은 것

- Flink 잡을 하나 빼도 TaskManager 의 1 GB 는 JVM 설정값이라 줄지 않는다. 회수는 CPU 와 운영 단순함이고, RAM 은 Flink 자체를 뺄 때만 온다.
- 다음 후보는 `Orderbook Pipeline`(파싱 + 1분 윈도우, MV 둘). 이상탐지를 1분 SQL 로 옮긴 뒤에야 CDC 잡을 뺄 수 있다. 호가장 재구성은 남는다.
- 급등 구간은 미측정이다. 다음 급등에서 거래소 대조(`dq_binance_reconcile_daily`)가 잰다. Flink 경로도 그 방식으로만 검증돼 있었다.
- 기준선 메모리 +38 MiB 는 컨슈머 버퍼로 추정된다. 총량 예산(docs/48 5-1)이 이 저장소의 가장 빠듯한 자원이라 다음 잡을 옮길 때 다시 잰다.
