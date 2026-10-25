# 53. 새 테이블 파이프라인 추가를 명령 하나로 - 수동 5단계 실측과 자동화 (2026-10-23)

- 왜: 마켓 추가는 5분 재구독으로 수동 0단계가 됐지만(docs/17), MySQL 표 하나를 CDC 에 태우는 일은 수동 5단계였고 소요 시간 기록이 없었다. 같은 일을 다섯 번 손으로 하면 다섯 번 다르게 한다.
- 결론: 수동으로 한 번 하면서 실수 2건을 냈고(토픽이 기본값 3 파티션으로 자동 생성, TIMESTAMP 를 epoch 정수로 잘못 파싱), 기존 행은 커넥터에 표를 추가해도 안 온다는 것을 확인했다. 이 셋을 고친 순서로 `scripts/ops/onboard_table.py` 를 만들었다. 같은 표를 다시 태우는 데 **12초**(기존 행 스냅샷·MySQL=ClickHouse 행 수 대조 포함), 손댄 파일 0, 실수 0. 드라이런과 역순 제거가 있다.

## 1. 전: 수동 절차 실측 (20:04~20:15 UTC, 표 `crypto_db.pipeline_onboard_probe` 9컬럼, 기존 행 3)

| 단계 | 한 일 | 시간 | 손댄 것 | 실수·발견 |
|---|---|---|---|---|
| ① DDL | MySQL 표 생성 + 행 3건 | 초 단위 | SQL 1 | |
| ② 커넥터 | config GET → `table.include.list` 편집 → PUT → RUNNING 확인 | 5초 | JSON 1 | **스냅샷 SKIPPED**. 기존 행 3건이 토픽에 온 것은 커넥터가 재시작하며 마지막 커밋 오프셋(INSERT 이전)부터 binlog 를 다시 읽은 우연이다. 오프셋이 INSERT 뒤였다면 안 왔다 |
| ③ 토픽 | `kafka-topics.sh` 에 한 줄 추가 후 실행 | 110초(스크립트가 전 토픽을 돈다) | sh 1 | **토픽이 이미 있었다**. ② 에서 커넥터가 발행하자 브로커 auto.create 로 3 파티션·72h·1GB 기본값으로 생겼다. 스크립트는 설정만 고치고 파티션 3 은 못 줄인다 |
| ④ ClickHouse | `cases` 패턴을 복사해 표·Kafka 엔진·MV 3개를 손으로 작성 | 작성 수 분(미계측), 적용 1초 | SQL 1 | `created_at` 을 `fromUnixTimestamp64Milli(JSONExtractInt(...))` 로 썼다. 실제 Debezium 은 TIMESTAMP 를 ISO 문자열로 보낸다(§2) → 1970 년으로 들어갔을 것. 검증 쿼리에 그 컬럼이 없어서 못 봤다 |
| ⑤ dbt | `sources.yml` 에 소스 블록 | 초 단위 | yml 1 | |
| ⑥ 검증 | INSERT/UPDATE/DELETE 한 건씩 → ClickHouse | 9초 | | 통과 (c/u/d 도착) |

합계: 손댄 파일 4, 실수 2(하나는 되돌릴 수 없고, 하나는 검증에 안 걸렸다), 순 작업 약 2분 + DDL 작성 시간. 벽시계로는 더 걸렸다(③ 앞에서 세션 대기가 있어 그 구간은 제외).

## 2. 기술적 핵심: 커넥터에 표를 추가하면 기존 행은 오는가

오지 않는다. `snapshot.mode=initial` 은 커넥터에 오프셋이 있으면 스냅샷을 건너뛴다(Connect 로그 `Snapshot ended with SnapshotResult [status=SKIPPED`). 새 표의 스키마는 `schema.history.internal.store.only.captured.tables.ddl` 기본값(false) 덕에 이미 history 토픽에 있어서 변경 이벤트 파싱은 되지만, 과거 행은 binlog 에 없으니 올 길이 없다.

해법은 Debezium 증분 스냅샷(signal 표). 실측(20:16 UTC):

| 단계 | 값 |
|---|---|
| 전제 | MySQL `crypto_db.debezium_signal (id VARCHAR(42) PK, type VARCHAR(32), data VARCHAR(2048))`, 커넥터에 `signal.data.collection=crypto_db.debezium_signal` + 그 표를 `table.include.list` 에 포함(커넥터 PUT 1회). 이 두 가지는 플랫폼 전제라 한 번만 한다 - ledger-cdc-connector 에 10-23 적용 |
| 신호 | `INSERT INTO debezium_signal VALUES ('onboard-probe-1', 'execute-snapshot', '{"data-collections": ["crypto_db.pipeline_onboard_probe"], "type": "incremental"}')` |
| 결과 | 5초 뒤 ClickHouse 에 `op='r'` 3행. 로그 `Requested 'INCREMENTAL' snapshot ... surrogate key 'PK of table will be used'`. 기존 MV 가 `op IN ('c','u','d','r')` 이라 추가 변경 없음 |
| 부작용 | signal 표에 `snapshot-window-open/close` 행이 스냅샷마다 2쌍 남는다(10-23 8행). 캡처 대상이라 토픽 `ledger.crypto_db.debezium_signal` 이 생긴다 - 정의가 없어 3 파티션으로 자동 생성됐고, `kafka-topics.sh` 에 1 파티션으로 정의해 다시 만들었다 |
| 중복 | 스냅샷 행은 이미 있는 행과 같은 (키, version) 이라 ReplacingMergeTree 가 접는다. FINAL 결과 동일 확인 |

GTID 가 켜져 있어 read-only 증분 스냅샷(Kafka signal 채널)도 가능하지만, signal 표 방식이 설정 두 줄로 끝나서 그쪽을 택했다.

Debezium JSON 의 실제 모양(`cases` 토픽 메시지 1건, `time.precision.mode=connect`, `decimal.handling.mode=string`):

| MySQL | JSON 값 | ClickHouse 매핑 (`map_column`) |
|---|---|---|
| TIMESTAMP(3) | `"2026-09-19T09:05:50.350Z"` 문자열 | `DateTime64(3)` ← `parseDateTime64BestEffortOrZero` |
| DATETIME | epoch ms 정수 | `DateTime64(3)` ← `fromUnixTimestamp64Milli` |
| DECIMAL(p,s) | `"1.50000000"` 문자열 | `Decimal(p, s)` ← `toDecimal128OrZero(..., s)` |
| BIGINT UNSIGNED / INT UNSIGNED | 정수 | `UInt64` / `UInt32` |
| TINYINT(1) | 0/1 정수 | `UInt8` |
| ENUM, VARCHAR(≤40) | 문자열 | `LowCardinality(String)` |
| VARCHAR(>40), TEXT, JSON | 문자열 | `String` |
| NULL 허용 | `null` | `Nullable(...)` + `JSONType = 'Null'` 분기 (LowCardinality 는 '' ) |

## 3. 설계: `scripts/ops/onboard_table.py <db>.<table> [--connector] [--dry-run] [--no-snapshot] [--dml-check] [--remove]`

순서를 수동 절차와 다르게 뒀다. 이유가 곧 §1 의 실수다.

| # | 단계 | 왜 이 순서 |
|---|---|---|
| 1 | 검증: MySQL 표 존재, PK 존재(RMT 정렬 키), `binlog_format=ROW`, include.list 중복, signal 설정 유무 | 없으면 멈춘다. PK 없는 표는 거부 |
| 2 | 토픽: `kafka-topics.sh` 에 정의 추가 + 그 토픽만 생성(p1 zstd 30일) | **커넥터보다 먼저**. 브로커가 먼저 만들면 파티션을 못 줄인다 |
| 3 | ClickHouse: 표(RMT(version 또는 source_ts_ms, is_deleted), ORDER BY PK) + Kafka 엔진 + MV 를 information_schema 에서 생성 | **커넥터보다 먼저**. 컨슈머 그룹이 첫 메시지 전에 있어야 한다 |
| 4 | 커넥터: include.list 추가 → PUT → RUNNING 대기 | |
| 5 | 증분 스냅샷 신호 → ClickHouse `op='r'` 행 수가 MySQL 행 수에 닿을 때까지 대기(최대 120초) | 기존 행 |
| 6 | dbt `sources.yml` raw 소스 블록 + PK not_null·unique | `dbt ls --select source:raw.<표>` 로 테스트 2개 생성 확인 |
| 7 | 검증: MySQL count = ClickHouse FINAL(is_deleted=0) count. `--dml-check` 면 INSERT→UPDATE→DELETE 한 건을 넣고 `version=3, is_deleted=1` 로 수렴하는지 | c/u/d 행 세 개를 세면 안 된다 - RMT 머지가 초 단위로 접는다(첫 판 34초 뒤 'd' 만 남아 실패로 보였다) |
| 8 | 감시: 커넥터 단위는 `check_kafka_connect`. 표 단위 적재 감시는 추가하지 않는다 | 표마다 유입 주기가 달라 신선도 임계를 정할 수 없다. 대조표가 생기면 `ledger_reconcile` 처럼 항목을 넣는다 |

`--dry-run` 은 토픽 정의·DDL 3개·include.list 변경·signal 문·dbt 블록을 출력만 한다. `--remove` 는 역순(커넥터 → ClickHouse DROP → 토픽 삭제 → 파일 되돌림)이고 MySQL 표는 건드리지 않는다. 비밀번호는 컨테이너 환경변수(`$MYSQL_ROOT_PASSWORD`, `$CLICKHOUSE_PASSWORD`)를 `docker exec` 안에서만 읽는다.

단위 테스트 `scripts/ops/tests/test_onboard_table.py` 4개(타입 매핑, DDL 스냅샷, version 없는 표, dbt 블록·토픽 줄). CI 에 단계 추가(도커 불필요).

## 4. 후: 자동 경로 실측 (20:20~20:23 UTC, 같은 표·같은 3행)

| 단계 | 시각(+초) |
|---|---|
| 드라이런 | 0.6초, 변경 0 |
| 1 검증 | +0.4 |
| 2 토픽 생성 (1 파티션) | +2.2 |
| 3 ClickHouse 3개 | +2.4 |
| 4 커넥터 PUT → RUNNING | +5.4 |
| 5 스냅샷 신호 → `op='r'` 3/3 | +12.0 |
| 6 dbt 소스 | +12.0 |
| 7 MySQL 3 = ClickHouse FINAL 3 | +12.3 |
| 7 DML e2e (--dml-check) | +46.8 (검증 로직 결함으로 34초 대기 - 고침, §3 7행) |

값 검증: `created_at` 이 `2026-10-23 20:20:14.284` 로 들어옴(수동 판의 epoch 오류 없음), `amount` Decimal, `note` NULL. 토픽 1 파티션. `dbt ls` 에 `source_not_null_raw_pipeline_onboard_probe_id`, `source_unique_raw_pipeline_onboard_probe_id`. 다른 원장 표 영향: `clickhouse-ledger-orders` 랙 0, Connect ERROR 0, 알림 0 (20:00~20:30 alert_events 는 PRICE_24H 3건뿐).

전후:

| | 수동 | 자동 |
|---|---|---|
| 소요 | 순 작업 약 2분 + DDL 작성(미계측), 토픽 스크립트 110초 | 12초 (스냅샷·대조 포함) |
| 손댄 파일 | 4 | 0 (스크립트가 2개를 고친다) |
| 실수 | 2 (파티션 3, TIMESTAMP 파싱) | 0 |
| 기존 행 | 우연히 옴 | 증분 스냅샷으로 확정 |
| 되돌리기 | 손으로 6곳 | `--remove` 6초 |

`--remove` 뒤 상태: include.list 원래 6표 + signal, ClickHouse 에 probe 표 0, 토픽 0, `git diff` 에 probe 흔적 0.

## 5. 한계

- 스키마 변경 추적은 없다. 온보딩 뒤 MySQL 에 컬럼이 추가되면 Debezium 은 보내지만 ClickHouse MV 는 그 컬럼을 모른다. 재온보딩(--remove 뒤 다시)이거나 MV 재생성이 필요하다.
- MySQL 전용. Postgres 커넥터(circuit)는 토픽 접두사·signal 방식이 달라 미지원.
- `version` 컬럼이 없는 표는 binlog 시각(`source_ts_ms`)으로 최신을 가린다. 같은 ms 안의 두 UPDATE 는 순서가 보장되지 않는다.
- 복합 PK 는 ORDER BY 에 그대로 들어가지만 실측은 단일 PK 뿐이다.
- 프로덕션 실사용 기록은 아직 없다. 2층 원장 6표는 09-19 에 손으로 들어갔고, 새로 필요한 표가 없어 실측용 표로만 검증했다.
- 커넥터 PUT 은 그 커넥터의 다른 표 스트리밍을 몇 초 멈춘다(재시작). 이번엔 랙 0 으로 돌아왔지만 큰 표가 있는 커넥터에선 그 몇 초가 보인다.
