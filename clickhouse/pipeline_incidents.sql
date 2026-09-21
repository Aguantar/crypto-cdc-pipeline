-- 파이프라인 단계별 사건 이력 (docs/41, 2026-09-20).
--
-- 왜 필요한가: 사용자 질문 "각 파이프라인 순으로 오류가 났는지랑 왜 났는지까지 Airflow 에 들어가?" → 반만 들어간다.
-- Airflow 가 보여주는 것은 Airflow 가 직접 실행하는 태스크(dbt·대조·백업·헬스체크)뿐이고,
-- 실시간 경로(수집기 → MySQL → Debezium → Kafka → Flink → ClickHouse)는 컨테이너가 돌린다.
-- health_check 가 10분마다 그 단계들을 찔러보지만 그것은 지금 상태 스냅샷이지 "언제 무엇이 왜 깨졌나"의 이력이 아니다.
-- 그래서 이력이 조각나 있었다: alert_events · ingest_repairs · schema_validation_runs · DLQ 토픽 · Flink 메트릭.
--
-- 이 표는 그 조각을 단계(stage) 축 하나로 모은다. 사람이 묻는 질문이 "어제 새벽에 어느 단계가 왜 깨졌나"이기 때문이다.
CREATE TABLE IF NOT EXISTS cdc_pipeline.pipeline_incidents
(
    detected_at DateTime,                   -- 우리가 알아챈 시각(사건 발생 시각이 아닐 수 있다)
    stage       LowCardinality(String),     -- collect | mysql | debezium | kafka | flink | clickhouse | dbt | orchestration
    component   LowCardinality(String),     -- 구체 대상 (cdc-orderbook-collector, mysql-cdc-connector, CDC Realtime Pipeline …)
    severity    LowCardinality(String),     -- warn | error
    title       String,                     -- 한 줄 요약
    detail      String,                     -- 판단 근거가 된 값
    source      LowCardinality(String),     -- 이 사건을 만든 주체 (health_check, quality_alerts, cron …)
    dedup_key   String                      -- 같은 사건을 반복해 쌓지 않기 위한 키
)
ENGINE = ReplacingMergeTree(detected_at)
ORDER BY (stage, dedup_key)
TTL detected_at + INTERVAL 180 DAY;
