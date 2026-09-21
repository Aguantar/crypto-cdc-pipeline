-- 토픽 계약 검증 결과 (docs/34 #8 의 한계 해소, 2026-09-20).
--
-- 왜 표에 적나: 검증기를 cron 에만 걸면 "위반이 있었는데 아무도 안 봤다"가 된다.
-- 결과를 남기면 ① Airflow 의 매시 품질 판정이 같은 경로로 알릴 수 있고(알림은 한 군데서 나가야 한다)
-- ② "언제부터 깨졌나"를 나중에 물을 수 있다.
--
-- 왜 Airflow 가 직접 실행하지 않나: 검증기는 Kafka CLI(docker exec)가 필요한데,
-- health_check 부터 모든 DAG 은 "네트워크 API 기반, Docker 소켓 불필요"를 지키고 있다(docs/20).
-- 소켓을 Airflow 에 주는 대가보다, 호스트 cron 이 결과를 적고 Airflow 가 그 표를 보는 쪽이 싸다.
CREATE TABLE IF NOT EXISTS cdc_pipeline.schema_validation_runs
(
    ran_at     DateTime,
    topic      LowCardinality(String),
    status     LowCardinality(String),   -- ok | violation | no_sample
    checked    UInt16,                   -- 검사한 메시지 수
    violations UInt16,
    detail     String                    -- 위반 앞부분(최대 6건). 원문은 토픽에 있다
)
ENGINE = MergeTree
ORDER BY (topic, ran_at)
TTL ran_at + INTERVAL 90 DAY;
