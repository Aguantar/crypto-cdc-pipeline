# CDC 실시간 파이프라인 - 기술스택 전체 정리

## 데이터 파이프라인 (핵심 흐름)

| 기술 | 버전 | 역할 |
|------|------|------|
| Upbit WebSocket API | - | 5개 코인(BTC, ETH, XRP, SOL, DOGE) 실시간 체결 데이터 수집 |
| MySQL | 8.0 | CDC 소스 DB (binlog ROW 포맷, GTID 모드) |
| Debezium | 2.5 | MySQL binlog → Kafka로 CDC 이벤트 전달 (앱 수정 없이) |
| Apache Kafka | 3.6 | 3-broker 클러스터, RF=3, 72시간 보관 |
| Apache Flink | 1.18 | 스트림 처리 - 5분 윈도우 집계 + Z-Score 이상탐지 (RocksDB, exactly-once) |
| ClickHouse | 24.1 | OLAP 저장소 - crypto_trades, trade_aggregations, anomaly_alerts (365일 TTL) |

## 오케스트레이션 & 변환

| 기술 | 버전 | 역할 |
|------|------|------|
| Apache Airflow | 2.8.1 | 2개 DAG - health_check(10분), daily_pipeline(매일 01:00 KST) |
| dbt | 1.7.9 | 3-layer 모델 (staging VIEW → intermediate TABLE: OHLCV → marts TABLE: 리포트) |
| PostgreSQL | 15 | Airflow 메타데이터 DB |

## 모니터링 & 알림

| 기술 | 버전 | 역할 |
|------|------|------|
| Grafana | 11.0 | 2개 대시보드 (CDC Pipeline 12패널 + Airflow Operations 12패널) |
| Prometheus | 2.50 | Airflow 메트릭 수집 (30초 간격) |
| StatsD Exporter | 0.26 | Airflow StatsD → Prometheus 변환 브릿지 |
| n8n | latest | 매분 ClickHouse 폴링 → 이상거래 시 Slack+Gmail 즉시 알림 |

## 인프라 & 네트워크

| 기술 | 버전 | 역할 |
|------|------|------|
| Docker / Docker Compose | - | 27개 컨테이너 오케스트레이션 |
| Caddy | 2.10 | 리버스 프록시 + HTTPS 자동 인증서 |
| Zookeeper | 7.5.3 | Kafka 클러스터 코디네이션 |
| Kafka UI | latest | Kafka 클러스터 웹 모니터링 |

## 프로그래밍 언어 & 빌드

| 기술 | 용도 |
|------|------|
| Java 11 | Flink Job (CdcPipelineJob, AnomalyDetector 등) |
| Python 3.10/3.11 | Airflow DAG, WebSocket Producer, dbt |
| SQL | MySQL/ClickHouse 스키마, dbt 모델 |
| Maven | Flink fat JAR 빌드 |

## 하드웨어

- 미니PC - Intel N100 (4코어), 16GB RAM, 500GB SSD, Ubuntu 24.04
