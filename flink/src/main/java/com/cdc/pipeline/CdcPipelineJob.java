package com.cdc.pipeline;

import com.cdc.pipeline.function.MarketAlertDetector;
import com.cdc.pipeline.function.CdcEventParser;
import com.cdc.pipeline.model.CryptoTradeEvent;
import com.cdc.pipeline.model.MarketAlert;
import com.cdc.pipeline.sink.ClickHouseSinks;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import com.cdc.pipeline.function.NullSafeStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDC Realtime Pipeline - 암호화폐 체결 데이터
 * 
 * Kafka CDC 토픽(cdc.crypto_db.crypto_trades)에서 Debezium 이벤트를 읽어:
 * 1. Raw 체결 이벤트 → ClickHouse crypto_trades (새 컬럼 recv_ms·ingest_source·stream_type 포함, 2026-09-19)
 * 2. PRICE_24H 등급 전이 → ClickHouse market_alerts (섀도, docs/22)
 * 3. 파싱 실패 원문 → Kafka DLQ 토픽 (docs/29 창2)
 * (마켓별 5분 처리 시간 집계는 2026-09-19 폐기 - 정지 뒤 따라붙는 행이 "지금" 창을 왜곡, 분 마트가 이벤트 시각으로 대체)
 */
public class CdcPipelineJob {

    private static final Logger LOG = LoggerFactory.getLogger(CdcPipelineJob.class);

    public static void main(String[] args) throws Exception {

        // 1. 실행 환경 설정
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 2026-09-09: 클러스터 기본(3회×10초)은 ClickHouse 재시작(실측 24초)보다 짧아 잡이 FAILED로 멈출 수 있음 → 20회×30초
        env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(
                20, org.apache.flink.api.common.time.Time.seconds(30)));

        // 2. 환경변수에서 설정 읽기
        String bootstrapServers = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka-1:29092,kafka-2:29093,kafka-3:29094"
        );
        String clickhouseUrl = System.getenv().getOrDefault(
            "CLICKHOUSE_URL",
            "jdbc:clickhouse://clickhouse:8123/cdc_pipeline"
        );
        // 2026-09-17 부하 실험(docs/15) 격리용. 기본값은 프로덕션과 동일하고, 실험 잡만 제출 시 env 로 바꾼다:
        //   CDC_TOPIC=load_test.trades CDC_GROUP_ID=flink-loadtest-consumer CLICKHOUSE_TABLE_PREFIX=load_test_
        //   MARKET_ALERTS_ENABLED=false (실험 체결이 섀도 평가에 섞이지 않게) JOB_NAME="CDC Realtime Pipeline [load_test]"
        String topic = System.getenv().getOrDefault("CDC_TOPIC", "cdc.crypto_db.crypto_trades");
        String groupId = System.getenv().getOrDefault("CDC_GROUP_ID", "flink-cdc-consumer");
        String tablePrefix = System.getenv().getOrDefault("CLICKHOUSE_TABLE_PREFIX", "");
        boolean alertsEnabled = !"false".equalsIgnoreCase(System.getenv().getOrDefault("MARKET_ALERTS_ENABLED", "true"));
        String jobName = System.getenv().getOrDefault("JOB_NAME", "CDC Realtime Pipeline");
        String dlqTopic = System.getenv().getOrDefault("CDC_DLQ_TOPIC", "cdc.dlq.crypto_trades");

        // 3. Kafka Source 설정
        //   2026-09-20 (docs/34 #6): 재처리 모드. CDC_START_TS_MS/CDC_END_TS_MS 를 주면 그 구간만 읽고 잡이 스스로 끝난다(setBounded).
        //   왜 같은 잡인가: 재처리를 SQL 로 따로 짜면 파서와 두 번째 구현이 생겨 드리프트한다. 같은 코드로 다시 흘리면 변환 로직이 하나로 유지된다.
        //   중복은 ClickHouse ReplacingMergeTree 가 (market, upbit_timestamp, sequential_id) 키로 정리하고 flink_ts 가 더 큰 재처리분이 이긴다 → 멱등.
        long startTsMs = Long.parseLong(System.getenv().getOrDefault("CDC_START_TS_MS", "0"));
        long endTsMs = Long.parseLong(System.getenv().getOrDefault("CDC_END_TS_MS", "0"));
        org.apache.flink.connector.kafka.source.KafkaSourceBuilder<String> sourceBuilder = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setValueOnlyDeserializer(new NullSafeStringSchema());
        if (startTsMs > 0) {
            sourceBuilder.setStartingOffsets(OffsetsInitializer.timestamp(startTsMs));
        } else {
            // 2026-09-09: savepoint 없이 재시작해도 커밋된 그룹 오프셋부터 재개 (없으면 latest) - 재시작 유실 방지
            sourceBuilder.setStartingOffsets(OffsetsInitializer.committedOffsets(org.apache.kafka.clients.consumer.OffsetResetStrategy.LATEST));
        }
        if (endTsMs > 0) {
            sourceBuilder.setBounded(OffsetsInitializer.timestamp(endTsMs));
        }
        KafkaSource<String> kafkaSource = sourceBuilder.build();

        // 4. Source → CryptoTradeEvent 파싱 (실패는 사이드 아웃풋 → DLQ)
        // uid 를 명시하는 이유(2026-09-19): 자동 uid 는 체인 구조의 해시라 연산자를 하나만 빼도 바뀌어 savepoint 의 오프셋이 안 붙는다.
        // 이번 재배포는 5분 집계를 빼므로 어차피 --allowNonRestoredState 이고, 소스는 커밋된 그룹 오프셋(체크포인트 시점)에서 이어 읽는다.
        SingleOutputStreamOperator<CryptoTradeEvent> parsed = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka CDC Source")
                .uid("kafka-cdc-source")
                .filter(msg -> msg != null)
                .process(new CdcEventParser())
                .uid("cdc-event-parser")
                .name("CDC Event Parser");
        DataStream<CryptoTradeEvent> tradeEvents = parsed;

        // 5. 파싱 실패 원문 → Kafka DLQ (at-least-once: 원문이 두 번 남는 것은 유실보다 낫다)
        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(dlqTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        parsed.getSideOutput(CdcEventParser.DLQ)
                .sinkTo(dlqSink)
                .uid("parse-dlq-sink")
                .name("Kafka DLQ Sink");

        // 6. Stream 2: 이상탐지 v2 - PRICE_24H 등급 전이 → market_alerts (섀도, docs/22)
        // uid 를 명시하는 이유: 구 AnomalyDetector 의 상태(lastPrice 등)를 이어받지 않고 새로 시작한다.
        // 재제출 시 savepoint 의 구 연산자 상태는 --allowNonRestoredState 로 의도적으로 버린다 (근거 없는 규칙의 상태는 보존 가치가 없다).
        if (alertsEnabled) {
            DataStream<MarketAlert> marketAlerts = tradeEvents
                    .filter(event -> "c".equals(event.getOp()))
                    .keyBy(CryptoTradeEvent::getMarket)
                    .process(new MarketAlertDetector())
                    .uid("market-alert-detector-v2")
                    .name("Market Alert Detector (PRICE_24H)");

            marketAlerts.print("ALERT");
            marketAlerts.addSink(ClickHouseSinks.marketAlertSink(clickhouseUrl))
                    .uid("market-alert-sink-v2")
                    .name("ClickHouse Market Alert Sink");
        }

        // 7. Stream 3: Raw 체결 이벤트 → ClickHouse
        tradeEvents.addSink(ClickHouseSinks.rawTradeSink(clickhouseUrl, tablePrefix))
                .uid("clickhouse-raw-trade-sink")
                .name("ClickHouse Raw Trade Sink");

        LOG.info("=== CDC Crypto Pipeline Started ===");
        LOG.info("Kafka: {}", bootstrapServers);
        LOG.info("ClickHouse: {}", clickhouseUrl);
        LOG.info("Topic: {} / group: {} / table prefix: '{}' / alerts: {} / dlq: {}", topic, groupId, tablePrefix, alertsEnabled, dlqTopic);
        LOG.info("Parallelism: {}", env.getParallelism());
        if (startTsMs > 0 || endTsMs > 0) LOG.info("REPROCESS MODE: start={} end={} (bounded={})", startTsMs, endTsMs, endTsMs > 0);

        env.execute(jobName);
    }
}
