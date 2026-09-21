package com.cdc.pipeline.binance;

import com.cdc.pipeline.function.NullSafeStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Binance 체결 잡 (docs/31 §3-2, 2026-09-20): binance.trades.v1(6p) → 파싱(DLQ) → ClickHouse binance_trades.
 * 별도 잡인 이유: 장애 격리 - 체결 CDC 잡의 24h 링 상태·세이브포인트와 무관하게 재배포/재시작한다.
 * 병렬 3(6 파티션의 절반), JDBC 배치 1,000/3s: docs/23 실측(병목 = 소스 체인의 동기 싱크, 배치 200→1,000 으로 정체 0).
 * 실행: flink run -d -c com.cdc.pipeline.binance.BinanceTradeJob /opt/flink/usrlib/flink-cdc-job-1.0.0.jar
 */
public class BinanceTradeJob {
    private static final Logger LOG = LoggerFactory.getLogger(BinanceTradeJob.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092");
        String topic = System.getenv().getOrDefault("BINANCE_TRADES_TOPIC", "binance.trades.v1");
        String dlqTopic = System.getenv().getOrDefault("BINANCE_DLQ_TOPIC", "binance.dlq.trades");
        String groupId = System.getenv().getOrDefault("BINANCE_GROUP_ID", "flink-binance-consumer");
        String clickhouseUrl = System.getenv().getOrDefault("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/cdc_pipeline");
        String table = System.getenv().getOrDefault("BINANCE_TABLE", "binance_trades");
        int parallelism = Integer.parseInt(System.getenv().getOrDefault("BINANCE_PARALLELISM", "3"));
        int batch = Integer.parseInt(System.getenv().getOrDefault("BINANCE_BATCH_SIZE", "1000"));
        String jobName = System.getenv().getOrDefault("JOB_NAME", "Binance Trade Pipeline");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(20, org.apache.flink.api.common.time.Time.seconds(30)));

        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(bootstrap).setTopics(topic).setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))   // 첫 기동은 토픽 처음부터(수집기가 먼저 쌓아둔 분), 이후는 커밋 오프셋
                .setValueOnlyDeserializer(new NullSafeStringSchema()).build();

        SingleOutputStreamOperator<BinanceTrade> trades = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Binance Trade Source").uid("binance-trade-source")
                .process(new BinanceTradeParser()).uid("binance-trade-parser").name("Binance Trade Parser");

        KafkaSink<String> dlq = KafkaSink.<String>builder().setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(dlqTopic).setValueSerializationSchema(new SimpleStringSchema()).build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
        trades.getSideOutput(BinanceTradeParser.DLQ).sinkTo(dlq).uid("binance-dlq-sink").name("Kafka DLQ Sink");

        trades.addSink(JdbcSink.sink(
                "INSERT INTO " + table + " (symbol, trade_id, price, qty, quote_qty, is_buyer_maker, trade_ms, event_ms, recv_ms, flink_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, t) -> {
                    // 2026-09-20 (docs/34 #5): Decimal 컬럼 - BigDecimal 그대로. quote_qty 는 파서가 계산한 정확한 곱(스케일 16)
                    ps.setString(1, t.symbol); ps.setLong(2, t.tradeId); ps.setBigDecimal(3, t.price); ps.setBigDecimal(4, t.qty); ps.setBigDecimal(5, t.quoteQty);
                    ps.setInt(6, t.buyerMaker ? 1 : 0); ps.setLong(7, t.tradeMs); ps.setLong(8, t.eventMs); ps.setLong(9, t.recvMs);
                    ps.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
                },
                JdbcExecutionOptions.builder().withBatchSize(batch).withBatchIntervalMs(3000).withMaxRetries(3).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(clickhouseUrl).withDriverName("com.clickhouse.jdbc.ClickHouseDriver").build()))
                .uid("binance-clickhouse-sink").name("ClickHouse Binance Trade Sink");

        LOG.info("=== Binance Trade Job: topic={} group={} table={} parallelism={} batch={} dlq={}", topic, groupId, table, parallelism, batch, dlqTopic);
        env.execute(jobName);
    }
}
