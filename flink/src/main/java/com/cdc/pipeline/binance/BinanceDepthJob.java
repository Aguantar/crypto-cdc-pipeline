package com.cdc.pipeline.binance;

import com.cdc.pipeline.function.NullSafeStringSchema;
import com.cdc.pipeline.orderbook.OrderbookAggregator;
import com.cdc.pipeline.orderbook.OrderbookEvent;
import com.cdc.pipeline.orderbook.OrderbookMinute;
import com.cdc.pipeline.orderbook.OrderbookSinks;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Binance 호가장 재구성 잡 (docs/31 §3-3): binance.depth.v1(증분+스냅샷) → 심볼별 키 상태 → 1초 상위 20 스냅샷(binance_orderbook_raw) + 1분 통계(binance_orderbook_1m).
 * Upbit 호가 잡과 같은 집계기·싱크(테이블 접두 binance_) → 두 거래소 호가가 같은 표 모양.
 * 실행: flink run -d -c com.cdc.pipeline.binance.BinanceDepthJob /opt/flink/usrlib/flink-cdc-job-1.0.0.jar  (BINANCE_DEPTH_PARALLELISM 기본 2)
 */
public class BinanceDepthJob {
    private static final Logger LOG = LoggerFactory.getLogger(BinanceDepthJob.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092");
        String topic = System.getenv().getOrDefault("BINANCE_DEPTH_TOPIC", "binance.depth.v1");
        String dlqTopic = System.getenv().getOrDefault("BINANCE_DEPTH_DLQ_TOPIC", "binance.dlq.depth");
        String groupId = System.getenv().getOrDefault("BINANCE_DEPTH_GROUP_ID", "flink-binance-depth-consumer");
        String clickhouseUrl = System.getenv().getOrDefault("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/cdc_pipeline");
        int parallelism = Integer.parseInt(System.getenv().getOrDefault("BINANCE_DEPTH_PARALLELISM", "2"));
        String jobName = System.getenv().getOrDefault("JOB_NAME", "Binance Depth Pipeline");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(20, org.apache.flink.api.common.time.Time.seconds(30)));

        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(bootstrap).setTopics(topic).setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)).setValueOnlyDeserializer(new NullSafeStringSchema()).build();

        SingleOutputStreamOperator<DepthMsg> msgs = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Binance Depth Source").uid("binance-depth-source")
                .process(new DepthParser()).uid("binance-depth-parser").name("Binance Depth Parser");
        KafkaSink<String> dlq = KafkaSink.<String>builder().setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(dlqTopic).setValueSerializationSchema(new SimpleStringSchema()).build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
        msgs.getSideOutput(DepthParser.DLQ).sinkTo(dlq).uid("binance-depth-dlq").name("Kafka DLQ Sink");

        WatermarkStrategy<OrderbookEvent> wm = WatermarkStrategy.<OrderbookEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((e, ts) -> e.getTs()).withIdleness(Duration.ofSeconds(30));
        DataStream<OrderbookEvent> books = msgs.keyBy(m -> m.symbol).process(new OrderBookReconstructor()).uid("binance-orderbook-reconstructor").name("Order Book Reconstructor")
                .assignTimestampsAndWatermarks(wm);

        books.addSink(OrderbookSinks.rawSink(clickhouseUrl, "binance_")).uid("binance-orderbook-raw-sink").name("ClickHouse Binance Orderbook Raw Sink");
        DataStream<OrderbookMinute> minutes = books.keyBy(OrderbookEvent::getMarket).window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new OrderbookAggregator(), new OrderbookAggregator.WindowEnricher()).uid("binance-orderbook-1m").name("1min Binance Orderbook Aggregation");
        minutes.addSink(OrderbookSinks.minuteSink(clickhouseUrl, "binance_")).uid("binance-orderbook-1m-sink").name("ClickHouse Binance Orderbook 1m Sink");

        LOG.info("=== Binance Depth Job: topic={} group={} parallelism={}", topic, groupId, parallelism);
        env.execute(jobName);
    }
}
