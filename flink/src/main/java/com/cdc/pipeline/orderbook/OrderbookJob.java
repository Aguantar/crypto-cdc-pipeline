package com.cdc.pipeline.orderbook;

import com.cdc.pipeline.function.NullSafeStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 호가 스트림 잡 (2026-09-09, 3차)
 *
 * Kafka upbit.orderbook.v1 → 파싱 →
 *   (1) orderbook_raw (전체 스냅샷, TTL 7일)
 *   (2) 마켓별 1분 이벤트타임 윈도우 → orderbook_1m (TTL 365일)
 *
 * 실행: flink run -d -c com.cdc.pipeline.orderbook.OrderbookJob /opt/flink/usrlib/flink-cdc-job-1.0.0.jar
 * 체결 잡(CdcPipelineJob)과 같은 JAR, 별도 잡·별도 컨슈머 그룹. 슬롯 1(병렬도 1).
 */
public class OrderbookJob {

    private static final Logger LOG = LoggerFactory.getLogger(OrderbookJob.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29093,kafka-3:29094");
        String topic = System.getenv().getOrDefault("ORDERBOOK_TOPIC", "upbit.orderbook.v1");
        String clickhouseUrl = System.getenv().getOrDefault("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/cdc_pipeline");
        int parallelism = Integer.parseInt(System.getenv().getOrDefault("ORDERBOOK_PARALLELISM", "1"));
        // 2026-09-17 부하 실험 격리(docs/15): 실험 잡만 ORDERBOOK_TOPIC=load_test.orderbook ORDERBOOK_GROUP_ID=... CLICKHOUSE_TABLE_PREFIX=load_test_
        String groupId = System.getenv().getOrDefault("ORDERBOOK_GROUP_ID", "flink-orderbook-consumer");
        String tablePrefix = System.getenv().getOrDefault("CLICKHOUSE_TABLE_PREFIX", "");
        String jobName = System.getenv().getOrDefault("JOB_NAME", "Orderbook Pipeline");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        // 체크포인트 간격/모드/백엔드는 클러스터 설정(flink-conf: 60s, EXACTLY_ONCE, hashmap)을 따른다.
        // 재시작 전략은 잡에서 지정: ClickHouse 재시작(실측 24초) 등 분 단위 장애를 넘기도록 20회×30초
        env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(
                20, org.apache.flink.api.common.time.Time.seconds(30)));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new NullSafeStringSchema())
                .build();

        // 이벤트타임 = 업비트 호가 시각(tms). 파티션이 잠시 비어도 워터마크가 멈추지 않도록 idleness 30초.
        WatermarkStrategy<OrderbookEvent> wm = WatermarkStrategy
                .<OrderbookEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> e.getTs())
                .withIdleness(Duration.ofSeconds(30));

        DataStream<OrderbookEvent> books = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Orderbook Source")
                .filter(msg -> msg != null)
                .flatMap(new OrderbookParser())
                .name("Orderbook Parser")
                .assignTimestampsAndWatermarks(wm);

        // (1) 원본 스냅샷
        books.addSink(OrderbookSinks.rawSink(clickhouseUrl, tablePrefix)).name("ClickHouse Orderbook Raw Sink");

        // (2) 1분 파생지표
        DataStream<OrderbookMinute> minutes = books
                .keyBy(OrderbookEvent::getMarket)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new OrderbookAggregator(), new OrderbookAggregator.WindowEnricher())
                .name("1min Orderbook Aggregation");
        minutes.addSink(OrderbookSinks.minuteSink(clickhouseUrl, tablePrefix)).name("ClickHouse Orderbook 1m Sink");

        LOG.info("=== Orderbook Job: topic={} bootstrap={} clickhouse={} parallelism={}", topic, bootstrap, clickhouseUrl, parallelism);
        env.execute(jobName);
    }
}
