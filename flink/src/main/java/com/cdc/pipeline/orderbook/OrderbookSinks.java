package com.cdc.pipeline.orderbook;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Timestamp;

/**
 * ClickHouse JDBC sink (호가). 배치 500건 / 2초 / 재시도 3회.
 * Array(Float64) 컬럼은 clickhouse-jdbc 0.6의 setObject(double[]) 로 바인딩.
 */
public class OrderbookSinks {

    private static final int BATCH_SIZE = 500;
    private static final long BATCH_INTERVAL_MS = 2000;
    private static final int MAX_RETRIES = 3;

    public static SinkFunction<OrderbookEvent> rawSink(String url) { return rawSink(url, ""); }

    /** tablePrefix: 부하 실험 격리용("load_test_"), 프로덕션은 "" (docs/15). */
    public static SinkFunction<OrderbookEvent> rawSink(String url, String tablePrefix) {
        return JdbcSink.sink(
            "INSERT INTO " + tablePrefix + "orderbook_raw (market, ts, level, total_ask_size, total_bid_size, ask_prices, ask_sizes, bid_prices, bid_sizes, stream_type, recv_ts, flink_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ps, e) -> {
                ps.setString(1, e.getMarket());
                ps.setTimestamp(2, new Timestamp(e.getTs()));
                ps.setDouble(3, e.getLevel());
                ps.setDouble(4, e.getTotalAskSize());
                ps.setDouble(5, e.getTotalBidSize());
                ps.setObject(6, e.getAskPrices());
                ps.setObject(7, e.getAskSizes());
                ps.setObject(8, e.getBidPrices());
                ps.setObject(9, e.getBidSizes());
                ps.setString(10, e.getStreamType());
                ps.setTimestamp(11, new Timestamp(e.getRecvTs()));
                ps.setTimestamp(12, new Timestamp(System.currentTimeMillis()));
            },
            executionOptions(), connectionOptions(url));
    }

    public static SinkFunction<OrderbookMinute> minuteSink(String url) { return minuteSink(url, ""); }

    public static SinkFunction<OrderbookMinute> minuteSink(String url, String tablePrefix) {
        return JdbcSink.sink(
            "INSERT INTO " + tablePrefix + "orderbook_1m (market, window_start, window_end, snapshots, mid_open, mid_close, mid_min, mid_max, spread_avg, spread_bp_avg, spread_bp_max, imb1_avg, imb5_avg, imb15_avg, ask_depth15_avg, bid_depth15_avg, total_ask_avg, total_bid_avg, recv_lag_ms_avg, flink_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ps, m) -> {
                ps.setString(1, m.market);
                ps.setTimestamp(2, new Timestamp(m.windowStart));
                ps.setTimestamp(3, new Timestamp(m.windowEnd));
                ps.setLong(4, m.snapshots);
                ps.setDouble(5, m.midOpen); ps.setDouble(6, m.midClose); ps.setDouble(7, m.midMin); ps.setDouble(8, m.midMax);
                ps.setDouble(9, m.spreadAvg); ps.setDouble(10, m.spreadBpAvg); ps.setDouble(11, m.spreadBpMax);
                ps.setDouble(12, m.imb1Avg); ps.setDouble(13, m.imb5Avg); ps.setDouble(14, m.imb15Avg);
                ps.setDouble(15, m.askDepth15Avg); ps.setDouble(16, m.bidDepth15Avg);
                ps.setDouble(17, m.totalAskAvg); ps.setDouble(18, m.totalBidAvg);
                ps.setDouble(19, m.recvLagMsAvg);
                ps.setTimestamp(20, new Timestamp(System.currentTimeMillis()));
            },
            executionOptions(), connectionOptions(url));
    }

    private static JdbcExecutionOptions executionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(BATCH_SIZE)
                .withBatchIntervalMs(BATCH_INTERVAL_MS)
                .withMaxRetries(MAX_RETRIES)
                .build();
    }

    private static JdbcConnectionOptions connectionOptions(String url) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url)
                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                .build();
    }
}
