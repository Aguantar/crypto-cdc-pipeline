package com.cdc.pipeline.sink;

import com.cdc.pipeline.model.CryptoTradeEvent;
import com.cdc.pipeline.model.MarketAlert;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Timestamp;

/**
 * ClickHouse JDBC Sink 팩토리 (암호화폐 버전)
 */
public class ClickHouseSinks {

    // 2026-09-17 부하 실험 S4(docs/23 §5): 배치 크기를 env 로. 기본 200 = 프로덕션 불변. 실험 잡만 CLICKHOUSE_BATCH_SIZE=1000 으로 제출.
    // 근거: 10k/s 에서 소스 체인(동기 JDBC 싱크 포함) busy 가 1.0 에 닿았고 그 시각 insert 평균 지연이 9→34ms 로 올랐다.
    // 서브태스크당 busy ≈ inserts/s × insert 지연. 배치를 키우면 inserts/s 가 그만큼 준다(3초 간격 상한은 그대로).
    private static final int BATCH_SIZE = Integer.parseInt(System.getenv().getOrDefault("CLICKHOUSE_BATCH_SIZE", "200"));
    private static final long BATCH_INTERVAL_MS = 3000;
    private static final int MAX_RETRIES = 3;

    /**
     * Raw 체결 데이터 → crypto_trades 테이블
     */
    public static SinkFunction<CryptoTradeEvent> rawTradeSink(String clickhouseUrl) {
        return rawTradeSink(clickhouseUrl, "");
    }

    /** tablePrefix: 부하 실험 격리용("load_test_"). 프로덕션은 "" (docs/15). */
    public static SinkFunction<CryptoTradeEvent> rawTradeSink(String clickhouseUrl, String tablePrefix) {
        return JdbcSink.sink(
            "INSERT INTO " + tablePrefix + "crypto_trades (op, trade_id, market, trade_price, trade_volume, trade_amount, ask_bid, upbit_timestamp, sequential_id, source_ts, cdc_ts, cdc_latency_ms, flink_ts, best_ask_price, best_ask_size, best_bid_price, best_bid_size, recv_ms, ingest_source, stream_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ps, event) -> {
                ps.setString(1, event.getOp());
                ps.setLong(2, event.getTradeId());
                ps.setString(3, event.getMarket());
                // 2026-09-20 (docs/34 #5): Decimal 컬럼에 BigDecimal 을 그대로. double 로 보내면 여기서 정밀도가 사라진다.
                ps.setBigDecimal(4, event.getTradePrice());
                ps.setBigDecimal(5, event.getTradeVolume());
                ps.setBigDecimal(6, event.getTradeAmount());
                ps.setString(7, event.getAskBid());
                ps.setLong(8, event.getUpbitTimestamp());
                ps.setLong(9, event.getSequentialId());
                ps.setTimestamp(10, new Timestamp(event.getSourceTimestamp()));
                ps.setTimestamp(11, new Timestamp(event.getCdcTimestamp()));
                ps.setLong(12, event.getCdcLatencyMs());
                ps.setTimestamp(13, new Timestamp(System.currentTimeMillis()));
                setNullableDouble(ps, 14, event.getBestAskPrice());
                setNullableDouble(ps, 15, event.getBestAskSize());
                setNullableDouble(ps, 16, event.getBestBidPrice());
                setNullableDouble(ps, 17, event.getBestBidSize());
                if (event.getRecvMs() == null) ps.setNull(18, java.sql.Types.BIGINT); else ps.setLong(18, event.getRecvMs());
                ps.setString(19, event.getIngestSource());
                ps.setString(20, event.getStreamType());
            },
            executionOptions(),
            connectionOptions(clickhouseUrl)
        );
    }

    private static void setNullableDouble(java.sql.PreparedStatement ps, int idx, Double v) throws java.sql.SQLException {
        // 최우선 호가: Nullable(Decimal(20,8)). 원천이 DECIMAL(20,8) 이라 double 을 거치면 같은 손실이 난다.
        if (v == null) ps.setNull(idx, java.sql.Types.DECIMAL);
        else ps.setBigDecimal(idx, new java.math.BigDecimal(Double.toString(v)).setScale(8, java.math.RoundingMode.HALF_UP));
    }

    // 5분 처리 시간 집계 싱크는 2026-09-19 폐기 (docs/29 창2): 정지 뒤 따라붙는 행이 "지금" 창에 섞여 과거 5분을 왜곡했고, 분 마트(docs/27)가 이벤트 시각으로 같은 값을 낸다.

    /**
     * 이상탐지 v2 - 마켓 등급 전이 → market_alerts (docs/22). 섀도 기간엔 이 테이블만 쓰고 발송은 없다.
     *
     * 2026-09-20 (docs/34 #5): 여기는 의도적으로 Float64 유지. value·threshold 는 퍼센트(비율)이고, ref_price·price 는
     * 탐지기의 24h 링(ValueState<double[]>)에서 나온 값이다. 링 타입을 바꾸면 세이브포인트 복원이 깨지고 규칙 동등성(116/116)을 다시 증명해야 한다.
     * 알럿은 회계 대상이 아니라 판정 근거이므로 Float64 로 충분하다 - 금액 계약이 필요한 곳은 체결 표(crypto_trades)다.
     */
    public static SinkFunction<MarketAlert> marketAlertSink(String clickhouseUrl) {
        return JdbcSink.sink(
            "INSERT INTO market_alerts (alert_type, market, level, prev_level, event_time, detected_at, value, threshold, ref_price, price, trade_id, rule_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ps, a) -> {
                ps.setString(1, a.getAlertType());
                ps.setString(2, a.getMarket());
                ps.setInt(3, a.getLevel());
                ps.setInt(4, a.getPrevLevel());
                ps.setTimestamp(5, new Timestamp(a.getEventTime()));
                ps.setTimestamp(6, new Timestamp(a.getDetectedAt()));
                ps.setDouble(7, a.getValue());
                ps.setDouble(8, a.getThreshold());
                ps.setDouble(9, a.getRefPrice());
                ps.setDouble(10, a.getPrice());
                ps.setLong(11, a.getTradeId());
                ps.setString(12, a.getRuleVersion());
            },
            executionOptions(),
            connectionOptions(clickhouseUrl)
        );
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
