package com.cdc.pipeline.function;

import com.cdc.pipeline.model.CryptoTradeEvent;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** docs/29 창2: 파싱 실패는 DLQ 사이드 아웃풋으로, 새 컬럼 3개는 그대로 통과 */
public class CdcEventParserTest {

    private OneInputStreamOperatorTestHarness<String, CryptoTradeEvent> h;

    private static final String AFTER = "{\"trade_id\":120176308,\"market\":\"KRW-ADA\",\"trade_price\":\"306.00000000\",\"trade_volume\":\"20.77271643\","
            + "\"trade_amount\":\"6356.4512\",\"ask_bid\":\"BID\",\"upbit_timestamp\":1789801975562,\"sequential_id\":17898019755620000,\"recv_ms\":%s,"
            + "\"created_at\":\"2026-09-19T07:12:57.533Z\",\"best_ask_price\":\"307.00000000\",\"best_ask_size\":\"1091760.63634493\",\"best_bid_price\":\"306.00000000\","
            + "\"best_bid_size\":\"9780.93201943\",\"ingest_source\":\"%s\",\"stream_type\":\"REALTIME\"}";

    private static String envelope(String op, String after) {
        return "{\"payload\":{\"before\":null,\"after\":" + after + ",\"source\":{\"ts_ms\":1789801977000},\"op\":\"" + op + "\",\"ts_ms\":1789801977100}}";
    }

    @Before
    public void setUp() throws Exception {
        h = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(new CdcEventParser()));
        h.open();
    }

    @After
    public void tearDown() throws Exception { h.close(); }

    private void in(String json) throws Exception { h.processElement(new StreamRecord<>(json)); }
    private List<CryptoTradeEvent> out() { return h.extractOutputValues(); }
    private int dlqCount() {
        ConcurrentLinkedQueue<StreamRecord<String>> q = h.getSideOutput(CdcEventParser.DLQ);
        return q == null ? 0 : q.size();
    }

    @Test
    public void validMessageCarriesNewColumns() throws Exception {
        in(envelope("c", String.format(AFTER, "1789801975800", "gapfill")));
        assertEquals(1, out().size());
        CryptoTradeEvent e = out().get(0);
        assertEquals(Long.valueOf(1789801975800L), e.getRecvMs());
        assertEquals("gapfill", e.getIngestSource());
        assertEquals("REALTIME", e.getStreamType());
        assertEquals(new BigDecimal("306.00000000"), e.getTradePrice());          // 원천 스케일 8 그대로
        assertEquals(new BigDecimal("20.77271643"), e.getTradeVolume());
        // 금액은 MySQL 의 trade_amount("6356.4512", DECIMAL(20,4) 반올림) 가 아니라 price×volume 의 정확한 곱
        assertEquals(new BigDecimal("306.00000000").multiply(new BigDecimal("20.77271643")), e.getTradeAmount());
        assertEquals(16, e.getTradeAmount().scale());
        assertEquals(100, e.getCdcLatencyMs());
        assertEquals(0, dlqCount());
    }

    @Test
    public void nullRecvMsStaysNullAndMissingSourceDefaults() throws Exception {
        String after = String.format(AFTER, "null", "ws").replace(",\"ingest_source\":\"ws\"", "");
        in(envelope("c", after));
        assertEquals(1, out().size());
        assertNull(out().get(0).getRecvMs());
        assertEquals("ws", out().get(0).getIngestSource());
    }

    @Test
    public void brokenJsonGoesToDlqWithRawAndReason() throws Exception {
        in("{\"payload\":{\"op\":\"c\",\"after\":{\"trade_id\":1,");
        assertEquals(0, out().size());
        assertEquals(1, dlqCount());
        String rec = h.getSideOutput(CdcEventParser.DLQ).peek().getValue();
        assertTrue(rec.contains("\"raw\":\"{\\\"payload\\\""));
        assertTrue(rec, rec.contains("\"error\":\"") && rec.contains("Exception"));
        assertTrue(rec.contains("\"failed_at\":"));
    }

    @Test
    public void createWithoutAfterIsAFailureNotASilentSkip() throws Exception {
        in("{\"payload\":{\"before\":null,\"after\":null,\"op\":\"c\",\"ts_ms\":1}}");
        assertEquals(0, out().size());
        assertEquals(1, dlqCount());
    }

    @Test
    public void tombstoneAndDeleteAreSkippedNotFailed() throws Exception {
        in(null);
        in("");
        in(envelope("d", "null").replace("\"after\":null", "\"after\":null,\"before\":{}"));
        assertEquals(0, out().size());
        assertEquals(0, dlqCount());
    }

    @Test
    public void dustTradeKeepsAmountInsteadOfRoundingToZero() throws Exception {
        // docs/34 #5 의 근거: MySQL trade_amount 는 DECIMAL(20,4) 라 가격×수량 < 0.00005 KRW 인 먼지 체결이 0 으로 저장된다(30일 64,669행·252마켓).
        // Flink 가 price×volume 을 계산하면 0 이 아니라 실제 값(4.2e-12)이 남는다.
        String after = "{\"trade_id\":1,\"market\":\"KRW-DUST\",\"trade_price\":\"0.00042000\",\"trade_volume\":\"0.00000001\","
                + "\"trade_amount\":\"0.0000\",\"ask_bid\":\"BID\",\"upbit_timestamp\":1789801975562,\"sequential_id\":17898019755620000,"
                + "\"recv_ms\":null,\"ingest_source\":\"ws\",\"stream_type\":\"REALTIME\"}";
        in(envelope("c", after));
        assertEquals(1, out().size());
        CryptoTradeEvent e = out().get(0);
        assertEquals(new BigDecimal("0.0000000000042000"), e.getTradeAmount());
        assertTrue(e.getTradeAmount().signum() > 0);
        assertEquals(0, dlqCount());
    }
}
