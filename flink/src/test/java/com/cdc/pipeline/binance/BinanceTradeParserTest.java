package com.cdc.pipeline.binance;

import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BinanceTradeParserTest {
    private OneInputStreamOperatorTestHarness<String, BinanceTrade> h;

    @Before public void setUp() throws Exception { h = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(new BinanceTradeParser())); h.open(); }
    @After public void tearDown() throws Exception { h.close(); }

    @Test
    public void parsesLiveShapedMessage() throws Exception {
        h.processElement(new StreamRecord<>("{\"e\":\"trade\",\"E\":1789804063322,\"s\":\"BTCUSDT\",\"t\":304703801,\"p\":\"81032.00000000\",\"q\":\"0.05677000\",\"T\":1789804063321,\"m\":true,\"M\":true,\"recv_ms\":1789804063340}"));
        assertEquals(1, h.extractOutputValues().size());
        BinanceTrade t = h.extractOutputValues().get(0);
        assertEquals("BTCUSDT", t.symbol); assertEquals(304703801L, t.tradeId);
        assertEquals(new BigDecimal("81032.00000000"), t.price); assertEquals(new BigDecimal("0.05677000"), t.qty);
        // quote_qty 는 저장 컬럼 Decimal(38,16) 과 같은 스케일의 정확한 곱
        assertEquals(new BigDecimal("81032.00000000").multiply(new BigDecimal("0.05677000")), t.quoteQty);
        assertEquals(16, t.quoteQty.scale());
        assertTrue(t.buyerMaker); assertEquals(1789804063321L, t.tradeMs); assertEquals(1789804063340L, t.recvMs);
    }

    @Test
    public void nonTradeAndBrokenGoToDlq() throws Exception {
        h.processElement(new StreamRecord<>("{\"e\":\"aggTrade\",\"s\":\"BTCUSDT\",\"t\":1,\"p\":\"1\",\"q\":\"1\",\"T\":1}"));
        h.processElement(new StreamRecord<>("{\"e\":\"trade\",\"s\":\"BTCUSDT\""));
        assertEquals(0, h.extractOutputValues().size());
        assertEquals(2, h.getSideOutput(BinanceTradeParser.DLQ).size());
        assertTrue(h.getSideOutput(BinanceTradeParser.DLQ).peek().getValue().contains("not a trade event"));
    }
}
