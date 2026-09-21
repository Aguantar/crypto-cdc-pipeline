package com.cdc.pipeline.binance;

import com.cdc.pipeline.orderbook.OrderbookEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/** Binance 공식 절차대로: 스냅샷 → 증분 적용(수량 0 삭제) → 옛 증분 무시 → 순번 끊김 desync → 스냅샷으로 복구. */
public class OrderBookReconstructorTest {
    private KeyedOneInputStreamOperatorTestHarness<String, DepthMsg, OrderbookEvent> h;

    @Before public void setUp() throws Exception { h = new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new OrderBookReconstructor()), m -> m.symbol, Types.STRING); h.open(); h.setProcessingTime(0); }
    @After public void tearDown() throws Exception { h.close(); }

    static DepthMsg snap(long id, double[][] bids, double[][] asks) { DepthMsg m = new DepthMsg(); m.snapshot = true; m.symbol = "BTCUSDT"; m.firstId = id; m.lastId = id; m.eventMs = 1000; m.recvMs = 1001; set(m, bids, asks); return m; }
    static DepthMsg diff(long U, long u, double[][] bids, double[][] asks) { DepthMsg m = new DepthMsg(); m.snapshot = false; m.symbol = "BTCUSDT"; m.firstId = U; m.lastId = u; m.eventMs = 2000; m.recvMs = 2001; set(m, bids, asks); return m; }
    static void set(DepthMsg m, double[][] bids, double[][] asks) {
        m.bidPx = new double[bids.length]; m.bidQty = new double[bids.length]; for (int i = 0; i < bids.length; i++) { m.bidPx[i] = bids[i][0]; m.bidQty[i] = bids[i][1]; }
        m.askPx = new double[asks.length]; m.askQty = new double[asks.length]; for (int i = 0; i < asks.length; i++) { m.askPx[i] = asks[i][0]; m.askQty[i] = asks[i][1]; }
    }
    private OrderbookEvent tick() throws Exception { h.setProcessingTime(h.getProcessingTime() + 1000); List<OrderbookEvent> o = h.extractOutputValues(); return o.get(o.size() - 1); }

    @Test
    public void snapshotThenDiffsWithDeleteAndOrdering() throws Exception {
        h.processElement(new StreamRecord<>(snap(100, new double[][]{{100, 1}, {99, 2}}, new double[][]{{101, 1}, {102, 2}})));
        OrderbookEvent e = tick();
        assertEquals(100.0, e.bestBid(), 0); assertEquals(101.0, e.bestAsk(), 0); assertEquals(3.0, e.getTotalBidSize(), 0);
        h.processElement(new StreamRecord<>(diff(101, 103, new double[][]{{100, 0}, {99.5, 5}}, new double[][]{{101, 0.5}})));   // 100 삭제, 99.5 추가, ask 101 수량 변경
        e = tick();
        assertEquals(99.5, e.bestBid(), 0); assertEquals(5.0, e.getBidSizes()[0], 0); assertEquals(0.5, e.getAskSizes()[0], 0); assertEquals(7.0, e.getTotalBidSize(), 0);
        h.processElement(new StreamRecord<>(diff(95, 99, new double[][]{{50, 100}}, new double[][]{})));   // 옛 증분(u <= lastId) → 무시
        e = tick(); assertEquals(99.5, e.bestBid(), 0);
    }

    @Test
    public void gapDesyncsUntilNextSnapshot() throws Exception {
        h.processElement(new StreamRecord<>(snap(100, new double[][]{{100, 1}}, new double[][]{{101, 1}})));
        int before = h.extractOutputValues().size(); tick();
        h.processElement(new StreamRecord<>(diff(105, 106, new double[][]{{100, 9}}, new double[][]{})));   // U=105 > lastId+1=101 → 끊김
        h.setProcessingTime(h.getProcessingTime() + 3000);
        assertEquals(before + 1, h.extractOutputValues().size());   // desync 동안은 출력 없음
        h.processElement(new StreamRecord<>(snap(200, new double[][]{{110, 3}}, new double[][]{{111, 4}})));
        OrderbookEvent e = tick(); assertEquals(110.0, e.bestBid(), 0); assertEquals(3.0, e.getBidSizes()[0], 0);
    }
}
