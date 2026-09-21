package com.cdc.pipeline.binance;

import com.cdc.pipeline.orderbook.OrderbookEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * 심볼별 호가장 재구성 (docs/31 §3-3). Binance 공식 절차:
 *   스냅샷(lastUpdateId) 으로 초기화 → 증분은 u <= lastUpdateId 면 버림, U <= lastUpdateId+1 <= u 면 적용(수량 0 = 레벨 삭제), 그 밖은 순번 끊김 → desync.
 * desync 면 다음 스냅샷까지 증분을 버린다(카운터 gaps). 스냅샷은 수집기가 5분마다 같은 키로 넣으므로 복구는 최대 5분.
 * 출력: EMIT_MS 마다(처리 시간 타이머) 상위 TOP_N 을 Upbit 호가와 같은 OrderbookEvent 로 - 두 거래소가 같은 표·같은 집계기를 쓴다.
 * 상태: TreeMap 두 개(가격→수량). 힙 백엔드라 직렬화 비용은 체크포인트 때만.
 */
public class OrderBookReconstructor extends KeyedProcessFunction<String, DepthMsg, OrderbookEvent> {
    static final int TOP_N = 20; static final long EMIT_MS = 1000;
    private transient ValueState<TreeMap<Double, Double>> bids, asks;   // bids 는 내림차순 키
    private transient ValueState<Long> lastId; private transient ValueState<Boolean> synced; private transient ValueState<Long> lastEventMs, lastRecvMs, nextTimer;
    private transient Counter gaps, snapshotsApplied, diffsApplied, staleDropped, unsyncedDropped;

    @Override
    public void open(Configuration c) {
        TypeInformation<TreeMap<Double, Double>> ti = TypeInformation.of(new TypeHint<TreeMap<Double, Double>>() {});
        bids = getRuntimeContext().getState(new ValueStateDescriptor<>("bids", ti)); asks = getRuntimeContext().getState(new ValueStateDescriptor<>("asks", ti));
        lastId = getRuntimeContext().getState(new ValueStateDescriptor<>("lastId", Types.LONG)); synced = getRuntimeContext().getState(new ValueStateDescriptor<>("synced", Types.BOOLEAN));
        lastEventMs = getRuntimeContext().getState(new ValueStateDescriptor<>("lastEventMs", Types.LONG)); lastRecvMs = getRuntimeContext().getState(new ValueStateDescriptor<>("lastRecvMs", Types.LONG));
        nextTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("nextTimer", Types.LONG));
        gaps = getRuntimeContext().getMetricGroup().counter("gaps"); snapshotsApplied = getRuntimeContext().getMetricGroup().counter("snapshotsApplied");
        diffsApplied = getRuntimeContext().getMetricGroup().counter("diffsApplied"); staleDropped = getRuntimeContext().getMetricGroup().counter("staleDropped"); unsyncedDropped = getRuntimeContext().getMetricGroup().counter("unsyncedDropped");
    }

    static void apply(TreeMap<Double, Double> book, double[] px, double[] qty) {
        for (int i = 0; i < px.length; i++) { if (qty[i] == 0) book.remove(px[i]); else book.put(px[i], qty[i]); }
    }

    @Override
    public void processElement(DepthMsg m, Context ctx, Collector<OrderbookEvent> out) throws Exception {
        Long id = lastId.value();
        if (m.snapshot) {
            if (id != null && m.lastId < id && Boolean.TRUE.equals(synced.value())) { staleDropped.inc(); return; }   // 이미 더 앞선 상태면 옛 스냅샷 무시
            TreeMap<Double, Double> b = new TreeMap<>(Collections.reverseOrder()), a = new TreeMap<>();
            apply(b, m.bidPx, m.bidQty); apply(a, m.askPx, m.askQty);
            bids.update(b); asks.update(a); lastId.update(m.lastId); synced.update(true); snapshotsApplied.inc();
        } else {
            if (id == null || !Boolean.TRUE.equals(synced.value())) { unsyncedDropped.inc(); return; }
            if (m.lastId <= id) { staleDropped.inc(); return; }
            if (m.firstId > id + 1) { gaps.inc(); synced.update(false); return; }   // 순번 끊김 → 다음 스냅샷까지 desync
            TreeMap<Double, Double> b = bids.value(), a = asks.value();
            apply(b, m.bidPx, m.bidQty); apply(a, m.askPx, m.askQty);
            bids.update(b); asks.update(a); lastId.update(m.lastId); diffsApplied.inc();
        }
        lastEventMs.update(m.eventMs); lastRecvMs.update(m.recvMs);
        if (nextTimer.value() == null) { long t = (ctx.timerService().currentProcessingTime() / EMIT_MS + 1) * EMIT_MS; nextTimer.update(t); ctx.timerService().registerProcessingTimeTimer(t); }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<OrderbookEvent> out) throws Exception {
        if (Boolean.TRUE.equals(synced.value()) && bids.value() != null) {
            out.collect(toEvent(ctx.getCurrentKey(), bids.value(), asks.value(), lastEventMs.value(), lastRecvMs.value()));
        }
        long t = ts + EMIT_MS; nextTimer.update(t); ctx.timerService().registerProcessingTimeTimer(t);
    }

    static OrderbookEvent toEvent(String symbol, TreeMap<Double, Double> b, TreeMap<Double, Double> a, Long eventMs, Long recvMs) {
        int nb = Math.min(TOP_N, b.size()), na = Math.min(TOP_N, a.size());
        double[] bp = new double[nb], bs = new double[nb], ap = new double[na], as = new double[na];
        int i = 0; for (Map.Entry<Double, Double> e : b.entrySet()) { if (i >= nb) break; bp[i] = e.getKey(); bs[i] = e.getValue(); i++; }
        i = 0; for (Map.Entry<Double, Double> e : a.entrySet()) { if (i >= na) break; ap[i] = e.getKey(); as[i] = e.getValue(); i++; }
        double tb = 0, ta = 0; for (double v : b.values()) tb += v; for (double v : a.values()) ta += v;
        OrderbookEvent ev = new OrderbookEvent();
        ev.setMarket(symbol); ev.setTs(eventMs == null ? System.currentTimeMillis() : eventMs); ev.setLevel(TOP_N);
        ev.setBidPrices(bp); ev.setBidSizes(bs); ev.setAskPrices(ap); ev.setAskSizes(as); ev.setTotalBidSize(tb); ev.setTotalAskSize(ta);
        ev.setStreamType("REALTIME"); ev.setRecvTs(recvMs == null ? ev.getTs() : recvMs);
        return ev;
    }
}
