package com.cdc.pipeline.orderbook;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 마켓별 1분 이벤트타임 윈도우 집계.
 * - mid = (bestAsk + bestBid) / 2, spread = bestAsk - bestBid, spread_bp = spread / mid * 1e4
 * - imbalance_N = (bidTopN - askTopN) / (bidTopN + askTopN), N ∈ {1, 5, 15}
 * - recv_lag = rts - tms (수집기 수신 지연)
 * 증분 집계(AggregateFunction)라 상태는 마켓×윈도우당 누적값 1세트뿐.
 */
public class OrderbookAggregator implements AggregateFunction<OrderbookEvent, OrderbookAggregator.Acc, OrderbookMinute> {

    public static class Acc implements java.io.Serializable {
        String market;
        long count;
        long firstTs = Long.MAX_VALUE, lastTs = Long.MIN_VALUE;
        double midOpen, midClose, midMin = Double.MAX_VALUE, midMax = -Double.MAX_VALUE;
        double spreadSum, spreadBpSum, spreadBpMax;
        double imb1Sum, imb5Sum, imb15Sum;
        double askDepth15Sum, bidDepth15Sum, totalAskSum, totalBidSum;
        double lagSum;
    }

    @Override
    public Acc createAccumulator() { return new Acc(); }

    @Override
    public Acc add(OrderbookEvent e, Acc a) {
        double ask = e.bestAsk(), bid = e.bestBid();
        if (ask <= 0 || bid <= 0) return a;
        double mid = (ask + bid) / 2.0;
        double spread = ask - bid;
        double spreadBp = spread / mid * 10000.0;
        a.market = e.getMarket();
        a.count++;
        if (e.getTs() < a.firstTs) { a.firstTs = e.getTs(); a.midOpen = mid; }
        if (e.getTs() >= a.lastTs) { a.lastTs = e.getTs(); a.midClose = mid; }
        a.midMin = Math.min(a.midMin, mid);
        a.midMax = Math.max(a.midMax, mid);
        a.spreadSum += spread;
        a.spreadBpSum += spreadBp;
        a.spreadBpMax = Math.max(a.spreadBpMax, spreadBp);
        a.imb1Sum += imbalance(e, 1);
        a.imb5Sum += imbalance(e, 5);
        a.imb15Sum += imbalance(e, 15);
        a.askDepth15Sum += OrderbookEvent.topSum(e.getAskSizes(), 15);
        a.bidDepth15Sum += OrderbookEvent.topSum(e.getBidSizes(), 15);
        a.totalAskSum += e.getTotalAskSize();
        a.totalBidSum += e.getTotalBidSize();
        a.lagSum += (e.getRecvTs() - e.getTs());
        return a;
    }

    private static double imbalance(OrderbookEvent e, int n) {
        double b = OrderbookEvent.topSum(e.getBidSizes(), n), s = OrderbookEvent.topSum(e.getAskSizes(), n);
        return (b + s) > 0 ? (b - s) / (b + s) : 0;
    }

    @Override
    public OrderbookMinute getResult(Acc a) {
        OrderbookMinute m = new OrderbookMinute();
        m.market = a.market;
        m.snapshots = a.count;
        double n = Math.max(1, a.count);
        m.midOpen = a.midOpen; m.midClose = a.midClose;
        m.midMin = a.count > 0 ? a.midMin : 0; m.midMax = a.count > 0 ? a.midMax : 0;
        m.spreadAvg = a.spreadSum / n; m.spreadBpAvg = a.spreadBpSum / n; m.spreadBpMax = a.spreadBpMax;
        m.imb1Avg = a.imb1Sum / n; m.imb5Avg = a.imb5Sum / n; m.imb15Avg = a.imb15Sum / n;
        m.askDepth15Avg = a.askDepth15Sum / n; m.bidDepth15Avg = a.bidDepth15Sum / n;
        m.totalAskAvg = a.totalAskSum / n; m.totalBidAvg = a.totalBidSum / n;
        m.recvLagMsAvg = a.lagSum / n;
        return m;
    }

    @Override
    public Acc merge(Acc x, Acc y) {
        Acc a = new Acc();
        a.market = x.market != null ? x.market : y.market;
        a.count = x.count + y.count;
        if (x.firstTs <= y.firstTs) { a.firstTs = x.firstTs; a.midOpen = x.midOpen; } else { a.firstTs = y.firstTs; a.midOpen = y.midOpen; }
        if (x.lastTs >= y.lastTs) { a.lastTs = x.lastTs; a.midClose = x.midClose; } else { a.lastTs = y.lastTs; a.midClose = y.midClose; }
        a.midMin = Math.min(x.midMin, y.midMin); a.midMax = Math.max(x.midMax, y.midMax);
        a.spreadSum = x.spreadSum + y.spreadSum; a.spreadBpSum = x.spreadBpSum + y.spreadBpSum; a.spreadBpMax = Math.max(x.spreadBpMax, y.spreadBpMax);
        a.imb1Sum = x.imb1Sum + y.imb1Sum; a.imb5Sum = x.imb5Sum + y.imb5Sum; a.imb15Sum = x.imb15Sum + y.imb15Sum;
        a.askDepth15Sum = x.askDepth15Sum + y.askDepth15Sum; a.bidDepth15Sum = x.bidDepth15Sum + y.bidDepth15Sum;
        a.totalAskSum = x.totalAskSum + y.totalAskSum; a.totalBidSum = x.totalBidSum + y.totalBidSum;
        a.lagSum = x.lagSum + y.lagSum;
        return a;
    }

    /** 윈도우 시작/종료 시각 부여 */
    public static class WindowEnricher extends ProcessWindowFunction<OrderbookMinute, OrderbookMinute, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<OrderbookMinute> elements, Collector<OrderbookMinute> out) {
            OrderbookMinute m = elements.iterator().next();
            m.market = key;
            m.windowStart = ctx.window().getStart();
            m.windowEnd = ctx.window().getEnd();
            if (m.snapshots > 0) out.collect(m);
        }
    }
}
