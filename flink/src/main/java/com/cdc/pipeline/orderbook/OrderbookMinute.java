package com.cdc.pipeline.orderbook;

import java.io.Serializable;

/** 마켓별 1분 파생지표 (orderbook_1m 행) */
public class OrderbookMinute implements Serializable {
    private static final long serialVersionUID = 1L;

    public String market;
    public long windowStart;
    public long windowEnd;
    public long snapshots;
    public double midOpen, midClose, midMin, midMax;
    public double spreadAvg, spreadBpAvg, spreadBpMax;
    public double imb1Avg, imb5Avg, imb15Avg;
    public double askDepth15Avg, bidDepth15Avg, totalAskAvg, totalBidAvg;
    public double recvLagMsAvg;

    @Override
    public String toString() {
        return String.format("OB1m{%s %d n=%d mid=%.1f spread=%.2fbp imb5=%.3f lag=%.0fms}",
                market, windowStart, snapshots, midClose, spreadBpAvg, imb5Avg, recvLagMsAvg);
    }
}
