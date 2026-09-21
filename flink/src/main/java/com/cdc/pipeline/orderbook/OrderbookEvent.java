package com.cdc.pipeline.orderbook;

import java.io.Serializable;

/**
 * Upbit 호가 스냅샷 1장 (WS orderbook, SIMPLE 포맷 + 수집기 수신시각 rts).
 * 15단 기준: prices/sizes 배열 길이 15, 최우선 호가가 index 0.
 */
public class OrderbookEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String market;        // cd
    private long ts;              // tms (업비트 호가 시각, ms)
    private double level;         // lv (호가 모아보기 단위, 0=기본)
    private double totalAskSize;  // tas
    private double totalBidSize;  // tbs
    private double[] askPrices;   // obu[].ap
    private double[] askSizes;    // obu[].as
    private double[] bidPrices;   // obu[].bp
    private double[] bidSizes;    // obu[].bs
    private String streamType;    // st: SNAPSHOT / REALTIME
    private long recvTs;          // rts (수집기 수신 시각, ms)

    public String getMarket() { return market; }
    public void setMarket(String market) { this.market = market; }
    public long getTs() { return ts; }
    public void setTs(long ts) { this.ts = ts; }
    public double getLevel() { return level; }
    public void setLevel(double level) { this.level = level; }
    public double getTotalAskSize() { return totalAskSize; }
    public void setTotalAskSize(double v) { this.totalAskSize = v; }
    public double getTotalBidSize() { return totalBidSize; }
    public void setTotalBidSize(double v) { this.totalBidSize = v; }
    public double[] getAskPrices() { return askPrices; }
    public void setAskPrices(double[] v) { this.askPrices = v; }
    public double[] getAskSizes() { return askSizes; }
    public void setAskSizes(double[] v) { this.askSizes = v; }
    public double[] getBidPrices() { return bidPrices; }
    public void setBidPrices(double[] v) { this.bidPrices = v; }
    public double[] getBidSizes() { return bidSizes; }
    public void setBidSizes(double[] v) { this.bidSizes = v; }
    public String getStreamType() { return streamType; }
    public void setStreamType(String v) { this.streamType = v; }
    public long getRecvTs() { return recvTs; }
    public void setRecvTs(long v) { this.recvTs = v; }

    /** 최우선 매도호가 (없으면 0) */
    public double bestAsk() { return askPrices != null && askPrices.length > 0 ? askPrices[0] : 0; }
    /** 최우선 매수호가 (없으면 0) */
    public double bestBid() { return bidPrices != null && bidPrices.length > 0 ? bidPrices[0] : 0; }

    /** 상위 n단 잔량 합 */
    public static double topSum(double[] sizes, int n) {
        if (sizes == null) return 0;
        double s = 0;
        for (int i = 0; i < Math.min(n, sizes.length); i++) s += sizes[i];
        return s;
    }

    @Override
    public String toString() {
        return String.format("Orderbook{%s ts=%d bid=%.0f ask=%.0f levels=%d %s}",
                market, ts, bestBid(), bestAsk(), askPrices == null ? 0 : askPrices.length, streamType);
    }
}
