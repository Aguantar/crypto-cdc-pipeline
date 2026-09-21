package com.cdc.pipeline.binance;

import java.io.Serializable;

/** binance.depth.v1 한 건: 증분(depthUpdate: U/u + bids/asks) 또는 수집기가 넣은 REST 스냅샷(snapshot: lastUpdateId + bids/asks). */
public class DepthMsg implements Serializable {
    private static final long serialVersionUID = 1L;
    public boolean snapshot; public String symbol; public long firstId; public long lastId; public long eventMs; public long recvMs;
    public double[] bidPx; public double[] bidQty; public double[] askPx; public double[] askQty;
}
