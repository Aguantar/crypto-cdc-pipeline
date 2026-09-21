package com.cdc.pipeline.binance;

import java.io.Serializable;
import java.math.BigDecimal;

/** Binance trade 스트림 한 건 (docs/31 §3-2). recvMs 는 수집기가 붙인 수신 시각. */
public class BinanceTrade implements Serializable {
    private static final long serialVersionUID = 1L;
    // 2026-09-20 (docs/34 #5): Binance 도 원문이 문자열("81032.00000000")이라 정밀도가 있었다. exchangeInfo 실측 = 가격 최대 8자리·수량 최대 5자리 → 스케일 8 로 충분.
    public String symbol; public long tradeId; public BigDecimal price; public BigDecimal qty; public BigDecimal quoteQty; public boolean buyerMaker;
    public long tradeMs; public long eventMs; public long recvMs;
}
