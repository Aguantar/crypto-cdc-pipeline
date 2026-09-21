package com.cdc.pipeline.model;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 암호화폐 체결 이벤트 모델
 * 
 * Debezium CDC 이벤트를 파싱하여 생성한다.
 * Upbit 실시간 체결 데이터의 핵심 필드를 담고 있다.
 */
public class CryptoTradeEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String op;              // CDC 오퍼레이션: r(snapshot), c(insert), u(update), d(delete)
    private long tradeId;           // MySQL AUTO_INCREMENT PK
    private String market;          // 마켓 코드 (KRW-BTC, KRW-ETH 등)
    // 2026-09-20 (docs/34 #5): 금액·수량은 BigDecimal. 원천이 MySQL DECIMAL(20,8)·Debezium decimal.handling.mode=string 이라
    // 정밀도는 문자열로 도착하는데 double 로 받아 버리고 있었다(저장 층 타입은 하류에 주는 계약 - DE 책임).
    // 스케일 규약: price·volume = 8(원천과 동일), amount = price×volume 이라 정확히 16.
    private BigDecimal tradePrice;  // 체결 가격 (KRW)
    private BigDecimal tradeVolume; // 체결 수량
    private BigDecimal tradeAmount; // 체결 금액 = price × volume (MySQL 의 DECIMAL(20,4) 값은 먼지 체결이 0 이라 쓰지 않는다)
    private String askBid;          // ASK(매도) / BID(매수)
    private long upbitTimestamp;    // Upbit 체결 시각 (Unix ms)
    private long sequentialId;      // Upbit 체결 고유 ID
    private long sourceTimestamp;   // MySQL INSERT 시각 (ms)
    private long cdcTimestamp;      // Debezium 처리 시각 (ms)
    private long cdcLatencyMs;      // CDC 레이턴시 (cdcTimestamp - sourceTimestamp)
    private Double bestAskPrice;    // 체결 시점 최우선 매도호가 (2026-09-09 추가, 이전 행은 null)
    private Double bestAskSize;     // 최우선 매도 잔량
    private Double bestBidPrice;    // 체결 시점 최우선 매수호가
    private Double bestBidSize;     // 최우선 매수 잔량
    // 2026-09-19 (docs/28 A-1·A-5, docs/29 창2): MySQL·ClickHouse 양쪽에 추가된 컬럼 3개를 그대로 흘린다
    private Long recvMs;            // producer WS 수신 epoch ms (producer 가 채우기 전엔 null)
    private String ingestSource;    // ws | gapfill | backfill
    private String streamType;      // Upbit stream_type: REALTIME | SNAPSHOT

    // --- Getters & Setters ---

    public String getOp() { return op; }
    public void setOp(String op) { this.op = op; }

    public long getTradeId() { return tradeId; }
    public void setTradeId(long tradeId) { this.tradeId = tradeId; }

    public String getMarket() { return market; }
    public void setMarket(String market) { this.market = market; }

    public BigDecimal getTradePrice() { return tradePrice; }
    public void setTradePrice(BigDecimal tradePrice) { this.tradePrice = tradePrice; }

    public BigDecimal getTradeVolume() { return tradeVolume; }
    public void setTradeVolume(BigDecimal tradeVolume) { this.tradeVolume = tradeVolume; }

    public BigDecimal getTradeAmount() { return tradeAmount; }
    public void setTradeAmount(BigDecimal tradeAmount) { this.tradeAmount = tradeAmount; }

    public String getAskBid() { return askBid; }
    public void setAskBid(String askBid) { this.askBid = askBid; }

    public long getUpbitTimestamp() { return upbitTimestamp; }
    public void setUpbitTimestamp(long upbitTimestamp) { this.upbitTimestamp = upbitTimestamp; }

    public long getSequentialId() { return sequentialId; }
    public void setSequentialId(long sequentialId) { this.sequentialId = sequentialId; }

    public long getSourceTimestamp() { return sourceTimestamp; }
    public void setSourceTimestamp(long sourceTimestamp) { this.sourceTimestamp = sourceTimestamp; }

    public long getCdcTimestamp() { return cdcTimestamp; }
    public void setCdcTimestamp(long cdcTimestamp) { this.cdcTimestamp = cdcTimestamp; }

    public long getCdcLatencyMs() { return cdcLatencyMs; }
    public void setCdcLatencyMs(long cdcLatencyMs) { this.cdcLatencyMs = cdcLatencyMs; }

    public Double getBestAskPrice() { return bestAskPrice; }
    public void setBestAskPrice(Double bestAskPrice) { this.bestAskPrice = bestAskPrice; }

    public Double getBestAskSize() { return bestAskSize; }
    public void setBestAskSize(Double bestAskSize) { this.bestAskSize = bestAskSize; }

    public Double getBestBidPrice() { return bestBidPrice; }
    public void setBestBidPrice(Double bestBidPrice) { this.bestBidPrice = bestBidPrice; }

    public Double getBestBidSize() { return bestBidSize; }
    public void setBestBidSize(Double bestBidSize) { this.bestBidSize = bestBidSize; }

    public Long getRecvMs() { return recvMs; }
    public void setRecvMs(Long recvMs) { this.recvMs = recvMs; }

    public String getIngestSource() { return ingestSource; }
    public void setIngestSource(String ingestSource) { this.ingestSource = ingestSource; }

    public String getStreamType() { return streamType; }
    public void setStreamType(String streamType) { this.streamType = streamType; }

    @Override
    public String toString() {
        return String.format(
            "CryptoTrade{op=%s, id=%d, %s, %s %.8f@%.0f=%.0fKRW, latency=%dms}",
            op, tradeId, market, askBid, tradeVolume, tradePrice, tradeAmount, cdcLatencyMs
        );
    }
}
