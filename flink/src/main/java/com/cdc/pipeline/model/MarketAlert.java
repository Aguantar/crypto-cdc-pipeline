package com.cdc.pipeline.model;

import java.io.Serializable;

/**
 * 이상탐지 v2 출력 - "마켓의 등급 전이" (docs/16 §5, docs/22).
 * 구 AnomalyAlert 는 체결 1건에 대한 판정이었고 근거가 없어 폐기했다. 이 모델은 업비트 시장경보(주의/경고/위험)와
 * 같은 의미 단위(마켓·등급·전이 시각)를 갖도록 설계해 거래소 지정 이력과 1:1 로 대조할 수 있게 한다.
 */
public class MarketAlert implements Serializable {
    private static final long serialVersionUID = 1L;

    private String alertType;     // PRICE_24H
    private String market;
    private int level;            // 0 해제 / 1 주의 / 2 경고 / 3 위험
    private int prevLevel;
    private long eventTime;       // 판정 근거 체결의 거래소 시각 (ms)
    private long detectedAt;      // 처리 시각 (ms)
    private double value;         // 24h 변동률 (%)
    private double threshold;     // 이 전이의 임계 (%)
    private double refPrice;      // 24h 전 분 종가
    private double price;
    private long tradeId;
    private String ruleVersion;

    public MarketAlert() {}

    public MarketAlert(String alertType, String market, int level, int prevLevel, long eventTime,
                       double value, double threshold, double refPrice, double price, long tradeId, String ruleVersion) {
        this.alertType = alertType; this.market = market; this.level = level; this.prevLevel = prevLevel;
        this.eventTime = eventTime; this.detectedAt = System.currentTimeMillis();
        this.value = value; this.threshold = threshold; this.refPrice = refPrice; this.price = price;
        this.tradeId = tradeId; this.ruleVersion = ruleVersion;
    }

    public String getAlertType() { return alertType; }
    public String getMarket() { return market; }
    public int getLevel() { return level; }
    public int getPrevLevel() { return prevLevel; }
    public long getEventTime() { return eventTime; }
    public long getDetectedAt() { return detectedAt; }
    public double getValue() { return value; }
    public double getThreshold() { return threshold; }
    public double getRefPrice() { return refPrice; }
    public double getPrice() { return price; }
    public long getTradeId() { return tradeId; }
    public String getRuleVersion() { return ruleVersion; }

    @Override
    public String toString() {
        return String.format("MarketAlert{%s %s level %d→%d %.1f%% (ref %.4f → %.4f) event=%d}",
                alertType, market, prevLevel, level, value, refPrice, price, eventTime);
    }
}
