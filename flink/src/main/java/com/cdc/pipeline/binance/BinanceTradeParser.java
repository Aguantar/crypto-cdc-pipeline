package com.cdc.pipeline.binance;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * {"e":"trade","E":..,"s":"BTCUSDT","t":..,"p":"..","q":"..","T":..,"m":true,"recv_ms":..} → BinanceTrade.
 * 실패는 DLQ 사이드 아웃풋 + parseFailures 카운터 (CdcEventParser 와 같은 규율, docs/29 창2).
 */
public class BinanceTradeParser extends ProcessFunction<String, BinanceTrade> {
    public static final OutputTag<String> DLQ = new OutputTag<String>("binance-parse-dlq") {};
    private transient ObjectMapper mapper;
    private transient Counter parseFailures;

    /** exchangeInfo 실측(09-20): USDT 현물 493심볼의 tickSize 최대 8자리·stepSize 최대 5자리 → 8 이면 반올림이 일어나지 않는다 */
    private static final int SCALE = 8;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();
        parseFailures = getRuntimeContext().getMetricGroup().counter("parseFailures");
    }

    @Override
    public void processElement(String json, Context ctx, Collector<BinanceTrade> out) {
        if (json == null || json.isEmpty()) return;
        try {
            JsonNode d = mapper.readTree(json);
            if (!"trade".equals(d.path("e").asText())) throw new IllegalArgumentException("not a trade event: " + d.path("e").asText());
            BinanceTrade t = new BinanceTrade();
            t.symbol = d.get("s").asText(); t.tradeId = d.get("t").asLong();
            t.price = new BigDecimal(d.get("p").asText()).setScale(SCALE, RoundingMode.HALF_UP);
            t.qty = new BigDecimal(d.get("q").asText()).setScale(SCALE, RoundingMode.HALF_UP);
            t.quoteQty = t.price.multiply(t.qty);   // 스케일 16 - 저장 컬럼 Decimal(38,16) 과 일치
            t.buyerMaker = d.path("m").asBoolean(false); t.tradeMs = d.get("T").asLong(); t.eventMs = d.path("E").asLong(t.tradeMs);
            t.recvMs = d.path("recv_ms").asLong(0L);
            if (t.symbol.isEmpty() || t.tradeId <= 0 || t.tradeMs <= 0) throw new IllegalArgumentException("missing key fields");
            if (t.price.signum() < 0 || t.qty.signum() < 0) throw new IllegalArgumentException("negative price/qty");
            out.collect(t);
        } catch (Exception e) {
            parseFailures.inc();
            try {
                com.fasterxml.jackson.databind.node.ObjectNode n = mapper.createObjectNode();
                n.put("error", e.getClass().getSimpleName() + ": " + e.getMessage()); n.put("raw", json); n.put("failed_at", System.currentTimeMillis());
                ctx.output(DLQ, mapper.writeValueAsString(n));
            } catch (Exception ignored) { }
        }
    }
}
