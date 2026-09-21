package com.cdc.pipeline.binance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 2026-09-20 (docs/34 #5): 호가는 의도적으로 double 유지. 호가 단은 합산되는 금액이 아니라 파생 지표(mid·spread_bp·imbalance·depth)의 재료이고,
 * 전부 비율이라 double 로 결과가 같다. ClickHouse Array(Decimal) 은 초당 300 스냅샷 × 40단에서 저장·연산 비용만 늘린다.
 */
public class DepthParser extends ProcessFunction<String, DepthMsg> {
    public static final OutputTag<String> DLQ = new OutputTag<String>("binance-depth-dlq") {};
    private transient ObjectMapper mapper; private transient Counter parseFailures;

    @Override public void open(Configuration c) { mapper = new ObjectMapper(); parseFailures = getRuntimeContext().getMetricGroup().counter("parseFailures"); }

    static void fill(JsonNode arr, double[] px, double[] qty) {
        for (int i = 0; i < arr.size(); i++) { px[i] = Double.parseDouble(arr.get(i).get(0).asText()); qty[i] = Double.parseDouble(arr.get(i).get(1).asText()); }
    }

    @Override
    public void processElement(String json, Context ctx, Collector<DepthMsg> out) {
        if (json == null || json.isEmpty()) return;
        try {
            JsonNode d = mapper.readTree(json); String e = d.path("e").asText();
            DepthMsg m = new DepthMsg(); m.symbol = d.get("s").asText(); m.recvMs = d.path("recv_ms").asLong(0L);
            JsonNode b, a;
            if ("snapshot".equals(e)) { m.snapshot = true; m.lastId = d.get("lastUpdateId").asLong(); m.firstId = m.lastId; m.eventMs = m.recvMs; b = d.get("bids"); a = d.get("asks"); }
            else if ("depthUpdate".equals(e)) { m.snapshot = false; m.firstId = d.get("U").asLong(); m.lastId = d.get("u").asLong(); m.eventMs = d.get("E").asLong(); b = d.get("b"); a = d.get("a"); }
            else throw new IllegalArgumentException("unknown event " + e);
            m.bidPx = new double[b.size()]; m.bidQty = new double[b.size()]; m.askPx = new double[a.size()]; m.askQty = new double[a.size()];
            fill(b, m.bidPx, m.bidQty); fill(a, m.askPx, m.askQty);
            if (m.symbol.isEmpty() || m.lastId <= 0) throw new IllegalArgumentException("missing key fields");
            out.collect(m);
        } catch (Exception ex) {
            parseFailures.inc();
            try { com.fasterxml.jackson.databind.node.ObjectNode n = mapper.createObjectNode(); n.put("error", ex.getClass().getSimpleName() + ": " + ex.getMessage()); n.put("raw", json); n.put("failed_at", System.currentTimeMillis()); ctx.output(DLQ, mapper.writeValueAsString(n)); } catch (Exception ignored) { }
        }
    }
}
