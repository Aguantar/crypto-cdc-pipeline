package com.cdc.pipeline.orderbook;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka(upbit.orderbook.v1)의 SIMPLE 포맷 JSON → OrderbookEvent.
 * 수집기(orderbook-collector)가 원문에 rts(수신시각)만 추가해 발행한다.
 * 파싱 실패는 카운트만 하고 버린다(방어적 파싱).
 */
public class OrderbookParser implements FlatMapFunction<String, OrderbookEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(OrderbookParser.class);
    private transient ObjectMapper mapper;
    private transient long failures;

    @Override
    public void flatMap(String json, Collector<OrderbookEvent> out) {
        if (json == null || json.isEmpty()) return;
        if (mapper == null) mapper = new ObjectMapper();
        try {
            JsonNode d = mapper.readTree(json);
            if (d == null || !"orderbook".equals(d.path("ty").asText())) return;
            JsonNode units = d.path("obu");
            int n = units.isArray() ? units.size() : 0;
            double[] ap = new double[n], as = new double[n], bp = new double[n], bs = new double[n];
            for (int i = 0; i < n; i++) {
                JsonNode u = units.get(i);
                ap[i] = u.path("ap").asDouble();
                as[i] = u.path("as").asDouble();
                bp[i] = u.path("bp").asDouble();
                bs[i] = u.path("bs").asDouble();
            }
            OrderbookEvent e = new OrderbookEvent();
            e.setMarket(d.path("cd").asText("UNKNOWN"));
            e.setTs(d.path("tms").asLong());
            e.setLevel(d.path("lv").asDouble(0));
            e.setTotalAskSize(d.path("tas").asDouble());
            e.setTotalBidSize(d.path("tbs").asDouble());
            e.setAskPrices(ap); e.setAskSizes(as); e.setBidPrices(bp); e.setBidSizes(bs);
            e.setStreamType(d.path("st").asText("REALTIME"));
            e.setRecvTs(d.path("rts").asLong(e.getTs()));
            if (e.getTs() > 0 && n > 0) out.collect(e);
        } catch (Exception ex) {
            failures++;
            if (failures <= 10 || failures % 1000 == 0) LOG.warn("호가 파싱 실패 #{}: {}", failures, ex.getMessage());
        }
    }
}
