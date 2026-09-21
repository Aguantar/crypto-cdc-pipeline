package com.cdc.pipeline.function;

import com.cdc.pipeline.model.CryptoTradeEvent;
import java.math.BigDecimal;
import java.math.RoundingMode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Debezium CDC JSON → CryptoTradeEvent 변환
 * 
 * 방어적 파싱:
 * - null/빈 메시지 무시 (Debezium tombstone)
 * - DELETE(op=d) 이벤트 스킵 (우리 파이프라인에서 불필요)
 * - 모든 필드 null-safe 처리
 *
 * 2026-09-19 (docs/29 창2): 파싱 실패를 로그 한 줄로 흘리지 않는다.
 * - 실패 원문 + 사유를 사이드 아웃풋 {@link #DLQ} 로 내보내 Kafka DLQ 토픽에 쌓는다 (하루 뒤 대조에서 "빠졌다"만 알고 "왜"를 못 찾던 문제).
 * - 카운터 parseFailures / skipped 를 Flink 메트릭으로 노출해 health_check 가 10분마다 본다.
 * 기준: 24시간 TM 로그에 실제 파싱 실패는 0건이었다(09-19 확인). 이 장치는 관찰된 사고의 수리가 아니라 다음 사고의 원인 보존이다.
 */
public class CdcEventParser extends ProcessFunction<String, CryptoTradeEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(CdcEventParser.class);
    /** 파싱 실패 원문: {"error":..., "raw":..., "failed_at": epoch ms} */
    public static final OutputTag<String> DLQ = new OutputTag<String>("parse-dlq") {};

    private transient ObjectMapper mapper;
    private transient Counter parseFailures;
    private transient Counter skipped;

    /** 금액·수량의 소수 자릿수 = 원천(MySQL DECIMAL(20,8))과 동일 */
    private static final int SCALE = 8;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();
        parseFailures = getRuntimeContext().getMetricGroup().counter("parseFailures");
        skipped = getRuntimeContext().getMetricGroup().counter("skipped");
    }

    @Override
    public void processElement(String json, Context ctx, Collector<CryptoTradeEvent> out) {
        // null/빈 메시지 방어 (Debezium tombstone 대응)
        if (json == null || json.isEmpty() || json.equals("null")) {
            skipped.inc();
            return;
        }

        try {
            JsonNode root = mapper.readTree(json);
            if (root == null || root.isNull()) { skipped.inc(); return; }

            // payload가 있으면 Debezium envelope, 없으면 직접 데이터
            JsonNode payload = root.has("payload") ? root.get("payload") : root;
            if (payload == null || payload.isNull()) { skipped.inc(); return; }

            String op = payload.has("op") ? payload.get("op").asText() : null;
            if (op == null) { fail(json, "no op field", ctx); return; }

            // DELETE 이벤트 스킵 - 커넥터가 skipped.operations=d 로 이미 거르지만(docs/26 §3) 방어로 남긴다
            if ("d".equals(op)) {
                skipped.inc();
                return;
            }

            // after 데이터 추출
            JsonNode data = payload.get("after");
            if (data == null || data.isNull()) { fail(json, "op=" + op + " without after", ctx); return; }

            // CDC 타임스탬프
            long cdcTs = payload.has("ts_ms") ? payload.get("ts_ms").asLong() : System.currentTimeMillis();

            // source 타임스탬프 (MySQL binlog 시각)
            long sourceTs = cdcTs;
            if (payload.has("source")) {
                JsonNode source = payload.get("source");
                if (source != null && source.has("ts_ms")) {
                    sourceTs = source.get("ts_ms").asLong();
                }
            }

            CryptoTradeEvent event = new CryptoTradeEvent();
            event.setOp(op);
            event.setTradeId(safeGetLong(data, "trade_id"));
            event.setMarket(safeGetString(data, "market", "UNKNOWN"));
            // 2026-09-20 (docs/34 #5): Debezium 이 문자열로 준 정밀도를 BigDecimal 로 그대로 받는다.
            // 금액은 MySQL 의 trade_amount(DECIMAL(20,4), 먼지 체결 30일 64,669행이 0)를 쓰지 않고 price×volume 으로 계산 → 스케일 16, 정의와 항상 일치.
            BigDecimal price = parseBigDecimal(data, "trade_price");
            BigDecimal volume = parseBigDecimal(data, "trade_volume");
            event.setTradePrice(price);
            event.setTradeVolume(volume);
            event.setTradeAmount(price.multiply(volume));
            event.setAskBid(safeGetString(data, "ask_bid", "UNKNOWN"));
            event.setUpbitTimestamp(safeGetLong(data, "upbit_timestamp"));
            event.setSequentialId(safeGetLong(data, "sequential_id"));
            event.setSourceTimestamp(sourceTs);
            event.setCdcTimestamp(cdcTs);
            event.setCdcLatencyMs(cdcTs - sourceTs);
            // 체결 시점 최우선 호가 (MySQL 컬럼 2026-09-09 추가, 이전 행은 null)
            event.setBestAskPrice(parseNullableDecimal(data, "best_ask_price"));
            event.setBestAskSize(parseNullableDecimal(data, "best_ask_size"));
            event.setBestBidPrice(parseNullableDecimal(data, "best_bid_price"));
            event.setBestBidSize(parseNullableDecimal(data, "best_bid_size"));
            // 2026-09-19 새 컬럼 3개. 스왑 이전 메시지(없음)는 테이블 기본값과 같은 값으로
            event.setRecvMs(data.has("recv_ms") && !data.get("recv_ms").isNull() ? data.get("recv_ms").asLong() : null);
            event.setIngestSource(safeGetString(data, "ingest_source", "ws"));
            event.setStreamType(safeGetString(data, "stream_type", "REALTIME"));

            out.collect(event);

        } catch (Exception e) {
            fail(json, e.getClass().getSimpleName() + ": " + e.getMessage(), ctx);
        }
    }

    private void fail(String raw, String reason, Context ctx) {
        parseFailures.inc();
        LOG.warn("CDC 이벤트 파싱 실패 → DLQ: {}", reason);
        try {
            com.fasterxml.jackson.databind.node.ObjectNode n = mapper.createObjectNode();
            n.put("error", reason);
            n.put("raw", raw);
            n.put("failed_at", System.currentTimeMillis());
            ctx.output(DLQ, mapper.writeValueAsString(n));
        } catch (Exception e) {
            LOG.error("DLQ 직렬화 실패 (원문 유실): {}", e.getMessage());
        }
    }

    /** 원천 스케일 8(MySQL DECIMAL(20,8))로 정규화. 없거나 못 읽으면 0 - 값이 0 이면 탐지기가 건너뛰고 대조에서 드러난다. */
    private BigDecimal parseBigDecimal(JsonNode data, String field) {
        if (data == null || !data.has(field) || data.get(field).isNull()) return BigDecimal.ZERO.setScale(SCALE);
        JsonNode node = data.get(field);
        try {
            return new BigDecimal(node.isTextual() ? node.asText() : node.asText()).setScale(SCALE, RoundingMode.HALF_UP);
        } catch (NumberFormatException e) {
            return BigDecimal.ZERO.setScale(SCALE);
        }
    }

    private double parseDecimal(JsonNode data, String field) {
        if (data == null || !data.has(field) || data.get(field).isNull()) return 0.0;
        JsonNode node = data.get(field);
        if (node.isTextual()) {
            try {
                return Double.parseDouble(node.asText());
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
        return node.asDouble();
    }

    /** decimal.handling.mode=string 이므로 문자열 → Double. 없거나 null이면 null 유지 */
    private Double parseNullableDecimal(JsonNode data, String field) {
        if (data == null || !data.has(field) || data.get(field).isNull()) return null;
        JsonNode node = data.get(field);
        try {
            return node.isTextual() ? Double.valueOf(node.asText()) : node.asDouble();
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private long safeGetLong(JsonNode data, String field) {
        if (data == null || !data.has(field) || data.get(field).isNull()) return 0L;
        return data.get(field).asLong();
    }

    private String safeGetString(JsonNode data, String field, String defaultValue) {
        if (data == null || !data.has(field) || data.get(field).isNull()) return defaultValue;
        return data.get(field).asText();
    }
}