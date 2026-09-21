package com.cdc.pipeline.function;

import com.cdc.pipeline.model.CryptoTradeEvent;
import com.cdc.pipeline.model.MarketAlert;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * PRICE_24H v2.1 - 분 종가 판정 (docs/22 §4-2). 판정은 분 끝 + 10초의 처리 시간 타이머에서 일어나므로
 * 테스트는 체결을 넣은 뒤 처리 시간을 그 시각까지 밀어야 출력이 나온다.
 */
public class MarketAlertDetectorTest {

    private KeyedOneInputStreamOperatorTestHarness<String, CryptoTradeEvent, MarketAlert> h;
    private static final long T0 = 1_789_600_020_000L;   // 분 경계에 정렬 (÷60000 = 29,826,667)
    private static final long MIN = 60_000L;

    @Before
    public void setUp() throws Exception {
        h = new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new MarketAlertDetector()), CryptoTradeEvent::getMarket, Types.STRING);
        h.open();
    }

    @After
    public void tearDown() throws Exception { h.close(); }

    private static CryptoTradeEvent trade(long id, String market, double price, long upbitTs, long sourceTs) {
        CryptoTradeEvent e = new CryptoTradeEvent();
        // 2026-09-20 (docs/34 #5): 이벤트는 BigDecimal 이 됐지만 판정은 비율 비교라 기대값이 하나도 안 바뀌어야 한다 - 이 테스트가 그 증거
        e.setOp("c"); e.setTradeId(id); e.setMarket(market);
        e.setTradePrice(BigDecimal.valueOf(price)); e.setTradeVolume(BigDecimal.ONE); e.setTradeAmount(BigDecimal.valueOf(price));
        e.setAskBid("BID"); e.setUpbitTimestamp(upbitTs); e.setSequentialId(upbitTs * 10_000); e.setSourceTimestamp(sourceTs);
        e.setCdcTimestamp(sourceTs + 5); e.setCdcLatencyMs(5);
        return e;
    }
    private void live(long id, double price, long upbitTs) throws Exception {
        h.processElement(new StreamRecord<>(trade(id, "KRW-LSK", price, upbitTs, upbitTs + 1_000)));
    }
    /** 처리 시간을 이 체결 시각의 분이 닫히는 시점(분 끝 + 여유)까지 민다 → 그 분까지 판정된다 */
    private void closeMinuteOf(long upbitTs) throws Exception {
        h.setProcessingTime(MarketAlertDetector.closeTimerFor(Math.floorDiv(upbitTs, MIN)));
    }
    private List<MarketAlert> out() { return h.extractOutputValues(); }

    @Test
    public void noJudgementWithoutTwentyFourHoursOfHistory() throws Exception {
        live(1, 100, T0);
        live(2, 300, T0 + 10 * MIN);
        closeMinuteOf(T0 + 10 * MIN);
        assertEquals(0, out().size());
    }

    @Test
    public void levelTransitionsUpAndDownAreEmittedOncePerMinuteClose() throws Exception {
        live(1, 100, T0);
        long t = T0 + 1_440 * MIN;
        live(2, 151, t);            closeMinuteOf(t);              // +51% → 주의
        live(3, 205, t + MIN);      closeMinuteOf(t + MIN);        // +105% → 경고
        live(4, 320, t + 2 * MIN);  closeMinuteOf(t + 2 * MIN);    // +220% → 위험
        live(5, 120, t + 3 * MIN);  closeMinuteOf(t + 3 * MIN);    // +20% → 해제
        List<MarketAlert> a = out();
        assertEquals(4, a.size());
        assertEquals(1, a.get(0).getLevel()); assertEquals(0, a.get(0).getPrevLevel()); assertEquals(50.0, a.get(0).getThreshold(), 0.001);
        assertEquals(2, a.get(1).getLevel());
        assertEquals(3, a.get(2).getLevel()); assertEquals(200.0, a.get(2).getThreshold(), 0.001);
        assertEquals(0, a.get(3).getLevel()); assertEquals(3, a.get(3).getPrevLevel());
        assertEquals(100.0, a.get(0).getRefPrice(), 0.001);
        assertTrue(a.get(0).getValue() > 50 && a.get(0).getValue() < 52);
        assertEquals(t + MIN, a.get(0).getEventTime());                 // event_time = 분 끝
        // 2026-09-20 승격: 섀도 접미사를 뗐다. 판정은 한 글자도 안 바뀌었고, 나머지 기대값이 전부 그대로인 것이 그 증거
        assertEquals("v2.1.1", a.get(0).getRuleVersion());
    }

    @Test
    public void intraMinuteSpikeThatRevertsBeforeCloseDoesNotFlap() throws Exception {
        // v2(체결 단위)라면 0→1→0 두 건이 나왔을 시나리오. 분 종가(148)는 +48% 라 전이 없음
        live(1, 100, T0);
        long t = T0 + 1_440 * MIN;
        live(2, 151, t);
        live(3, 149, t + 5_000);
        live(4, 152, t + 20_000);
        live(5, 148, t + 40_000);
        closeMinuteOf(t);
        assertEquals(0, out().size());
        live(6, 160, t + MIN);       // 다음 분 종가 +60% → 주의 1건
        closeMinuteOf(t + MIN);
        assertEquals(1, out().size());
        assertEquals(160.0, out().get(0).getPrice(), 0.001);   // price = 분 종가
    }

    @Test
    public void referenceIsTheCloseOfThatMinuteNotLaterTrades() throws Exception {
        live(1, 100, T0);
        live(2, 110, T0 + 30_000);          // 같은 분의 마지막 체결 = 종가 110
        live(3, 200, T0 + 5 * MIN);
        long t = T0 + 1_440 * MIN;
        live(4, 160, t);                    // 참조 110 → +45% → 주의 아님
        closeMinuteOf(t);
        assertEquals(0, out().size());
        live(5, 166, t + MIN);              // +50.9% → 주의
        closeMinuteOf(t + MIN);
        assertEquals(1, out().size());
        assertEquals(110.0, out().get(0).getRefPrice(), 0.001);
    }

    @Test
    public void emptyMinutesAreForwardFilledAndStillJudged() throws Exception {
        // 24h 전 가격이 낮아지는 구간(100 → 60)에 현재는 체결이 없어도, 채워진 종가 100 이 참조 60 대비 +67% 가 되는 분에 전이가 나야 한다
        live(1, 100, T0);
        live(2, 60, T0 + 10 * MIN);
        live(3, 100, T0 + 20 * MIN);
        long t = T0 + 1_440 * MIN;
        live(4, 100, t);                    // 참조 100(T0 분) → 0%
        closeMinuteOf(t);
        assertEquals(0, out().size());
        h.setProcessingTime(MarketAlertDetector.closeTimerFor(Math.floorDiv(t, MIN) + 10));   // 체결 없이 10분 경과: 타이머 체인이 빈 분을 닫는다
        List<MarketAlert> a = out();
        assertEquals(1, a.size());          // T0+10분의 참조 60 대비 채워진 종가 100 → +67% 주의
        assertEquals(60.0, a.get(0).getRefPrice(), 0.001);
        assertEquals(0L, a.get(0).getTradeId());
    }

    @Test
    public void lateRowsDoNotTouchStateOrEmit() throws Exception {
        live(1, 100, T0);
        h.processElement(new StreamRecord<>(trade(2, "KRW-LSK", 50, T0 - 6 * 24 * 60 * MIN, T0 + 2_000)));
        long t = T0 + 1_440 * MIN;
        live(3, 151, t);
        closeMinuteOf(t);
        List<MarketAlert> a = out();
        assertEquals(1, a.size());
        assertEquals(100.0, a.get(0).getRefPrice(), 0.001);
    }

    @Test
    public void closeIsLastByEventOrderEvenIfAnEarlierTradeArrivesLater() throws Exception {
        // v2.1 까지는 도착 순 마지막이 종가였다 → 재정렬된 더 이른 체결(seq 작음)이 뒤에 오면 종가가 낮아져 경계에서 전이가 뒤집힘
        live(1, 100, T0);
        long t = T0 + 1_440 * MIN;
        live(2, 152, t + 30_000);                                          // 이벤트 순 마지막(+30s), 먼저 도착: +52%
        h.processElement(new StreamRecord<>(trade(3, "KRW-LSK", 148, t + 25_000, t + 31_000)));   // 더 이른 체결(+25s)이 늦게 도착: +48%
        closeMinuteOf(t);
        assertEquals(1, out().size());                                      // 종가 152 → 주의 (도착 순이었다면 148 → 0건)
        assertEquals(152.0, out().get(0).getPrice(), 0.001);
    }

    @Test
    public void reorderIntoPreviousOpenMinuteUsesEventOrderToo() throws Exception {
        live(1, 100, T0);
        long t = T0 + 1_440 * MIN;
        live(2, 152, t + 50_000);            // 분 t 의 마지막 체결
        live(3, 130, t + MIN + 1_000);       // 다음 분 시작 → 분 t 는 아직 열려 있음(타이머 전)
        h.processElement(new StreamRecord<>(trade(4, "KRW-LSK", 140, t + 40_000, t + MIN + 2_000)));   // 분 t 의 더 이른 체결이 늦게 도착
        closeMinuteOf(t);
        assertEquals(1, out().size());
        assertEquals(152.0, out().get(0).getPrice(), 0.001);              // 도착 순이었다면 140 → +40% → 전이 없음
    }

    @Test
    public void reorderWithinSecondsIsNotLate() throws Exception {
        live(1, 100, T0);
        h.processElement(new StreamRecord<>(trade(2, "KRW-LSK", 101, T0 - 4_000, T0 + 1_000)));   // 4초 재정렬: 늦은 행 아님, 이전 분 종가 갱신
        long t = T0 + 1_440 * MIN;
        live(3, 155, t);
        closeMinuteOf(t);
        assertEquals(1, out().size());
    }
}
