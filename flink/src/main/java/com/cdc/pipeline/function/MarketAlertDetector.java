package com.cdc.pipeline.function;

import com.cdc.pipeline.model.CryptoTradeEvent;
import com.cdc.pipeline.model.MarketAlert;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * PRICE_24H - 업비트 "가격 급등락" 경보의 재현 (docs/16 §4-1, docs/22).
 *
 * 규칙: 분 종가 / 24시간 전 분 종가 − 1 의 절대값이 50% 이상이면 주의(1), 100% 이상 경고(2), 200% 이상 위험(3).
 *   임계는 내가 고른 값이 아니다. 거래소가 실제로 지정한 순간의 지표값 분포(6개월 167건 독립 검증: 주의 p50 51.7%, 경고 101.2%, 위험 196.9%)에서
 *   역산했고, 반대 방향(≥50% 인 분 3,221개 중 3,220개가 지정 구간 안)으로 검증했다. 05-18 이후 월별 p50 51.5~52.3% 로 정적.
 *
 * 판정 단위 = 분 종가 (v2.1, 2026-09-17, docs/22 §4-2). 처음(v2)엔 체결마다 판정했는데, 임계 근처에서 체결 단위로 0→1→0 이 초 단위로 반복됐다
 *   (재계산: LSK 하루 0→1 전이 86회 vs 거래소 지정 9회). 이유 세 가지로 분 종가로 바꿨다:
 *   ① 정답(거래소 지정·해제 기록)의 해상도가 분이다(지속 최소 1분, 중앙값 6분) - 규칙은 정답보다 잘게 판정할 수 없다.
 *   ② 임계의 근거(docs/16)가 1분봉 종가 분포다 - 같은 숫자를 체결가(분 안의 극값)에 쓰면 검증이 옮겨지지 않는다.
 *   ③ 참조가 이미 분 종가라 분 종가 대 분 종가가 거래소 정의("24h 전 종가 대비")와 같은 대칭 비교다.
 *   히스테리시스(N분 유지)는 넣지 않는다 - 거래소도 유지하지 않고(중앙값 6분), N 은 근거 없는 상수가 된다. 기각 대안: 체결 단위 + 디바운스, 체결 단위 + 24h 유지.
 *
 * 구현: 분이 닫히면(처리 시간 타이머 = 분 끝 + 10초 여유; e2e p95 4.7s·재정렬 최대 4.8s 를 덮는다) 그 분의 종가로 판정한다.
 *   체결이 없는 분은 직전 종가로 채우고(forward-fill) 같은 방식으로 판정한다 - 24h 전 가격이 움직이면 체결 없이도 등급이 바뀔 수 있다.
 *   타이머 체인은 마지막 체결로부터 24h 까지만 이어 간다(상장폐지·거래정지 마켓의 타이머가 영원히 남지 않게).
 *   전이의 event_time = 분 끝 시각, price = 분 종가, tradeId = 0 (분 단위 판정에는 체결 하나가 대응하지 않는다).
 *
 * 상태: 마켓별 분 종가 링(1,500분 = 24h + 여유) + 마지막 분 + 현재 등급 + 마지막 판정 분 + 마지막 체결 분.
 *   MapState 대신 배열인 이유: 참조 조회가 O(1) 이어야 한다(피크 초당 59 체결). 체크포인트 크기 287 × 1,500 × 8B ≈ 3.4MB.
 *   v2 → v2.1 은 링·마지막 분·등급 상태를 그대로 이어받는다(새 상태 2개는 비어 있어도 됨) → savepoint 복원 후 워밍업 없음.
 * 늦은 이벤트 가드(docs/20): 적재 지연 > 60초인 행(백필·gap-fill)은 상태·판정 모두 건너뛴다.
 * 승격 (2026-09-20): rule_version 'v2.1.1-shadow' → 'v2.1.1'. 기준(docs/22 §4)은 "실전 전이 10건 이상이
 *   1분봉 재계산과 전부 일치" 였고, 09-19 에 전이 116건 중 미매칭 0 으로 충족했다(dq_alert_parity_daily.parity_ok=1).
 *   달력이 아니라 전이 표본 수가 기준인 이유: 임계 자체는 6개월 역검증(3,220/3,221)이 근거이고,
 *   섀도가 확인할 것은 임계가 아니라 이 구현(링·forward-fill·늦은 이벤트 가드)이 그 백테스트와 같은 답을 내는가 였다.
 *   승격 뒤에도 판정 로직은 한 글자도 바뀌지 않는다 - 바뀌는 것은 '이 전이를 사람에게 보내는가' 뿐이다.
 *   발송은 Airflow market_alerts_notify DAG 가 한다(10분, alert_events 로 중복 제거). 섀도 기간 기록은 문자열로 남는다.
 */
public class MarketAlertDetector extends KeyedProcessFunction<String, CryptoTradeEvent, MarketAlert> {

    static final long LATE_EVENT_MS = 60_000;
    static final long MINUTE_MS = 60_000;
    static final long CLOSE_SLACK_MS = 10_000;   // 분 끝 뒤 이만큼 기다렸다가 닫는다 (e2e p95 4.7s + 재정렬 4.8s)
    static final int RING = 1_500;               // 분 단위 슬롯
    static final int LOOKBACK = 1_440;           // 24h
    static final long TIMER_CHAIN_MINUTES = 1_440; // 마지막 체결 뒤 이 시간까지만 빈 분을 계속 닫는다
    static final double[] THRESHOLDS = {0.5, 1.0, 2.0};
    static final String RULE_VERSION = "v2.1.1";   // 2026-09-20 승격. 섀도 기간 행은 "v2.1.1-shadow" 로 남아 구분된다

    private transient ValueState<double[]> ring;
    private transient ValueState<Long> lastMinute;       // 링에 값이 있는 가장 최근 분 (체결 또는 채움)
    private transient ValueState<Integer> level;
    private transient ValueState<Long> evaluatedMinute;  // 이 분까지 종가 판정 완료
    private transient ValueState<Long> lastTradeMinute;  // 마지막 실제 체결의 분
    private transient ValueState<Long> lastSeqCur;       // 현재 분(lastMinute)에서 본 가장 늦은 sequential_id - 종가는 이벤트 순 마지막이어야 한다
    private transient ValueState<Long> lastSeqPrev;      // 직전 분(lastMinute−1, 아직 안 닫힘)의 같은 값
    private transient Counter lateEventsSkipped;
    private transient Counter transitions;
    private transient Counter minutesEvaluated;

    @Override
    public void open(Configuration parameters) {
        ring = getRuntimeContext().getState(new ValueStateDescriptor<>("closeRing", PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));
        lastMinute = getRuntimeContext().getState(new ValueStateDescriptor<>("lastMinute", Types.LONG));
        level = getRuntimeContext().getState(new ValueStateDescriptor<>("level", Types.INT));
        evaluatedMinute = getRuntimeContext().getState(new ValueStateDescriptor<>("evaluatedMinute", Types.LONG));
        lastTradeMinute = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTradeMinute", Types.LONG));
        lastSeqCur = getRuntimeContext().getState(new ValueStateDescriptor<>("lastSeqCur", Types.LONG));
        lastSeqPrev = getRuntimeContext().getState(new ValueStateDescriptor<>("lastSeqPrev", Types.LONG));
        lateEventsSkipped = getRuntimeContext().getMetricGroup().counter("lateEventsSkipped");
        transitions = getRuntimeContext().getMetricGroup().counter("levelTransitions");
        minutesEvaluated = getRuntimeContext().getMetricGroup().counter("minutesEvaluated");
    }

    static int idx(long minute) { return (int) Math.floorMod(minute, (long) RING); }

    static int levelOf(double absChange) {
        if (absChange >= THRESHOLDS[2]) return 3;
        if (absChange >= THRESHOLDS[1]) return 2;
        if (absChange >= THRESHOLDS[0]) return 1;
        return 0;
    }

    static long closeTimerFor(long minute) { return (minute + 1) * MINUTE_MS + CLOSE_SLACK_MS; }

    @Override
    public void processElement(CryptoTradeEvent e, Context ctx, Collector<MarketAlert> out) throws Exception {
        if (e.getSourceTimestamp() - e.getUpbitTimestamp() > LATE_EVENT_MS) {
            lateEventsSkipped.inc();
            return;
        }
        long m = Math.floorDiv(e.getUpbitTimestamp(), MINUTE_MS);
        // 2026-09-20 (docs/34 #5): 이벤트는 BigDecimal 이지만 여기서만 double 로 받는다.
        // 이유 ① 판정이 비율 비교(±50/100/200%)라 double 로도 결과가 같다 ② 상태(ValueState<double[]> 1,500분 링)의 타입을 바꾸면
        // 세이브포인트 복원이 깨진다 - 규칙 동등성(docs/22 §4-5, 116/116)을 유지하려면 상태 타입을 건드리면 안 된다.
        double p = e.getTradePrice().doubleValue();
        if (p <= 0) return;

        double[] r = ring.value();
        Long lm = lastMinute.value();
        if (r == null) {
            r = new double[RING];
            Arrays.fill(r, Double.NaN);
        }
        if (lm == null) {                                  // 첫 체결: 링 시작. 판정은 이 분부터
            r[idx(m)] = p;
            ring.update(r);
            lastMinute.update(m);
            evaluatedMinute.update(m - 1);
        } else {
            Long ev = evaluatedMinute.value();
            if (ev == null) evaluatedMinute.update(lm - 1); // v2 savepoint 에서 복원: 이력은 있고 판정 분만 없음 → 현재 분부터
            // 2026-09-19 v2.1.1: 분 종가 = 이벤트 순(sequential_id) 마지막 체결. 도착 순으로 덮으면 재정렬(5.87%, ≤4.8s) 때
            // 더 이른 체결이 종가가 되어 ±임계 경계에서 1분짜리 전이가 생긴다(첫 동등성 판정 66/68, 미매칭 6 = KRW-G ±100% 경계 3쌍).
            long seq = e.getSequentialId();
            if (m > lm) {                                  // 빈 분을 직전 종가로 채운다 (가장 최근 RING 분까지만)
                double last = r[idx(lm)];
                long from = Math.max(lm + 1, m - RING + 1);
                for (long k = from; k < m; k++) r[idx(k)] = last;
                r[idx(m)] = p;
                lastSeqPrev.update(m == lm + 1 ? lastSeqCur.value() : null);
                lastSeqCur.update(seq);
                lastMinute.update(m);
            } else if (m == lm) {
                Long cur = lastSeqCur.value();
                if (cur == null || seq > cur) { r[idx(m)] = p; lastSeqCur.update(seq); }   // 같은 분: 이벤트 순으로 더 늦을 때만 종가 갱신
            } else if (m == lm - 1 && m > evaluatedMinute.value()) {
                Long prev = lastSeqPrev.value();
                if (prev == null || seq > prev) { r[idx(m)] = p; lastSeqPrev.update(seq); } // 재정렬로 직전(아직 안 닫힌) 분에 도착: 같은 규칙
            }                                              // 더 오래된 분·이미 닫힌 분에 도착: 무시
            ring.update(r);
        }
        lastTradeMinute.update(m);
        ctx.timerService().registerProcessingTimeTimer(closeTimerFor(m));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<MarketAlert> out) throws Exception {
        double[] r = ring.value();
        Long lm = lastMinute.value();
        Long ev = evaluatedMinute.value();
        if (r == null || lm == null || ev == null) return;
        long target = Math.floorDiv(timestamp - CLOSE_SLACK_MS, MINUTE_MS) - 1;   // 이 타이머가 닫는 분
        if (target <= ev) return;
        long from = Math.max(ev + 1, target - RING + 1);
        Integer prevObj = level.value();
        int prev = prevObj == null ? 0 : prevObj;
        for (long k = from; k <= target; k++) {
            if (k > lm) {                                  // 체결 없이 지나간 분: 직전 종가로 채운다
                r[idx(k)] = r[idx(k - 1)];
                lastMinute.update(k);
                lm = k;
            }
            double close = r[idx(k)];
            double ref = r[idx(k - LOOKBACK)];
            minutesEvaluated.inc();
            if (Double.isNaN(close) || Double.isNaN(ref) || ref <= 0 || close <= 0) continue;   // 아직 24h 전 슬롯이 없음
            double change = close / ref - 1.0;
            int lvl = levelOf(Math.abs(change));
            if (lvl != prev) {
                double threshold = THRESHOLDS[Math.max(lvl, prev) - 1] * 100.0;
                out.collect(new MarketAlert("PRICE_24H", ctx.getCurrentKey(), lvl, prev, (k + 1) * MINUTE_MS,
                        change * 100.0, threshold, ref, close, 0L, RULE_VERSION));
                prev = lvl;
                transitions.inc();
            }
        }
        level.update(prev);
        evaluatedMinute.update(target);
        ring.update(r);
        Long ltm = lastTradeMinute.value();
        if (ltm != null && target - ltm < TIMER_CHAIN_MINUTES) {
            ctx.timerService().registerProcessingTimeTimer(closeTimerFor(target + 1));   // 체결이 없어도 다음 분을 닫는다
        }
    }
}
