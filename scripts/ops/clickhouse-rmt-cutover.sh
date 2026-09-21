#!/usr/bin/env bash
# crypto_trades → ReplacingMergeTree 컷오버 (docs/19 #11, 2026-09-18). 전제: crypto_trades_rmt 가 만들어져 있고 닫힌 파티션이 복사돼 있다.
# 순서와 이유:
#  1) 현재 달 복사(T1 까지)          - 큰 덩어리를 교체 전에 옮겨 교체 창을 초 단위로 줄인다
#  2) MV DETACH                       - 교체 뒤 잔여분 복사가 MV 를 다시 트리거해 지연 통계를 이중 집계하지 않게. 재부착 시 이름으로 새 테이블에 바인딩된다
#  3) 차이분 복사(T1~T2)              - RMT 라 겹쳐 넣어도 머지에서 정리된다(멱등)
#  4) EXCHANGE TABLES (원자적)        - Flink JDBC 는 이름으로 쓰므로 다음 배치부터 새 테이블에 들어간다. 정지 없음
#  5) 잔여분 복사(교체 직전 옛 테이블에 들어간 행) - 옛 테이블(_old)에서 flink_ts >= T2
#  6) MV ATTACH, 검증
set -euo pipefail
CH(){ docker exec cdc-clickhouse clickhouse-client -q "$1"; }
say(){ echo "[$(date -u +%T)] $*"; }
NEW=cdc_pipeline.crypto_trades_rmt; CUR=cdc_pipeline.crypto_trades

say "1. 현재 달 복사"
T1=$(CH "SELECT toString(now64(3))"); say "   T1=$T1"
# 24M 행을 한 번에 넣으면 서버 총 메모리 한도(1.57GiB)를 넘긴다(실측 1.84GiB) → 일 단위 슬라이스, 스레드 1
for d in $(CH "SELECT arrayStringConcat(arrayMap(x -> toString(x), groupArray(d)), ' ') FROM (SELECT DISTINCT toDate(source_ts) AS d FROM $CUR WHERE toYYYYMM(source_ts) = toYYYYMM(now()) ORDER BY d)"); do
  docker exec cdc-clickhouse clickhouse-client --max_insert_threads 1 --max_threads 1 -q "INSERT INTO $NEW SELECT * FROM $CUR WHERE toDate(source_ts) = '$d' AND flink_ts < '$T1'"
  say "   $d 복사"
done
say "   현재 달 복사 완료: $(CH "SELECT sum(rows) FROM system.parts WHERE table='crypto_trades_rmt' AND active AND partition=toString(toYYYYMM(now()))") rows"

say "2. MV detach"
CH "DETACH TABLE cdc_pipeline.mv_latency_stats"

say "3. 차이분 복사 (T1 ~ T2)"
T2=$(CH "SELECT toString(now64(3))"); say "   T2=$T2"
CH "INSERT INTO $NEW SELECT * FROM $CUR WHERE flink_ts >= '$T1' AND flink_ts < '$T2'"

say "4. EXCHANGE TABLES"
CH "EXCHANGE TABLES $CUR AND $NEW"
say "   교체 완료: crypto_trades 엔진 = $(CH "SELECT engine FROM system.tables WHERE database='cdc_pipeline' AND name='crypto_trades'")"

say "5. 잔여분 복사 (옛 테이블 = crypto_trades_rmt 이름이 됨, flink_ts >= T2)"
CH "INSERT INTO $CUR SELECT * FROM $NEW WHERE flink_ts >= '$T2'"
say "   잔여 $(CH "SELECT count() FROM $NEW WHERE flink_ts >= '$T2'") rows 복사"

say "6. MV attach"
CH "ATTACH TABLE cdc_pipeline.mv_latency_stats"

say "7. 검증"
sleep 20
say "   새 테이블 최근 60초 적재(Flink 가 새 테이블에 쓰나): $(CH "SELECT count() FROM $CUR WHERE flink_ts >= now() - INTERVAL 60 SECOND")"
say "   MV 최근 1분 집계 갱신: $(CH "SELECT max(minute) FROM cdc_pipeline.mv_latency_stats")"
say "   09-18 재기동 창 중복(FINAL): $(CH "SELECT count() - uniqExact(trade_id) FROM $CUR FINAL WHERE trade_id BETWEEN 117208512 AND 117210331") (0 이어야), FINAL 없이: $(CH "SELECT count() - uniqExact(trade_id) FROM $CUR WHERE trade_id BETWEEN 117208512 AND 117210331")"
say "   파티션별 행(새/옛): $(CH "SELECT partition, sum(rows) FROM system.parts WHERE table='crypto_trades' AND active GROUP BY partition ORDER BY partition FORMAT TSV" | tr '\t' ':' | tr '\n' ' ')"
say "   옛 테이블(crypto_trades_rmt)은 7일 뒤 DROP - 그때까지 롤백 = EXCHANGE 역실행"
say DONE
