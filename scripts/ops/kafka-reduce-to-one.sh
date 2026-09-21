#!/usr/bin/env bash
# 브로커 3→1 축소 런북 (docs/24 §4). 정지 없이: minISR 1 → 전 파티션을 브로커 1로 재할당(RF1) → 검증. 브로커 2·3 정지는 마지막에 사람이 한다.
# 왜 이 순서인가: 재할당 중 RF 가 1 로 떨어지는 순간 정적 min.insync.replicas=2 를 만족 못 해 acks=all 프로듀서가 막힌다 → minISR 을 먼저 1 로.
# RF3 토픽은 브로커 1 이 이미 전체 복제본을 갖고 있어 재할당이 메타데이터 변경뿐이다. 복사가 생기는 건 RF1·RF2 토픽(game-events, upbit.orderbook.v1)의 브로커 1 에 없는 파티션.
# 실행: 사용자 터미널에서 (세션 권한 분류기가 프로덕션 Kafka 변경을 막는다, 2026-09-17)
#   cd ~/cdc-realtime-pipeline && scripts/ops/kafka-reduce-to-one.sh 2>&1 | tee ~/kafka-reassign/run-$(date -u +%Y%m%dT%H%M).log
set -euo pipefail
cd "$(dirname "$0")/../.."
K="docker exec cdc-kafka-1"
BS="--bootstrap-server kafka-1:29092"
OUT=/home/calme/kafka-reassign; mkdir -p $OUT; TS=$(date -u +%Y%m%dT%H%M)
say(){ echo "[$(date -u +%H:%M:%S)] $*"; }
PHASE=${1:-all}

say "0. 사전 점검"
$K kafka-topics $BS --describe > $OUT/describe-before-$TS.txt
TOTAL=$(grep -c "Partition:" $OUT/describe-before-$TS.txt); say "   파티션 총 $TOTAL"
UR=$($K kafka-topics $BS --describe --under-replicated-partitions 2>/dev/null | grep -c "Partition:" || true); say "   under-replicated=$UR (0 이어야)"
[ "$UR" = "0" ] || { say "   중단: 이미 복제 부족 상태"; exit 1; }

say "1. 실험 토픽 삭제 (load_test.*: 실험 종료, 옮길 가치 없음)"
for t in load_test.trades load_test.orderbook; do $K kafka-topics $BS --delete --topic $t 2>/dev/null && say "   deleted $t" || say "   $t 없음"; done
sleep 3

say "2. min.insync.replicas=1 - 클러스터 기본(동적) + 전 토픽 명시"
$K kafka-configs $BS --entity-type brokers --entity-default --alter --add-config min.insync.replicas=1 | sed 's/^/   /'
for t in $($K kafka-topics $BS --list); do $K kafka-configs $BS --entity-type topics --entity-name "$t" --alter --add-config min.insync.replicas=1 >/dev/null; done
say "   토픽 $($K kafka-topics $BS --list | wc -l)개 minISR=1"
$K kafka-configs $BS --entity-type topics --entity-name cdc.crypto_db.crypto_trades --describe | grep -o "min.insync.replicas=[0-9]" | head -1 | sed 's/^/   체결 토픽: /'
[ "$PHASE" = "prepare" ] && exit 0

say "3. 재할당 JSON 생성 (모든 토픽·파티션 → replicas [1])"
$K kafka-topics $BS --describe | grep "Partition:" | awk '{print $2, $4}' | python3 -c "
import sys,json
parts=[{'topic':t,'partition':int(p),'replicas':[1]} for t,p in (l.split() for l in sys.stdin)]
json.dump({'version':1,'partitions':parts}, open('$OUT/reassign-to-1-$TS.json','w'))
print('   partitions in plan:', len(parts))"
docker cp $OUT/reassign-to-1-$TS.json cdc-kafka-1:/tmp/reassign.json
# 롤백용: 현재 배치를 그대로 저장 (describe 로 재구성)
grep "Partition:" $OUT/describe-before-$TS.txt | awk '{print $2, $4, $8}' | python3 -c "
import sys,json
parts=[{'topic':t,'partition':int(p),'replicas':[int(x) for x in r.split(',')]} for t,p,r in (l.split() for l in sys.stdin)]
json.dump({'version':1,'partitions':parts}, open('$OUT/rollback-assignment-$TS.json','w'))
print('   롤백용 현재 배치 저장:', len(parts), 'partitions →', '$OUT/rollback-assignment-$TS.json')"

say "4. 재할당 실행"
$K kafka-reassign-partitions $BS --reassignment-json-file /tmp/reassign.json --execute | grep -E "Successfully|rror" | sed 's/^/   /'
for i in $(seq 1 180); do
  st=$($K kafka-reassign-partitions $BS --reassignment-json-file /tmp/reassign.json --verify 2>/dev/null || true)
  left=$(echo "$st" | grep -c "still in progress" || true)
  say "   진행 중 파티션: $left"
  [ "$left" = "0" ] && break
  sleep 10
done

say "5. 검증"
$K kafka-topics $BS --describe > $OUT/describe-after-$TS.txt
say "   replicas=1 / Isr=1 인 파티션: $(grep -cE 'Replicas: 1[[:space:]]+Isr: 1[[:space:]]*$' $OUT/describe-after-$TS.txt) / $(grep -c 'Partition:' $OUT/describe-after-$TS.txt)"
say "   브로커 2·3 에 남은 replica: $(grep -E 'Replicas: [0-9,]*[23]' $OUT/describe-after-$TS.txt | wc -l) (0 이어야)"
say "   under-replicated: $($K kafka-topics $BS --describe --under-replicated-partitions | grep -c 'Partition:' || true)"
say "   최근 60초 프로듀서 오류(Connect/수집기 로그): connect=$(docker logs cdc-kafka-connect --since 60s 2>&1 | grep -ci 'NOT_ENOUGH_REPLICAS\|error' ) collector=$(docker logs cdc-orderbook-collector --since 60s 2>&1 | grep -ci 'delivery 실패\|produce 실패')"
say "done - 다음: 사용자 터미널에서 'docker stop cdc-kafka-2 cdc-kafka-3' 후 24h 관찰 (docs/24 §4 단계 4)"
