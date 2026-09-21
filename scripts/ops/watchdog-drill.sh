#!/usr/bin/env bash
# 외부 감시자 장애 훈련. heartbeat 송신을 끊고 탐지까지 실측한다.
#
# 검증 안 된 페일오버는 없는 것보다 나쁘다 - 있다고 믿고 안 보게 된다. 실제로 기존 감시자가
# 8시간 38분 조용히 고장나 있었고 아무도 몰랐다.
# 한계: 송신만 멈추므로 전원 차단·커널 패닉은 재현하지 못한다. 그건 사람이 현장에서 해야 한다.
set -uo pipefail
REMOTE=${HB_REMOTE:-ubuntu@10.88.0.1}
KEY=${HB_KEY:-/home/calme/.ssh/oci_key}
RDIR=${HB_RDIR:-/home/ubuntu/cdc-watchdog}
SSH="ssh -i $KEY -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no"
MAXW=${MAXW:-1500}
say(){ echo "[$(date -u +%T)] $*"; }

restore(){
  if crontab -l 2>/dev/null | grep -q "^#DRILL#.*heartbeat-push"; then
    crontab -l | sed 's|^#DRILL#||' | crontab -
    say "cron 복구됨"
  fi
}
trap restore EXIT INT TERM

say "0. 사전 상태"
$SSH "$REMOTE" "cat $RDIR/heartbeat.json" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("   마지막 heartbeat:", d["sent_at"], "verdict:", d["verdict"])'
$SSH "$REMOTE" "cat $RDIR/state.json 2>/dev/null || echo '{\"level\":\"ok\"}'" | sed 's/^/   감시자 상태: /'

say "1. heartbeat 송신 중단 (미니PC 사망 모의)"
crontab -l | sed 's|^\(\*/5 .*heartbeat-push.*\)$|#DRILL#\1|' | crontab -
crontab -l | grep -q "^#DRILL#" && say "   중단됨" || { say "   중단 실패 - 훈련 취소"; exit 1; }
T0=$(date -u +%s)
LAST=$($SSH "$REMOTE" "cat $RDIR/heartbeat.json" | python3 -c 'import sys,json; print(json.load(sys.stdin)["sent_at_epoch"])')
say "   기준 시각 = 마지막 heartbeat $(date -u -d @$LAST +%T)Z"

say "2. Oracle 이 스스로 down 을 판정할 때까지 대기 (한계 12분 + cron 5분 간격)"
DOWN_AT=""
while [ $(( $(date -u +%s) - T0 )) -lt $MAXW ]; do
  LV=$($SSH "$REMOTE" "cat $RDIR/state.json 2>/dev/null" | python3 -c 'import sys,json;
try: print(json.load(sys.stdin)["level"])
except: print("?")' 2>/dev/null)
  EL=$(( $(date -u +%s) - LAST ))
  say "   heartbeat 침묵 ${EL}초 / 감시자 판정=$LV"
  [ "$LV" = "down" ] && { DOWN_AT=$(date -u +%s); break; }
  sleep 60
done
[ -n "$DOWN_AT" ] || { say "   한계 시간 내 탐지 실패 - 훈련 실패"; exit 1; }
# 폴링으로 관측한 시각이 아니라 감시자가 실제로 판정한 시각을 쓴다.
# 관측 시각을 쓰면 이 스크립트의 폴링 주기(60초)가 탐지 시간에 섞여 실제보다 나쁘게 보고된다
JUDGED=$($SSH "$REMOTE" "grep -m1 'level=down' $RDIR/watchdog.log | tail -1 | cut -d' ' -f1")
JEPOCH=$(date -u -d "$(echo "$JUDGED" | sed 's/T/ /; s/Z//')" +%s 2>/dev/null || echo "$DOWN_AT")
say "   *** 탐지: 마지막 heartbeat 로부터 $(( JEPOCH - LAST ))초 ($(( (JEPOCH-LAST)/60 ))분 $(( (JEPOCH-LAST)%60 ))초) ***"
say "       (감시자 판정 시각 $JUDGED / 훈련 스크립트 관측 $(date -u -d @$DOWN_AT +%TZ) - 관측은 폴링 60초 오차 포함)"
say "       이론상 최악 = 임계 ${DOWN_SEC_SHOWN:-720}초 + cron 간격 300초"
$SSH "$REMOTE" "tail -3 $RDIR/watchdog.log" | sed 's/^/   /'

say "3. 복구 (heartbeat 재개)"
restore; trap - EXIT INT TERM
/home/calme/cdc-realtime-pipeline/scripts/ops/heartbeat-push.sh | sed 's/^/   /'
$SSH "$REMOTE" ". $RDIR/watchdog.env && python3 $RDIR/cdc-watchdog.py" | sed 's/^/   /'
REC_AT=$(date -u +%s)

say "4. 결과"
echo "   탐지 시간 (마지막 heartbeat → down 판정): $(( DOWN_AT - LAST ))초"
echo "   복구 시간 (down 판정 → ok 복귀):          $(( REC_AT - DOWN_AT ))초"
echo "   Slack 수신 2건을 사람이 확인해야 완결: ① DOWN ② 복구"
$SSH "$REMOTE" "cat $RDIR/state.json" | sed 's/^/   최종 상태: /'
say DONE
