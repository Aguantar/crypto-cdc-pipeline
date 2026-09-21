#!/usr/bin/env bash
# 외부 감시자를 Oracle 로 배포한다. 코드는 repo, 실행은 Oracle - 감시 대상과 같은 상자에
# 있으면 같이 죽는다. 비밀값은 repo 에 넣지 않고 Airflow Variable 과 Caddyfile 에서 읽어
# Oracle 의 600 권한 파일로만 옮긴다. 코드·설정·cron 모두 멱등이라 재실행해도 안전하다.
set -euo pipefail
REMOTE=${HB_REMOTE:-ubuntu@10.88.0.1}
KEY=${HB_KEY:-/home/calme/.ssh/oci_key}
RDIR=${HB_RDIR:-/home/ubuntu/cdc-watchdog}
SRC=/home/calme/cdc-realtime-pipeline/scripts/ops/cdc-watchdog.py
SSH="ssh -i $KEY -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no"
say(){ echo "[$(date -u +%T)] $*"; }

say "1. 비밀값 수집 (화면에 찍지 않는다)"
WEBHOOK=$(docker exec cdc-airflow-webserver airflow variables get slack_webhook_url 2>/dev/null || true)
[ -n "$WEBHOOK" ] && [ "$WEBHOOK" != "None" ] || { echo "중단: Airflow Variable slack_webhook_url 을 못 읽음"; exit 1; }
GDOMAIN=$(awk '/^grafana\..*\{/ {print $1; exit}' /etc/caddy/Caddyfile 2>/dev/null || true)
say "   webhook ${#WEBHOOK}자 확보, grafana 도메인 $([ -n "$GDOMAIN" ] && echo 확보 || echo '없음(선택)')"

say "2. 감시자 코드 전송"
$SSH "$REMOTE" "mkdir -p $RDIR && chmod 700 $RDIR"
$SSH "$REMOTE" "cat > $RDIR/cdc-watchdog.py && chmod 755 $RDIR/cdc-watchdog.py" < "$SRC"

say "3. 설정 파일 (600, Oracle 에만 존재)"
# cron 은 /bin/sh 로 도므로 `. file` 로 읽힌다. python 까지 가려면 export 가 필요하다
{
  echo "export SLACK_WEBHOOK_URL='$WEBHOOK'"
  [ -n "$GDOMAIN" ] && echo "export WD_GRAFANA='https://$GDOMAIN/d/cdc-pipeline-main/cdc-crypto-realtime-pipeline?orgId=1'"
  echo "export WD_DIR='$RDIR'"
} | $SSH "$REMOTE" "cat > $RDIR/watchdog.env && chmod 600 $RDIR/watchdog.env"

say "4. Oracle cron 등록 (멱등)"
$SSH "$REMOTE" bash -s <<EOF
set -e
RDIR=$RDIR
CUR=\$(crontab -l 2>/dev/null || true)
ADD=""
L1="*/5 * * * * . \$RDIR/watchdog.env && /usr/bin/python3 \$RDIR/cdc-watchdog.py >> \$RDIR/watchdog.log 2>&1  # CDC 외부 감시자 (2026-09-20~)"
# 주간 점검: 이 알림이 안 오면 감시자 자신이 죽은 것이다. 월요일 09:00 KST(=00:00 UTC)
L2="0 0 * * 1 . \$RDIR/watchdog.env && /usr/bin/python3 \$RDIR/cdc-watchdog.py --weekly-ok >> \$RDIR/watchdog.log 2>&1  # 감시자 생존 주간 보고"
echo "\$CUR" | grep -qF "cdc-watchdog.py >>" || ADD="\$ADD\$L1\n"
echo "\$CUR" | grep -qF -- "--weekly-ok" || ADD="\$ADD\$L2\n"
if [ -n "\$ADD" ]; then printf '%s\n%b' "\$CUR" "\$ADD" | grep -v '^\$' | crontab -; echo "   cron 추가됨"; else echo "   cron 이미 등록됨"; fi
crontab -l | grep cdc-watchdog | sed 's/^/   /'
EOF

say "5. 검증 - 감시자 1회 실행"
$SSH "$REMOTE" ". $RDIR/watchdog.env && python3 $RDIR/cdc-watchdog.py" | sed 's/^/   /'
say DONE
