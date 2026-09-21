#!/usr/bin/env bash
# Grafana 도메인에 basic_auth 를 건다 (docs/21 §5-12, 2026-09-20).
#
# 왜: Grafana 도메인이 basic_auth 없이 localhost:3000 으로 넘어가고 있었다.
#   airflow·code 블록에는 있는 잠금이 grafana 블록에만 없어서, 대시보드와 /api/ds/query 가
#   인터넷 전체에 열려 있었다(실측 200). 지금은 Grafana 익명을 꺼서 막았지만,
#   그러면 설정 하나가 유일한 잠금이다. 앞단에도 문을 둔다(airflow 와 같은 방식).
#
# 왜 '완전히 닫기'가 아니라 '암호'인가: 사용자 판단 - 누가 궁금해하면 보여줄 수 있어야 한다.
#   그래서 Caddy basic_auth 를 문으로 두고, 통과한 사람에게는 Grafana 익명 Viewer 로 바로 보여준다.
#   보여주는 범위는 데이터소스 계정(grafana_public)이 정한다: cdc_pipeline 읽기만, 쓰기·게임 DB 없음.
#
# 실행: sudo scripts/ops/caddy-grafana-auth.sh '보여줄사람에게줄비밀번호'
set -euo pipefail
[ "$(id -u)" = "0" ] || { echo "sudo 로 실행해야 한다"; exit 1; }
PASS="${1:?보여줄 비밀번호를 인자로 넘겨라: sudo $0 '비밀번호'}"
USER_NAME="${CADDY_DEMO_USER:-demo}"
CF=/etc/caddy/Caddyfile

grep -q "^grafana\..*{" "$CF" || { echo "$CF 에 grafana 블록이 없다"; exit 1; }
if awk '/^grafana\..*\{/,/^\}/' "$CF" | grep -q basic_auth; then
  echo "이미 basic_auth 가 있다. 비밀번호만 바꾸려면 해당 줄을 직접 고쳐라."; exit 0
fi

BAK="$CF.bak.pre-grafana-auth.$(date -u +%Y%m%d%H%M)"
cp -a "$CF" "$BAK"; echo "백업: $BAK"

HASH=$(caddy hash-password --plaintext "$PASS")
# grafana 블록의 여는 줄 바로 다음에 basic_auth 를 넣는다 (airflow 블록과 같은 모양)
awk -v u="$USER_NAME" -v h="$HASH" '
  /^grafana\..*\{/ { print; print "\tbasic_auth {"; print "\t\t" u " " h; print "\t}"; next }
  { print }
' "$BAK" > "$CF"

if ! caddy validate --config "$CF" >/dev/null 2>&1; then
  echo "설정이 유효하지 않다 → 되돌린다"; cp -a "$BAK" "$CF"; exit 1
fi
systemctl reload caddy
sleep 2
echo
echo "적용됨. 확인:"
# 도메인은 설정 파일에서 읽는다 (문서·스크립트에 도메인을 박아 두지 않는다)
DOMAIN=$(awk '/^grafana\..*\{/ {print $1; exit}' "$CF")
echo "  무인증:  curl -s -o /dev/null -w '%{http_code}\\n' https://$DOMAIN/   → 401 이어야"
echo "  인증:    curl -s -o /dev/null -w '%{http_code}\\n' -u '$USER_NAME:<비밀번호>' https://$DOMAIN/  → 200"
echo
echo "다음: Grafana 익명 Viewer 를 켜면 통과한 사람이 두 번 로그인하지 않아도 된다(요청하면 반영)."
