#!/usr/bin/env bash
# LAN → 프로덕션 관리 포트 차단 (docs/29 §5). 도커가 퍼블리시한 포트는 INPUT 이 아니라 DOCKER-USER 체인(FORWARD)을 지난다.
# 허용: 호스트 자신(lo), 도커 브리지(172.16/12), WireGuard(10.88.0.0/24). 차단: 그 외에서 오는 아래 PORTS.
#
# `ss -ltnp` 로 실제 열린 포트를 세어 목록을 만들었다. 6379(requirepass 없는 redis)가 가장 위험하고,
# 5432 는 이 프로젝트 것이 아니지만 같은 호스트라 같이 막는다.
# 일부러 열어 두는 것: 22, 3000(Grafana), 8085(Airflow UI), 8089 - 인증이 있고 LAN 에서 쓰는 화면들이다.
#
# 실행: sudo scripts/ops/lan-firewall.sh
# 재부팅 영속화는 systemd/cdc-lan-firewall.service 가 맡는다. 스냅샷을 저장하지 않고 도커 뒤에
# 이 스크립트를 다시 돌리는 방식인데, DOCKER-USER 체인을 도커 데몬이 만들기 때문이다.
set -euo pipefail
PORTS="9092,2181,8083,8081,8123,6379,5432"
iptables -C DOCKER-USER -s 172.16.0.0/12 -j RETURN 2>/dev/null || iptables -I DOCKER-USER 1 -s 172.16.0.0/12 -j RETURN
iptables -C DOCKER-USER -s 10.88.0.0/24 -j RETURN 2>/dev/null || iptables -I DOCKER-USER 2 -s 10.88.0.0/24 -j RETURN
iptables -C DOCKER-USER -s 127.0.0.0/8 -j RETURN 2>/dev/null || iptables -I DOCKER-USER 3 -s 127.0.0.0/8 -j RETURN
# 2026-09-20 수정: 처음엔 multiport + `--ctorigdstport $PORTS` 한 줄이었는데 실행하면 이렇게 죽는다.
#   iptables v1.8.10 (nf_tables): Port "9092,2181,..." does not resolve to anything.
# conntrack 의 --ctorigdstport 는 포트 하나(또는 a:b 범위)만 받는다. 쉼표 목록을 못 받는다.
# 스크립트를 한 번도 실행해 본 적이 없어서 여태 안 드러났다 - 안 돌려 본 런북은 런북이 아니다.
# → 포트마다 한 줄씩 넣는다. --ctorigdstport 를 쓰는 이유는 도커가 DNAT 를 먼저 하기 때문이다.
#   FORWARD(=DOCKER-USER)에 도달할 때 목적지 포트는 이미 컨테이너 포트라, 우리가 막으려는 '퍼블리시된
#   호스트 포트'로 판단하려면 conntrack 이 기억하는 원래 목적지 포트를 봐야 한다.
# -A(끝에 추가)가 아니라 -I 4(4번째에 삽입)인 이유: DOCKER-USER 체인의 마지막 줄은 도커가 넣은 RETURN이다.
# 끝에 붙이면 RETURN 뒤라 절대 평가되지 않는다 - 규칙이 들어간 것처럼 보이고 아무것도 안 막는다.
# 위에서 허용(RETURN) 3줄을 1·2·3 에 넣었으므로 그다음 자리가 4 다. DROP 들끼리는 포트가 겹치지 않아 순서 무관.
for p in ${PORTS//,/ }; do
  iptables -C DOCKER-USER -p tcp --dport "$p" -m conntrack --ctorigdstport "$p" -j DROP 2>/dev/null \
    || iptables -I DOCKER-USER 4 -p tcp --dport "$p" -m conntrack --ctorigdstport "$p" -j DROP
done
echo "현재 DOCKER-USER 규칙:"
iptables -L DOCKER-USER -n --line-numbers
echo
N_DROP=$(iptables -S DOCKER-USER | grep -c ctorigdstport); N_WANT=$(echo "$PORTS" | tr ',' '\n' | wc -l)
echo "DROP 규칙 ${N_DROP}개 / 기대 ${N_WANT}개"
# 규칙이 RETURN 뒤에 있으면 들어가 있어도 아무것도 안 막는다 → 자리까지 확인한다
LAST_DROP=$(iptables -L DOCKER-USER -n --line-numbers | awk '/ctorigdstport|dpt:/ {n=$1} END {print n+0}')
FINAL_RETURN=$(iptables -L DOCKER-USER -n --line-numbers | awk '$2=="RETURN" && $5=="0.0.0.0/0" {n=$1} END {print n+0}')
if [ "$N_DROP" -eq "$N_WANT" ] && { [ "$FINAL_RETURN" -eq 0 ] || [ "$LAST_DROP" -lt "$FINAL_RETURN" ]; }; then
  echo "OK: DROP 규칙이 전부 들어갔고 마지막 RETURN 앞에 있다"
else
  echo "경고: 규칙 수 또는 순서 확인 필요 (DROP 마지막줄 $LAST_DROP, RETURN $FINAL_RETURN)"
fi
HOST_IP=$(hostname -I | awk '{print $1}')
echo
echo "확인 (LAN 의 다른 기기에서):"
echo "  막혀야 함:  for p in 9092 2181 8081 6379 5432; do nc -vz -w3 $HOST_IP \$p; done   → 전부 timeout"
echo "  살아야 함:  nc -vz -w3 $HOST_IP 3000 ; nc -vz -w3 $HOST_IP 8085                  → succeeded"
echo
echo "재부팅 영속화: systemd cdc-lan-firewall.service (설치 여부 → systemctl is-enabled cdc-lan-firewall)"
