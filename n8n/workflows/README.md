# n8n 워크플로 보관본

## 주의: n8n 은 두 대에서 돈다

`미니PC`(calme-mini-server)와 `Oracle`(free-arm-server) 양쪽에 n8n 인스턴스가 있고,
**같은 이름의 워크플로가 서로 다른 활성 상태·다른 내용**으로 존재할 수 있다.
2026-09-20 에 이것 때문에 사고가 났다(docs/43 §1):

- `docs/34` 가 **미니PC 만 보고** "n8n 알림 워크플로 3개가 전부 비활성" 이라고 적었으나,
  Oracle 쪽 `CDC Pipeline - Anomaly & Health Monitor` 는 **active 인 채 매분 돌고 있었다.**
- 이 디렉터리의 `CDC_Pipeline_-_Anomaly_Health_Monitor.json` 도 **09-09 시점 판본**이라
  실제로 돌던 Oracle 판본과 달랐다. 복원해도 **09-20 에 DROP 된 `anomaly_alerts`** 를 찌른다.

**규칙**: 워크플로를 보관할 때 **어느 인스턴스에서 내보냈는지 파일명에 남긴다.**
인스턴스를 안 적은 파일은 출처 불명으로 간주하고 복원 근거로 쓰지 않는다.

## 파일

| 파일 | 출처 | 비고 |
|---|---|---|
| `CDC_Pipeline_-_Anomaly_Health_Monitor.oracle.json` | **Oracle**, 2026-09-20 내보냄 | 실제로 돌던 판본. 09-20 13:2x 비활성화. 도메인만 `<내-도메인>` 으로 가림 |
| `CDC_Pipeline_-_Anomaly_Health_Monitor.json` | 출처 미표기(09-09 판본) | 옛 판본. `anomaly_alerts`(DROP됨) 참조 - **복원용으로 쓰지 말 것** |
| 그 외 | 출처 미표기 | 이 프로젝트와 무관하거나 비활성 |

## 왜 비활성화했나 (2026-09-20)

`CDC Pipeline - Anomaly & Health Monitor` 는 **pull 구조**라 미니PC 가 죽으면 질의가 실패해
판정에 도달하지 못했다. 게다가 09-20 04:01 의 포트 하드닝으로 경로가 끊겨 **8시간 38분간
매분 에러**(누적 2,007건)만 쌓고 있었다. 기능은 `scripts/ops/heartbeat-push.sh` 의
heartbeat payload 가 흡수했고, 판정은 `scripts/ops/cdc-watchdog.py` 가 **push 부재 기준**으로 한다.
자세한 경위는 `docs/43`.
