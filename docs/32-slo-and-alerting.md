# 32. SLO 와 알럿 체계 - 무엇을, 누구에게, 무엇을 하라고 (2026-09-20)

> 사용자: "실제 현업이라면 이 데이터와 알람을 어떻게 쓸지 뒷단까지 생각해 봐라. 단 개인 규모니 나에게 알림이 오고, 어디를 고쳐야 하는지, 언제 트래픽이 몰리는지 알려 주는 게 낫다."
> 원칙: 알럿은 **받아서 행동할 사람이 있을 때만** 울린다. 임계는 실측 기준선에서 나오고, 여기 한 표에 모은다. 울린 이력은 데이터로 남겨 주간에 되돌아본다.

## 1. SLO 표 - 지표 · 목표 · 근거 · 어디서 재나 · 어긋나면 누가 무엇을
| 층 | 지표 | 목표 | 근거(실측 기준선) | 측정 | 알럿 경로 | 행동 |
|---|---|---|---|---|---|---|
| 신선도 | Upbit 체결 최근 10분 적재 | > 0 행, 마지막 행 15분 이내 | 24h 평균 36/s | health_check 10분 | Slack 즉시 | producer·Connect·Flink 순으로 확인(Connect 는 자동 재시작) |
| 신선도 | Binance 체결 60초 적재 | > 0 행 | 24h 평균 361/s | health_check | Slack 즉시 | 수집기/잡 재기동 |
| 지연 | 체결 적재 지연 p50(source_ts − upbit_ts) | < 60s | 정상 1.1s, 사고 때 36.9h | health_check | Slack 즉시 | producer 상한·버퍼 확인(docs/08) |
| 지연 | ClickHouse insert p95 | < 60ms | 기준선 27~38ms | health_check | Slack 즉시 | 머지·배치 겹침 확인(docs/23) |
| 포화 | Flink 소스 busy | < 500ms/s | 기준선 3~50, 1,000 = 포화 | health_check | Slack 즉시 | 배치 크기·병렬도(docs/23) |
| 커버리지 | 거래소 대비 60초 넘게 없는 마켓 | 0 | 정상 지연 최대 7.75s | health_check | Slack 즉시 | 구독 목록·gap-fill 확인 |
| 무결성 | 파싱 실패 | 증가 0 | 24h 실패 0 | health_check(카운터) | Slack 즉시 | DLQ 원문 확인(docs/29) |
| 정확성 | 원장 3자 대조 | 불일치 0, 2h 안 대조 | 09-19~20 불일치 0 | health_check | Slack 즉시 | 생성기·CDC 확인 |
| **완전성** | Upbit 일 대조 가중 비율 | ≥ 99.9%, 셀 최소 ≥ 99% | 10일 100% | dq_reconcile_daily | **quality_alerts(일)** | 백필 도구(docs/13·17) |
| 완전성 | Binance 일 대조 | ≥ 99.9%, 0행 셀 0 | 첫 시간 100% | dq_binance_reconcile_daily | quality_alerts | 수집기 재연결 공백 확인 |
| 완전성 | 호가 유실 창 | 하루 0건 | 정지 실험 때만 발생 | dq_orderbook_gaps_daily | quality_alerts | 브로커·수집기 로그 |
| 정확성 | 규칙 동등성 | parity_ok = 1 (온전한 날) | 09-19 116/116 | dq_alert_parity_daily | quality_alerts | 종가 정의·타이머(docs/22) |
| 정확성 | 원장 3자(일) | my_ch_mismatch = 0 | 0 | dq_ledger_daily | quality_alerts | MV·RMT 버전 확인 |
| 자원 | 호스트 load1 | 5분 값 < 4.0 지속 안 함 | 확장 전 2.3, 후 3~4.7 | ops_metrics_5m | **주간 다이제스트** | 심볼 수·배치 조정 |
| 자원 | 컨테이너 메모리 | 한도의 90% 미만 | MySQL 95% 였음(→1.25G) | ops_metrics_5m | 주간 | 한도 상향·프로세스 수 |
| 자원 | 디스크 | 가득 차기까지 > 60일 | 30%, 증가 ~1GB/일 | ops_metrics_5m | 주간 | TTL·보존 조정 |

## 2. 알럿 세 층 (개인 규모에 맞춘 뒷단)
| 층 | 언제 | 무엇 | 왜 이렇게 |
|---|---|---|---|
| ① 즉시 | 10분(health_check) | 파이프라인이 죽었거나 곧 죽을 신호(위 표 "Slack 즉시") | 사람이 지금 손대야 하는 것만. 같은 이름은 시간당 1회로 접는다(`alert_events` dedup) |
| ② 일 1회 판정 | 매시 :50 검사, 새 dq 결과가 있을 때 하루 1회 | 완전성·정확성 SLO 위반 | dq 는 리포트가 아니라 **판정**이어야 한다. 위반이면 Slack + 원인 추적 시작점(문서 링크) |
| ③ 주간 다이제스트 | 월요일 09:00 KST | 트래픽 프로필(시간대별 피크), 지연 추세, 울린 알럿 집계, 품질 추세, 자원(load·메모리·디스크 여유 일수), **"고칠 것" 목록** | 사람이 계획을 세우는 단위. 알럿이 아니라 판단 재료 |

## 3. 실무라면 뒷단이 어떻게 되나 → 여기서는 무엇으로 대신하나
| 실무 | 여기(개인 규모) |
|---|---|
| 온콜 로테이션·페이지(PagerDuty) | Slack 한 채널, 즉시 층만 소리 |
| 알럿 → 티켓 → 런북 → 포스트모템 | 알럿 이력 표(`alert_events`) + 문서 링크 + worklog 사고 기록(docs/30 형식) |
| SLO 대시보드·에러 버짓 | SLO 표 한 장 + 주간 다이제스트의 위반 횟수 |
| 데이터 계약 위반을 생산자 팀에 통보 | DLQ + 파싱 실패 카운터(생산자 = 나) |
| 소비자에게 신선도 SLA 게시 | dq 표 + 마트 갱신 시각 |
| 용량 계획 회의 | 주간 "고칠 것": load·메모리·디스크 여유 일수·피크 시간대 |
| 알럿 피로 관리(뮤트·dedup·에스컬레이션) | 시간당 1회 dedup, 주간 집계에서 반복 알럿 = 임계 재검토 대상 |

## 4. 구현 (오늘)
- `ops_metrics_5m`(ClickHouse): 기존 5분 cron 수집기가 호스트 load·메모리·디스크·컨테이너 CPU/메모리·유입률을 한 행으로 INSERT (Prometheus 대체, 300MB → 0).
- `alert_events`: health_check·quality_alerts 가 울릴 때마다 한 행(dedup_key 로 반복 접기).
- `quality_alerts` DAG(매시 :50): dq 표의 최신 온전한 날을 SLO 와 비교, 하루 1회만 Slack.
- `weekly_digest` DAG(월 00:00 UTC): 위 ③ 을 Slack + `ops_digest` 표에.

## 5. 실행 기록 (09-20 02:40 ~ 02:55 UTC)
| 항목 | 결과 |
|---|---|
| `ops_metrics_5m` | 기존 5분 cron 수집기 끝에 INSERT 추가. 첫 판 실패: `read a b <<< "count\tp95"` 로 두 값을 나누다 열 순서가 어긋나 UInt32 자리에 4.25(Code 27) → 값마다 스칼라 쿼리로. cron 이 자동으로 새 코드를 돈다 |
| `quality_alerts` | 첫 실행 500: `toString(day) AS day … WHERE day < today()` - ClickHouse 는 SELECT 별칭이 WHERE 의 열을 가려 String vs Date 비교 → 별칭 day_s. 재실행: 규칙 4개 검사, 위반 0, Binance 는 온전한 날이 아직 없어 skip |
| `weekly_digest` | 첫 본문: 트래픽 피크 Upbit KST 9시 74.9/s·Binance 11시 393.8/s, e2e p95 7일 4.57~4.78s(백필 행 제외 - 첫 판은 09-16 이 512,082s 로 나와 1시간 넘게 늦은 행 제외), 대조 Upbit 6일 100%·Binance 100%, 동등성 09-18 FAIL→09-19 OK, 자원은 표본 2개라 "24h 뒤부터". `ops_digest` 저장 실패 → ClickHouseHook 이 str 을 latin-1 로 보내던 버그(한글·① 문자) → utf-8 bytes 로 수정(health_check 의 한글 알럿 기록도 같은 경로였다) |
| health_check → `alert_events` | 코드 반영, 아직 울린 것 없음(0행) |
| 테스트 | DAG 13/13 (quality_alerts·weekly_digest 구조 테스트 추가) |
| 실수 | `airflow tasks test` 에 미래 시각을 줘서 두 번 "Dependencies not met" - 출력이 비어 있으면 실행이 안 된 것부터 의심 |
