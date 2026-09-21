# 🚀 On-Premise Real-time CDC Pipeline

> **물리 서버 기반 암호화폐 실시간 변경 데이터 캡처 및 이상 탐지 플랫폼**
>
> 🔗 **Live 운영**: 24시간 상시 가동 (Grafana·Airflow는 인증 뒤 운영 - 요청 시 통제된 라이브 데모 제공)

## 💡 이 프로젝트가 증명하는 것 (2026-09-20 결론 중심으로 다시 씀)

한 줄: **거래소 실데이터를 220일째 상시 운영하며, 지연·유실·중복을 "느낌"이 아니라 대조와 실측으로 찾아 고친 기록.** 아래 여덟 줄은 전부 문서에 수치와 조건이 있다.

| 주장 | 근거(실측) | 문서 |
|---|---|---|
| 지연과 유실을 구분해 진단한다 | "건수 반토막" 사고 → 거래소 일봉과 두 기준(적재·체결 시각)으로 대조 → 유실 0, **최대 36.9시간 적재 지연**(producer 상한 포화) | docs/08 |
| 유실은 대조가 찾는다, 지표가 아니다 | 7일 100% 대조 뒤 DAG 첫 실행에서 유실 2건(분모가 우리 DB), sequential_id 마켓 간 충돌로 3% 조용한 유실 → 유니크 키 교체·7일 백필 210,469행 | docs/13·17·18 |
| 중복은 원인별로 갈라 본다 | 누적 91.97M 중 중복 1.80% - 지배 원인은 장애 복구 재소비, 상시 0.0005%; 재전송 중복 27+239건 실측 뒤 ReplacingMergeTree 로 저장 층에서 제거 | docs/07·25 |
| 임계를 실측하고 병목을 특정한다 | 녹화-재생 부하 실험: 5,000/s 까지 실시간, 10,000/s 정체. 병목은 예상(ClickHouse)이 아니라 **소스 체인에 묶인 동기 JDBC 싱크**, 배치 200→1,000 한 줄로 정체 0. 현 구성 여유 피크 85배 | docs/23 |
| 구성은 실측으로 줄인다 | 브로커 3→1: 단일 호스트라 복제는 형식뿐임을 24h 대조·정지 실험으로 확인 → 무정지 재할당, 재기동 14초·유실 0 | docs/24 |
| 스키마는 조회·삭제 기준으로 잡는다 | binlog 시각 파티션 → 체결 시각 파티션·이벤트 키로 무정지 재생성(MySQL RENAME, ClickHouse EXCHANGE). 1시간 조회 **39/39 파트 1.0s → 5/56 파트 0.10s**, 옛 키가 못 거른 중복 4행 정리 | docs/28 A |
| CDC 는 변경을 캡처해야 CDC 다 | 체결은 INSERT 뿐이라 2층 원장을 둠: Binance Testnet(실돈 없음) 주문 생애주기를 MySQL 거울 테이블에 반영 → 삭제까지 살리는 두 번째 커넥터 → ClickHouse RMT(version, is_deleted). 전이 5종 실증, **거래소=MySQL=ClickHouse 3자 대조 불일치 0**(340주문), 거래소→Kafka 219ms | docs/28 B |
| 규칙엔 정답이 있어야 한다 | 근거 없던 3규칙 삭제 → 거래소 시장경보 6개월 15,588건을 정답으로 ±50/100/200% 역산 → 분 종가 판정으로 플래핑 86→9, 동등성 대조로 승격 판단 | docs/16·22 |

부수적으로: 16GB 미니PC에서 컨테이너 ~20개 공존(메모리 한도·실측 배분), Airflow 5 DAG(헬스 10분·대조·규칙·일일·케이스)·dbt 품질 층(dq_* 7개), 백업·복원 리허설(1.1억 행 79초), 접근 통제.

**정직하게 못 하는 말**: 대규모(피크 수십 rows/s, 원장 하루 수백 건), 팀·클라우드, 실제 회사 도메인. 규모는 부하 실험 문장으로, 나머지는 실무 경력으로 답한다.

---

## 📊 실시간 모니터링 대시보드

> **🔗 실시간 대시보드**: Grafana 12패널 (아래 캡처 · 라이브 데모는 요청 시 제공)

<img width="2527" height="1235" alt="image" src="https://github.com/user-attachments/assets/edca902b-962a-4738-a538-f9ab973200a2" />


### 대시보드 레이아웃

```
┌─────────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┐
│  Active Alerts  │  Total Trades   │ Avg CDC Latency │  Total Volume   │ Markets Tracked │
│   (이상 탐지)    │  (총 체결 건수)  │ (평균 지연시간)  │(최근1시간 거래액)│ (모니터링 마켓)  │
│      ~13/hr     │   91,061,136    │   p50 3ms       │    ₩41.1B       │       5         │
└─────────────────┴─────────────────┴─────────────────┴─────────────────┴─────────────────┘
┌──────────────────────────────────────┬──────────────────────────────────────┐
│  BTC Price (실시간 BTC 가격)           │  Bid vs Ask (매수/매도 비율)           │
│  🔴 빨간 점선 = 이상 탐지              │  ██ BTC  ██ XRP  ██ ETH  █ SOL █DOG │
│  price(녹) / low(노) / high(파)       │  마켓별 매수/매도 건수 막대 차트        │
└──────────────────────────────────────┴──────────────────────────────────────┘
┌──────────────────────────────────────┬─────────────────────────────┬───────┐
│  Trade Volume (5분 총 거래금액)        │  CDC Latency (CDC 지연시간)  │🟢LIVE │
│  5개 코인 합산 라인 차트 (₩ 단위)       │  avg(녹색) / max(주황) 추이  │       │
└──────────────────────────────────────┴─────────────────────────────┴───────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│  Anomaly Alerts (이상 탐지 내역)                                             │
│  시간 | alert_type | market | message (콤마 포맷) | value | threshold       │
│  최근 50건, value/threshold 숫자 콤마 포맷 적용                               │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│  Recent Trades (최근 체결 내역)                                               │
│  시간 | market | ask_bid | trade_price | volume | amount | cdc_latency_ms  │
│  최근 5분 이내 20건 표시                                                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 패널 상세 (12개)

**상단 KPI (5개)** - 파이프라인 핵심 지표 한눈에

| 패널 | 데이터 범위 | 설명 |
|------|-----------|------|
| **⚠ Active Alerts (이상 탐지)** | 최근 1시간 | 이상 탐지 룰에 걸린 알림 건수 (빨간 배경 강조) |
| **Total Trades (총 체결 건수)** | 전체 누적 | 파이프라인 가동 이후 총 체결 건수 (현재 9,100만+) |
| **Avg CDC Latency (평균 지연시간)** | 최근 1시간 | MySQL → ClickHouse 평균 CDC 지연 (목표: <10ms, 실측: ~3ms) |
| **Total Volume (최근 1시간 거래금액)** | 최근 1시간 | 5개 마켓 합산 체결 금액 (₩ 자동 포맷: K, M, B) |
| **Markets Tracked (모니터링 마켓)** | 고정값 | BTC, ETH, XRP, SOL, DOGE (5개) |

**중단 차트 (4개) + Pipeline Status** - 시장 흐름 + 이상 탐지 시각화

| 패널 | 위치 | 데이터 소스 | 설명 |
|------|------|-----------|------|
| **BTC Price (실시간 BTC 가격)** | 좌상 | `crypto_trades` | 분 단위 평균/최저/최고 + 🔴 **이상 탐지 빨간 점선** (Grafana Annotation) |
| **Bid vs Ask (매수/매도 비율)** | 우상 | `crypto_trades` (2026-09-19, 체결 시각) | 마켓별 매수/매도 건수 막대 차트 (1시간 집계, 자동 갱신) |
| **Trade Volume (5분 총 거래금액)** | 좌하 | `crypto_trades` 5분 버킷 (2026-09-19, 체결 시각) | 5개 코인 합산 거래금액 라인 차트 (₩ 단위, Flink 5분 윈도우 집계) |
| **CDC Latency (CDC 지연시간)** | 우하 | `crypto_trades` | 평균(녹색)/최대(주황) 레이턴시 추이 (ms 단위) |
| **Pipeline Status (파이프라인 상태)** | 우하 끝 | `crypto_trades` | 5분 내 데이터 유입 여부 (🟢 LIVE / 🔴 STALE, 글씨색 표시) |

**하단 테이블 (2개)** - 상세 데이터 조회

| 패널 | 표시 건수 | 특징 |
|------|----------|------|
| **Anomaly Alerts (이상 탐지 내역)** | 최근 50건 | alert_type, market, message, value/threshold **콤마 포맷** 적용 |
| **Recent Trades (최근 체결 내역)** | 최근 5분 내 20건 | trade_price (₩ 포맷), trade_amount (₩ 포맷), cdc_latency_ms |
---

## 🔍 이상탐지 v2 - 거래소 경보의 재현과 검증 (2026-09-17 교체)

> 이전 버전(LARGE_TRADE 1억 · PRICE_SPIKE 직전 대비 3% · VOLUME_SURGE 수량 EMA×150)은 **삭제**했다.
> 코드 주석은 그 규칙들을 업비트 이상거래 감시 유형(시세관여·체결관여·매매집중)에 대응시켰지만, 감시정책 원문을 읽어 보니 그 유형들은 전부
> **계정·주문 단위 관여율**이라 공개 체결·호가로는 계산할 수 없다. 숫자는 "시간당 30~50건 나오게" 맞춘 값이었다. 상세 `docs/16 §1`.

### 무엇을 재현하나
업비트 **시장경보제도(주의/경고/위험)**. 자동 지정·해제이고, 지정 결과가 API 로 내려오므로 **정답 데이터**가 된다.
이력 API(`crix/market-event-records`)에서 6개월 15,588건을 확보했다(`docs/16 §3`).

| 규칙 | 정의 | 어디서 | 근거 (실측) |
|---|---|---|---|
| **PRICE_24H** | 분 종가 / 24h 전 분 종가 − 1 이 ±50% 주의, ±100% 경고, ±200% 위험. 분이 닫힐 때 판정, **등급 전이** 시에만 기록 (v2.1: 체결 단위는 임계 근처 플래핑 86회/일 → 분 단위로 정답과 정확히 일치, docs/22 §4-3) | Flink `MarketAlertDetector` (섀도) | 거래소 지정 시각의 지표값: 주의 p50 51.7%, 경고 101.2%, 위험 196.9% (6개월 167건 1분봉 독립 검증). 역검증: ≥50% 인 분 3,221개 중 3,220개가 지정 구간 안 |
| **VOLUME_24H** | 전일 거래대금 ≥ 7일 평균 × 4 AND ≥ 10억 | dbt `int_volume_surge_daily` (일 1회) | 6개월 1,981 라벨 대비 정밀도 0.79 / 재현율 0.80. 실전 첫 3일 0.83/1.0, 0.68/0.87, 0.75/0.94 |
| **EXCHANGE_FLAG** | 거래소 지정·해제 전이 자체 | `upbit_market_events` 1분 폴링 → n8n Slack | 우리가 계산 못 하는 유형(입금량·소수계정)의 유일한 신호 |

임계값은 내가 고른 값이 아니다. 거래소가 실제로 지정한 순간의 지표값 분포에서 역산했고, 월별 분포로 정적임을 확인했으며(05-18 참조 시각 변경 전후만 다름), 반대 방향으로도 검증했다.
근거 쿼리는 `scripts/analysis/rule_basis_check.sh` 로 재실행되고 `docs/16-appendix-queries.md` 에 출력이 있다.

### 규칙 검증 기반 (규칙보다 이것이 DE 의 산출물)
- `market_alerts` - v2 출력(마켓·등급·전이 시각·`rule_version`). 섀도 기간엔 발송 없음.
- `dq_rule_eval_daily` - 매일 우리 규칙 vs 거래소 지정의 **정밀도·재현율·선행 시간**. "왜 그 임계인가"에 대한 답이 매일 갱신되는 숫자로 남는다.
- 승격 기준(`docs/22 §4`, 09-17 정정): PRICE_24H 는 실전 전이 10건 이상이 1분봉 재계산과 전부 일치(구현 동등성 - 임계 근거는 이미 6개월 역검증), VOLUME_24H 는 일 평가 7회 연속 정밀도·재현율 ≥ 0.75(요일 주기). 미달이면 규칙이 아니라 정의 차이(참조 시각·창·예외)를 먼저 의심한다.
- 늦은 이벤트 가드: 적재 지연 > 60초인 행(백필·gap-fill)은 상태·판정에서 제외 - 백필 1건이 오탐 2건을 만들던 기전 제거(`docs/17 §4-4`, `docs/20`).

### 한계 (인정)
- 1층 재현은 누구나 API 로 받을 수 있는 플래그의 재현이다. 가치는 선행 시간과, 규칙을 정의·검증·배포하는 기반에 있다.
- 배포 직후 24시간은 PRICE_24H 참조(24h 전 종가)가 없어 출력이 없다. 상태 부트스트랩은 후속.
- 알림을 받아 행동할 사람이 아직 없다. 이상탐지 정의는 본래 도메인 팀 몫이고, 여기서는 그 기반을 만든 것이다.

## 🔔 n8n 자동 알림 시스템

![n8n Workflow](docs/images/n8n-workflow.png)

### 아키텍처

```
┌─────────────────┐
│ Schedule Trigger │ (매 1분)
└────────┬────────┘
         ▼
┌─────────────────┐
│   ClickHouse    │  이상거래 건수 + 파이프라인 상태 + 알림 상세
│   HTTP 쿼리     │  (Docker 네트워크로 직접 접근)
└────────┬────────┘
         ▼
┌─────────────────┐
│  Parse & Combine │  숫자 포맷 (콤마 구분) + 알림 분류
└────────┬────────┘
         │
    ┌────┴────────┐
    ▼             ▼
┌────────┐   ┌────────┐
│  IF    │   │  IF    │
│이상거래│   │파이프  │
│ >0건?  │   │라인    │
│        │   │ 장애?  │
└───┬────┘   └───┬────┘
    │             │
    ▼             ▼
 [Slack]       [Slack]
 [Gmail]       [Gmail]
 FDS 알림     CDC 장애
```

### 알림 종류

| 알림 | 조건 | 채널 | 의미 |
|------|------|------|------|
| **🚨 FDS 이상거래 탐지** | anomaly_count > 0 (최근 1분) | Slack + Gmail | 이상 탐지 룰 발동, 상세 내역 포함 |
| **🔴 CDC 파이프라인 장애** | 최근 5분간 데이터 0건 | Slack + Gmail | 파이프라인 중단, 복구 가이드 포함 |

### 알림 메시지 예시

**Slack 알림**

![Slack Alert](docs/images/slack-alert.png)

**Gmail 알림**

![Gmail Alert](docs/images/gmail-alert.png)

**FDS 알림 (Slack)** - 2026-09-17 부터 n8n 은 우리 규칙이 아니라 **거래소 시장경보 지정·해제 전이**(`upbit_market_events`, 1분 폴링)를 보낸다.
우리 규칙(PRICE_24H·VOLUME_24H)은 `market_alerts` 에 섀도로만 쌓이고, 승격 기준(`docs/22 §4`)을 넘기 전엔 발송하지 않는다.
아래는 교체 전(v1 규칙) 메시지 예시로, 형식 참고용이다.

```
🚨 FDS 이상거래 탐지!  (v1, 2026-02 - 규칙은 폐기됨)

최근 1분간: 3건
상세 내역:
• VOLUME_SURGE | KRW-BTC: 거래량 EMA 대비 52.3배 급증
• VOLUME_SURGE | KRW-ETH: 거래량 EMA 대비 61.7배 급증
```

**CDC 파이프라인 장애 (Gmail)**

```
🔴 CDC 파이프라인 장애 알림

상태: 데이터 유입 중단
최근 5분 거래: 0건
총 적재 건수: 6,764,002건

확인 사항:
• Flink Job 상태 확인
• Kafka LAG 확인
• MySQL 접속 확인

📊 Grafana 대시보드 바로가기
```

---

## 📋 프로젝트 개요

### 데이터 소스
- **체결(trade)**: Upbit WebSocket, KRW 전 마켓 **287개** (2026-09-09 확장. 그 전 7개월은 BTC/ETH/XRP/SOL/DOGE 5개)
  - 287마켓 실측: 평균 25~30 rows/s, 오전 피크 53.6 msg/s, 일 약 400만 건 (5개 마켓 시절: 4.17 rows/s, 일 36만 건)
- **호가(orderbook)**: Upbit WebSocket `orderbook.15`, KRW 287마켓 전체 스냅샷 (2026-09-09 신설)
  - 실측: 154~262 msg/s(시간대별), payload 158~273 KB/s, 일 약 2,200만 건

### 파이프라인 흐름
```
[실시간 스트리밍 - 체결: CDC 경로]
Upbit WebSocket → MySQL → Debezium CDC → Kafka (1-broker, 2026-09-18 축소) → Flink → ClickHouse → Grafana
                                                                          │
[실시간 스트리밍 - 호가: 직접 발행 경로 (2026-09)]                            │
Upbit WebSocket → orderbook-collector → Kafka upbit.orderbook.v1 → Flink → ClickHouse (raw 7일 / 1분 파생 365일)
                                                                          │
[실시간 스트리밍 - Binance 체결: 직접 발행 경로 (2026-09-20, docs/31)]                     │
Binance WebSocket(USDT 493심볼 trade) → binance-collector → Kafka binance.trades.v1(6p) → Flink(병렬 2) → ClickHouse binance_trades (30일)
Binance WebSocket(상위 10 depth@100ms + 5분 REST 스냅샷) → depth 수집기 → Kafka binance.depth.v1 → Flink 호가장 재구성(키별 상태·순번 검증) → binance_orderbook_raw/1m
                                                                          │
[2층 원장 - CDC 거울 경로 (2026-09-19, docs/28 B·C)]                          │
Binance Testnet 주문(실돈 없음) → virtual-trader → MySQL(주문 거울·체결·케이스) → Debezium #2(삭제 유지) → Kafka ledger.* → ClickHouse RMT(version,is_deleted)
                                                                          │
[배치 오케스트레이션]                                                       │
Airflow (Scheduler) → dbt (staging → intermediate → marts) ────────────────┘
    │                                                                      │
    └─ health_check (10분) ─→ 이상 시 Slack 알림                            │
    └─ daily_pipeline (01:00 KST) ─→ 품질검증 + 일일 리포트 → Slack         │
                                                                           │
[알림 - 2026-09-20 Airflow 로 일원화 (docs/35 §4)]                           │
Airflow health_check(10분, 12체크) · quality_alerts(매시, 품질 SLO) · weekly_digest(월) → Slack
  ※ n8n 알림 워크플로는 v1 규칙 삭제(09-17)와 함께 **미니PC 쪽이** 멈췄고 되살리지 않았다. 정의는 n8n/workflows/ 에 보관.
  **(09-20 정정)** Oracle 쪽 n8n 의 헬스 모니터는 그 뒤로도 계속 돌고 있었다 - 즉 "멈췄다" 는 절반만 맞았다. 이 감시자는 09-20 04:01 ClickHouse 포트를 127.0.0.1 로 좁히면서 경로가 끊겼고, 지금은 **외부 감시자**(docs/43)가 그 역할을 대체한다.
```

### 차별화 포인트

| 일반 프로젝트 | 이 프로젝트 |
|--------------|-------------|
| AWS/GCP 관리형 서비스 | **On-Premise 물리 서버 직접 구축** |
| 로컬에서 잠깐 테스트 | **24시간 상시 운영 (2026-02부터 200일+, 체결 9,200만건 적재)** |
| 시연할 때만 실행 | **24시간 실제 운영** (요청 시 통제된 라이브 데모) |
| 무제한 리소스 | **16GB 메모리에서 30개 컨테이너 공존 (메모리 제한·실측 기반 배분)** |
| 시뮬레이션 데이터 | **Upbit 실시간 체결(287마켓) + 호가(287마켓) 실데이터** |
| 감으로 튜닝 | **사고 분석과 실측으로 결정** - 37시간 적재 지연 사고 분석(docs/08), 체크포인트 625MB→18KB(docs/10), 호가 압축률 14배 실측(docs/11), 모든 결정 근거는 docs/worklog.md |
| 고정 임계값 이상 탐지 | **업비트 정책 + 학술 논문 + 실측 분포 분석 기반 동적 임계값** |
| cron으로 dbt 실행 | **Airflow 오케스트레이션 (Custom Operator + Dynamic Task Mapping + Slack 리포트)** |
| 탐지만 하고 끝 | **알림 3층 (10분 헬스 · 매시 품질 SLO 판정 · 주간 다이제스트) + 알럿 이력 표** (docs/32) |

---

## 🏗️ 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                 On-Premise Real-time CDC Pipeline                           │
│                    (Mini PC Server - 24/7 운영)                              │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────┐
  │   Upbit     │
  │ WebSocket   │  실시간 체결 (5 마켓)
  │   API       │  ~8 TPS
  └──────┬──────┘
         ▼
  ┌─────────────┐     ┌──────────────┐
  │  WebSocket  │────▶│    MySQL     │  binlog 활성화
  │  Producer   │     │  (Source DB) │  7일 보존 + 매시간 cleanup
  └─────────────┘     └──────┬───────┘
                             │ CDC (binlog)
                             ▼
                      ┌──────────────┐
                      │   Debezium   │  MySQL CDC Connector
                      │   Connect    │  스냅샷 + 실시간 캡처
                      └──────┬───────┘
                             │
                             ▼
  ┌──────────────────────────────────────────────────────────────┐
  │                  Kafka (1 Broker, 2026-09-18 축소 - docs/24)      │
  │  ┌──────────┐                                                 │
  │  │ Broker 1 │  RF=1, 체결 토픽 zstd·7일 보존, DLQ 토픽         │
  │  │  512MB   │  (3 브로커·RF3 는 24h 실측 뒤 "형식뿐" 으로 축소)  │
  │  └──────────┘                                                 │
  └──────────────────────┬───────────────────────────────────────┘
                         │
                         ▼
  ┌──────────────────────────────────────────────────────────────┐
  │                   Flink Cluster                               │
  │  ┌─────────────┐  ┌──────────────┐                           │
  │  │ JobManager  │  │ TaskManager  │  Parallelism: 2           │
  │  │   512MB     │  │    1GB       │  Checkpoint: 60s          │
  │  └─────────────┘  └──────────────┘  Restart: 3x/10s         │
  │                                                               │
  │  ┌─────────────────────────────────────────────────┐         │
  │  │              Flink DataStream Job                │         │
  │  │                                                  │         │
  │  │  KafkaSource → NullSafeSchema → CdcEventParser  │         │
  │  │       │              │              │            │         │
  │  │       ▼              ▼              ▼            │         │
  │  │  [Raw Sink]   [5min Aggregation]  [Anomaly      │         │
  │  │                                    Detector]     │         │
  │  └─────────────────────────────────────────────────┘         │
  └──────────┬──────────────┬──────────────┬─────────────────────┘
             │              │              │
             ▼              ▼              ▼
  ┌──────────────────────────────────────────────────────────────┐
  │                    ClickHouse (OLAP)                          │
  │                                                               │
  │  crypto_trades (원본)     365일 TTL                           │
  │  trade_aggregations (집계) 365일 TTL                          │
  │  anomaly_alerts (이상탐지) 365일 TTL                          │
  │  + dbt marts (daily_summary, volume_spike, alert_rate)       │
  └──────┬──────────────────────┬───────────────┬───────────────┘
         │                      │               │
         ▼                      ▼               ▼
  ┌──────────────┐    ┌──────────────┐   ┌──────────────┐
  │   Grafana    │    │   Airflow    │   │     n8n      │  매분 폴링
  │  2 Dashboard │    │ Orchestrator │   │  Monitoring  │
  │  (CDC +      │    │              │   └──────┬───────┘
  │   Airflow)   │    │ ┌──────────┐ │          │
  └──────┬───────┘    │ │health_   │ │   ┌──────┼──────────┐
         │            │ │check     │ │   ▼      ▼          ▼
         │            │ │(*/10min) │ │ [Slack] [Slack]  [Gmail]
         │            │ └──────────┘ │  FDS     CDC     FDS+CDC
         │            │ ┌──────────┐ │
         │            │ │daily_    │ │
         │            │ │pipeline  │──→ dbt run/test
         │            │ │(01:00KST)│      → Dynamic 코인별 품질검증
         │            │ └──────────┘      → 일일 리포트 → Slack
         │            └──────────────┘
         ▼                    │
  ┌──────────────┐    StatsD → Prometheus
  │    Caddy     │
  │ (Reverse     │
  │   Proxy)     │
  └──────┬───────┘
         │
         ▼
  Grafana (대시보드)
  Airflow (오케스트레이션)
```

### 2026-09 확장: 호가 경로 + Flink 재구성

위 다이어그램은 체결(CDC) 경로다. 2026-09-09에 아래가 추가·변경됐다(상세 `docs/10`, `docs/11`).

```
  Upbit WS orderbook.15 (287마켓, 단일 커넥션)
        │  154~262 msg/s
        ▼
  orderbook-collector (Python, confluent-kafka)   key=market, zstd, idempotent producer
        │
        ▼
  Kafka upbit.orderbook.v1   6 파티션 · RF2 · 24h / 6GB·파티션   (zstd 후 415 B/msg → RF2 약 18.5GB/일)
        │
        ▼
  Flink OrderbookJob (슬롯 1, 이벤트타임 1분 윈도우)   ─┐  같은 TaskManager (2g, 슬롯 4, state.backend=hashmap)
        │                                              │  잡 3개: CDC(2슬롯) + Orderbook(1) + Circuit Connect(1)
        ├─▶ orderbook_raw  (15단 Array(Float64)×4, TTL 7일, 39.7 B/행 on-disk = JSON 대비 26배 압축)
        └─▶ orderbook_1m   (mid·spread·depth imbalance 1/5/15단·recv lag, TTL 365일)
```

| 변경 | 전 | 후 | 근거 |
|---|---|---|---|
| Flink 상태 백엔드 | RocksDB, 체크포인트 625MB(MANIFEST 비대) | hashmap, **17.8KB**, e2e 1.4s → 51ms | 키드 상태가 수십 KB뿐임을 로컬 db 디렉터리로 실측 |
| Flink TaskManager | 1g, task heap 25.6MiB, Metaspace 90% | 2g, task heap 692MiB, 슬롯 4 | Flink 메모리 모델 계산 |
| producer flush | 2초당 1배치(20행) = 최대 10 rows/s | 버퍼 소진까지 반복, 상한 2,500 rows/s, `buffer=` 지표 | 8월 37시간 적재 지연 사고 원인 |
| Kafka 시작 오프셋 | `latest()` (재시작 시 유실) | `committedOffsets(LATEST)` | 재시작 유실 방지 |
| 체결 이벤트 | 6필드 | + best bid/ask 4필드 (Upbit 신규 필드) | 호가 없이도 스프레드 확보 |
| MySQL 정리 | 매시 25K DELETE | 10분 40K DELETE, Debezium tombstone 비활성 | 전 코인 유입 400만/일 대응. 관찰 뒤 DROP PARTITION 전환 예정 |

### 리소스 배분 (16GB RAM, 30개 컨테이너)

| 컴포넌트 | 메모리 Limit | 비고 |
|----------|-------------|------|
| MySQL | 1GB | CDC Source DB |
| Kafka × 3 | 3.75GB | 1.25GB per broker (단일 호스트라 HA 아님 - 복제 의미론·장애 실험용, 실험 뒤 1브로커 축소 예정) |
| Zookeeper | 384MB | Kafka coordination (실험 뒤 KRaft 전환 예정) |
| Debezium Connect | 1.25GB | CDC connector |
| Flink (JM 896M + TM 2304M) | 3.2GB | 잡 3개, 슬롯 4, hashmap |
| ClickHouse | 1.75GB | OLAP storage (호가 인서트 추가 후 1.25→1.75GB) |
| Grafana | 256MB | 2개 대시보드 (CDC + Airflow) |
| Airflow (Webserver + Scheduler) | 1.28GB | 640MB each |
| Airflow PostgreSQL | 256MB | Airflow 메타 DB |
| StatsD Exporter | 128MB | Airflow 메트릭 변환 |
| Prometheus | 256MB | 메트릭 수집/저장 |
| Producer | 256MB | Upbit WebSocket 체결 (287마켓) |
| Orderbook Collector | 256MB | Upbit WebSocket 호가 (287마켓) |
| Kafka UI | 384MB | 클러스터 모니터링 |
| **제한 합계** | **~14.3GB** | **실사용 합 약 7.5GB (2026-09-09 실측), 비CDC 컨테이너 11개는 별도 mem_limit** |

---

## 🛠️ 기술 스택

| 컴포넌트 | 기술 | 버전 | 역할 |
|----------|------|------|------|
| Source DB | MySQL | 8.0 | CDC 소스 (binlog) |
| CDC | Debezium | 2.5 | 실시간 변경 캡처 |
| Message Queue | Apache Kafka | 3.6 | 이벤트 스트리밍 (3-broker → 1-broker, docs/24) |
| Stream Processing | Apache Flink | 1.18 | 체결 적재·PRICE_24H 등급 전이·호가 1분 집계·파싱 DLQ (5분 집계는 2026-09-19 폐기) |
| OLAP | ClickHouse | 24.1 | 분석 쿼리 + 대시보드 백엔드 |
| Orchestration | Apache Airflow | 2.8.1 | 배치 오케스트레이션 (2 DAGs, Custom Operator) |
| Data Transform | dbt | 1.7.9 | ClickHouse 데이터 변환 (3계층: staging → intermediate → marts) |
| Dashboard | Grafana | 11.0 | 실시간 시각화 (CDC 12패널 + Airflow 12패널) |
| Metrics | Prometheus | 2.50 | Airflow 메트릭 수집 (StatsD → Prometheus → Grafana) |
| Realtime Alerting | n8n | latest | FDS 이상거래 + CDC 장애 알림 (Slack, Gmail) |
| Daily Report | Airflow + Slack | - | 일일 파이프라인 리포트 (품질검증 + CDC 지연 + 이상탐지 요약) |
| Data Source | Upbit WebSocket | - | 암호화폐 실시간 체결 + 호가(orderbook.15), KRW 287마켓 |
| Orderbook Collector | Python + confluent-kafka | 2.15 | 호가 스냅샷 → Kafka 직접 발행 (zstd, idempotent) |
| Observability | cron + ClickHouse SQL | - | 5분 간격 87개 파이프라인 지표 (`scripts/observe/`), 적재 지연 알림(health_check) |
| Reverse Proxy | Caddy | 2.10 | HTTPS + 자동 인증서 |
| Language | Java 17 | - | Flink DataStream Job |
| Language | Python 3.10 | - | Upbit Producer, Airflow DAGs |

---

## 📅 구현 단계

### Phase 1: 인프라 구축 ✅
- [x] Docker Compose 구성 (12개 컨테이너, 메모리 최적화)
- [x] MySQL binlog 설정 (ROW 포맷, server-id, gtid)
- [x] Kafka 3-broker 클러스터 (RF=3, 72시간 보존) → 2026-09-18 1-broker·RF1 로 축소 (docs/24), 체결 토픽 zstd·7일 보존 (docs/29)
- [x] Zookeeper + 전체 healthcheck 구성

### Phase 2: CDC 파이프라인 ✅
- [x] Debezium MySQL CDC Connector 설정
- [x] INSERT/UPDATE/DELETE 이벤트 캡처 검증
- [x] Connect 내부 토픽 RF 문제 해결 (startup.sh 자동화)
- [x] Kafka 토픽 생성 및 메시지 흐름 확인

### Phase 3: Flink 스트리밍 ✅
- [x] Java DataStream API Job 개발
- [x] 5분 윈도우 집계 (거래량, 체결건수, 매수/매도) → 2026-09-19 폐기(처리 시간 창 왜곡, docs/29 §7). 분 마트(docs/27)·Grafana 원본 조회로 대체
- [x] 이상 탐지 4가지 룰 설계 (LARGE_TRADE, PRICE_SPIKE, VOLUME_SURGE, RAPID_TRADES)
- [x] ClickHouse JDBC Sink (3개 테이블)
- [x] NullSafeStringSchema (Debezium tombstone 방어)
- [x] Restart 전략 (fixedDelay 3회/10초)

### Phase 4: ClickHouse + Grafana ✅
- [x] ClickHouse 테이블 설계 (MergeTree, 365일 TTL)
- [x] Grafana 프로비저닝 (datasource + dashboard JSON)
- [x] 12개 패널 대시보드 구성
- [x] Caddy 리버스 프록시 + HTTPS 자동 인증서 외부 접근

### Phase 5: 암호화폐 실시간 수집 ✅
- [x] Upbit WebSocket Producer (Python, 5개 마켓)
- [x] MySQL 스키마 전환 (주식 → 암호화폐)
- [x] Flink Job 수정 (파싱, 집계, 이상탐지 전환)
- [x] 데이터 라이프사이클 관리 (MySQL 7일, Kafka 72시간, ClickHouse 365일)

### Phase 6: 이상 탐지 고도화 + 장애 복구 ✅
- [x] 업비트 이상거래 감시정책 기반 임계값 재설계
- [x] 학술 논문 근거 반영 (EWMA 동적 임계값)
- [x] MySQL cleanup → Flink crash 장애 복구 (tombstone NPE)
- [x] Grafana annotation 자동 동기화 (cron 매분)
- [x] 숫자 포맷 통일 (₩ 단위, 콤마 구분)
- [x] v3 임계값: 24시간 분포 분석 기반 VOLUME_SURGE 150x + RAPID_TRADES 비활성화

### Phase 7: n8n 자동 알림 시스템 ✅
- [x] n8n → ClickHouse 네트워크 연결 (Docker 외부 네트워크)
- [x] FDS 이상거래 탐지 알림 (Slack + Gmail)
- [x] CDC 파이프라인 장애 알림 (Slack + Gmail)
- [x] 숫자 포맷 (콤마 구분) + 대시보드 바로가기 링크

### Phase 8: Airflow 오케스트레이션 + dbt ✅
- [x] Airflow 2.8.1 Docker 구축 (LocalExecutor, PostgreSQL 메타 DB)
- [x] Custom Operator 개발 (ClickHouseOperator - HTTP API, FlinkHealthOperator - REST API)
- [x] Custom Hook 개발 (ClickHouseHook - HTTP 인터페이스, 추가 드라이버 불필요)
- [x] DAG 1: `health_check` (10분 간격) - 5개 컴포넌트 병렬 체크 → XCom 수집 → 이상 시 Slack
- [x] DAG 2: `daily_pipeline` (매일 01:00 KST) - dbt run/test → Dynamic Task Mapping 코인별 품질검증 → quality gate → 일일 Slack 리포트
- [x] DAG 3: `reconcile_trades` (매일 06:35 UTC) - 업비트 시간봉(진실값) 적재 → dbt build(유실·커버리지 테스트) → 실패 시 Slack. 첫 실행에서 상장 누락 마켓(BFC)과 재연결 5초 유실을 잡음 (docs/17)
- [x] DAG 4: `backup_daily` (매일 01:20 UTC) - ClickHouse 증분 백업 + 호가 Parquet(zstd) 120일 롤링 → Oracle rsync → 원격 보존 → 동기 검증. 복원 리허설 79초/1.1억 행 (docs/21)
- [x] DAG 5: `rules_daily` (매일 01:05 UTC) - VOLUME_24H 규칙 판정 + 규칙 평가(정밀도·재현율·선행 vs 거래소 지정) 갱신 (docs/22)
- [x] dbt 3계층 모델 (staging: stg_trades → intermediate: int_ohlcv_1h, int_ohlcv_daily → marts: mart_daily_summary, mart_volume_spike, mart_alert_rate)
- [x] Airflow 메트릭 모니터링 (StatsD → Prometheus → Grafana Airflow Operations 대시보드 12패널)
- [x] Fernet Key 암호화 (Slack Webhook URL 등 시크릿 보호)
- [x] Caddy reverse proxy (HTTPS 자동 인증서)
- [x] 과거 25일치 일일 리포트 Slack 백필
- [x] DAG 테스트 9/9 통과 (pytest)
- [x] 기존 dbt cron 비활성화 → Airflow 완전 이관

### Phase 9: 적재 지연 사고 분석 + Flink 재구성 + 전 코인·호가 확장 (2026-09-09) ✅
- [x] 중복 감사 마감 (`docs/07`): 91.06M 적재 / 89.42M 고유, idempotent producer + 일일 (source_ts, trade_id) 게이트
- [x] 8/29 "건수 반토막" 판별 → 유실 아님, producer 10 rows/s 상한 포화로 **최대 36.9시간 적재 지연** (`docs/08`)
- [x] 호가 확장 사전 검증: Upbit WS 한도(5연결/s, 429), 287마켓 단일 커넥션, count별 크기, 압축, Oracle vs 미니PC 수신 지연 비교 (`~/cdc-orderbook-probe/REPORT.md`, `docs/09`)
- [x] Flink RocksDB → hashmap (체크포인트 625MB → 17.8KB), TM 1g → 2g, savepoint 복원으로 유실 0·중복 0 (`docs/10`)
- [x] producer flush 상한 제거(10 → 2,500 rows/s) + best bid/ask 4필드 ClickHouse까지 통과
- [x] 호가 경로 신설: collector → Kafka → Flink OrderbookJob → orderbook_raw / orderbook_1m (`docs/11`)
- [x] 체결 287마켓 확장 + MySQL 정리 상향 + Kafka retention 상향 + tombstone 비활성, 1시간 dry-run 통과
- [x] health_check: 잡 3개 감시, 적재 지연(source_ts − upbit_timestamp) 알림 추가
- [x] 7일 무변경 관찰 시작 (2026-09-09 06:10 UTC, 태그 `obs-week1-start`, 계획 `docs/12`)
- [x] 관찰 중 예외 조치: sequential_id 마켓 간 충돌 유실 발견 → 유니크 키 교체 + 7일 원장 백필 210,469건 (이슈 7, `docs/13`)
- [ ] 관찰 뒤 1순위: `reconcile_trades` DAG(일일 원장 대조·자동 백필·품질 테이블) + dbt `(market, sequential_id)` 유일성·커버리지·freshness 테스트
- [x] 관찰 종료·분석 (`docs/14`) - 가설 12개 중 10 통과 / 1 조건부 / 1 위반(알림 편중), 튜닝 순위 8건
- [x] 이상탐지 기준 재설계 (`docs/16`, 부록 `docs/16-appendix-queries.md`) - 업비트 시장경보 재현, 6개월 라벨 15,588건 대비 정밀도·재현율 실측
- [x] DAG 3 `reconcile_trades` + dbt 계약 (`docs/17`) - 첫 실행에서 유실 2건 적발·복구 (이슈 8)
- [x] 마켓 목록 5분 주기 갱신·재구독 (신규 상장 자동 반영)
- [x] 배포 1 (`docs/20`): 늦은 이벤트 가드(재정렬 5.87% 실측으로 설계 변경) · 기동/재연결 gap-fill(독립 재대조 누락 0) · 수리 계보 · 10분 커버리지 · CI
- [x] 백업 + 접근 통제 (`docs/21`): Oracle 150GB 오프사이트, 복원 리허설 79초/1.1억 행, ClickHouse 사용자 4종 분리·전 클라이언트 인증, 타 프로젝트 영향 범위 점검
- [x] 이상탐지 v2 + dbt 품질 층 (`docs/22`): 구 3규칙 삭제, PRICE_24H 등급 전이(섀도)·VOLUME_24H(dbt, 실전 정밀도 0.83/재현율 1.0)·거래소 전이 발송, dq_* 품질 층·규칙 평가 모델, 품질 대시보드, 일일 리포트 초점 전환
- [x] 부하 실험 (`docs/23`): 임계 5,000~10,000/s, 병목 = 소스 체인의 동기 JDBC 싱크, 배치 200→1,000 으로 정체 0, 현 설정 여유 피크 85배, 브로커 정지 실측 3회
- [x] 이상탐지 v2.1: 체결 단위 → 분 종가 판정(플래핑 86→9, 거래소 지정과 시각 일치), 동등성 대조 모델 (`docs/22 §4`)
- [x] 되돌릴 수 없는 데이터 정리: 호가 유실 창 모델, 수집기 버퍼 600s, Connect 자동 복구 (`docs/23 §7`)
- [x] 브로커 3→1 (`docs/24`): 105 파티션 RF1 재할당 무정지, 24h 대조 100%, 재기동 실측 14초 정지·유실 0·중복 239, 메모리 +1.5GB·디스크 −27GB
- [x] ReplacingMergeTree (`docs/25`): 재전송 중복 27+239 근거, 무정지 교체, 중복 0
- [x] CDC 구간 재검토 (`docs/26`): 삭제 이벤트 46% 제거·정리 DELETE 59s→1s·binlog 30일, #1 철회, `dim_markets`(상장일 근사·커버리지 공백 BFC 6일)
- [x] 체결×호가 분 단위 마트 (`docs/27`): 10일 3.78M 행, EURC 되튐의 원인은 스프레드가 아니라 체결/깊이(≥300bp 변동 분의 중앙값 1.0)
- [x] 체결 테이블 재설계 (`docs/28` A): 체결 시각 일 파티션·시각 포함 키·recv_ms/ingest_source/stream_type, 무정지 RENAME 교체(Debezium 이어 받음), DELETE→DROP PARTITION, 체결 토픽 키 market·zstd·7일 보존
- [x] 2층 원장 B (`docs/28`): Binance Testnet 주문 생애주기(실돈 없음)를 MySQL 거울 테이블에 반영 → 두 번째 Debezium 커넥터(삭제 유지) → ClickHouse RMT(version, is_deleted). 전이 5종 실증, 거래소=MySQL=ClickHouse 3자 대조 불일치 0, 거래소→Kafka 219ms
- [x] SLO·알럿 체계 (`docs/32`): SLO 표 15줄(지표·목표·근거·행동), 알럿 3층(즉시 10분 / 품질 판정 매시·하루 1회 / 주간 다이제스트 = 트래픽 피크·지연·알럿 집계·품질·자원·고칠 것), 알럿 이력 표, 5분 호스트 지표(Prometheus 대체)
- [x] Binance 2단계 호가장 재구성 (`docs/31` §5): 증분+스냅샷 → 키별 상태(TreeMap)·U/u 순번·desync 복구, 1초 상위 20 을 Upbit 호가와 같은 스키마로. gap 0, 최우선 호가 = 거래소 bookTicker
- [x] Binance 실시간 확장 1단계 (`docs/31`): 스택 해체 분석(볼륨으로 일하는 건 호가 경로뿐) → USDT 전 심볼 체결 수집(Kafka 직행, 6파티션 키=symbol) → Flink 별도 잡(병렬 2, DLQ) → ClickHouse RMT 30일 → REST 1h 캔들 체결 수 대조 DAG. 체결 경로 유입 36/s → ~370/s
- [x] 인계 층 C (`docs/28` C): Upbit 경보 플래그 SCD(거래소 이력 16,202구간 + 폴링), 케이스 테이블(거울 CDC 두 번째, cases_hourly 자동 생성·사람 판정), 교차 거래소 신호(같은 코인 단위, 가격 비교 안 함)
- [x] 이상탐지 승격 + 알림 일원화 (`docs/22` §6): PRICE_24H v2.1.1 섀도 → 정식(전이 116건 미매칭 0). 발송 `market_alerts_notify` DAG(10분, 승급만, 마켓·등급·시 dedup). 검증 중 "성공처럼 보였던 실패" 3건 적발 - 재배포가 옛 JAR 을 배포, dedup 이 플래핑에 뚫림, **DAG 4개가 만들어진 뒤 한 번도 돈 적 없음**(paused)
- [x] 계약 검증 상시화 + 공지 라벨링 (`docs/37`): 토픽 계약을 매시 검증해 표에 적고 Airflow 가 판정(위반뿐 아니라 **검증기가 멈춘 것**도 알린다). 거래소 공지 1,200건을 받아 거래를 멈추는 것만 분류(카테고리는 못 믿는다 - `점검` 대부분이 신분증·입출금) → 수집 공백에 라벨
- [x] 보강 프로그램 #1~#10 (`docs/34`): 세 관점 평가(`docs/33`)에서 나온 약점을 항목마다 **왜 → 무엇 → 검증**으로. 보안 3건·하루 규약 통일·차원/사이드·**금액·수량 Decimal 무정지 전환**·재처리 런북·죽은 산출물 정리(`docs/35`)·테스트/계약(dbt test 68/68, 지표 사전 `docs/36`, 토픽 JSON Schema 5종 + 검증기)·마켓 상태 SCD(폐지·정지를 유실과 구분 - REST 에 없고 웹소켓에만 있는 필드, 지금 폐지 예정 2건)·백업 제외 기준 재정의(증분 8.7GB→5.0GB)와 복원 리허설 스크립트화
- [ ] 이후(2026-09-20): 섀도 승격 판단(동등성 10건 누적) → 정리 주간(README·여정 인덱스·교훈 통합·블로그) → 스키마 계약·JMX → KRaft 컷오버(체결 Kafka 선기록·markets 마스터·가상 매매 원장) → RMT → 녹화-재생 증폭 실험 3계층(① Upbit 코퍼스 ② Binance 공개 데이터 코퍼스 ③ 브로커 단독 상한, 브로커 장애 시나리오 포함, 설계 `docs/15`) → 브로커 3→1 + KRaft → CDC 유의미화(가상 매매 원장 + 이상탐지 케이스 관리) · MySQL DROP PARTITION 청소 전환 · ReplacingMergeTree

---

## 🔧 운영 이슈 & 트러블슈팅

### 이슈 1: MySQL Cleanup DELETE 폭주 → Flink 장애

| 항목 | 내용 |
|------|------|
| **현상** | 2일간 ClickHouse 데이터 적재 중단 |
| **원인** | 일 1회 50K건 DELETE → Debezium tombstone 메시지 → Flink NPE → Job FAILED |
| **근본 원인** | `SimpleStringSchema`가 null 바이트 처리 불가 + CdcEventParser DELETE 미처리 |
| **해결** | NullSafeStringSchema 구현, DELETE 스킵, 매시간 25K건 분산 삭제로 전환 |
| **교훈** | CDC 파이프라인에서 대량 DML은 반드시 시간 분산 처리 |
| **2026-09 후속** | 체결 토픽 메시지의 84%가 delete+tombstone임을 실측 → `tombstones.on.delete=false`(compact 토픽이 아니라 무용), 전 코인 확장으로 정리 10분×40K. 근본 해결은 관찰 뒤 일 단위 파티션 + DROP PARTITION(행 단위 이벤트가 생기지 않음) |

### 이슈 2: Flink Checkpoint Offset 복원 문제

| 항목 | 내용 |
|------|------|
| **현상** | Kafka offset 리셋해도 Flink가 과거 offset으로 회귀 |
| **원인** | Flink checkpoint가 Kafka consumer group보다 우선 |
| **해결** | Checkpoint 삭제 + `OffsetsInitializer.latest()` 변경 |
| **교훈** | Flink offset 관리는 checkpoint 우선, consumer group 리셋만으로 불충분 |

### 이슈 3: 이상 탐지 과다 알림

| 항목 | 내용 |
|------|------|
| **현상** | v1: 시간당 651건 (DOGE 1원 변동 매번 발동), v2: 시간당 72건 (VOLUME_SURGE 간신히 초과하는 노이즈) |
| **원인** | v1: 고정 임계값이 암호화폐 변동성 미반영, v2: EMA×50이 정상 변동의 상단 경계에 위치 |
| **해결** | v3: 24시간 분포 분석(p90=3.5x) 기반 EMA×150 적용 + RAPID_TRADES 비활성화(API 전송한계) → 시간당 ~13건 (31일 실측) |
| **교훈** | 임계값은 도메인 지식 + 실측 데이터 분포 분석(percentile) 기반으로 반복 조정 필수 |

### 이슈 4: Kafka Cluster ID 불일치

| 항목 | 내용 |
|------|------|
| **현상** | Broker 재시작 시 ClusterIdMismatch로 기동 실패 |
| **원인** | Docker volume 재생성 시 기존 meta.properties와 충돌 |
| **해결** | startup.sh에서 Connect 내부 토픽 자동 재생성 로직 추가 |

### 이슈 5: 적재 지연 36.9시간 - 유실로 오인될 뻔한 사고 (2026-08-19 ~ 08-30)

| 항목 | 내용 |
|------|------|
| **현상** | 08-29를 경계로 일별 적재 건수 690K → 364K 급감. 재연결 직후라 "부분 구독 실패 = 유실" 의심 |
| **판별** | 업비트 일봉(외부 기준)과 대조: 적재시각(`source_ts`) 기준 비율은 41~322% 요동, **체결시각(`upbit_timestamp`) 기준은 전 기간 97.5~99.9% 일치 → 유실 아님** |
| **원인** | producer `flush()`가 2초당 1배치(20행)만 INSERT → 최대 10 rows/s. 08-22 시간당 72.75 msg/s 버스트에 큐 108만 행 적체, 8일간 8 rows/s로 배수(포화 서명: 시간당 처리량 CV 0.047) |
| **왜 못 봤나** | health_check·Grafana·Flink 지표가 전부 `source_ts` 이후 구간만 측정. 거래소 체결시각 기준 지연 지표가 없었음 |
| **해결** | flush를 버퍼 소진까지 반복(상한 2,500 rows/s), `buffer=` 지표·경고, health_check에 `source_ts − upbit_timestamp` p50 > 60초 알림 |
| **교훈** | "유실"과 "지연"은 외부 기준(거래소 시각)과 대조해야 구분된다. 지연 지표는 소스 이벤트 시각 기준이어야 한다. 상세 `docs/08-ingest-lag-incident.md` |

### 이슈 6: mem_limit로 n8n 크래시 루프 24분 (2026-09-09)

| 항목 | 내용 |
|------|------|
| **현상** | 비CDC 컨테이너에 mem_limit 적용 후 n8n이 exit 134(`JavaScript heap out of memory`) 23초 간격 57회 재시작, CDC 알림 24분 중단 |
| **원인** | cgroup 제한을 V8가 힙 상한으로 환산(576M → 312MB). 스왑 상태 RSS(270MiB) 기준 1.5배 규칙이 실제 워킹셋을 과소산정 |
| **해결** | n8n 제한 제거(compose 재생성). Node 앱은 `NODE_OPTIONS=--max-old-space-size`와 함께 정해야 함 |
| **교훈** | 스왑이 많은 호스트에서 `docker stats` RSS는 메모리 산정 근거로 부적합 |

### 이슈 7: 유실 사고 #2 - sequential_id 마켓 간 충돌로 INSERT IGNORE가 체결을 폐기 (2026-09-10)

| 항목 | 내용 |
|------|------|
| **현상** | 287마켓 확장 후 업비트 시간봉 대비 거래량 커버리지 **94.7%**(활동 시간대 89.7%). 5코인 시절 일 단위 대조는 99%+라 보이지 않았음 |
| **위치 특정** | ① MySQL vs ClickHouse 951,279 = 951,279 → CDC 이후 무결. ② REST 원장(`/v1/trades/ticks`) vs ClickHouse `sequential_id` → BTC 1시간 4,111건 중 265 누락, 버스트 초에 집중 |
| **오판과 정정** | producer가 "받은 만큼 썼다"는 카운터를 근거로 **"업비트 WS 미전달"로 결론** → 더블체크 요구에 따라 DB 없는 **독립 WS 클라이언트**를 20분 동시 연결 → REST = 독립 클라이언트 100%, 파이프라인만 BTC −306(10.3%) → 결론 철회, producer 유실 확정 |
| **근본 원인** | `sequential_id` = 체결 ms × 10,000 + ms 내 순번 → **마켓별** 유일. MySQL `UNIQUE KEY (sequential_id)` + `INSERT IGNORE`가 다른 마켓의 같은 ms 체결을 "중복"으로 폐기(누락 BTC sid가 MySQL에 XRP 행으로 존재). 폐기 건수를 `duplicates`로 집계해 정상으로 오인 |
| **해결** | 유니크 키 `(market, sequential_id)`로 교체(`ALGORITHM=INPLACE, LOCK=NONE`, 404만 행 32초 무중단, Debezium DDL 추적). 재검증: 독립 클라이언트 10분 재실험 파이프라인 누락 0 |
| **복구** | REST 원장(7일 창)으로 백필 **210,469건**(287마켓 구간 56,700 = 2.94%, 5코인 구간 153,769 = 6.71%) → MySQL 경유 CDC로 ClickHouse까지 전파. 재대조 287마켓×12h **100.0%**. 09-03 이전 손실은 원장 창 밖이라 복구 불가(추정 5.3%) |
| **부작용** | 백필 체결이 "현재"로 처리돼 PRICE_SPIKE 오탐 1,646건(삭제), 5분 집계 창 오염(기록). 백필 창 하한을 소스(MySQL 7일 보존) 기준으로 잡아 ClickHouse 중복 53,012행 발생 → 삭제. 교훈: 백필 창은 타깃 기준 |
| **교훈** | 외부 ID 유일성은 프로파일링으로 검증 후 키에 넣는다. 조용히 버리는 쓰기에는 지표·알림을 붙인다. 정합성 불일치는 소스 탓으로 결론내기 전에 독립 수신기로 재현한다. 대조는 거래량이 아닌 건수·ID 단위로, 원장 보존 창 안에 매일 자동으로. 상세 `docs/13-sequential-id-collision-incident.md` |

### 이슈 8: 유실 사고 #3 - 대조의 분모를 우리 DB에서 만들어 6일간 안 보였다 (2026-09-16)

| 항목 | 내용 |
|------|------|
| **현상** | 7일 관찰 내내 일일 원장 대조가 **6일 연속 가중 100.0%**. 같은 대조를 Airflow DAG + dbt 테스트로 옮긴 첫 실행에서 즉시 2건 실패 |
| **유실 A - 구독 누락** | KRW-BFC 가 09-10 상장했으나 체결·호가 **0건, 6일간**. producer·수집기가 기동 시 마켓 목록을 1회만 조회하고 갱신하지 않음 |
| **왜 안 보였나** | 관찰 cron 대조가 **비교할 마켓 목록을 ClickHouse 에서** 가져왔다. 우리가 안 받은 마켓은 분모에 없으므로 구조적으로 탐지 불가. "100.0%"는 그 마켓을 제외한 값이었다 |
| **유실 B - 재연결 구멍** | 09-15 20:47:00 WS 끊김 → 20:47:05 재연결. 45초 창 287마켓 대조에서 REST 996 / 보유 836 → **160건(16.06%)** 누락. TIA 20시 셀 50.6%. 하루 가중으로는 99.3%+ 라 임계에 안 걸림 |
| **해결** | ① 커버리지 테스트 분리 - 마켓 목록 기준을 **거래소**로 두고 "거래소에 거래가 있는데 우리는 0건"을 별도 판정(유실은 비율로, 구독 누락은 부재로 나타나므로) ② 셀(마켓×시간) 단위 99% 임계 ③ 마켓 목록 5분 주기 갱신 + **같은 연결에서 재구독**(프로브로 교체·연결 유지 실측 후 적용, 재연결 없으므로 갱신 자체가 공백을 만들지 않음) |
| **복구** | BFC 261,713건(전량) + 재연결 구간 160건. 재대조 09-10~15 **가중 100.000%, 99% 미만 셀 0, 0건 셀 0** |
| **부작용·새 발견** | 백필이 Flink `AnomalyDetector` 의 마켓별 `lastPrice` keyed state 를 덮어, **한 번의 백필이 두 번 오탐**(백필된 옛 체결 vs 현재가 5건 + 직후 실시간 체결 vs 백필된 옛 가격 5건). 09-10 백필의 1,687건도 같은 기전이었음을 사후 특정. gap-fill 자동화 설계 전제로 기록 |
| **교훈** | 대조의 **분모를 외부 기준으로** 잡는다. 우리가 만든 목록으로 우리를 검증하면 못 본 것은 영원히 못 본다. 유실 비율과 커버리지는 다른 테스트다. 임계는 집계 단위에 따라 통과·실패가 갈리므로 셀 단위로 본다. 배치 백필이 스트림 상태를 공유하면 오염된다. 상세 `docs/17-reconcile-dag-dbt-contracts.md`, `docs/18-findings-and-lessons.md` |

---

## 📊 성능 지표

| 지표 | 목표 | 실측 |
|------|------|------|
| CDC Latency (binlog → Debezium) | < 10ms | **p50: 3ms, p95: 5ms, p99: 7ms** ✅ (단, 이 구간만 재면 producer 앞단 지연을 못 봄 - 이슈 5) |
| 적재 지연 (거래소 체결시각 → MySQL) | p95 < 5s | **287마켓 실측 p50 1.1~1.2s / p95 2.1s / max 2.8s** (2026-09-09 dry-run 1h). 개선 전 최대 36.9시간 |
| 호가 e2e (거래소 → ClickHouse) | p95 < 3s | **p50 849ms / p95 1.9s** (JDBC 배치 2초 창이 대부분) |
| Throughput (체결) | > 100 TPS | **287마켓 25~30 rows/s, 피크 53.6 msg/s (Upbit 제공량이 상한)**; producer 처리 상한 2,500 rows/s |
| Throughput (호가) | - | **154~262 msg/s, 273 KB/s** |
| 데이터 정합성 | 중복 0% | **실측 중복 1.80% - 지배 원인은 46시간 장애 복구 재소비, 상시 유입은 0.0005% (07-dedup-audit.md). (source_ts, trade_id) 일일 감시 게이트 운영. 2026-09-09 재제출 3회 모두 유실 0·중복 0(savepoint)** |
| Flink 체크포인트 | - | **17.8KB / e2e avg 51ms** (RocksDB 시절 625MB / 1,388ms) |
| 체결 e2e (거래소 → ClickHouse), 2026-09-19 | - | **거래소→수신 82ms, 수신→MySQL 1.1s(배치), MySQL→Flink 1.7s(3초 배치)** - 시각 6개가 한 행에 (docs/28 A-8) |
| 원장 e2e (거래소 이벤트 → Kafka), 2026-09-19 | - | **p50 219ms** (수신 21 → binlog +6 → Debezium +3 → Kafka +192). 3자 대조 불일치 0 (docs/28 B-7) |
| 체결 시각 파티션 효과 | - | 1시간 조회 **39/39 파트 1.0s → 5/56 파트 0.10s** (docs/28 A-7) |
| 장애 복구 시간 | < 5분 | **Flink restart 30초 이내**, 재시작 전략 20회×30초 |
| 메모리 사용 | < 14GB | **used 약 9GB + swap 4GB (30개 컨테이너, 2026-09-09)** |
| 24시간 운영 | ✅ | **2026-02-13 가동 시작, 200일+** ✅ |
| 외부 접근 | ✅ | **인증 뒤 운영 · 요청 시 라이브 데모** ✅ |
| 총 적재 | - | **체결 누적 91.97M행(2026-09-09), 고유 이벤트 기준 89.42M+ (중복 1.80%는 장애 복구 재소비가 지배 원인)** |
| 호가 저장 효율 | - | **on-disk 39.7 B/스냅샷 (압축 전 562B, JSON 1,050B)** → 원본 7일 약 6GB |
| 이상 탐지 | 의미 있는 알림 | **~13건/시간 (v1 대비 98% 감소, 31일 실측)** ✅ |
| 실시간 알림 | 매분 | **n8n → Slack + Gmail** ✅ |
| 일일 리포트 | 매일 01:00 KST | **Airflow → 품질검증 + CDC 지연 + 이상탐지 요약 → Slack** ✅ |
| dbt 품질검증 | 코인별 자동 | **Dynamic Task Mapping 5개 코인 병렬, 100% 통과** ✅ |

---

## 📁 프로젝트 구조

```
cdc-realtime-pipeline/
├── README.md
├── docker-compose.yml
├── .env / .env.example
│
├── producer/                    # Upbit WebSocket Producer (체결 → MySQL, CDC 경로)
│   ├── producer.py              # 287마켓 체결 수집, 버퍼 소진 flush, best bid/ask
│   ├── Dockerfile
│   └── requirements.txt
│
├── orderbook-collector/         # Upbit WebSocket 호가 → Kafka 직접 발행 (2026-09)
│   ├── collector.py             # orderbook.15 287마켓, zstd, idempotent, STATS(lag p50/p95, queue)
│   ├── Dockerfile
│   └── requirements.txt
│
├── mysql/
│   ├── init.sql                 # crypto_trades 스키마
│   └── my.cnf                   # binlog + event_scheduler 설정
│
├── debezium/
│   └── connector-config.json    # MySQL CDC Connector 설정
│
├── kafka/
│   └── config/                  # Broker 설정
│
├── flink/
│   ├── pom.xml
│   ├── Dockerfile               # 멀티스테이지 빌드
│   └── src/main/java/com/cdc/pipeline/
│       ├── CdcPipelineJob.java          # 메인 Job (Source → Sink)
│       ├── model/
│       │   └── CryptoTradeEvent.java    # 체결 이벤트 POJO
│       ├── function/
│       │   ├── CdcEventParser.java      # Debezium JSON 파싱 (null-safe)
│       │   ├── AnomalyDetector.java     # FDS 이상 탐지 3가지 룰 (RAPID_TRADES 비활성화)
│       │   ├── CdcEventParser.java      # Debezium JSON 파싱, 실패 → DLQ 사이드 아웃풋 + 카운터 (2026-09-19)
│       │   └── NullSafeStringSchema.java # Tombstone 방어 Deserializer
│       ├── sink/
│       │   └── ClickHouseSinks.java     # 원본/집계/이상탐지 JDBC Sink (best bid/ask 포함)
│       └── orderbook/                   # 호가 잡 (2026-09, 같은 JAR의 별도 메인)
│           ├── OrderbookJob.java        # Kafka → raw sink + 1분 이벤트타임 윈도우
│           ├── OrderbookParser.java / OrderbookEvent.java
│           ├── OrderbookAggregator.java / OrderbookMinute.java
│           └── OrderbookSinks.java      # Array(Float64) 바인딩
│
├── clickhouse/
│   ├── init.sql                 # 3개 테이블 (trades, aggregations, alerts)
│   ├── orderbook.sql            # orderbook_raw (TTL 7d) / orderbook_1m (TTL 365d)
│   └── config.d/system-logs-ttl.xml  # system.* 로그 14일 TTL
│
├── grafana/
│   └── provisioning/
│       ├── datasources/                # ClickHouse + Prometheus
│       └── dashboards/json/
│           ├── cdc-pipeline.json       # CDC 실시간 대시보드 (12 패널)
│           └── airflow-operations.json # Airflow 운영 대시보드 (12 패널)
│
├── airflow/
│   ├── Dockerfile                      # Custom image (dbt + statsd)
│   ├── webserver_config.py             # 비로그인 Viewer 설정
│   ├── dbt_profiles/profiles.yml       # Docker 네트워크용 dbt 프로필
│   ├── dags/
│   │   ├── health_check.py             # DAG 1: 10분 헬스체크
│   │   └── daily_pipeline.py           # DAG 2: 일일 배치 + Slack 리포트
│   ├── plugins/
│   │   ├── hooks/clickhouse_hook.py    # ClickHouse HTTP API Hook
│   │   ├── operators/
│   │   │   ├── clickhouse_operator.py  # ClickHouse 쿼리 Operator
│   │   │   └── flink_health_operator.py # Flink REST API Operator
│   │   └── callbacks/slack_callbacks.py # Slack 알림 (실패/SLA/일일리포트)
│   └── tests/test_dags.py              # DAG 구조 테스트 (9개)
│
├── dbt_cdc_pipeline/
│   ├── models/
│   │   ├── staging/stg_trades.sql      # VIEW: 원본 정제
│   │   ├── intermediate/
│   │   │   ├── int_ohlcv_1h.sql        # TABLE: 시간별 OHLCV
│   │   │   └── int_ohlcv_daily.sql     # TABLE: 일별 OHLCV
│   │   └── marts/
│   │       ├── mart_daily_summary.sql  # TABLE: 일일 요약 (리포트용)
│   │       ├── mart_volume_spike.sql   # TABLE: 거래량 급등
│   │       └── mart_alert_rate.sql     # TABLE: 이상탐지 비율
│   └── tests/                          # dbt 데이터 테스트
│
├── monitoring/
│   ├── prometheus.yml                  # Prometheus 스크래핑 설정
│   └── statsd_mapping.yml             # Airflow StatsD → Prometheus 매핑
│
├── schemas/                            # 토픽 계약 (JSON Schema 5종 + 왜 필요한지)
│   └── README.md                       # Debezium schemas.enable=false → 메시지에 스키마가 없다. 계약을 밖에 적는다
│
├── scripts/
│   ├── startup.sh                      # 전체 파이프라인 기동
│   ├── build-flink-job.sh              # Flink Job 빌드 + 배포
│   ├── sync-annotations.sh            # Grafana annotation 자동 동기화
│   ├── ops/validate-topic-schemas.py   # 살아 있는 토픽 표본 ↔ schemas/ 대조, 위반이면 exit 1 (--self-test 로 검증기 자체 확인)
│   ├── labels/poll_market_state.py     # 마켓 거래 상태 10분 폴링 (상장폐지·정지를 유실과 구분) - 이 정보는 웹소켓에만 있다
│   ├── labels/poll_exchange_notices.py # 거래소 공지 매시 수집 (수집 공백이 점검인지 유실인지 가르는 근거)
│   ├── lib/minws.py                    # 표준 라이브러리 웹소켓 클라이언트 (의존성을 늘리지 않으려고 직접)
│   ├── ops/reprocess-day.sh            # 하루 재처리 런북 (보존 경계 표 + 커버리지 측정)
│   ├── ops/restore-rehearsal.sh        # 복원 리허설 - 표 단위 복원(DB째 하면 Kafka 엔진 표가 프로덕션 컨슈머를 가로챈다) + 금액 합 대조
│   └── observe/collect_metrics.sh      # 7일 관찰용 5분 지표 스냅샷 (87컬럼, crontab)
│
└── docs/
    ├── 02-infrastructure.md
    ├── 03-cdc-pipeline.md
    ├── 04-flink-streaming.md
    ├── 05-clickhouse-grafana.md
    ├── 06-phase6-record.md             # 이상탐지 임계값 + 46시간 장애 복구
    ├── 07-dedup-audit.md               # 중복 적재 감사 (91.06M / 89.42M)
    ├── 08-ingest-lag-incident.md       # 적재 지연 36.9시간 사고 분석 (유실 아님)
    ├── 09-orderbook-phase1-execution.md
    ├── 10-phase2-flink-producer-upgrade.md
    ├── 11-orderbook-launch.md
    ├── 12-observation-plan.md          # 7일 관찰 가설·임계값
    ├── 13-sequential-id-collision-incident.md  # 유실 사고 #2: sid 마켓 간 충돌, 백필 210,469건
    ├── 14-observation-week1.md       # 7일 무변경 관찰 결과: 가설 12개 중 10 통과·1 조건부·1 위반, 튜닝 순위 8건
    ├── 15-load-experiment-design.md    # 재생 증폭 부하 실험 설계(3계층, Binance 코퍼스, 준확정)
    ├── 16-anomaly-rule-basis.md        # 이상탐지 기준 재설계 - 업비트 시장경보 재현, 6개월 라벨 대비 정밀도·재현율
    ├── 16-appendix-queries.md          # 위 결론을 재생산하는 쿼리 10개와 출력 (scripts/analysis/rule_basis_check.sh)
    ├── 17-reconcile-dag-dbt-contracts.md  # 원장 대조 DAG·dbt 계약 설계 결정 + 첫 실행이 잡은 유실 2건
    ├── 18-findings-and-lessons.md      # 2026-09-16 발견 총괄 - 무엇을 찾았고 어떻게 찾았나, 정정 기록 포함
    ├── 19-loss-prevention-gap-map.md   # 유실 방지 구조 실무 대비 현황표(14항목) + 작업 순서 - 이후 작업의 기준
    ├── 20-deployment-1.md              # 배포 1: 늦은 이벤트 가드·gap-fill·수리 계보·10분 커버리지·CI - 설계 변경 근거와 실측 검증
    ├── 21-backup-and-access-control.md # 백업(Oracle 오프사이트, 복원 리허설 79초)·접근 통제(사용자 분리, 컷오버 런북) - 판단 오류 2건 정정 포함
    ├── 22-rules-v2-and-quality-layer.md # 이상탐지 v2(PRICE_24H 등급 전이 섀도, VOLUME_24H dbt 규칙) + dq_* 품질 층 + 규칙 평가 모델 + 승격 기준
    ├── 29-kafka-usage-review.md      # Kafka 사용 방식 냉정 평가: 체결 키 trade_id → market(재정렬 원인), 처리 시간 창, 압축·보존, DLQ, 스키마 계약
    ├── 30-lessons-consolidated.md # 실수를 교훈으로 통합: 부재 단정 3회·측정 오염·검증 전 커밋·분할·문서vs실측·조용한 실패 + 체크리스트
    ├── 31-stack-dissection-and-binance-expansion.md # 스택 해체 분석(무엇이 일하나) + Binance 실시간 확장 1·2단계, 첫 시간 대조 100%, 스케줄러 OOM 사고, 운영 정리
    ├── 32-slo-and-alerting.md        # SLO 표 15줄, 알럿 3층(즉시/품질 판정/주간 다이제스트), 실무 뒷단 ↔ 개인 규모, alert_events·ops_metrics_5m
    ├── 33-three-lens-review.md       # 금융·DE·AE 세 관점 냉정 평가: 강점의 '왜', 약점의 원인·조치, 우선순위 10, 예상 질문 12
    ├── 34-hardening-program.md       # 보강 프로그램: docs/33 약점 10건을 왜·무엇·검증·상태로. #1~#10 실행 기록
    ├── 35-data-inventory.md          # 데이터 인벤토리: 표별 소유·보존·소비자·재생성, 삭제 예정일, 09-20 정리 내역
    ├── 36-metrics.md                 # 지표 사전: 모든 지표의 수식·단위·산출 위치·함정 (taker_side 방향, 김프 분모, e2e 지연의 여섯 타임스탬프)
    ├── 37-contract-cron-and-notice-labeling.md # 계약 검증 상시화(위반 + '검증기가 도나') + 거래소 공지로 수집 공백 라벨링(카테고리를 믿으면 틀린다)
    ├── 28-layer2-design.md           # 2층 설계: 체결 시각 파티션 재설계(설계 부채 해소)·가상 매매 원장(CDC 제자리)·케이스/내부 신호 (결정 대기)
    ├── 27-trade-orderbook-mart.md    # 체결×호가 분 결합 마트: 우리만의 데이터, volume_over_depth15, EURC 되튐 = 깊이 대비 체결량(500배)
    ├── 26-cdc-segment-review.md      # CDC 구간 재검토: 삭제 이벤트가 토픽의 46%·정리 DELETE 풀스캔·binlog 무기한 → skipped.operations/인덱스/보존, #1 철회, dim_markets (결정 대기)
    ├── 25-replacing-merge-tree.md    # 싱크 중복을 저장 층에서: RMT 전환 무정지, 재전송 중복 239→0, 메모리 한도 실패와 슬라이스 복사
    ├── 24-broker-reduction-design.md # 브로커 3→1 축소 설계·실행·실측: 선행 조건(정지 실험 2회), ZK 유지 재할당 vs KRaft 컷오버, 24h 판정, 재기동 14초·유실 0
    ├── 23-load-experiment-results.md # 부하 실험 결과: 임계 5k~10k/s(병목 = 동기 JDBC 싱크·머지), 배치 1,000 으로 정체 5→0, 여유 85배(현 설정)·170배, 브로커 정지 무손실, 선행 지표 알림
    ├── journey-index.md                # 블로그 원고용 에피소드 인덱스
    └── worklog.md                      # 결정 표(근거 포함) + 시간순 작업 기록
```

관련 리포: 녹화-재생 증폭 실험 도구와 관찰 데이터 분석은 별도 리포(`pipeline-load-lab`, 준비 중)에 둔다.

---

## 🎤 예상 질문

### Q1. 왜 On-Premise를 선택했나요?
> "AWS같은 클라우드 시스템이 아닌, On-Premise 클러스터 구축을 해보고 싶어서, 클라우드 관리형 서비스가 아닌 물리 서버에서 직접 구축하고 24시간 운영하며 실제 장애 대응까지 경험했습니다."

### Q2. 16GB 메모리에서 어떻게 최적화했나요?
> "30개 컨테이너에 전부 메모리 제한을 걸고 실측으로 배분합니다. 2026-09 기준 Kafka broker당 1.25GB, Flink TaskManager 2GB(메모리 모델로 task heap 692MiB 확보), ClickHouse 1.75GB, 실사용 합은 약 7.5GB입니다. MySQL은 7일 보존 + 10분 단위 분산 삭제, ClickHouse는 체결 365일·호가 원본 7일 TTL, system 로그 14일 TTL로 디스크를 자동 관리합니다. 한 번은 스왑 상태의 RSS 기준으로 제한을 잡았다가 n8n이 24분간 크래시 루프에 빠진 적이 있어, 지금은 재기동 후 실사용을 다시 재서 정합니다."

### Q3. CDC 파이프라인에서 가장 어려웠던 장애는?
> "두 가지입니다. 하나는 MySQL cleanup DELETE 5만 건이 Debezium delete+tombstone 10만 메시지를 만들고 파서의 NPE로 Flink가 46시간 멈춘 사고입니다. NullSafeStringSchema, DELETE 스킵, 삭제 분산으로 해결했습니다. 다른 하나는 더 교묘했는데, 8월에 일별 적재 건수가 반토막 나서 유실을 의심했지만 업비트 일봉과 체결시각 기준으로 대조하니 유실은 0이었고, producer가 2초당 20행만 쓰는 구조라 최대 36.9시간 적재 지연이 났던 겁니다. 기존 지표가 전부 적재 이후 구간만 재고 있어서 못 본 거였고, 거래소 체결시각 기준 지연 지표와 알림을 추가했습니다."

### Q4. 이상 탐지 임계값은 어떻게 설정했나요?
> "3단계 반복 조정을 거쳤습니다. 먼저 업비트 감시정책과 학술 논문(EWMA 기반)으로 초기 설계하고, 실시간 데이터로 검증하며 조정했습니다. v1(651건/시간) → v2(72건) → v3(~13건, 31일 실측)으로, 최종적으로 ClickHouse에 적재된 24시간 알림 분포의 percentile 분석으로 p90 기준 임계값을 확정했습니다. RAPID_TRADES는 데이터 분석 결과 Upbit API 전송한계(100건/10초)에 의한 오탐임을 확인하고 비활성화했습니다."

### Q5. 왜 Debezium CDC를 선택했나요? 체결은 CDC가 꼭 필요한가요?
> "솔직히 체결 피드 자체는 실무라면 호가처럼 Kafka에 직접 넣는 게 정답입니다. CDC는 이미 업무용으로 존재하는 DB의 변경을 서비스 코드를 건드리지 않고 뽑을 때 쓰는 기술이고, 이 프로젝트에서는 그 운영 경험을 끝까지 겪어 보려고 체결을 MySQL 경유로 두었습니다. 그 대가로 스키마 변경 추적(온라인 ADD COLUMN을 Debezium이 추적), 대량 삭제 폭주로 인한 46시간 장애, 봉투 오버헤드 +34%, 삭제 부산물이 토픽 트래픽의 84%라는 비용을 전부 실측했고, 그래서 신규 데이터인 호가는 직접 발행으로 갔습니다. 관찰이 끝나면 CDC가 진짜 필요한 자리(가상 매매 원장, 이상탐지 케이스 관리 같은 상태 테이블)로 옮길 계획입니다."

### Q6. Kafka를 3-broker로 구성했다가 1-broker로 줄인 이유는?
> "단일 호스트라 진짜 고가용성은 아닙니다. 디스크가 하나라 내구성 이득도 없습니다. 그래도 복제·ISR·min.insync.replicas·리더 선출·컨슈머 페일오버가 실제로 동작하는 환경이 필요했고, 재생 증폭 실험에서 브로커 1대를 죽였을 때의 동작을 실측하려고 유지합니다. 비용은 브로커 3개 RAM 약 1.9GB와 RF3 디스크 쓰기 3배로 재 두었고, 실험이 끝나면 1브로커 KRaft combined 모드로 축소할 계획입니다."
> (2026-09-18 추가) 그 확인이 끝난 뒤 24시간 실측에서 3대는 같은 호스트·같은 NVMe 라 내구성이 형식뿐이었고 CPU·디스크만 썼다. 브로커 1대·RF1 로 무정지 재할당해 줄였고, 재기동 정지 14초·유실 0 을 실측했다(docs/24). 우리가 쓰는 Kafka 의 의의는 분산이 아니라 로그(오프셋 재개·생산자/소비자 분리·다중 소비자·키 순서)다(docs/29).

### Q7. n8n 알림을 왜 추가했나요?
> "탐지만 하고 끝나면 운영 의미가 없습니다. FDS 이상거래는 즉시 Slack + Gmail로 상세 내역을 발송하고, 파이프라인 장애는 별도 채널로 복구 가이드와 함께 알림합니다. 이전 FDS Pipeline Lab 프로젝트에서도 같은 패턴으로 SLA 모니터링을 구축한 경험이 있습니다."

### Q8. RAPID_TRADES를 왜 비활성화했나요?
> "데이터 분석 결과입니다. 24시간 동안 73건이 탐지됐는데 전부 정확히 100건이었고, 101건 이상은 단 한 건도 없었습니다. ClickHouse에서 10초 윈도우 분석을 해보니 BTC도 최대 100건이 천장이었고, 이는 Upbit WebSocket API의 전송 한계였습니다. 이상거래가 아니라 API 제약이므로 비활성화했고, 코드 구조는 유지하여 추후 거래소 내부 데이터 연동 시 재활성화할 수 있도록 했습니다."

### Q9. Airflow를 왜 도입했고, cron 대비 장점은?
> "dbt를 cron으로 돌리면 실패 여부를 알 수 없고, 코인별 품질검증이나 후속 작업 연동이 불가능합니다. Airflow 도입으로 dbt run → dbt test → 코인별 품질검증(Dynamic Task Mapping) → quality gate → Slack 리포트까지 하나의 DAG으로 오케스트레이션하고, 실패 시 자동 재시도 + Slack 알림까지 처리됩니다. Custom Operator(ClickHouseOperator, FlinkHealthOperator)를 직접 개발하여 추가 드라이버 없이 HTTP API로 ClickHouse에 접근합니다."

### Q10. dbt 3계층 모델 구조는 어떤 기준으로 설계했나요?
> "staging(stg_trades)은 원본 정제 VIEW, intermediate(int_ohlcv_1h, int_ohlcv_daily)는 시간/일별 OHLCV 집계 TABLE, marts(mart_daily_summary, mart_volume_spike, mart_alert_rate)는 리포트/대시보드용 최종 테이블입니다. Flink가 실시간 적재한 raw 데이터를 dbt가 배치로 가공하여, 실시간 스트리밍과 배치 분석을 분리합니다."

### Q12. Flink 상태 백엔드를 RocksDB에서 hashmap으로 바꾼 이유는?
> "체크포인트가 625MB였는데 TaskManager 로컬 db 디렉터리를 열어 보니 SST 파일 합계는 15KB이고 352MB짜리 MANIFEST 파일이 크기의 대부분이었습니다. 5마켓×5개 ValueState라 실제 상태는 수십 KB인데 네이티브 풀 체크포인트가 RocksDB의 버전 기록 파일을 매번 통째로 복사한 겁니다. canonical savepoint를 떠 보니 20KB로 확인됐고, hashmap으로 바꾸자 체크포인트 17.8KB, e2e 1.4초→51ms가 됐습니다. 상태가 메모리에 들어가는 규모면 RocksDB의 오버헤드를 낼 이유가 없습니다."

### Q13. 호가는 왜 체결과 다른 경로로 수집하나요?
> "실측 결과 호가는 체결의 14배 건수, 48배 바이트였습니다. MySQL과 binlog를 거치면 하루 13~23GB를 DB에 쓰고 Debezium 봉투로 34%가 더 붙습니다. 호가는 초당 수백 번 갱신되는 스냅샷이라 원장에 남길 이유도 없습니다. 그래서 수집기가 Kafka에 직접 발행하고, 장애 격리 차원에서도 호가 폭주가 체결 적재를 밀어내지 않게 프로세스를 분리했습니다."

### Q14. 왜 7일 동안 아무것도 바꾸지 않고 관찰하나요?
> "10분 확인은 '깨지지 않았다'만 알려 줍니다. 8월 지연 사고도 하루 단위 데이터를 봐야 보였습니다. 피크 시간대(KST 09시, 22~24시)와 주말 저거래 구간을 한 사이클 겪어야 언제 밀리는지 알 수 있어서, 가설 12개와 임계값을 먼저 적어 두고 5분마다 87개 지표를 쌓습니다. 관찰 결과로 튜닝 순서를 정하고, 그 뒤에 같은 실데이터를 배속 재생하는 부하 실험으로 개선 전후를 비교합니다."

### Q15. Kafka 를 "분산 처리"로 쓰고 있나요?
> "아니요. 단일 호스트라 복제·병렬·확장은 24시간 실측 뒤 껐습니다(브로커 3→1). 쓰는 건 로그 성질입니다 - 오프셋 재개(세이브포인트 재배포 때 trade_id +1 연속), 생산자·소비자 분리(Flink 30분 정지에도 원장 무손실), 한 토픽 다중 소비자(CDC·서킷·부하 실험·ClickHouse 엔진), 키 순서(market 키로 재정렬 5.87% 원인 제거). Kafka 없이 Flink CDC 직결로도 됐고 처음 고른 이유는 역량 증명이었습니다. 규모가 필요해지는 지점과 그때 손댈 순서(파티션→브로커)는 부하 실험으로 말합니다."

### Q16. 체결은 INSERT 뿐인데 그게 CDC 인가요?
> "체결만 보면 큐와 같습니다. 그래서 2층에 상태가 바뀌는 원장(주문)을 두고, 상태를 바꾸는 주체를 우리가 아니라 거래소 매칭 엔진(Binance Testnet, 실돈 없음)으로 뒀습니다. UPDATE·DELETE 가 키 순서로 흘러 ClickHouse 에서 최종 상태가 재구성되고, 거래소·MySQL·ClickHouse 3자 대조로 매일 증명합니다. 체결 커넥터는 삭제를 버리고(보관소), 원장 커넥터는 살립니다(거울) - 같은 도구의 두 모드를 상황에 맞게 골랐습니다."

### Q17. 파티션 키를 왜 바꿨나요?
> "처음 표는 '언제 들어왔나'(binlog 시각)로 나뉘어 있었는데 조회는 전부 '언제 일어났나'(체결 시각)로 걸어서 프루닝이 안 됐습니다. 1시간 조회가 7개월을 훑었습니다(39/39 파트, 1.0초). 체결 시각 파티션·이벤트 키로 무정지 재생성해 5/56 파트 0.10초가 됐고, 키를 업무 정체성(마켓+sequential_id)으로 바꾸자 옛 키가 못 거르던 재스냅샷 중복 4행이 걸러졌습니다. MySQL 도 같은 기준이라 재처리·삭제 단위가 양쪽에서 같습니다."

### Q18. "없다"고 했다가 틀린 적이 있나요?
> "세 번 있습니다. 유실을 소스 탓으로, 시장경보 이력이 없다고, Binance 가 같은 범주라고. 셋 다 실제로 구독하거나 번들을 뒤지니 나왔습니다. 그 뒤 규칙이 '외부 API 의 없다·같다는 공식 문서와 실제 연결 두 갈래로 확인한 뒤에만'이고, 그 확인으로 amend keepPriority·Demo Mode·announcement 스트림을 찾았습니다."

### Q11. 알림 체계가 n8n과 Airflow 두 개인 이유는?
> "역할이 다릅니다. n8n은 매분 ClickHouse를 폴링하여 FDS 이상거래와 CDC 장애를 **즉시** Slack + Gmail로 알립니다. Airflow는 매일 01:00 KST에 전날 데이터를 **일일 리포트**로 종합합니다 - CDC 지연 percentile, 코인별 품질검증, 이상탐지 요약, 거래량 급등 등. health_check DAG은 10분 간격으로 파이프라인 컴포넌트 상태를 점검하되, 이상 시에만 알림을 보내 alert fatigue를 방지합니다."

---

## 🔗 관련 프로젝트

- [FDS Pipeline Lab](https://github.com/Aguantar/fds-pipeline-lab) - 이상거래 탐지 파이프라인 (Redis+Consumer로 TPS 70→17,500, 250배 최적화)

---

## 🖥️ 서버 환경

| 항목 | 스펙 |
|------|------|
| 하드웨어 | Mini PC (On-Premise) |
| CPU | Intel N100 (4코어) |
| RAM | 16GB |
| Disk | 500GB SSD |
| OS | Ubuntu 24.04 |
| 운영 | 24시간 상시 (2026-02-13 시작, 200일+ 가동 중) |
