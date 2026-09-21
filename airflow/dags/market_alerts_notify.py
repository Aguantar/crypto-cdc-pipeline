"""DAG: market_alerts_notify - 승격된 이상탐지 전이를 사람에게 보낸다 (docs/22 §4 승격, 2026-09-20). 10분마다.

왜 이 DAG 이 생겼나:
  PRICE_24H v2.1.1 이 승격 기준(전이 10건 이상에서 1분봉 재계산과 미매칭 0)을 09-19 에 116건으로 충족했다.
  승격이 바꾸는 것은 판정이 아니라 **이 전이를 사람에게 보내는가** 뿐이다. 판정 코드는 한 글자도 안 바뀌었다.
  원래 계획은 "n8n 발송에 우리 전이 추가" 였는데, 09-20 에 n8n 워크플로 3개가 전부 비활성인 것이 드러나
  알림을 Airflow 로 일원화했다(docs/35 §4). 그래서 발송도 여기서 한다.

무엇을 보내나 (그리고 무엇을 안 보내나):
  - 승급(level > prev_level)만 보낸다. 강등·해제는 하루 88건 중 61건이라 보내면 그것만으로 채널이 찬다.
    해제는 일일 리포트에서 본다. 알림은 '지금 볼 것'이고 리포트는 '되짚을 것'이다.
  - 섀도는 안 보낸다: rule_version 에 '-shadow' 가 붙은 것(아직 VOLUME_24H 가 여기 있다, 판정 09-23).
    승격 여부를 코드가 아니라 **데이터에 적힌 버전 문자열**로 판단한다 - 승격은 배포이지 이 DAG 의 수정이 아니다.

시끄러움 제어 (실측 기반):
  7일 평균 승급은 등급1 13.7건/일, 등급2 2.4, 등급3 0.1. 그런데 어제 하루는 등급1 이 53건이었다 -
  마켓 6개가 오르내린 것(플래핑, docs/22 §4-2). 그래서 dedup 을 (마켓, 종류, 등급, 시각의 시) 로 잡는다:
  같은 마켓이 같은 등급으로 한 시간에 몇 번을 오르내려도 한 번만 간다. 전이 자체는 market_alerts 에 다 남는다.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks.slack_callbacks import send_health_alert, task_failure_callback

default_args = {"owner": "calme", "retries": 1, "retry_delay": timedelta(minutes=2),
    # 2026-09-20 (docs/39 §2 ⑥): 외부 API·컨테이너가 흔들릴 때 고정 간격 재시도는 같은 실패를 반복한다
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10), "on_failure_callback": task_failure_callback}

LEVEL_NAME = {1: "주의", 2: "경고", 3: "위험"}

# 최근 30분을 보는 이유: 10분 주기인데 창을 10분으로 잡으면 한 번 실패하면 그 구간이 영영 안 간다.
# 겹쳐 읽고 dedup 으로 거르는 것이 빠뜨리는 것보다 낫다 (재처리 런북과 같은 원칙, docs/34 #6).
SQL = """
SELECT alert_type, market, level, prev_level, toString(event_time) AS event_time_s,
       toStartOfHour(event_time) AS hour_bucket, round(value, 1) AS value, threshold,
       round(price, 4) AS price, round(ref_price, 4) AS ref_price, rule_version
FROM cdc_pipeline.market_alerts
WHERE event_time >= now() - INTERVAL 30 MINUTE
  AND level > prev_level
  AND rule_version NOT LIKE '%-shadow'
ORDER BY level DESC, event_time
"""


def _notify(**context) -> dict:
    from hooks.clickhouse_hook import ClickHouseHook

    log = context["ti"].log
    hook = ClickHouseHook()
    rows = hook.get_records(SQL)
    if not rows:
        log.info("승급 전이 없음")
        return {"candidates": 0, "sent": 0}

    fired = []
    # 2026-09-20 첫 실행에서 잡은 결함: alert_events 조회만으로는 같은 실행 안의 중복을 못 막는다.
    # 적재는 맨 끝에 한 번 하므로, 배치 안에서 같은 키가 두 번 나오면 둘 다 통과했다(KRW-ZIL 이 07:32·07:34 에
    # 0→1 로 두 번 올라 같은 키로 두 건 발송). 플래핑을 막으려고 만든 dedup 이 플래핑에 뚫린 것이다.
    seen: set[str] = set()
    for r in rows:
        key = f"market_alert:{r['market']}:{r['alert_type']}:L{r['level']}:{r['hour_bucket']}"
        if key in seen:
            continue
        already = hook.get_scalar(f"SELECT count() FROM cdc_pipeline.alert_events WHERE dedup_key = '{key}'")
        if already and int(already) > 0:
            continue
        seen.add(key)
        lvl = int(r["level"])
        msg = (f"{r['market']} {LEVEL_NAME.get(lvl, lvl)}(등급 {r['prev_level']}→{lvl}) "
               f"24h {r['value']}% ≥ {r['threshold']}% | 종가 {r['price']} ← 기준 {r['ref_price']} | {r['event_time_s']} UTC")
        fired.append({"name": f"[{r['alert_type']}] {r['market']}", "message": msg, "dedup_key": key,
                      "severity": f"level{lvl}", "rule_version": r["rule_version"]})

    if fired:
        send_health_alert([{"name": f["name"], "message": f["message"]} for f in fired])
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        hook.execute("INSERT INTO cdc_pipeline.alert_events FORMAT JSONEachRow\n" + "\n".join(json.dumps(
            {"fired_at": now, "source": "market_alerts_notify", "name": f["name"], "severity": f["severity"],
             "message": f["message"][:500], "dedup_key": f["dedup_key"]}, ensure_ascii=False) for f in fired))
    log.info("candidates %d sent %d", len(rows), len(fired))
    return {"candidates": len(rows), "sent": len(fired), "markets": [f["name"] for f in fired][:10]}


with DAG(
    dag_id="market_alerts_notify",
    default_args=default_args,
    description="승격된 이상탐지 승급 전이를 Slack 으로 (10분, 마켓·등급·시 단위 dedup) - docs/22 §4",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 9, 20),
    catchup=False,
    tags=["alert", "rules"],
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="notify_market_alerts",
        python_callable=_notify,
        # 10분 주기 발송이 5분을 넘으면 알림이 밀린다(docs/39 §2 ⑤)
        sla=timedelta(minutes=5),
    )
