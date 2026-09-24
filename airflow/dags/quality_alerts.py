"""DAG: quality_alerts - 데이터 품질 SLO 판정 (docs/32 §1·§2 ②, 2026-09-20). 매시 :50.

dq 표는 각자 다른 시각에 갱신된다(Upbit 대조 06:35, 일일 16:00, 규칙 01:15, Binance 00:40 UTC). 그래서 매시 최신 '온전한 날' 행을 SLO 와 비교하고,
같은 (규칙, 날) 은 alert_events 의 dedup_key 로 하루 1회만 Slack 에 보낸다. 리포트가 아니라 판정: 위반이면 이름·값·목표·문서 링크.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks.slack_callbacks import send_health_alert, task_failure_callback

default_args = {"owner": "calme", "retries": 1, "retry_delay": timedelta(minutes=5), "on_failure_callback": task_failure_callback}

# (이름, SQL, 위반 조건 함수, 메시지 함수, 문서). SQL 은 최신 온전한 날(오늘 UTC 제외) 한 행을 돌려준다.
# 별칭을 day_s 로 두는 이유: ClickHouse 는 SELECT 별칭이 WHERE 의 열 이름을 가려서 `toString(day) AS day ... WHERE day_utc < today()` 가 String vs Date 비교로 500 이 났다(첫 실행).
RULES = [
    ("Upbit Reconcile", "SELECT toString(day_utc) AS day_s, weighted_pct, min_cell_pct, cells_no_rows, worst_cell FROM cdc_pipeline.dq_reconcile_daily WHERE day_utc < today() ORDER BY day_utc DESC LIMIT 1",
     lambda r: float(r["weighted_pct"]) < 99.9 or float(r["min_cell_pct"]) < 99, lambda r: f"{r['day_s']} weighted {r['weighted_pct']}% (≥99.9) min cell {r['min_cell_pct']}% (≥99) worst {r['worst_cell']}", "docs/17"),
    ("Binance Reconcile", "SELECT toString(day_utc) AS day_s, weighted_pct, min_cell_pct, cells_no_rows, cells_above_101, worst_cell FROM cdc_pipeline.dq_binance_reconcile_daily WHERE day_utc < today() ORDER BY day_utc DESC LIMIT 1",
     lambda r: float(r["weighted_pct"]) < 99.9 or int(r["cells_no_rows"]) > 0 or int(r["cells_above_101"]) > 0, lambda r: f"{r['day_s']} weighted {r['weighted_pct']}% no-row cells {r['cells_no_rows']} >101% cells {r['cells_above_101']} worst {r['worst_cell']}", "docs/31"),
    ("Orderbook Gaps", "SELECT toString(day_utc) AS day_s, gap_windows, gap_seconds_total, gap_longest_s, est_lost_snapshots FROM cdc_pipeline.dq_orderbook_gaps_daily WHERE day_utc < today() ORDER BY day_utc DESC LIMIT 1",
     lambda r: int(r["gap_windows"]) > 0, lambda r: f"{r['day_s']} gap windows {r['gap_windows']} total {r['gap_seconds_total']}s longest {r['gap_longest_s']}s est lost {r['est_lost_snapshots']}", "docs/23 §7"),
    ("Rule Parity", "SELECT toString(day_utc) AS day_s, sql_transitions, flink_transitions, matched, parity_ok FROM cdc_pipeline.dq_alert_parity_daily WHERE day_utc < today() ORDER BY day_utc DESC LIMIT 1",
     lambda r: int(r["parity_ok"]) == 0 and int(r["sql_transitions"]) >= 10, lambda r: f"{r['day_s']} sql {r['sql_transitions']} flink {r['flink_transitions']} matched {r['matched']}", "docs/22 §4"),
    ("Ledger 3-way", "SELECT toString(day_utc) AS day_s, sum(ex_my_mismatch) AS ex_my, sum(my_ch_mismatch) AS my_ch, count() AS symbols FROM cdc_pipeline.dq_ledger_daily WHERE day_utc < today() GROUP BY day_utc ORDER BY day_utc DESC LIMIT 1",
     lambda r: int(r["ex_my"]) > 0 or int(r["my_ch"]) > 0, lambda r: f"{r['day_s']} exchange≠mysql {r['ex_my']} mysql≠clickhouse {r['my_ch']} of {r['symbols']} symbols", "docs/28 B-5"),
    # 토픽 계약 (2026-09-20, docs/34 #8 의 "cron 에 없다" 한계 해소).
    # 검증기는 호스트 cron 이 매시 돌려 결과를 표에 적고, 판정·발송은 여기서 한다 - 알림은 한 경로로만 나간다.
    # 두 가지를 본다: ① 마지막 실행에 위반이 있나 ② 검증기가 아직 도나(3시간 넘게 기록이 없으면 멈춘 것).
    # ②가 필요한 이유: 검증기가 죽으면 위반이 0 으로 보인다. "위반 없음"과 "검사를 안 함"은 다르다.
    # 별칭 주의: sum(violations) AS violations 로 두면 argMax(detail, violations) 가 "집계 안의 집계"가 되어 500.
    # ClickHouse 의 SELECT 별칭이 같은 이름의 원본 열을 가린다 - docs/34 #3 에서 WHERE 로 겪은 것과 같은 함정.
    ("Topic Schema", """SELECT toString(toDate(max(ran_at))) AS day_s,
                               toString(max(ran_at)) AS last_run,
                               dateDiff('minute', max(ran_at), now()) AS stale_min,
                               sum(violations) AS violation_count,
                               countIf(status = 'violation') AS bad_topics,
                               argMax(detail, violations) AS worst
                        FROM cdc_pipeline.schema_validation_runs
                        WHERE ran_at >= (SELECT max(ran_at) FROM cdc_pipeline.schema_validation_runs)""",
     lambda r: int(r["bad_topics"]) > 0 or int(r["stale_min"]) > 180,
     lambda r: (f"토픽 계약 위반 {r['bad_topics']}개 토픽 / {r['violation_count']}건 - {str(r['worst'])[:200]}"
                if int(r["bad_topics"]) > 0
                else f"계약 검증기가 {r['stale_min']}분째 기록 없음(마지막 {r['last_run']}) - cron 확인. 위반 없음과 검사 안 함은 다르다"),
     "docs/34 #8 · schemas/"),
    # 호스트 cron 신선도 (2026-09-20, docs/39 §2 ③).
    # Airflow 밖에 있는 cron 이 9개다. Airflow 밖에 둔 이유는 타당하지만(Kafka CLI·docker exec 가 필요한데
    # DAG 은 "네트워크 API 기반, 소켓 불필요" 원칙을 지킨다), 죽어도 모르는 것이 문제였다.
    # cron 은 자기 산출물을 표에 남기므로, 그 표의 마지막 기록이 오래됐으면 cron 이 멈춘 것이다.
    # 허용 지연은 주기의 3배 안팎 - 한 번 걸러도 울리지 않게.
    ("Cron Freshness", """SELECT toString(today()) AS day_s,
                                 arrayStringConcat(arrayFilter(x -> x != '', [
                                   if(ops_min    > 30,  concat('ops_metrics_5m ',   toString(ops_min),   '분'), ''),
                                   if(state_min  > 30,   concat('market_state ',     toString(state_min), '분'), ''),
                                   if(notice_min > 180, concat('exchange_notices ', toString(notice_min),'분'), ''),
                                   if(flag_min   > 30,  concat('market_events ',    toString(flag_min),  '분'), '')
                                 ]), ', ') AS stale_list,
                                 ops_min, state_min, notice_min, flag_min
                          FROM (
                            SELECT
                              (SELECT dateDiff('minute', max(ts), now())         FROM cdc_pipeline.ops_metrics_5m)            AS ops_min,
                              (SELECT dateDiff('minute', max(ts), now()) FROM cdc_pipeline.cron_heartbeats WHERE job = 'market_state') AS state_min,  -- 2026-09-24 (docs/46): 전이 표가 아니라 생존 신호로
                              (SELECT dateDiff('minute', max(updated_at), now())  FROM cdc_pipeline.exchange_notices)          AS notice_min,
                              (SELECT dateDiff('minute', max(observed_at), now()) FROM cdc_pipeline.upbit_market_events)       AS flag_min
                          )""",
     lambda r: str(r.get("stale_list") or "") != "",
     lambda r: (f"호스트 cron 정지 의심: {r['stale_list']} - 해당 스크립트 로그 확인"
                f" (ops {r['ops_min']}분 · 상태 {r['state_min']}분 · 공지 {r['notice_min']}분 · 플래그 {r['flag_min']}분)"),
     "docs/39 §2 ③"),
]


def _judge(**context) -> dict:
    from hooks.clickhouse_hook import ClickHouseHook
    hook = ClickHouseHook(); log = context["ti"].log
    fired, checked, skipped = [], 0, []
    for name, sql, violated, msg, doc in RULES:
        try:
            r = hook.get_first(sql)
        except Exception as e:  # noqa: BLE001
            skipped.append(f"{name}: {e}"); continue
        if not r:
            skipped.append(f"{name}: no row"); continue
        checked += 1
        if violated(r):
            key = f"quality:{name}:{r['day_s']}"
            already = hook.get_scalar(f"SELECT count() FROM cdc_pipeline.alert_events WHERE dedup_key = '{key}'")
            if already and int(already) > 0:
                log.info("%s violated on %s but already alerted", name, r["day_s"]); continue
            fired.append({"name": name, "message": f"{msg(r)} - {doc}", "dedup_key": key})
    if fired:
        send_health_alert([{"name": f"[품질 SLO] {f['name']}", "message": f["message"]} for f in fired])
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        hook.execute("INSERT INTO cdc_pipeline.alert_events FORMAT JSONEachRow\n" + "\n".join(json.dumps(
            {"fired_at": now, "source": "quality_alerts", "name": f["name"], "severity": "daily", "message": f["message"][:500], "dedup_key": f["dedup_key"]}, ensure_ascii=False) for f in fired))
    log.info("checked %d fired %d skipped %s", checked, len(fired), skipped)
    return {"checked": checked, "fired": [f["name"] for f in fired], "skipped": skipped}


with DAG(
    dag_id="quality_alerts",
    default_args=default_args,
    description="데이터 품질 SLO 판정: dq 표 최신 온전한 날 vs 목표, 위반은 하루 1회 Slack (docs/32)",
    # 2026-09-20 (docs/39 §2 ②): 매시 :50 은 "dbt 가 그때쯤 끝났겠지"라는 시간 추측이다.
    # 고치려고 Dataset 구독으로 바꿔 봤는데, Airflow 2.8.1 은 cron 과 Dataset 을 같이 못 쓴다
    # (DatasetOrTimeSchedule 은 2.9+). Dataset 만 쓰면 매시 실행이 사라져 dbt 가 안 도는 시간대의
    # cron 신선도·계약 검증을 놓친다 → 매시는 그대로 두고, dbt 직후 실행은 daily_pipeline 이
    # TriggerDagRunOperator 로 명시적으로 부른다. 2.9 로 올리면 한 줄로 합칠 수 있다.
    schedule="50 * * * *",
    start_date=datetime(2026, 9, 20),
    catchup=False,
    tags=["quality", "alert"],
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="judge_quality",
        python_callable=_judge,
        # 2026-09-20 (docs/39 §2 ⑤): 실패는 알리는데 지연은 안 알리고 있었다.
        # 매시 판정이 20분을 넘기면 다음 주기를 침범한다 → SLA 로 늦는 것도 알린다.
        sla=timedelta(minutes=20),
    )
