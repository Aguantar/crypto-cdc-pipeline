"""DAG 5: rules_daily - 배치 이상탐지 규칙(VOLUME_24H)과 규칙 평가(dq_rule_eval_daily)를 매일 01:15 UTC 에 갱신 (docs/22).

왜 별도 DAG 인가: daily_pipeline 은 16:00 UTC 에 돌아 전일 UTC 기준으로는 15시간 늦다. 거래소는 01:00 UTC 에 일괄 지정하므로
그 직후에 우리 규칙도 판정해야 '선행/동시/지연'을 같은 시각 축에서 비교할 수 있다. 라벨 동기화(매시 07분) 보다는 앞서지만
평가 모델은 16:00 daily_pipeline 에서 한 번 더 재계산되므로 라벨 지연은 거기서 흡수된다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from callbacks.slack_callbacks import task_failure_callback

default_args = {
    "owner": "calme",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
}

with DAG(
    dag_id="rules_daily",
    default_args=default_args,
    description="VOLUME_24H 규칙 판정 + 규칙 평가(정밀도·재현율·선행) 갱신",
    schedule="15 1 * * *",   # 2026-09-18: 01:05 → 01:15. 거래소 라벨 fetch(cron 매시 :07)보다 뒤여야 같은 날 01:00 지정을 평가에 넣는다(01:05 실행은 exchange=0 을 냈다)
    start_date=datetime(2026, 9, 16),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "rules", "quality"],
    doc_md=__doc__,
) as dag:

    dbt_build_rules = BashOperator(
        task_id="dbt_build_rules",
        bash_command=(
            "cd /opt/airflow/dbt && dbt build --select int_volume_surge_daily dq_rule_eval_daily int_alert_transitions_recomputed dq_alert_parity_daily "
            "--profiles-dir /opt/airflow/dbt_profiles 2>&1"
        ),
    )
