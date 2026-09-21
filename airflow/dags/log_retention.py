"""DAG: log_retention - Airflow 태스크 로그 정리 (docs/39 §4-1 ⑩). 주 1회.

왜 DAG 인가: 호스트 cron 으로 두면 Airflow 밖에서 도는 것이 하나 더 늘고, 죽어도 모른다(docs/39 §2 ③).
로그는 Airflow 자신의 산출물이므로 Airflow 가 치우는 것이 맞다 - 실패하면 알림이 오고 재시도도 된다.
Docker 소켓은 필요 없다. 스케줄러 컨테이너에 로그 디렉터리가 이미 마운트돼 있다.

왜 30일인가: 장애를 되짚는 실제 단위가 '지난 몇 주'다. 그보다 오래된 태스크 로그를 연 적이 없고,
되짚어야 할 사실은 로그가 아니라 표(alert_events·pipeline_incidents·dq_*)에 남긴다.
2026-09-20 기준 3.4 GB · 14일 초과 파일 157,210개였다.
"""
from __future__ import annotations

import os
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks.slack_callbacks import task_failure_callback

LOG_DIR = "/opt/airflow/logs"
KEEP_DAYS = int(os.getenv("AIRFLOW_LOG_KEEP_DAYS", "30"))

default_args = {
    "owner": "calme",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
}


def _prune(**context) -> dict:
    log = context["ti"].log
    cutoff = datetime.utcnow().timestamp() - KEEP_DAYS * 86400
    before = shutil.disk_usage(LOG_DIR).used
    removed = freed = 0
    # 2026-09-20 첫 실행에서 errors 58,699 가 나왔는데 정작 30일 초과 파일은 0개 남았다(=삭제는 다 됐다).
    # 숫자만 있고 이유가 없으면 다음 사람이 무시하거나 겁먹는다 → 종류별로 센다.
    err_kinds: dict[str, int] = {}

    for root, dirs, files in os.walk(LOG_DIR, topdown=False):
        for name in files:
            path = os.path.join(root, name)
            try:
                st = os.stat(path)
                if st.st_mtime < cutoff:
                    freed += st.st_size
                    os.remove(path)
                    removed += 1
            except OSError as e:
                err_kinds[type(e).__name__] = err_kinds.get(type(e).__name__, 0) + 1
        # 파일을 지워 비게 된 디렉터리는 같이 치운다 (빈 디렉터리 수십만 개가 남는다)
        for d in dirs:
            p = os.path.join(root, d)
            try:
                if not os.listdir(p):
                    os.rmdir(p)
            except OSError:
                pass

    result = {
        "keep_days": KEEP_DAYS,
        "removed_files": removed,
        "freed_mb": round(freed / 1024 / 1024, 1),
        "errors": sum(err_kinds.values()),
        "error_kinds": err_kinds,
        "log_dir_mb": round(sum(
            os.path.getsize(os.path.join(r, f))
            for r, _, fs in os.walk(LOG_DIR) for f in fs
            if os.path.exists(os.path.join(r, f))
        ) / 1024 / 1024, 1),
    }
    log.info("log retention: %s", result)
    return result


with DAG(
    dag_id="log_retention",
    default_args=default_args,
    description=f"Airflow 태스크 로그 {KEEP_DAYS}일 보존 (docs/39 ⑩)",
    schedule="30 3 * * 0",          # 일요일 03:30 UTC - 백업(01:20)·대조(06:35)와 겹치지 않는다
    start_date=datetime(2026, 9, 20),
    catchup=False,
    tags=["ops"],
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="prune_logs",
        python_callable=_prune,
        sla=timedelta(minutes=30),
    )
