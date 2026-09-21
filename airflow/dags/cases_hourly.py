"""DAG: cases_hourly - 확인이 필요한 건을 케이스로 연다 (docs/28 C, 2026-09-19).

근거 셋을 ClickHouse 에서 읽어 MySQL `cases` 에 INSERT IGNORE(evidence_key 유니크 → 같은 근거로 두 번 열지 않음).
  ① LEDGER_MISMATCH        - 마지막 원장 3자 대조에서 mismatch=1 인 심볼
  ② TESTNET_RESET          - 생성기가 남긴 RESET_DETECTED 원문 이벤트(24h)
  ③ MARKET_FLAG_ON_TRADED_COIN - 우리가 거래한 코인의 Upbit KRW 마켓에 경보 플래그가 켜짐(최근 2시간 전이). 교차 거래소(Binance 주문 ↔ Upbit 경보), 코인 단위.
판정은 사람이 SQL 로(status·verdict·note). 그 UPDATE 가 CDC 거울로 ClickHouse 에 간다 - 케이스가 두 번째 '변경되는 행'.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks.slack_callbacks import task_failure_callback

default_args = {"owner": "calme", "retries": 1, "retry_delay": timedelta(minutes=2), "on_failure_callback": task_failure_callback}


def _collect_evidence(**context) -> list:
    from hooks.clickhouse_hook import ClickHouseHook
    hook = ClickHouseHook()
    now_ms = int(time.time() * 1000)
    found = []
    for r in hook.get_records("""
        SELECT symbol, detail, reconciled_ms FROM cdc_pipeline.ledger_reconcile FINAL
        WHERE mismatch = 1 AND reconciled_ms = (SELECT max(reconciled_ms) FROM cdc_pipeline.ledger_reconcile)"""):
        found.append({"case_type": "LEDGER_MISMATCH", "subject": r["symbol"], "evidence_key": f"mismatch:{r['symbol']}:{r['reconciled_ms']}",
                      "evidence": {"detail": r["detail"], "reconciled_ms": int(r["reconciled_ms"])}})
    for r in hook.get_records("""
        SELECT dedup_key, event_ms, event_raw FROM cdc_pipeline.binance_user_events
        WHERE event_type = 'RESET_DETECTED' AND event_ms >= toUnixTimestamp(now() - INTERVAL 24 HOUR) * 1000"""):
        found.append({"case_type": "TESTNET_RESET", "subject": "binance-testnet", "evidence_key": f"reset:{r['dedup_key']}",
                      "evidence": {"event_ms": int(r["event_ms"]), "raw": r["event_raw"][:400]}})
    # 우리가 거래한 코인 → Upbit KRW 마켓. 심볼의 기준 자산은 USDT 를 뗀 것(BTCUSDT → BTC). Upbit 에 없는 코인(BNB)은 자연히 빠진다.
    for r in hook.get_records("""
        WITH (SELECT groupUniqArray(concat('KRW-', replaceOne(symbol, 'USDT', ''))) FROM cdc_pipeline.virtual_orders FINAL) AS traded
        SELECT market, flag, observed_at FROM cdc_pipeline.upbit_market_events
        WHERE kind = 'transition' AND state = 1 AND has(traded, market) AND observed_at >= now() - INTERVAL 2 HOUR"""):
        obs = r["observed_at"].strftime("%Y-%m-%d %H:%M:%S") if hasattr(r["observed_at"], "strftime") else str(r["observed_at"])
        found.append({"case_type": "MARKET_FLAG_ON_TRADED_COIN", "subject": r["market"], "evidence_key": f"flag:{r['market']}:{r['flag']}:{obs}",
                      "evidence": {"flag": r["flag"], "observed_at": obs, "venue_note": "cross-venue: Upbit flag vs our Binance orders on the same coin"}})
    for f in found:
        f["opened_ms"] = now_ms
    context["ti"].log.info("evidence found: %d (%s)", len(found), {t: sum(1 for f in found if f["case_type"] == t) for t in {f["case_type"] for f in found}})
    return found


def _open_cases(**context) -> dict:
    import mysql.connector
    found = context["ti"].xcom_pull(task_ids="collect_evidence") or []
    if not found:
        return {"opened": 0, "found": 0}
    conn = mysql.connector.connect(host="cdc-mysql", port=3306, user="ledger", password=os.environ["LEDGER_MYSQL_PASSWORD"], database="crypto_db", autocommit=True)
    cur = conn.cursor(); opened = 0
    for f in found:
        cur.execute("INSERT IGNORE INTO cases (case_type, subject, evidence_key, evidence, opened_ms, updated_ms) VALUES (%s,%s,%s,%s,%s,%s)",
                    (f["case_type"], f["subject"], f["evidence_key"], json.dumps(f["evidence"], ensure_ascii=False), f["opened_ms"], f["opened_ms"]))
        opened += cur.rowcount
    cur.close(); conn.close()
    context["ti"].log.info("cases opened: %d / found %d", opened, len(found))
    return {"opened": opened, "found": len(found)}


with DAG(
    dag_id="cases_hourly",
    default_args=default_args,
    description="확인이 필요한 건을 케이스로 연다 (원장 불일치·테스트넷 리셋·거래 코인의 Upbit 경보)",
    schedule="20 * * * *",
    start_date=datetime(2026, 9, 19),
    catchup=False,
    tags=["ledger", "cases"],
    max_active_runs=1,
) as dag:
    collect_evidence = PythonOperator(task_id="collect_evidence", python_callable=_collect_evidence)
    open_cases = PythonOperator(task_id="open_cases", python_callable=_open_cases)
    collect_evidence >> open_cases
