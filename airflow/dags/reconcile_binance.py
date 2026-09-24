"""DAG: reconcile_binance - Binance 체결 원장 대조 (docs/31 §3-2, 2026-09-20). 매일 00:40 UTC, 전날.

정답: REST /api/v3/klines?interval=1h 의 "체결 수"(n). 심볼당 1요청(weight 2, 24봉) → 683 심볼 ≈ 1,366 weight(한도 6,000/분의 23%, 0.15초 간격).
우리: binance_trades 를 (symbol, hour) 로 센다(RMT 라 FINAL 없이 count 하면 재연결 중복이 섞인다 → dbt 모델이 FINAL 로 센다).
Upbit 대조(일봉 거래량)와 같은 원리 - 정답이 거래소에 있고, 셀 단위 최소값을 함께 본다.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from callbacks.slack_callbacks import task_failure_callback

default_args = {"owner": "calme", "retries": 1, "retry_delay": timedelta(minutes=5),
    # 2026-09-20 (docs/39 §2 ⑥): 외부 API·컨테이너가 흔들릴 때 고정 간격 재시도는 같은 실패를 반복한다
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10), "on_failure_callback": task_failure_callback}
REST = "https://api.binance.com"


def _fetch_hourly_candles(**context) -> dict:
    from hooks.clickhouse_hook import ClickHouseHook
    import json
    hook = ClickHouseHook()
    day = (context["data_interval_end"] - timedelta(days=1)).strftime("%Y-%m-%d")
    start = int(datetime.strptime(day, "%Y-%m-%d").timestamp() * 1000); end = start + 86400_000 - 1
    symbols = [r["symbol"] for r in hook.get_records("SELECT DISTINCT symbol FROM cdc_pipeline.binance_trades WHERE toDate(fromUnixTimestamp64Milli(trade_ms)) = toDate('%s')" % day)]
    session = requests.Session(); rows = []; failed = []
    for i, s in enumerate(symbols):
        try:
            r = session.get(f"{REST}/api/v3/klines", params={"symbol": s, "interval": "1h", "startTime": start, "endTime": end, "limit": 24}, timeout=15)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", "5"))); r = session.get(f"{REST}/api/v3/klines", params={"symbol": s, "interval": "1h", "startTime": start, "endTime": end, "limit": 24}, timeout=15)
            r.raise_for_status()
            for k in r.json():
                rows.append({"symbol": s, "hour_utc": datetime.utcfromtimestamp(k[0] / 1000).strftime("%Y-%m-%d %H:%M:%S"), "open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
                             "volume": float(k[5]), "quote_volume": float(k[7]), "trade_count": int(k[8])})
        except Exception as e:  # noqa: BLE001
            failed.append(f"{s}:{type(e).__name__}")
        time.sleep(0.15)
    if rows:
        hook.execute("INSERT INTO cdc_pipeline.binance_hourly_candles FORMAT JSONEachRow\n" + "\n".join(json.dumps(r) for r in rows))
    context["ti"].log.info("day %s symbols %d rows %d failed %d %s", day, len(symbols), len(rows), len(failed), failed[:5])
    return {"day": day, "symbols": len(symbols), "rows": len(rows), "failed": len(failed)}


def _fetch_exchange_info(**context) -> dict:
    """심볼 마스터 스냅샷(docs/34 #4): base/quote/status 를 거래소가 준 값으로 dim_coins 에 공급. RMT(fetched_at) 라 매일 덮어쓴다."""
    from hooks.clickhouse_hook import ClickHouseHook
    import json
    info = requests.get(f"{REST}/api/v3/exchangeInfo", params={"permissions": "SPOT"}, timeout=30).json()
    rows = [{"symbol": s["symbol"], "base_asset": s["baseAsset"], "quote_asset": s["quoteAsset"], "status": s["status"], "is_spot": 1 if s.get("isSpotTradingAllowed", True) else 0} for s in info["symbols"]]
    ClickHouseHook().execute("INSERT INTO cdc_pipeline.binance_symbols FORMAT JSONEachRow\n" + "\n".join(json.dumps(r) for r in rows))
    return {"symbols": len(rows)}


with DAG(
    dag_id="reconcile_binance",
    default_args=default_args,
    description="Binance 체결 대조: REST 1h 캔들 체결 수 vs 우리 binance_trades (전날)",
    schedule="40 0 * * *",
    start_date=datetime(2026, 9, 20),
    catchup=False,
    tags=["reconcile", "binance"],
    max_active_runs=1,
) as dag:
    fetch_hourly_candles = PythonOperator(task_id="fetch_hourly_candles", python_callable=_fetch_hourly_candles, pool="default_pool")
    dbt_reconcile = BashOperator(task_id="dbt_reconcile", pool="dbt", bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt_profiles --select dq_binance_reconcile_daily 2>&1")
    fetch_exchange_info = PythonOperator(task_id="fetch_exchange_info", python_callable=_fetch_exchange_info)
    fetch_exchange_info >> fetch_hourly_candles >> dbt_reconcile
