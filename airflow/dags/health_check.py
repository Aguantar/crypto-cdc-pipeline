"""DAG 1: health_check - CDC 파이프라인 헬스체크 (10분 간격).

Custom Operator(ClickHouseOperator, FlinkHealthOperator)를 사용하여
파이프라인 컴포넌트 상태를 확인하고, XCom으로 결과를 전달하여 종합 판단합니다.
이상 발견 시 Slack으로 컨텍스트 포함 알림을 전송합니다.

모든 체크는 네트워크 API 기반 (Docker 소켓 불필요).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks.slack_callbacks import send_health_alert, task_failure_callback
from operators.clickhouse_operator import ClickHouseOperator
from operators.flink_health_operator import FlinkHealthOperator

# 알럿 이름 → 파이프라인 단계 매핑 (docs/41, 2026-09-20).
# 왜 필요한가: 사람이 묻는 질문은 "어제 새벽에 어느 단계가 왜 깨졌나"인데,
# 지금까지 이력은 alert_events(이름만) · ingest_repairs · DLQ · Flink 메트릭으로 흩어져 있었다.
# 이름에 단계를 붙여 한 표(pipeline_incidents)에 모으면 단계 축으로 되짚을 수 있다.
INCIDENT_STAGE = {
    "Producer Activity":        ("collect",    "cdc-upbit-producer"),
    "Binance Ingest":           ("collect",    "cdc-binance-collector"),
    "Market Coverage":          ("collect",    "upbit-websocket"),
    "Ingest Lag":               ("mysql",      "crypto_db.crypto_trades"),
    "Kafka Connect":            ("debezium",   "mysql-cdc-connector"),
    "Kafka Health":             ("kafka",      "cdc-kafka-1"),
    "CDC Parse Failures":       ("flink",      "CDC Realtime Pipeline"),
    "Flink Jobs":               ("flink",      "jobmanager"),
    "Flink Source Saturation":  ("flink",      "kafka-source"),
    "ClickHouse Insert Latency":("clickhouse", "cdc-clickhouse"),
    "ClickHouse Ingest":        ("clickhouse", "cdc_pipeline.crypto_trades"),
    "Ledger Reconcile Stale":   ("orchestration", "virtual-trader"),
}


def _incident_stage(name: str):
    """정확히 일치하지 않아도 접두사로 찾는다. 모르면 unknown - 모르는 것을 아는 척하지 않는다."""
    if name in INCIDENT_STAGE:
        return INCIDENT_STAGE[name]
    for k, v in INCIDENT_STAGE.items():
        if name.startswith(k):
            return v
    return ("unknown", name)


def coverage_verdict(market, state_row):
    """커버리지 판정에서 이 마켓을 뺄 것인가 (2026-09-20, docs/34 #9).

    'exclude'  거래 정지·폐지된 마켓 - 거래소도 체결을 안 만들므로 우리에게 없는 게 정상이다.
    'alert'    그 외 - 상태를 모르는 마켓(UNKNOWN)도 포함한다. 모른다고 넘어가면 진짜 유실을 놓친다.

    분기를 함수로 둔 이유: 지금 거래불가 마켓이 0개라 프로덕션에서는 이 분기가 타지 않는다.
    타지 않는 코드는 믿을 수 없으므로 테스트로 증명한다.
    """
    if state_row and int(state_row.get("is_tradable", 1)) == 0:
        return "exclude"
    return "alert"


default_args = {
    "owner": "calme",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # 2026-09-20 (docs/39 §2 ⑥): 외부 API·컨테이너가 흔들릴 때 고정 간격 재시도는 같은 실패를 반복한다
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": task_failure_callback,
}

with DAG(
    dag_id="health_check",
    default_args=default_args,
    description="CDC 파이프라인 전체 컴포넌트 헬스체크 (10분 간격)",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 3, 11),
    catchup=False,
    tags=["monitoring", "health"],
    max_active_runs=1,
    max_active_tasks=6,   # 2026-09-20: 체크 12개를 한꺼번에 띄우면 스케줄러 컨테이너(640M) OOM - 두 물결로
) as dag:

    # ── ClickHouse 데이터 적재 확인 ──────────────────────────
    check_clickhouse_ingest = ClickHouseOperator(
        task_id="check_clickhouse_ingest",
        sql="""
            SELECT
                count() AS recent_count,
                max(source_ts) AS latest_ts,
                dateDiff('minute', max(source_ts), now()) AS minutes_since_last
            FROM cdc_pipeline.crypto_trades
            WHERE source_ts >= now() - INTERVAL 10 MINUTE
        """,
        result_type="first",
    )

    # ── Flink 잡 상태 확인 (REST API) ────────────────────────
    check_flink_jobs = FlinkHealthOperator(
        task_id="check_flink_jobs",
        flink_base_url="http://flink-jobmanager:8081",
        expected_jobs=5,  # 2026-09-20: CDC + Circuit + Orderbook + Binance Trade + Binance Depth (docs/31)
    )

    # ── Kafka 브로커 상태 확인 (ClickHouse 기반 간접 확인) ───
    check_kafka_health = ClickHouseOperator(
        task_id="check_kafka_health",
        sql="""
            SELECT
                count() AS recent_count,
                uniqExact(market) AS active_markets
            FROM cdc_pipeline.crypto_trades
            WHERE source_ts >= now() - INTERVAL 5 MINUTE
        """,
        result_type="first",
    )

    # ── Producer 상태 확인 (데이터 유입 기반 간접 확인) ───────
    check_producer_activity = ClickHouseOperator(
        task_id="check_producer_activity",
        sql="""
            SELECT
                count() AS last_1min_count,
                uniqExact(market) AS active_markets
            FROM cdc_pipeline.crypto_trades
            WHERE source_ts >= now() - INTERVAL 1 MINUTE
        """,
        result_type="first",
    )

    # ── 적재 지연 확인 (2026-09-09 추가) ─────────────────────
    # source_ts(MySQL 적재 시각) - upbit_timestamp(거래소 체결 시각).
    # 2026-08-19~30 producer 상한 포화로 최대 36.9시간 지연이 났으나 기존 체크는
    # 전부 source_ts 이후 구간만 봐서 감지 못했음 (docs/08-ingest-lag-incident.md).
    check_ingest_lag = ClickHouseOperator(
        task_id="check_ingest_lag",
        sql="""
            SELECT
                round(quantile(0.5)(toUnixTimestamp64Milli(source_ts) - upbit_timestamp) / 1000, 1) AS lag_p50_s,
                round(max(toUnixTimestamp64Milli(source_ts) - upbit_timestamp) / 1000, 1) AS lag_max_s,
                count() AS rows_10min
            FROM cdc_pipeline.crypto_trades
            WHERE source_ts >= now() - INTERVAL 10 MINUTE
        """,
        result_type="first",
    )

    # ── 마켓별 커버리지 (2026-09-16 추가, docs/19 #8) ─────────
    # 배경: KRW-BFC 상장 후 6일 무수집을 하루 뒤 대조로만 알았다. 감지를 10분으로 당긴다.
    # 방법: 거래소 ticker(한 호출, 전 마켓)의 마지막 체결 시각 vs 우리 마켓별 마지막 체결 시각.
    #       우리가 최신 체결을 갖고 있으면 차이는 적재 지연(관찰 주간 최대 7.75초)뿐이다.
    # 판정: 거래소가 60초 넘게 전에 본 마지막 체결이 우리에게 없다 (ex_ms > ours_last AND now − ex_ms > 60s).
    #   첫 실행(09-16 22:25)에서 배운 것 - "거래소 마지막 − 우리 마지막" 차이로 재면, 체결 직후 1~2초 사이에 체크가 돌 때
    #   아직 적재되기 전의 직전 체결과 비교되어 뜸한 마켓(KRW-G)이 263초 뒤처진 것으로 나온다. 최신 체결에 도착할 시간(60초)을
    #   준 뒤에도 없어야 누락이다.
    # 임계 60초: 정상 적재 지연(관찰 주간 최대 7.75초)의 8배이고, 늦은 이벤트 가드·적재 지연 알림과 같은 "실시간 아님" 기준(docs/20).
    # 대상: 거래소 마지막 체결이 최근 24시간 안인 마켓만 (하루 이상 거래 없는 마켓은 비교 대상 아님).
    # 재연결 직후 gap-fill 이 끝나기 전 1회 걸릴 수 있다 - 그 알림은 실제 상태이므로 억제하지 않는다.
    def _check_market_coverage(**context) -> dict:
        import time
        import requests
        from hooks.clickhouse_hook import ClickHouseHook

        markets = [m["market"] for m in requests.get("https://api.upbit.com/v1/market/all?is_details=false", timeout=15).json()
                   if m["market"].startswith("KRW-")]
        ticker = requests.get("https://api.upbit.com/v1/ticker", params={"markets": ",".join(markets)}, timeout=15).json()
        exchange_last = {t["market"]: int(t["trade_timestamp"]) for t in ticker}

        rows = ClickHouseHook().get_records("""
            SELECT market, max(upbit_timestamp) AS last_ms
            FROM cdc_pipeline.crypto_trades
            WHERE source_ts >= now() - INTERVAL 1 DAY
            GROUP BY market
        """)
        ours_last = {r["market"]: int(r["last_ms"]) for r in rows}

        # 2026-09-20 (docs/34 #9): 거래 정지·폐지 마켓은 거래소도 체결을 안 만든다. 우리에게 없는 게 정상이다.
        # 이걸 빼지 않으면 "폐지 = 유실"로 보이고, 그런 알럿은 몇 번 반복되면 무시당한다.
        # 상태는 웹소켓 ticker 에만 있어(REST 에는 없다) 10분 폴러가 ClickHouse 에 적재한 것을 읽는다.
        state_rows = ClickHouseHook().get_records("""
            SELECT market, market_state, is_tradable, toString(delisting_date) AS delisting_date
            FROM cdc_pipeline.dim_market_state_scd WHERE is_current = 1
        """)
        states = {r["market"]: r for r in state_rows}

        now_ms = int(time.time() * 1000)
        missing, excluded = [], []
        for market, ex_ms in exchange_last.items():
            if now_ms - ex_ms > 86_400_000:
                continue
            if ex_ms > ours_last.get(market, 0) and now_ms - ex_ms > 60_000:
                st = states.get(market)
                if coverage_verdict(market, st) == "exclude":
                    excluded.append({"market": market, "state": st["market_state"]})
                    continue
                missing.append({"market": market, "behind_s": round((now_ms - ex_ms) / 1000),
                                "ours": market in ours_last,
                                "state": (st or {}).get("market_state", "UNKNOWN"),
                                "delisting_date": (st or {}).get("delisting_date") or None})
        missing.sort(key=lambda x: -x["behind_s"])
        result = {"checked": len(exchange_last), "missing": missing[:20], "missing_count": len(missing),
                  "excluded_not_tradable": excluded, "state_known": len(states)}
        context["ti"].log.info("market coverage: %s", result)
        return result

    check_market_coverage = PythonOperator(
        task_id="check_market_coverage",
        python_callable=_check_market_coverage,
        pool="upbit_rest",
    )

    # ── Kafka Connect 상태 확인 (REST API) ───────────────────
    def _check_kafka_connect(**context) -> dict:
        """Kafka Connect REST API로 커넥터 상태를 확인합니다."""
        import requests as req

        try:
            resp = req.get(
                "http://kafka-connect:8083/connectors", timeout=10
            )
            resp.raise_for_status()
            connectors = resp.json()

            statuses = {}
            restarted = []
            for name in connectors:
                status_resp = req.get(
                    f"http://kafka-connect:8083/connectors/{name}/status",
                    timeout=10,
                )
                status_resp.raise_for_status()
                status = status_resp.json()
                connector_state = status.get("connector", {}).get("state", "UNKNOWN")
                tasks_state = [
                    t.get("state", "UNKNOWN")
                    for t in status.get("tasks", [])
                ]
                statuses[name] = {
                    "connector": connector_state,
                    "tasks": tasks_state,
                }
                # 2026-09-17 자동 복구 (docs/23 §6, 브로커 축소 선행 조건): Connect 는 FAILED 태스크를 스스로 살리지 않는다.
                # 브로커 전체 정지 뒤 프로듀서 재시도 한도를 넘긴 태스크가 FAILED 로 남으면 MySQL 원장에 쌓인 체결이 영영 안 흐른다.
                # 재시작하면 Debezium 이 binlog 오프셋에서 이어 읽는다(binlog 보존 무기한). 알림은 그대로 보내되 "재시작했다"를 붙인다.
                for i, t in enumerate(status.get("tasks", [])):
                    if t.get("state") == "FAILED":
                        r = req.post(f"http://kafka-connect:8083/connectors/{name}/tasks/{t.get('id', i)}/restart", timeout=10)
                        restarted.append({"connector": name, "task": t.get("id", i), "http": r.status_code,
                                          "trace": (t.get("trace") or "")[:200]})
                # 2026-09-17 07:09 브로커 전체 정지 1분 재실험(docs/23 §7-1): 재기동 후 워커 리밸런스에서 커넥터가 UNASSIGNED,
                # 태스크는 RUNNING 이라 보고하면서 6분간 아무것도 발행하지 않았다(binlog 클라이언트 유령). FAILED 조건으로는 못 잡는다
                # → 커넥터 상태가 RUNNING 이 아니면 태스크 포함 재시작. 수동 재시작 실측: 20초 만에 binlog 재접속, 60초에 8,195행 따라붙음
                if connector_state != "RUNNING":
                    r = req.post(f"http://kafka-connect:8083/connectors/{name}/restart?includeTasks=true&onlyFailed=false", timeout=10)
                    restarted.append({"connector": name, "task": "connector+tasks", "http": r.status_code, "trace": f"state={connector_state}"})

            all_running = all(
                s["connector"] == "RUNNING"
                and all(t == "RUNNING" for t in s["tasks"])
                for s in statuses.values()
            )

            return {
                "healthy": all_running,
                "connectors": statuses,
                "count": len(connectors),
                "restarted": restarted,
            }
        except req.RequestException as e:
            return {"healthy": False, "error": str(e), "connectors": {}, "count": 0}

    check_connect = PythonOperator(
        task_id="check_kafka_connect",
        python_callable=_check_kafka_connect,
    )

    # ── 종합 판단 (XCom 수집) ────────────────────────────────
    # ── 포화 선행 지표 (2026-09-17 추가, docs/23 §5 부하 실험) ─────────
    # 실험에서 임계(10,000/s)에 닿기 전에 먼저 움직인 지표 둘: ① Flink 소스 체인 busy(500→5,000/s 에서 48→240 ms/s 선형, 임계에서 1,000),
    # ② ClickHouse insert 평균 지연(기준선 9ms → 24ms 부터 정체 시작). 프로덕션 기준선: busy 3 ms/s, insert 평균 14~18 ms(24h 시간별).
    # 임계: busy > 500 ms/s(포화의 절반 - 실험에서 651 은 이미 정체), insert 평균 > 30 ms(기준선 2배). 둘 다 10분 창.
    # 백프레셔는 쓰지 않는다: 소스가 체인의 머리라 10,000/s 에서도 0 이었다(docs/23 §5).
    def _check_source_busy(**context) -> dict:
        import requests
        base = "http://flink-jobmanager:8081"
        jobs = requests.get(f"{base}/jobs/overview", timeout=10).json()["jobs"]
        js = [j for j in jobs if j["name"] == "CDC Realtime Pipeline" and j["state"] == "RUNNING"]
        if not js:
            return {"busy_max_ms": None, "error": "prod CDC job not running"}
        jid = js[0]["jid"]
        vertices = requests.get(f"{base}/jobs/{jid}", timeout=10).json()["vertices"]
        src = [v for v in vertices if v["name"].startswith("Source")][0]["id"]
        m = requests.get(f"{base}/jobs/{jid}/vertices/{src}/subtasks/metrics",
                         params={"get": "busyTimeMsPerSecond", "agg": "max"}, timeout=10).json()
        busy = float(m[0]["max"]) if m else None
        return {"busy_max_ms": busy, "job_id": jid}

    check_source_busy = PythonOperator(
        task_id="check_source_busy",
        python_callable=_check_source_busy,
    )

    # ── 파싱 실패 카운터 (2026-09-19, docs/29 창2) ────────────
    # 파서가 실패 원문을 cdc.dlq.crypto_trades 로 보내고 parseFailures 카운터를 올린다. 카운터는 잡 재시작에 0 이 되므로
    # "이전 실행보다 늘었나"로 판정한다(절대값 > 0 은 재시작 전 사고를 10분마다 다시 알리게 된다).
    def _check_parse_failures(**context) -> dict:
        import requests
        base = "http://flink-jobmanager:8081"
        jobs = requests.get(f"{base}/jobs/overview", timeout=10).json()["jobs"]
        js = [j for j in jobs if j["name"] == "CDC Realtime Pipeline" and j["state"] == "RUNNING"]
        if not js:
            return {"parse_failures": None, "error": "prod CDC job not running"}
        jid = js[0]["jid"]
        vertices = requests.get(f"{base}/jobs/{jid}", timeout=10).json()["vertices"]
        src = [v for v in vertices if v["name"].startswith("Source")][0]["id"]
        # 연산자 메트릭 id 는 공백이 _ 로 바뀌고, 서브태스크 합산은 /subtasks/metrics 가 준다 (vertex /metrics 는 "0.…" 접두 id)
        m = requests.get(f"{base}/jobs/{jid}/vertices/{src}/subtasks/metrics",
                         params={"get": "CDC_Event_Parser.parseFailures", "agg": "sum"}, timeout=10).json()
        cur = int(float(m[0]["sum"])) if m else None
        prev = context["ti"].xcom_pull(task_ids="check_parse_failures", include_prior_dates=True) or {}
        return {"parse_failures": cur, "prev": prev.get("parse_failures"), "job_id": jid}

    check_parse_failures = PythonOperator(
        task_id="check_parse_failures",
        python_callable=_check_parse_failures,
    )

    # ── 2층 원장 3자 대조 (2026-09-19, docs/28 B-5) ────────────
    # 생성기가 시간당 거래소(REST) vs MySQL 을 ledger_reconcile 에 남긴다. 마지막 대조에서 mismatch 가 있으면 CDC 가 변경을 놓친 것.
    check_ledger_reconcile = ClickHouseOperator(
        task_id="check_ledger_reconcile",
        sql="""
            SELECT count() AS symbols, countIf(mismatch = 1) AS mismatched,
                   dateDiff('minute', fromUnixTimestamp64Milli(max(reconciled_ms)), now()) AS minutes_since_last,
                   arrayStringConcat(groupArrayIf(concat(symbol, ':', detail), mismatch = 1), '; ') AS detail
            FROM cdc_pipeline.ledger_reconcile FINAL
            WHERE reconciled_ms = (SELECT max(reconciled_ms) FROM cdc_pipeline.ledger_reconcile)
        """,
        result_type="first",
    )

    # ── Binance 체결 적재 (2026-09-20, docs/31): 수집기·잡 어느 쪽이 멈춰도 60초 0행으로 드러난다. 24h 평균 361/s 라 60초 0 은 확실한 이상
    check_binance_ingest = ClickHouseOperator(
        task_id="check_binance_ingest",
        sql="""
            SELECT count() AS rows_60s, uniqExact(symbol) AS symbols_60s,
                   round(quantile(0.95)(toUnixTimestamp64Milli(flink_ts) - trade_ms) / 1000, 2) AS e2e_p95_s
            FROM cdc_pipeline.binance_trades WHERE flink_ts >= now() - INTERVAL 60 SECOND
        """,
        result_type="first",
    )

    check_insert_latency = ClickHouseOperator(
        task_id="check_insert_latency",
        sql="""
            SELECT
                round(avg(query_duration_ms), 1) AS insert_avg_ms,
                round(quantile(0.95)(query_duration_ms)) AS insert_p95_ms,
                max(query_duration_ms) AS insert_max_ms,
                count() AS inserts_10min
            FROM system.query_log
            WHERE type = 'QueryFinish' AND query_kind = 'Insert'
              AND has(tables, 'cdc_pipeline.crypto_trades')
              AND event_time >= now() - INTERVAL 10 MINUTE
        """,
        result_type="first",
    )

    def _evaluate_health(**context) -> dict:
        ti = context["ti"]

        ch_result = ti.xcom_pull(task_ids="check_clickhouse_ingest")
        flink_result = ti.xcom_pull(task_ids="check_flink_jobs")
        kafka_result = ti.xcom_pull(task_ids="check_kafka_health")
        producer_result = ti.xcom_pull(task_ids="check_producer_activity")
        connect_result = ti.xcom_pull(task_ids="check_kafka_connect")
        lag_result = ti.xcom_pull(task_ids="check_ingest_lag")
        coverage_result = ti.xcom_pull(task_ids="check_market_coverage")
        busy_result = ti.xcom_pull(task_ids="check_source_busy")
        insert_result = ti.xcom_pull(task_ids="check_insert_latency")
        parse_result = ti.xcom_pull(task_ids="check_parse_failures")
        ledger_result = ti.xcom_pull(task_ids="check_ledger_reconcile")
        binance_result = ti.xcom_pull(task_ids="check_binance_ingest")

        unhealthy = []

        # Binance 체결: 60초 0행 또는 e2e p95 > 30s
        if binance_result is not None:
            if int(binance_result.get("rows_60s", 0)) == 0:
                unhealthy.append({"name": "Binance Ingest", "message": "No binance_trades rows in last 60s (collector or Flink job down)"})
            elif float(binance_result.get("e2e_p95_s") or 0) > 30:
                unhealthy.append({"name": "Binance Ingest Lag", "message": f"e2e p95 {binance_result['e2e_p95_s']}s (> 30) rows_60s {binance_result['rows_60s']}"})

        # 원장 3자 대조: 마지막 대조에 불일치가 있으면 알린다 (대조가 2시간 넘게 없어도 - 생성기 정지)
        if ledger_result and int(ledger_result.get("symbols", 0)) > 0:
            if int(ledger_result.get("mismatched", 0)) > 0:
                unhealthy.append({"name": "Ledger Reconcile", "message": f"{ledger_result['mismatched']}/{ledger_result['symbols']} symbols mismatch: {ledger_result.get('detail')}"})
            elif int(ledger_result.get("minutes_since_last", 0)) > 120:
                unhealthy.append({"name": "Ledger Reconcile Stale", "message": f"last reconcile {ledger_result['minutes_since_last']} min ago (> 120) - virtual-trader 정지?"})

        # 파싱 실패: 이전 실행보다 늘었으면 알린다. 원문은 DLQ 토픽에 있다 (docs/29 창2)
        if parse_result and parse_result.get("parse_failures") is not None:
            cur = int(parse_result["parse_failures"]); prev = parse_result.get("prev")
            if prev is not None and cur > int(prev):
                unhealthy.append({"name": "CDC Parse Failures",
                                  "message": f"parseFailures {prev} → {cur} (+{cur - int(prev)}) - 원문: kafka topic cdc.dlq.crypto_trades"})

        # 포화 선행 지표 (docs/23 §5): 유실 전에, 실시간이 깨지기 전에 알린다
        if busy_result and busy_result.get("busy_max_ms") is not None and float(busy_result["busy_max_ms"]) > 500:
            unhealthy.append({"name": "Flink Source Saturation",
                              "message": f"source busy max {busy_result['busy_max_ms']:.0f} ms/s (> 500; 1,000 = 포화, 기준선 3)"})
        # 2026-09-18 정정: 첫 밤 알림 2건이 평균의 착시였다(p50 18·p95 28~32 는 기준선인데 7.5초짜리 insert 2~4건이 평균을 42~66 으로 끌어올림, dbt 배치 시각과 겹침).
        # 부하 실험의 정체 신호는 분포 전체가 오르는 것(9→24~34ms)이었으므로 p95 로 보고(기준선 27~38 → 임계 60), 단발 스톨은 max 로 따로 알린다.
        if insert_result and int(insert_result.get("inserts_10min", 0)) > 0:
            p95 = float(insert_result.get("insert_p95_ms") or 0); mx = float(insert_result.get("insert_max_ms") or 0)
            if p95 > 60:
                unhealthy.append({"name": "ClickHouse Insert Latency",
                                  "message": f"insert p95 {p95:.0f} ms over 10 min (> 60; 기준선 27~38) avg {insert_result['insert_avg_ms']} ms"})
            elif mx > 5000:
                unhealthy.append({"name": "ClickHouse Insert Stall",
                                  "message": f"단발 insert {mx/1000:.1f}s (p95 {p95:.0f} ms 는 정상) - 배치·머지와 겹침 여부 확인"})

        # 마켓 커버리지: 거래소에는 최신 체결이 있는데 우리에게 60초 넘게 없는 마켓
        if coverage_result and int(coverage_result.get("missing_count", 0)) > 0:
            # 상태를 함께 적는 이유(2026-09-20): "뒤처짐"만 보면 받는 사람이 유실인지 폐지 절차인지 모른다.
            # PREDELISTING 은 거래가 줄어드는 게 정상이라 대응이 다르다.
            def _fmt(m):
                tail = "" if m["ours"] else ", 무수집"
                st = m.get("state")
                if st and st != "ACTIVE":
                    tail += f", {st}" + (f" 폐지 {m['delisting_date']}" if m.get("delisting_date") else "")
                return f"{m['market']}({m['behind_s']}s{tail})"
            top = ", ".join(_fmt(m) for m in coverage_result["missing"][:8])
            excl = coverage_result.get("excluded_not_tradable") or []
            note = f" (거래불가 {len(excl)}개 제외)" if excl else ""
            unhealthy.append(
                {
                    "name": "Market Coverage",
                    "message": f"{coverage_result['missing_count']}/{coverage_result['checked']} markets behind exchange{note}: {top}",
                }
            )

        # 적재 지연: 최근 10분 p50 > 60초면 producer 포화 (max는 참고 표기)
        if lag_result and int(lag_result.get("rows_10min", 0)) > 0:
            lag_p50 = float(lag_result.get("lag_p50_s", 0))
            lag_max = float(lag_result.get("lag_max_s", 0))
            if lag_p50 > 60:
                unhealthy.append(
                    {
                        "name": "Ingest Lag",
                        "message": f"source_ts - upbit_ts p50 {lag_p50}s (max {lag_max}s) in last 10min - producer backlog",
                    }
                )

        # ClickHouse: 최근 10분간 데이터 없으면 이상
        if ch_result:
            recent_count = int(ch_result.get("recent_count", 0))
            minutes_since = int(ch_result.get("minutes_since_last", 999))
            if recent_count == 0 or minutes_since > 15:
                unhealthy.append(
                    {
                        "name": "ClickHouse Ingest",
                        "message": f"Recent 10min: {recent_count} rows, last data: {minutes_since}min ago",
                    }
                )
        else:
            unhealthy.append(
                {"name": "ClickHouse", "message": "Query returned no result"}
            )

        # Flink: RUNNING 잡 수 부족
        if flink_result and not flink_result.get("healthy", False):
            running = flink_result.get("running_jobs", 0)
            expected = flink_result.get("expected_jobs", 2)
            job_details = ", ".join(
                f"{j['name']}({j['state']})" for j in flink_result.get("jobs", [])
            )
            unhealthy.append(
                {
                    "name": "Flink Jobs",
                    "message": f"Running: {running}/{expected}. Jobs: {job_details}",
                }
            )

        # Kafka: 데이터 유입 기반 확인
        if kafka_result:
            active_markets = int(kafka_result.get("active_markets", 0))
            if active_markets < 5:
                unhealthy.append(
                    {
                        "name": "Kafka/Pipeline",
                        "message": f"Only {active_markets} markets active in last 5min (expected 20+)",
                    }
                )

        # Producer: 최근 1분 데이터 유입 확인
        if producer_result:
            last_1min = int(producer_result.get("last_1min_count", 0))
            if last_1min == 0:
                msg = "No data in last 1 minute"
                # 2026-09-17 (docs/23 §7-1): 커넥터가 RUNNING 이라 보고해도 발행이 멈춘 유령 상태가 있었다. 적재 0 이면 CDC 커넥터를 태스크 포함 재시작한다.
                # producer(WS) 쪽 장애라면 이 재시작은 무해하고, Debezium 은 binlog 오프셋에서 이어 읽으므로 중복도 없다.
                try:
                    import requests as req
                    r = req.post("http://kafka-connect:8083/connectors/mysql-cdc-connector/restart?includeTasks=true&onlyFailed=false", timeout=10)
                    msg += f" | CDC connector restarted (HTTP {r.status_code})"
                except Exception as e:  # noqa: BLE001
                    msg += f" | CDC connector restart failed: {e}"
                unhealthy.append({"name": "Upbit Producer", "message": msg})

        # Kafka Connect: 커넥터 상태
        if connect_result and not connect_result.get("healthy", False):
            error = connect_result.get("error", "")
            connectors = connect_result.get("connectors", {})
            failed = [
                name
                for name, s in connectors.items()
                if s.get("connector") != "RUNNING"
            ]
            msg = f"Failed connectors: {failed}" if failed else f"Error: {error}"
            restarted = connect_result.get("restarted", [])
            if restarted:
                msg += " | auto-restarted: " + ", ".join(f"{r['connector']}/task{r['task']} (HTTP {r['http']})" for r in restarted)
            unhealthy.append({"name": "Kafka Connect", "message": msg})

        if unhealthy:
            send_health_alert(unhealthy)
            ti.log.warning("Health check FAILED: %s", unhealthy)
            # 2026-09-20 (docs/32): 울린 알럿을 데이터로 - 주간 다이제스트가 반복·임계 재검토 대상을 집계한다
            try:
                import json as _json
                from hooks.clickhouse_hook import ClickHouseHook
                now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"); hour = datetime.utcnow().strftime("%Y-%m-%dT%H")
                hook = ClickHouseHook()
                rows = [{"fired_at": now, "source": "health_check", "name": u["name"], "severity": "immediate", "message": str(u["message"])[:500], "dedup_key": f"{u['name']}:{hour}"} for u in unhealthy]
                hook.execute("INSERT INTO cdc_pipeline.alert_events FORMAT JSONEachRow\n" + "\n".join(_json.dumps(r, ensure_ascii=False) for r in rows))
                # 2026-09-20 (docs/41): 같은 사건을 단계 축으로도 남긴다.
                # alert_events 는 '무엇이 울렸나', pipeline_incidents 는 '어느 단계가 깨졌나' 에 답한다.
                inc = []
                for u in unhealthy:
                    stage, comp = _incident_stage(u["name"])
                    inc.append({"detected_at": now, "stage": stage, "component": comp, "severity": "error",
                                "title": u["name"], "detail": str(u["message"])[:800],
                                "source": "health_check", "dedup_key": f"{u['name']}:{hour}"})
                hook.execute("INSERT INTO cdc_pipeline.pipeline_incidents FORMAT JSONEachRow\n" + "\n".join(_json.dumps(r, ensure_ascii=False) for r in inc))
            except Exception as e:  # noqa: BLE001
                ti.log.warning("alert_events insert failed: %s", e)
        else:
            ti.log.info("All components healthy")

        return {"healthy": len(unhealthy) == 0, "issues": unhealthy}

    evaluate_health = PythonOperator(
        task_id="evaluate_health",
        python_callable=_evaluate_health,
    )

    (
        [
            check_clickhouse_ingest,
            check_flink_jobs,
            check_kafka_health,
            check_producer_activity,
            check_connect,
            check_ingest_lag,
            check_market_coverage,
            check_source_busy,
            check_insert_latency,
            check_parse_failures,
            check_ledger_reconcile,
            check_binance_ingest,
        ]
        >> evaluate_health
    )
