"""DAG 2: daily_pipeline - 일일 배치 파이프라인 오케스트레이션.

dbt 실행 → 동적 코인별 품질검증 → quality gate → 일일 리포트.
스케줄: 매일 01:00 KST - 전날(00:00~23:59 KST) 데이터를 집계하여 Slack 리포트.

핵심 Airflow 기능:
- Jinja 템플릿 + target_date: idempotent, backfill 가능
- Dynamic Task Mapping: 코인 목록 동적 로딩 → 코인별 품질검증 병렬
- XCom: 태스크 간 검증 결과 전달
- Dataset: dbt 완료 시 후속 품질검증 트리거
- SLA: dbt가 2시간 이내 완료되지 않으면 알림
- Callback: 실패 시 컨텍스트 포함 Slack 알림

중복 게이트 FAIL 대응: 상시 유입원이 sink 재시도(월 수십 건 수준, 07-dedup-audit.md)이므로
간헐적 소량 FAIL은 정상 동작 - 건수만 기록하고 무시. 배치 크기(200건) 이상이 반복되면 조사.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.macros import ds_add

from callbacks.slack_callbacks import (
    send_daily_report,
    sla_miss_callback,
    task_failure_callback,
)
from operators.clickhouse_operator import ClickHouseOperator

QUALITY_GATE_MIN_PASS_RATE = 90.0   # % (docs/39 §2 ⑧)

# Dataset: dbt 완료를 나타내는 논리적 데이터셋
DBT_COMPLETED = Dataset("clickhouse://cdc_pipeline/dbt_models")


def _get_target_date(context) -> str:
    """대상 날짜를 결정합니다.

    스케줄 0 16 * * * (01:00 KST)에서 ds = data_interval_start = 전날 날짜.
    즉 ds가 그대로 리포트 대상일입니다.
    수동 트리거 시 params.target_date로 오버라이드 가능.
    """
    conf_date = context.get("params", {}).get("target_date")
    if conf_date:
        return conf_date
    return context["ds"]


def _kst_day_bounds_ms(target_date: str) -> tuple[int, int]:
    """KST 하루 [00:00, 24:00) 를 Unix ms 로. '하루' 정의 통일(docs/17): 리포트·분석은 KST 체결시각 기준,
    운영 대조(reconcile)는 UTC. 종전에는 toDate(source_ts)(UTC 적재시각)로 걸러 KST 리포트에 9시간 어긋난 하루가 들어갔다."""
    from datetime import timezone
    start = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone(timedelta(hours=9)))
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)

default_args = {
    "owner": "calme",
    # 2026-09-20 (docs/39 §2 ⑦)에 True 로 걸었다가 2026-09-24 (docs/44 §6) 에 뺐다.
    #   의도는 "전날이 실패했는데 오늘이 돌면 증분에 구멍이 조용히 남는다" 였는데, 실제로는 실패한 날이 하나 생기면
    #   그 뒤 모든 날이 영원히 멈춘다(09-18 dbt_test 실패 -> 09-19 이후 전부 대기, 사람이 풀기 전까지). 구멍은
    #   오늘을 막아서가 아니라 실패한 날을 다시 돌려서 메우는 것이고, 표가 굳는 것은 health_check 의 Mart Freshness 가 잡는다.
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
}

with DAG(
    dag_id="daily_pipeline",
    default_args=default_args,
    description="dbt → 코인별 품질검증(동적) → quality gate → 일일 리포트",
    schedule="0 16 * * *",  # 01:00 KST = 16:00 UTC (전날 데이터 집계)
    start_date=datetime(2026, 3, 11),
    catchup=False,
    tags=["dbt", "data-quality", "report"],
    # 2026-09-24 (docs/44 §3·§6): 밀린 실행을 풀 때 임시로 2, 3 까지 올렸다가 되돌렸다. depends_on_past 와
    #   max_active_runs=1 이 겹치면 실패한 run 이 재시도 슬롯을 못 받는 양방향 데드락이 생겼는데, 원인 쪽
    #   (depends_on_past) 을 뺐으므로 1 로 둔다. 하루 한 번이 두 개 겹쳐 돌 이유가 없다.
    max_active_runs=1,
    sla_miss_callback=sla_miss_callback,
    params={"target_date": ""},  # 수동 트리거 시 날짜 지정 가능 (빈값=오늘 KST)
) as dag:

    # ── Step 0: dbt source freshness ─────────────────────────
    # 소스가 멈춘 상태에서 mart 를 재계산하면 "정상 완료"로 위장된다. freshness 는 dbt 산출물(sources.json)에 남는 표준 점검.
    # health_check(10분)와 중복이 아니라 역할 분리: 그쪽은 즉시 알림, 이쪽은 일일 배치의 전제조건 기록.
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt source freshness --profiles-dir /opt/airflow/dbt_profiles 2>&1"
        ),
    )

    # ── Step 1: dbt run ──────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        pool="dbt",  # 2026-09-24 (docs/44 §6): dbt 를 부르는 태스크 5개가 한 풀(1슬롯)을 쓴다
        bash_command=(
            "cd /opt/airflow/dbt && "
            # 2026-09-20 (docs/40 ⑩ · docs/39 §3): 논리 날짜를 dbt 에 넘긴다.
            # 전에는 dbt 가 now() 로 창을 잘라 과거 날짜를 재실행해도 오늘을 다시 만들었다 -
            # 대조·백업은 {{ ds }} 를 쓰는데 dbt 만 안 써서 한 파이프라인 안에서 재실행 가능 여부가 갈렸다.
            # 이제 Airflow UI 에서 과거 날짜를 clear 하면 그 날이 다시 만들어진다.
            'dbt run --profiles-dir /opt/airflow/dbt_profiles --vars \'{"run_date": "{{ ds }}"}\' 2>&1'
        ),
        sla=timedelta(hours=2),
    )

    # ── Step 2: dbt test ─────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        pool="dbt",
        bash_command=(
            "cd /opt/airflow/dbt && "
            'dbt test --profiles-dir /opt/airflow/dbt_profiles --vars \'{"run_date": "{{ ds }}"}\' 2>&1'
        ),
        outlets=[DBT_COMPLETED],  # Dataset 트리거
    )

    # ── Step 3: 코인 목록 동적 로딩 ──────────────────────────
    def _get_coin_list(**context) -> list[str]:
        """ClickHouse에서 활성 코인 목록을 동적으로 가져옵니다."""
        from hooks.clickhouse_hook import ClickHouseHook

        target_date = _get_target_date(context)
        ms0, ms1 = _kst_day_bounds_ms(target_date)
        hook = ClickHouseHook()
        result = hook.get_records(
            f"""
            SELECT DISTINCT market
            FROM cdc_pipeline.crypto_trades
            WHERE upbit_timestamp >= {ms0} AND upbit_timestamp < {ms1}
            ORDER BY market
            """
        )
        coins = [row["market"] for row in result]
        context["ti"].log.info("Target date: %s, Found %d active coins: %s", target_date, len(coins), coins)
        return coins

    get_coins = PythonOperator(
        task_id="get_coin_list",
        python_callable=_get_coin_list,
    )

    # ── Step 4: 코인별 품질검증 (Dynamic Task Mapping) ───────
    def _validate_coin(coin: str, **context) -> dict:
        """개별 코인의 데이터 품질을 검증합니다."""
        from hooks.clickhouse_hook import ClickHouseHook

        target_date = _get_target_date(context)
        ms0, ms1 = _kst_day_bounds_ms(target_date)
        hook = ClickHouseHook()
        issues = []

        # 4-1. 일일 거래 건수
        count_result = hook.get_scalar(
            f"SELECT count() FROM cdc_pipeline.crypto_trades "
            f"WHERE market = '{coin}' AND upbit_timestamp >= {ms0} AND upbit_timestamp < {ms1}"
        )
        trade_count = int(count_result or 0)
        if trade_count == 0:
            issues.append(f"No trades on {target_date}")
        elif trade_count < 100:
            issues.append(f"Low trade count: {trade_count}")

        # 4-2. NULL 비율 (trade_price)
        null_result = hook.get_scalar(
            f"SELECT round(countIf(trade_price = 0 OR trade_price IS NULL) * 100.0 "
            f"/ count(), 2) FROM cdc_pipeline.crypto_trades "
            f"WHERE market = '{coin}' AND upbit_timestamp >= {ms0} AND upbit_timestamp < {ms1}"
        )
        null_pct = float(null_result or 0)
        if null_pct > 1.0:
            issues.append(f"Null/zero price ratio: {null_pct}%")

        # 4-3. CDC 지연 이상치 (1초 초과)
        latency_result = hook.get_scalar(
            f"SELECT countIf(cdc_latency_ms > 1000) FROM cdc_pipeline.crypto_trades "
            f"WHERE market = '{coin}' AND upbit_timestamp >= {ms0} AND upbit_timestamp < {ms1}"
        )
        high_latency = int(latency_result or 0)
        if high_latency > 100:
            issues.append(f"High latency events (>1s): {high_latency}")

        result = {
            "coin": coin,
            "date": target_date,
            "trade_count": trade_count,
            "null_pct": null_pct,
            "high_latency_count": high_latency,
            "passed": len(issues) == 0,
            "issues": issues,
        }
        context["ti"].log.info("Validation [%s] %s: %s", target_date, coin, result)
        return result

    validate_coins = PythonOperator.partial(
        task_id="validate_coin",
        python_callable=_validate_coin,
    ).expand(op_kwargs=get_coins.output.map(lambda coin: {"coin": coin}))

    # ── Step 5: Quality Gate (XCom 수집 → 종합 판단) ─────────
    def _quality_gate(**context) -> dict:
        """모든 코인의 검증 결과를 수집하여 종합 pass/fail 판단."""
        ti = context["ti"]
        raw_results = ti.xcom_pull(task_ids="validate_coin")

        if not raw_results:
            return {"passed": 0, "failed": 0, "total": 0, "failed_coins": []}

        # Dynamic task mapping의 XCom은 LazyXComAccess → list로 변환
        results = list(raw_results)

        passed = [r for r in results if isinstance(r, dict) and r.get("passed")]
        failed = [r for r in results if isinstance(r, dict) and not r.get("passed")]

        gate_result = {
            "passed": len(passed),
            "failed": len(failed),
            "total": len(results),
            "failed_coins": [
                {"coin": r["coin"], "issues": r["issues"]} for r in failed
            ],
            "pass_rate": round(len(passed) / len(results) * 100, 1)
            if results
            else 0,
        }

        # 중복 적재 검사 결과를 게이트에 반영 (0건 아니면 FAIL 항목 추가)
        dup_rows = ti.xcom_pull(task_ids="check_duplicates")
        dup_rows = int(dup_rows or 0)
        gate_result["duplicate_rows"] = dup_rows
        if dup_rows > 0:
            gate_result["failed"] += 1
            gate_result["total"] += 1
            gate_result["failed_coins"].append(
                {"coin": "DEDUP", "issues": [f"중복 {dup_rows}건 (source_ts, trade_id)"]}
            )
            gate_result["pass_rate"] = round(
                gate_result["passed"] / gate_result["total"] * 100, 1
            )

        ti.log.info(
            "Quality Gate: %d/%d passed (%.1f%%)",
            gate_result["passed"],
            gate_result["total"],
            gate_result["pass_rate"],
        )

        if failed:
            ti.log.warning("Failed coins: %s", gate_result["failed_coins"])

        # 2026-09-20 (docs/39 §2 ⑧): 여기서 예외를 던지지 않아 이름만 게이트였다.
        # 검증 실패 코인이 있어도 generate_report >> slack_daily_report 가 그대로 돌아
        # 틀린 값이 Slack 으로 나갔다. 게이트는 막아야 게이트다.
        #
        # 임계를 '0건 실패'가 아니라 비율로 두는 이유: 얇은 마켓 한두 개가 순간적으로 조건을 못 맞추는 일은
        # 늘 있고, 그때마다 리포트를 막으면 사람이 게이트를 꺼 버린다. 90% 는 '대부분 멀쩡한데 몇 개가 이상'과
        # '전반이 깨짐'을 가르는 선이다.
        if gate_result["total"] > 0 and gate_result["pass_rate"] < QUALITY_GATE_MIN_PASS_RATE:
            raise AirflowFailException(
                f"품질 게이트 미달: {gate_result['passed']}/{gate_result['total']} "
                f"({gate_result['pass_rate']:.1f}% < {QUALITY_GATE_MIN_PASS_RATE}%) "
                f"실패 코인 {gate_result['failed_coins'][:10]} - 리포트를 보내지 않는다"
            )
        if dup_rows > 0:
            raise AirflowFailException(f"당일 중복 적재 {dup_rows}건 - 리포트를 보내지 않는다")

        return gate_result

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=_quality_gate,
    )

    # ── Step 6: 중복 체크 ────────────────────────────────────
    def _check_duplicates(**context) -> int:
        """당일 중복 적재 건수를 확인합니다.

        키는 (source_ts, trade_id) 조합 - trade_id는 MySQL auto_increment
        리셋으로 재사용된 이력이 있어 단독으로는 유니크하지 않음 (07-dedup-audit.md).
        """
        from hooks.clickhouse_hook import ClickHouseHook

        target_date = _get_target_date(context)
        ms0, ms1 = _kst_day_bounds_ms(target_date)
        hook = ClickHouseHook()
        result = hook.get_scalar(
            f"""
            SELECT count() - uniqExact(source_ts, trade_id) AS dup_rows
            FROM cdc_pipeline.crypto_trades
            WHERE upbit_timestamp >= {ms0} AND upbit_timestamp < {ms1}
            SETTINGS max_memory_usage = 500000000, max_threads = 2
            """
        )
        dup_rows = int(result or 0)
        context["ti"].log.info("Duplicate rows [%s]: %d", target_date, dup_rows)
        return dup_rows

    check_duplicates = PythonOperator(
        task_id="check_duplicates",
        python_callable=_check_duplicates,
    )

    # ── Step 7: 일일 요약 리포트 생성 ────────────────────────
    def _generate_report(**context) -> dict:
        """ClickHouse mart에서 일일 요약 데이터를 조회합니다."""
        from hooks.clickhouse_hook import ClickHouseHook

        target_date = _get_target_date(context)
        ms0, ms1 = _kst_day_bounds_ms(target_date)
        hook = ClickHouseHook()

        summary = hook.get_records(
            f"""
            SELECT
                market,
                trade_count,
                round(close, 0) AS close_price,
                round(volume, 4) AS total_volume,
                round(amount, 0) AS total_amount,
                round(close_change_pct, 2) AS close_change_pct,
                round(volume_change_pct, 2) AS volume_change_pct
            FROM cdc_pipeline.mart_daily_summary
            WHERE day_kst = '{target_date}'
            ORDER BY amount DESC
            """
        )

        # 파이프라인 품질 (2026-09-17, docs/22): 리포트의 중심을 이상탐지 건수에서 "어제 데이터가 맞는가"로 옮긴다.
        # 원장 대조·수리·지연은 dbt 품질 층(dq_*)에서 읽는다. 대조는 UTC 하루 기준이라 전날 UTC 로 조회.
        quality_metrics = hook.get_first(
            f"""
            SELECT
                (SELECT weighted_pct FROM cdc_pipeline.dq_reconcile_daily WHERE day_utc = toDate('{target_date}') - 1) AS reconcile_pct,
                (SELECT cells_below_99 FROM cdc_pipeline.dq_reconcile_daily WHERE day_utc = toDate('{target_date}') - 1) AS cells_below_99,
                (SELECT sum(repairs) FROM cdc_pipeline.dq_repairs_daily WHERE day_utc = toDate('{target_date}')) AS repairs,
                (SELECT sum(rows_recovered) FROM cdc_pipeline.dq_repairs_daily WHERE day_utc = toDate('{target_date}')) AS rows_recovered,
                (SELECT lag_p95_s FROM cdc_pipeline.dq_ingest_daily WHERE day_utc = toDate('{target_date}')) AS lag_p95_s,
                (SELECT late_rows_gt_60s FROM cdc_pipeline.dq_ingest_daily WHERE day_utc = toDate('{target_date}')) AS late_rows,
                (SELECT count() FROM cdc_pipeline.market_alerts WHERE toDate(event_time) = toDate('{target_date}') AND prev_level = 0 AND level > 0) AS shadow_alerts
            """
        ) or {}

        # CDC 지연 통계 (p50/p95/p99/max)
        latency_stats = hook.get_records(
            f"""
            SELECT
                round(quantile(0.5)(cdc_latency_ms), 1) AS p50,
                round(quantile(0.95)(cdc_latency_ms), 1) AS p95,
                round(quantile(0.99)(cdc_latency_ms), 1) AS p99,
                round(max(cdc_latency_ms), 1) AS max_val
            FROM cdc_pipeline.crypto_trades
            WHERE upbit_timestamp >= {ms0} AND upbit_timestamp < {ms1}
            """
        )
        latency = {}
        if latency_stats:
            row = latency_stats[0]
            latency = {
                "p50": row.get("p50", "?"),
                "p95": row.get("p95", "?"),
                "p99": row.get("p99", "?"),
                "max": row.get("max_val", "?"),
            }

        # 이상탐지 v2 (섀도): 등급 전이 건수 - 발송 전이라 리포트에 건수만
        anomaly_rows = hook.get_records(
            f"""
            SELECT concat(alert_type, ' L', toString(level)) AS alert_type, count() AS cnt
            FROM cdc_pipeline.market_alerts
            WHERE toDate(event_time) = toDate('{target_date}') AND prev_level = 0 AND level > 0
            GROUP BY alert_type, level
            """
        )
        anomaly_counts = {
            row["alert_type"]: int(row["cnt"]) for row in anomaly_rows
        }

        # 거래소 시장경보 요약 (2026-09-21, docs/43 §12)
        # 전에는 전이가 생길 때마다 Slack 즉시 알림이었다 - 실측 하루 148.7건. 거래소 플래그가 플래핑해서
        # (KRW-MANTRA 하루 53회, 전이 간격 중앙값 180초) 채널이 그것만으로 찼고, 그 소음 때문에 정작
        # 감시자가 8시간 38분 멈춘 것을 아무도 눈치채지 못했다. 시장경보는 우리가 조치할 장애가 아니라
        # 시장 맥락이므로 여기 하루 한 줄로 옮긴다. 개별 사건은 대시보드에 그대로 있다.
        # 해제(state=0)는 세지 않는다 - market_alerts_notify 가 "강등·해제는 하루 88건 중 61건" 이라며
        # 같은 결론을 이미 내렸다.
        # markets 와 designations 를 둘 다 보여주는 이유: 둘이 벌어지면 그 자체가 플래핑의 증거다.
        exchange_flag_rows = hook.get_records(
            f"""
            SELECT flag,
                   uniqExact(market) AS markets,
                   count() AS designations,
                   arrayStringConcat(arraySlice(groupUniqArray(market), 1, 5), ', ') AS sample
            FROM cdc_pipeline.upbit_market_events
            WHERE kind = 'transition' AND state = 1
              AND toDate(observed_at + INTERVAL 9 HOUR) = toDate('{target_date}')
            GROUP BY flag
            ORDER BY markets DESC
            """
        ) or []
        exchange_flags = [
            {"flag": r["flag"], "markets": int(r["markets"]),
             "designations": int(r["designations"]), "sample": r["sample"]}
            for r in exchange_flag_rows
        ]

        return {
            "date": target_date,
            "summary": summary,
            "quality_metrics": quality_metrics,
            "latency_stats": latency,
            "anomaly_counts": anomaly_counts,
            "exchange_flags": exchange_flags,
            "coin_count": len(summary),
        }

    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
    )

    # ── Step 8: Slack 일일 리포트 발송 ───────────────────────
    def _send_report(**context) -> None:
        """품질 검증 + 일일 요약을 종합하여 Slack으로 전송."""
        ti = context["ti"]
        quality = ti.xcom_pull(task_ids="quality_gate")
        report = ti.xcom_pull(task_ids="generate_report")
        dup_rows = int(ti.xcom_pull(task_ids="check_duplicates") or 0)

        # 파이프라인 실행 시간 계산
        dag_run = context.get("dag_run")
        exec_seconds = "?"
        if dag_run and dag_run.start_date:
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            exec_seconds = int((now - dag_run.start_date).total_seconds())

        target_date = _get_target_date(context)
        report_data = {
            "date": target_date,
            "quality": quality,
            "summary": report.get("summary", []) if report else [],
            "quality_metrics": report.get("quality_metrics", {}) if report else {},
            "latency_stats": report.get("latency_stats", {}) if report else {},
            "anomaly_counts": report.get("anomaly_counts", {}) if report else {},
            "exchange_flags": report.get("exchange_flags", []) if report else [],
            "duplicates_found": dup_rows,
            "execution_seconds": exec_seconds,
        }

        send_daily_report(report_data)
        ti.log.info("Daily report sent to Slack")

    slack_report = PythonOperator(
        task_id="slack_daily_report",
        python_callable=_send_report,
    )

    # ── DAG 의존성 ───────────────────────────────────────────
    # 2026-09-20 (docs/39 §2 ②): dbt 가 끝났다는 사실을 품질 판정에 직접 전달한다.
    # 전에는 quality_alerts 가 매시 :50 에 돌며 "그때쯤 끝났겠지"라고 추측했고, dbt 가 늦으면 옛 데이터로 판정했다.
    # wait_for_completion=False: 판정이 이 DAG 을 붙잡지 않게(리포트가 판정을 기다릴 이유가 없다).
    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_quality_alerts",
        trigger_dag_id="quality_alerts",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    dbt_source_freshness >> dbt_run >> dbt_test >> get_coins >> validate_coins >> quality_gate
    dbt_test >> trigger_quality
    dbt_test >> check_duplicates >> quality_gate
    quality_gate >> generate_report >> slack_report
