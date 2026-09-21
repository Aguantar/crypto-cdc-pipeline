"""Slack 알림 콜백 모듈.

DAG/태스크의 성공/실패 시 Slack Webhook으로 컨텍스트 포함 알림을 전송합니다.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


def _get_webhook_url() -> str | None:
    """Airflow Variable에서 Slack Webhook URL을 가져옵니다."""
    try:
        return Variable.get("slack_webhook_url", default_var=None)
    except Exception:
        return None


def _send_slack_message(payload: dict[str, Any]) -> None:
    """Slack Webhook으로 메시지를 전송합니다."""
    webhook_url = _get_webhook_url()
    if not webhook_url:
        logger.warning("Slack webhook URL not configured (Variable: slack_webhook_url)")
        return

    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("Failed to send Slack notification: %s", e)


def task_failure_callback(context: dict[str, Any]) -> None:
    """태스크 실패 시 컨텍스트 포함 Slack 알림."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("execution_date", datetime.now())
    exception = context.get("exception", "No exception info")
    log_url = task_instance.log_url if task_instance else ""

    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Task Failed: {dag_id}.{task_id}",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id}"},
                    {"type": "mrkdwn", "text": f"*Task:*\n{task_id}"},
                    {
                        "type": "mrkdwn",
                        "text": f"*Execution Date:*\n{execution_date}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Error:*\n```{str(exception)[:300]}```",
                    },
                ],
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Log"},
                        "url": log_url,
                    }
                ],
            },
        ]
    }
    _send_slack_message(payload)


def task_success_callback(context: dict[str, Any]) -> None:
    """태스크 성공 시 Slack 알림 (DAG-level에서만 사용 권장)."""
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    execution_date = context.get("execution_date", datetime.now())

    payload = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*DAG Completed:* `{dag_id}` | {execution_date}",
                },
            }
        ]
    }
    _send_slack_message(payload)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """SLA miss 시 Slack 알림."""
    task_names = [t.task_id for t in task_list]
    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "SLA Miss Alert",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n{dag.dag_id}"},
                    {
                        "type": "mrkdwn",
                        "text": f"*Tasks:*\n{', '.join(task_names)}",
                    },
                ],
            },
        ]
    }
    _send_slack_message(payload)


def send_health_alert(unhealthy_components: list[dict[str, Any]]) -> None:
    """헬스체크 이상 발견 시 Slack 알림."""
    fields = []
    for comp in unhealthy_components:
        fields.append(
            {
                "type": "mrkdwn",
                "text": f"*{comp['name']}:*\n{comp['message']}",
            }
        )

    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Pipeline Health Alert",
                },
            },
            {"type": "section", "fields": fields[:10]},
        ]
    }
    _send_slack_message(payload)


# 거래소 시장경보 플래그의 한글 이름 (2026-09-21). 영문 상수를 그대로 보여주면 읽는 사람이
# 매번 머릿속에서 번역해야 한다. 모르는 플래그가 새로 생기면 원문 그대로 둔다 - 아는 척하지 않는다.
EXCHANGE_FLAG_KO = {
    "TRADING_VOLUME_SOARING": "거래량 급등",
    "DEPOSIT_AMOUNT_SOARING": "입금량 급등",
    "GLOBAL_PRICE_DIFFERENCES": "국내외 가격차",
    "PRICE_FLUCTUATIONS": "가격 급변",
    "CONCENTRATION_OF_SMALL_ACCOUNTS": "소수 계정 집중",
}


def send_daily_report(report_data: dict[str, Any]) -> None:
    """일일 리포트 Slack 전송."""
    date = report_data.get("date", "N/A")
    quality = report_data.get("quality", {})
    summary = report_data.get("summary", [])
    qm = report_data.get("quality_metrics", {}) or {}
    duplicates = report_data.get("duplicates_found", 0)
    latency = report_data.get("latency_stats", {})
    anomaly_counts = report_data.get("anomaly_counts", {})
    exchange_flags = report_data.get("exchange_flags", []) or []
    execution_sec = report_data.get("execution_seconds", "?")

    # Quality 상태 이모지
    pass_rate = quality.get("pass_rate", 0)
    if pass_rate == 100:
        q_emoji = ":large_green_circle:"
    elif pass_rate >= 80:
        q_emoji = ":large_yellow_circle:"
    else:
        q_emoji = ":red_circle:"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"CDC Daily Report - {date}",
            },
        },
        # 파이프라인 실행 요약
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{q_emoji} *Pipeline Summary*\n"
                    f"실행 시간: {execution_sec}s | "
                    f"코인: {quality.get('total', 0)}개 | "
                    f"품질 통과: {quality.get('passed', 0)}/{quality.get('total', 0)} "
                    f"({pass_rate}%)"
                ),
            },
        },
        {"type": "divider"},
    ]

    # 실패 코인 상세 (있을 때만)
    failed_coins = quality.get("failed_coins", [])
    if failed_coins:
        fail_lines = []
        for fc in failed_coins[:5]:
            issues = ", ".join(fc.get("issues", []))
            fail_lines.append(f"• {fc['coin']}: {issues}")
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":warning: *Quality Issues*\n" + "\n".join(fail_lines),
            },
        })

    # CDC 지연 통계
    if latency:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    ":stopwatch: *CDC Latency*\n"
                    f"p50: {latency.get('p50', '?')}ms | "
                    f"p95: {latency.get('p95', '?')}ms | "
                    f"p99: {latency.get('p99', '?')}ms | "
                    f"max: {latency.get('max', '?')}ms"
                ),
            },
        })

    # Top 코인 (전일 대비 변화 포함)
    if summary:
        coin_lines = []
        for row in summary[:5]:
            market = row.get("market", "?")
            trades = row.get("trade_count", "?")
            close = row.get("close_price", "?")
            vol_chg = row.get("volume_change_pct", None)
            price_chg = row.get("close_change_pct", None)

            chg_parts = []
            if price_chg is not None and price_chg != 0:
                sign = "+" if float(price_chg) > 0 else ""
                chg_parts.append(f"가격 {sign}{price_chg}%")
            if vol_chg is not None and vol_chg != 0:
                sign = "+" if float(vol_chg) > 0 else ""
                chg_parts.append(f"거래량 {sign}{vol_chg}%")

            chg_text = f" ({', '.join(chg_parts)})" if chg_parts else ""
            coin_lines.append(
                f"  {market}: {trades:,} trades, ₩{close:,}{chg_text}"
                if isinstance(trades, int) and isinstance(close, (int, float))
                else f"  {market}: {trades} trades, ₩{close}{chg_text}"
            )

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":chart_with_upwards_trend: *Top Coins*\n```\n"
                + "\n".join(coin_lines) + "\n```",
            },
        })

    # 이상 탐지 요약
    if anomaly_counts:
        total_anomalies = sum(anomaly_counts.values())
        if total_anomalies > 0:
            anomaly_parts = [f"{k}: {v}" for k, v in anomaly_counts.items() if v > 0]
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f":rotating_light: *Market Alerts v2 (섀도, 미발송)* ({total_anomalies}건 전이)\n"
                        + " | ".join(anomaly_parts)
                    ),
                },
            })

    # 거래소 시장경보 (2026-09-21, docs/43 §12): 즉시 알림에서 하루 한 번 요약으로 옮겼다.
    # 지정(state=1)만 센다. 마켓 수와 지정 횟수를 나란히 두어, 둘이 벌어지면 거래소 플래그가
    # 플래핑 중이라는 뜻임을 한눈에 보이게 한다.
    if exchange_flags:
        flag_lines = []
        for f in exchange_flags:
            name = EXCHANGE_FLAG_KO.get(f["flag"], f["flag"])
            mk, dg = f["markets"], f["designations"]
            repeat = f" (지정 {dg}회 - 반복 지정·해제)" if dg > mk * 2 else ""
            flag_lines.append(f"  {name}: {mk}개 마켓{repeat}\n    {f['sample']}" + ("  외" if mk > 5 else ""))
        total_mk = sum(f["markets"] for f in exchange_flags)
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (f":triangular_flag_on_post: *거래소 시장경보* (어제 지정 {total_mk}건, 즉시 알림 없음)\n```\n"
                         + "\n".join(flag_lines) + "\n```"),
            },
        })

    # 파이프라인 품질 (docs/22): 어제 데이터가 맞는가 - 원장 대조·수리·지연·늦은 행·섀도 전이
    if qm:
        rp = qm.get("reconcile_pct") or "-"
        cb = qm.get("cells_below_99") or 0
        rep = qm.get("repairs") or 0
        rec = qm.get("rows_recovered") or 0
        pq = ":large_green_circle:" if str(cb) == "0" and rp not in ("-", "", None) else ":large_yellow_circle:"
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{pq} *Pipeline Quality*\n"
                    f"원장 대조(전날 UTC) {rp}% · 99% 미만 셀 {cb} | 수리 {rep}회 / {rec}행 | "
                    f"지연 p95 {qm.get('lag_p95_s') or '-'}s · 늦은 행 {qm.get('late_rows') or 0} | 섀도 전이 {qm.get('shadow_alerts') or 0}"
                ),
            },
        })

    # 중복 + 푸터
    footer_parts = []
    if duplicates > 0:
        footer_parts.append(f":warning: 중복 적재: {duplicates}건 (source_ts, trade_id)")
    else:
        footer_parts.append(":white_check_mark: 중복 0건")

    blocks.append({
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": " | ".join(footer_parts)}
        ],
    })

    _send_slack_message({"blocks": blocks})


def send_text_report(title: str, lines: list[str]) -> None:
    """제목 + 줄 목록(마크다운). 주간 다이제스트·품질 판정용 (docs/32). 3,000자 블록 한도를 넘지 않게 여러 블록으로 나눈다."""
    blocks: list[dict[str, Any]] = [{"type": "header", "text": {"type": "plain_text", "text": title[:150]}}]
    chunk: list[str] = []; size = 0
    for line in lines:
        if size + len(line) + 1 > 2800 and chunk:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(chunk)}}); chunk = []; size = 0
        chunk.append(line); size += len(line) + 1
    if chunk:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(chunk)}})
    _send_slack_message({"blocks": blocks[:50]})
