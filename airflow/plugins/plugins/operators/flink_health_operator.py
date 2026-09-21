"""Flink Health Check Operator for Apache Airflow.

Flink REST API를 호출하여 잡 상태를 확인하고 결과를 XCom에 push합니다.
"""

from __future__ import annotations

from typing import Any, Sequence

import requests
from airflow.models import BaseOperator


class FlinkHealthOperator(BaseOperator):
    """Flink REST API로 잡 상태를 확인하는 Operator.

    :param flink_base_url: Flink JobManager REST API URL
    :param expected_jobs: RUNNING 상태여야 하는 최소 잡 수
    """

    template_fields: Sequence[str] = ()
    ui_color = "#d4e6f1"
    ui_fgcolor = "#000000"

    def __init__(
        self,
        *,
        flink_base_url: str = "http://flink-jobmanager:8081",
        expected_jobs: int = 2,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.flink_base_url = flink_base_url
        self.expected_jobs = expected_jobs

    def execute(self, context: Any) -> dict[str, Any]:
        self.log.info("Checking Flink jobs at %s", self.flink_base_url)

        try:
            response = requests.get(
                f"{self.flink_base_url}/jobs/overview", timeout=10
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self.log.error("Flink API unreachable: %s", e)
            return {
                "healthy": False,
                "error": str(e),
                "running_jobs": 0,
                "jobs": [],
            }

        jobs = data.get("jobs", [])
        running_jobs = [j for j in jobs if j.get("state") == "RUNNING"]

        result = {
            "healthy": len(running_jobs) >= self.expected_jobs,
            "running_jobs": len(running_jobs),
            "expected_jobs": self.expected_jobs,
            "jobs": [
                {
                    "name": j.get("name", "unknown"),
                    "state": j.get("state", "unknown"),
                    "start_time": j.get("start-time", 0),
                }
                for j in jobs
            ],
        }

        self.log.info(
            "Flink status: %d/%d jobs running",
            len(running_jobs),
            self.expected_jobs,
        )

        if not result["healthy"]:
            self.log.warning(
                "Flink health check FAILED: expected %d running jobs, found %d",
                self.expected_jobs,
                len(running_jobs),
            )

        return result
