"""ClickHouse Hook for Apache Airflow.

HTTP 인터페이스(8123)를 사용하여 ClickHouse에 쿼리를 실행합니다.
별도 Python 드라이버 설치 없이 requests(Airflow 내장)만으로 동작합니다.
"""

from __future__ import annotations

import csv
import io
from typing import Any

import requests
from airflow.hooks.base import BaseHook


class ClickHouseHook(BaseHook):
    """ClickHouse HTTP 인터페이스 기반 Hook.

    Airflow Connection 설정:
        conn_id: clickhouse_default
        host: cdc-clickhouse (Docker 네트워크 내 컨테이너명)
        port: 8123
        schema: cdc_pipeline
        login: default
    """

    conn_name_attr = "clickhouse_conn_id"
    default_conn_name = "clickhouse_default"
    conn_type = "http"

    def __init__(self, clickhouse_conn_id: str = "clickhouse_default") -> None:
        super().__init__()
        self.clickhouse_conn_id = clickhouse_conn_id

    def _get_base_url(self) -> str:
        conn = self.get_connection(self.clickhouse_conn_id)
        host = conn.host or "cdc-clickhouse"
        port = conn.port or 8123
        return f"http://{host}:{port}"

    def _get_params(self) -> dict[str, str]:
        conn = self.get_connection(self.clickhouse_conn_id)
        params: dict[str, str] = {}
        if conn.login:
            params["user"] = conn.login
        if conn.password:
            params["password"] = conn.password
        if conn.schema:
            params["database"] = conn.schema
        return params

    def execute(self, sql: str) -> str:
        """SQL을 실행하고 raw 텍스트 결과를 반환합니다."""
        base_url = self._get_base_url()
        params = self._get_params()
        # 2026-09-20: str 을 그대로 주면 requests 가 latin-1 로 인코딩해 한글(①·무수집 등)이 UnicodeEncodeError - bytes 로 보낸다 (docs/32 다이제스트 저장 실패에서 발견)
        response = requests.post(base_url, params=params, data=sql.encode("utf-8"), headers={"Content-Type": "text/plain; charset=utf-8"}, timeout=60)
        response.raise_for_status()
        return response.text.strip()

    def get_records(self, sql: str) -> list[dict[str, Any]]:
        """SQL 실행 결과를 dict 리스트로 반환합니다 (FORMAT CSVWithNames)."""
        if "FORMAT" not in sql.upper():
            sql = sql.rstrip().rstrip(";") + " FORMAT CSVWithNames"
        raw = self.execute(sql)
        if not raw:
            return []
        reader = csv.DictReader(io.StringIO(raw))
        return list(reader)

    def get_first(self, sql: str) -> dict[str, Any] | None:
        """첫 번째 행만 반환합니다."""
        records = self.get_records(sql)
        return records[0] if records else None

    def get_scalar(self, sql: str) -> str | None:
        """단일 값을 반환합니다 (COUNT, MAX 등)."""
        result = self.execute(sql)
        return result if result else None

    def test_connection(self) -> tuple[bool, str]:
        """연결 테스트."""
        try:
            result = self.execute("SELECT 1")
            return True, f"Connection successful: {result}"
        except Exception as e:
            return False, str(e)
