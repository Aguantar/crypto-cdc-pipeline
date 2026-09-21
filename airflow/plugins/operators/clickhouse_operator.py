"""ClickHouse Operator for Apache Airflow.

ClickHouseHook을 사용하여 쿼리를 실행하고 결과를 XCom에 push합니다.
Jinja 템플릿({{ ds }} 등)을 지원하여 idempotent 실행이 가능합니다.
"""

from __future__ import annotations

from typing import Any, Sequence

from airflow.models import BaseOperator

from hooks.clickhouse_hook import ClickHouseHook


class ClickHouseOperator(BaseOperator):
    """ClickHouse 쿼리를 실행하고 결과를 XCom에 push하는 Operator.

    :param sql: 실행할 SQL 쿼리 (Jinja 템플릿 지원)
    :param clickhouse_conn_id: Airflow Connection ID
    :param result_type: 결과 반환 형태 ('records', 'scalar', 'first')
    """

    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#e8f7e4"
    ui_fgcolor = "#000000"

    def __init__(
        self,
        *,
        sql: str,
        clickhouse_conn_id: str = "clickhouse_default",
        result_type: str = "records",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.clickhouse_conn_id = clickhouse_conn_id
        self.result_type = result_type

    def execute(self, context: Any) -> Any:
        hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)
        self.log.info("Executing ClickHouse query: %s", self.sql[:200])

        if self.result_type == "scalar":
            result = hook.get_scalar(self.sql)
        elif self.result_type == "first":
            result = hook.get_first(self.sql)
        else:
            result = hook.get_records(self.sql)

        self.log.info("Query returned: %s", str(result)[:500])
        return result
