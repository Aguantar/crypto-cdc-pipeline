FROM apache/airflow:2.8.1-python3.10

USER airflow

RUN pip install --no-cache-dir \
    statsd>=3.3 \
    pytest>=7.4  # DAG 구조 테스트(airflow/tests)를 컨테이너 안에서 실행

# 2026-09-24 (docs/45): dbt 는 Airflow 와 다른 venv 에 둔다. dbt-core 1.11 은 protobuf 6·opentelemetry 1.44 를 요구해
# Airflow 2.8.1 의 pin(protobuf<5, opentelemetry 1.22)과 충돌한다. 같은 site-packages 에 넣으면 pip check 가 5건 깨진다.
RUN python -m venv /home/airflow/dbt-venv \
    && PIP_USER=false /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.11.6 dbt-clickhouse==1.10.0 \
    && ln -s /home/airflow/dbt-venv/bin/dbt /home/airflow/.local/bin/dbt
