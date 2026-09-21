FROM apache/airflow:2.8.1-python3.10

USER airflow

RUN pip install --no-cache-dir \
    dbt-core==1.7.9 \
    dbt-clickhouse==1.7.2 \
    statsd>=3.3 \
    pytest>=7.4  # DAG 구조 테스트(airflow/tests)를 컨테이너 안에서 실행
