{#
  증분 모델의 '기준 시각' (docs/40 ⑩ · docs/39 §3 2단계, 2026-09-20).

  문제: 증분 모델이 창을 `now()` 로 잘라서 **과거 날짜를 다시 만들 수 없었다.**
  Airflow 에서 9월 12일을 재실행해도 오늘 창을 다시 계산했다. 대조·백업은 `{{ ds }}` 를 쓰는데
  dbt 만 안 써서, 한 파이프라인 안에서 재실행 가능 여부가 갈렸다.
  "9월 12일 마트가 틀렸으니 그날만 다시 만들어라"에 답이 없는 상태였다.

  해법: 기준 시각을 변수로 받는다. **기본값은 now() 라 평소 동작은 그대로다.**
    평소   dbt run
    백필   dbt run --vars '{"run_date": "2026-09-12"}'  (Airflow 는 {{ ds }} 를 넘긴다)
#}

{%- macro run_anchor() -%}
    {%- set rd = var('run_date', none) -%}
    {%- if rd -%}
        toDateTime('{{ rd }} 00:00:00')
    {%- else -%}
        now()
    {%- endif -%}
{%- endmacro -%}

{#- 백필 모드인가 (상한을 걸어야 하는가) -#}
{%- macro is_backfill() -%}
    {{ 'true' if var('run_date', none) else 'false' }}
{%- endmacro -%}
