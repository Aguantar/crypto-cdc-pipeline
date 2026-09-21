#!/bin/bash
cd ~/cdc-realtime-pipeline/dbt_cdc_pipeline
dbt run --profiles-dir . >> /tmp/dbt_run.log 2>&1
dbt test --profiles-dir . >> /tmp/dbt_test.log 2>&1
echo "[$(date)] dbt run+test completed" >> /tmp/dbt_cron.log
