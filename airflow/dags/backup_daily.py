"""DAG 4: backup_daily - ClickHouse 증분 백업 + 호가 원본 Parquet 롤링 + Oracle 오프사이트 동기화 (일 1회).

무엇을 (docs/21 §1·§4):
  1. ClickHouse 네이티브 백업. 최신 전체가 7일 넘게 오래됐으면 전체, 아니면 최신 전체를 base 로 한 증분. 대상 = cdc_pipeline 전체 − BACKUP_EXCLUDE.
  2. orderbook_raw 의 전날(UTC) 파티션을 Parquet(zstd) 로 내보내 75일 롤링. 원본은 7일 TTL 로 지워지므로 이것이 유일한 장기 보존본.
  3. rsync 로 Oracle /mnt/backup 에 동기화하고, 원격 보존 정책(Parquet 75일, 전체 2세대, 증분은 최신 전체 이후 3일)을 적용하고,
     로컬 스테이징을 원격과 같은 규칙으로 정리한 뒤, dry-run rsync 로 "전송할 것 0" 을 확인한다(검증은 정리가 실패해도 돈다).
왜 이렇게:
  - 백업은 다른 호스트·다른 디스크에 있어야 백업이다. 미니PC 로컬 사본은 스테이징이고 며칠만 둔다.
  - 전송을 Airflow 에서 하는 이유: 실패가 DAG 실패로 보이고 재시도·알림이 같은 자리에서 된다. 호스트 cron 은 조용히 실패한다.
  - Airflow 컨테이너가 ClickHouse(uid 101) 가 만든 750 디렉터리를 읽어야 해서 보조 그룹 101 로 실행한다(compose group_add). ssh 키는 저장소 밖 사본(uid 50000, 600).
  - 검증은 "전송했다"가 아니라 "원격이 로컬과 같다"(rsync -n 결과 0건)로 한다. 백업이 있다는 말은 리허설(docs/21) 뒤에만 한다 - 복구 리허설은 분기마다 수동.
  - Parquet 은 zstd: 같은 하루(19.7M행)가 lz4 977MB → zstd 726MB (docs/21 실측).
  - 2026-10-07 (docs/51) 용량 재산정. 09-17 의 "증분 80MB/일" 은 base 직후 하루치였고, 머지가 파트를 다시 쓰므로 증분은 매일 커져
    보름이면 전체 크기(16GB)가 됐다. 월 1회 전체 + 전체 3세대 보존 = 3개월치 증분을 다 들고 있는 설계였고 원격 147GB 가 10-01 에 찼다.
    새 산정(실측 기준): 전체 16GB × 2세대 32 + 증분(최신 전체 이후 3일, 하루 약 +1GB 로 커져 최대 5+6+7) 18 + Parquet 0.8GB × 75일 60
    = 약 110GB = 75%. 헬스체크가 85% 에서 울린다. 증분을 전체 기준으로 두는 이유: 체인(전날 기준)은 하나라도 깨지면 그 뒤를 전부 못 쓴다.
  - 2026-10-07 (docs/51) 실패가 조용했던 이유 둘을 고쳤다. `rsync … | tail` 은 tail 의 종료 코드를 돌려줘 "No space left" 가 6일간
    success 로 찍혔다 → 모든 bash 태스크에 `set -euo pipefail`. 검증(verify)이 prune 뒤에 있어 prune 이 권한으로 죽자 검증이 한 번도
    안 돌았다 → 검증은 prune 의 성공 여부와 무관하게 돈다(trigger_rule all_done).
  - 2026-10-08 (docs/51 §8) 첫 실행에서 검증이 15,400 파일로 실패했다. 원격 보존이 지운 옛 증분을 로컬이 아직 들고 있어서다. 검증의 뜻이
    "원격 ⊇ 로컬" 이므로 로컬 정리가 원격과 같은 규칙으로 검증보다 먼저 돌아야 한다. 정리가 실패하면 검증도 실패하는데, 그건 맞는 신호다.
  - 백업 이름은 실행일(data_interval_end), Parquet 이름은 데이터 날짜(ds). 백업은 '그 시점의 상태'이고 Parquet 은 '그 날의 데이터'라서.
  - 대상일 = {{ ds }} (전날). 스케줄 01:20 UTC: 전날 UTC 파티션이 닫힌 뒤이고 06:35 대조·16:00 daily_pipeline 과 겹치지 않는다.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from callbacks.slack_callbacks import task_failure_callback

# 백업에서 빼는 표와 그 이유 (2026-09-20, docs/34 #10).
# 원칙: "지금 복구에 쓸 수 있는 유일한 사본인가". 아니면 뺀다. 크기가 기준이 아니다.
BACKUP_EXCLUDE = {
    # 호가 원본: 같은 DAG 이 하루치를 Parquet(zstd, 원격 120일)으로 따로 내보낸다. 백업에 또 넣으면 두 배다.
    "orderbook_raw",
    # 2026-09-25: 전환 롤백본 넷(crypto_trades_rmt·_v2·*_float_bak)은 이날 전부 DROP 했다(docs/48). 09-20 증분이
    # 8.7GB 가 된 원인이 그 넷이었고, 살아 있는 표만 남았으니 여기 적을 것이 없다.
}
# binance_orderbook_raw 는 일부러 넣는다. Parquet 아카이브가 없어 백업이 30일 TTL 밖의 유일한 사본이고,
# 23 MiB 라 비용이 사실상 없다. docs/34 #10 의 원래 계획("Binance 표 제외")을 실측 뒤 뒤집은 것 - 제외 기준은
# 거래소별 소속이 아니라 '다른 사본이 있는가' 여야 한다.

BACKUP_ROOT = "/backups"                     # 호스트 ~/clickhouse-backups (compose 바인드 마운트, ClickHouse 컨테이너와 공유)
REMOTE = "ubuntu@10.88.0.1"                  # Oracle, WireGuard 터널
REMOTE_ROOT = "/mnt/backup"
SSH = "ssh -i /opt/airflow/secrets/oci_key -o UserKnownHostsFile=/opt/airflow/secrets/known_hosts -o StrictHostKeyChecking=yes -o ConnectTimeout=15"
PARQUET_KEEP_DAYS_REMOTE = 60                # 2026-10-08 (docs/51 §8): 75 → 60. 전체 백업이 10-08 에 20.4GB 였고 Binance 30일 TTL 이 차면 약 24GB.
                                             #   2세대 48 + 증분 3일 7 + Parquet 0.85GB × 60일 51 = 106GB = 72%. 75일이면 81% 로 알림선(85%)에 붙는다
PARQUET_KEEP_DAYS_LOCAL = 3
INCR_KEEP_LOCAL = 3                          # 2026-10-08: 로컬 증분 규칙을 원격과 같게(최신 전체 이후 것 중 최근 3개). 10-07 의 mtime 3일 규칙은
                                             #   원격 보존이 지운 증분을 로컬이 들고 있게 해 전체 백업 날마다 검증(원격 ⊇ 로컬)을 깨뜨렸다 (docs/51 §8)
INCR_KEEP_DAYS_REMOTE = 3                    # 최신 전체 이후 것만, 그중 최근 3일
FULL_KEEP_REMOTE = 2                         # 2026-10-07: 3 → 2. 주 1회 전체이므로 2세대 = 약 2주
FULL_EVERY_DAYS = 7                          # 2026-10-07: 월 1회 → 7일마다. 최신 전체가 이보다 오래되면 전체


def _ch_exec(sql: str, **params) -> str:
    """ClickHouse HTTP 로 실행 (pipeline 사용자, BACKUP 권한 있음). 요청마다 설정을 명시한다."""
    import requests

    url = "http://cdc-clickhouse:8123/"
    p = {"user": os.environ["CLICKHOUSE_PIPELINE_USER"], "password": os.environ["CLICKHOUSE_PIPELINE_PASSWORD"], **params}
    r = requests.post(url, params=p, data=sql.encode(), timeout=3600)
    r.raise_for_status()
    return r.text.strip()


def _except() -> str:
    """EXCEPT TABLES 절. 실재하지 않는 표를 적으면 ClickHouse 가 에러를 내므로 지금 있는 것만 넣는다
    (롤백본은 삭제 예정이라 곧 사라진다 - 그때 백업이 깨지면 안 된다)."""
    live = set(_ch_exec("SELECT name FROM system.tables WHERE database = 'cdc_pipeline' FORMAT TSV").split())
    names = sorted(BACKUP_EXCLUDE & live)
    return ", ".join(names)


def _latest_full() -> str | None:
    fulls = sorted(d for d in os.listdir(BACKUP_ROOT) if d.startswith("full_") and os.path.isdir(os.path.join(BACKUP_ROOT, d)))
    return fulls[-1] if fulls else None


def _clickhouse_backup(**context) -> dict:
    run_day = context["data_interval_end"]          # 실행일 = 백업 시점
    tag = run_day.strftime("%Y%m%d")
    base = _latest_full()
    base_age_days = (run_day.date() - datetime.strptime(base[5:], "%Y%m%d").date()).days if base else None
    # 2026-10-07 (docs/51): 달력(매월 1일)이 아니라 최신 전체의 나이로 정한다. 하루 실패해도 다음 날 전체가 만들어진다.
    if base is None or base_age_days >= FULL_EVERY_DAYS:
        name = f"full_{tag}"
        sql = f"BACKUP DATABASE cdc_pipeline EXCEPT TABLES {_except()} TO File('{BACKUP_ROOT}/{name}')"
    else:
        name = f"incr_{tag}"
        sql = f"BACKUP DATABASE cdc_pipeline EXCEPT TABLES {_except()} TO File('{BACKUP_ROOT}/{name}') SETTINGS base_backup = File('{BACKUP_ROOT}/{base}')"
    if os.path.isdir(os.path.join(BACKUP_ROOT, name)):
        context["ti"].log.info("backup %s already exists - idempotent skip", name)
    else:
        out = _ch_exec(sql)
        context["ti"].log.info("BACKUP → %s", out)
    size = subprocess.run(["du", "-sb", os.path.join(BACKUP_ROOT, name)], capture_output=True, text=True).stdout.split()[0]
    status = _ch_exec(f"SELECT status, num_files, formatReadableSize(total_size) FROM system.backups WHERE name = 'File(\\'{BACKUP_ROOT}/{name}\\')' ORDER BY start_time DESC LIMIT 1 FORMAT TSV")
    result = {"name": name, "base": base if name.startswith("incr_") else None, "bytes": int(size), "status": status}
    context["ti"].log.info("clickhouse backup: %s", result)
    return result


def _export_parquet(**context) -> dict:
    ds = context["ds"]
    out = os.path.join(BACKUP_ROOT, "parquet", f"orderbook_raw_{ds}.parquet")
    src_rows = int(_ch_exec(f"SELECT count() FROM cdc_pipeline.orderbook_raw WHERE toDate(ts) = '{ds}'"))
    if src_rows == 0:
        raise RuntimeError(f"orderbook_raw has 0 rows for {ds} - nothing to archive (TTL 7d: was it already dropped?)")
    if not os.path.exists(out):
        import requests
        p = {"user": os.environ["CLICKHOUSE_PIPELINE_USER"], "password": os.environ["CLICKHOUSE_PIPELINE_PASSWORD"],
             "max_memory_usage": 800_000_000, "max_threads": 2,
             "output_format_parquet_row_group_size": 200_000, "output_format_parquet_compression_method": "zstd"}
        tmp = out + ".part"
        with requests.post("http://cdc-clickhouse:8123/", params=p,
                           data=f"SELECT * FROM cdc_pipeline.orderbook_raw WHERE toDate(ts) = '{ds}' ORDER BY market, ts FORMAT Parquet".encode(),
                           stream=True, timeout=3600) as r:
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 << 20):
                    f.write(chunk)
        os.replace(tmp, out)
    size = os.path.getsize(out)
    with open(out, "rb") as f:
        head = f.read(4); f.seek(-4, 2); tail = f.read(4)
    if head != b"PAR1" or tail != b"PAR1" or size < 1 << 20:
        raise RuntimeError(f"parquet sanity failed: size={size} head={head!r} tail={tail!r}")
    result = {"file": os.path.basename(out), "bytes": size, "source_rows": src_rows}
    context["ti"].log.info("parquet export: %s", result)
    return result


default_args = {
    "owner": "calme",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # 2026-09-20 (docs/39 §2 ⑥): 외부 API·컨테이너가 흔들릴 때 고정 간격 재시도는 같은 실패를 반복한다
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": task_failure_callback,
}

with DAG(
    dag_id="backup_daily",
    default_args=default_args,
    description="ClickHouse 증분 백업 + 호가 Parquet 롤링 + Oracle 오프사이트 동기화·검증",
    schedule="20 1 * * *",
    start_date=datetime(2026, 9, 16),
    catchup=False,
    max_active_runs=1,
    tags=["backup", "storage"],
    doc_md=__doc__,
) as dag:

    clickhouse_backup = PythonOperator(task_id="clickhouse_backup", python_callable=_clickhouse_backup, execution_timeout=timedelta(minutes=30))
    export_parquet = PythonOperator(task_id="export_orderbook_parquet", python_callable=_export_parquet, execution_timeout=timedelta(minutes=30))

    # rsync: 새 디렉터리·파일만 전송(-a), 부분 전송 이어받기. --delete 는 쓰지 않는다 - 원격 보존은 아래 정책 태스크가 명시적으로 한다.
    # --chmod=ugo+rX: ClickHouse 가 750 으로 만든 디렉터리를 원격에서 복원 컨테이너(uid 101)가 읽게 (리허설 첫 실패의 원인, docs/21 §1)
    sync_to_oracle = BashOperator(
        task_id="sync_to_oracle",
        sla=timedelta(hours=2),   # 2026-09-20 (docs/39 §2 ⑤): 백업이 늦으면 다음 백업 창과 겹친다,
        bash_command=(
            f'set -euo pipefail; rsync -a --partial --chmod=ugo+rX --info=stats1 -e "{SSH}" {BACKUP_ROOT}/ {REMOTE}:{REMOTE_ROOT}/clickhouse/ 2>&1 | tail -6'
        ),
        execution_timeout=timedelta(minutes=60),
    )

    # 원격 보존: Parquet 75일, 전체 최근 2세대, 증분은 최신 전체 이후 것 중 최근 3일만. 마지막 줄의 df 는 로그용이고 판정은 health_check 가 한다.
    apply_remote_retention = BashOperator(
        task_id="apply_remote_retention",
        bash_command=(
            f'set -euo pipefail; {SSH} {REMOTE} \'set -e; cd {REMOTE_ROOT}/clickhouse; '
            f'find parquet -name "orderbook_raw_*.parquet" -mtime +{PARQUET_KEEP_DAYS_REMOTE} -print -delete | sed "s/^/pruned parquet: /"; '
            f'fulls=$(ls -d full_* 2>/dev/null | sort); keep=$(echo "$fulls" | tail -n {FULL_KEEP_REMOTE}); '
            f'for d in $fulls; do echo "$keep" | grep -qx "$d" || {{ echo "pruned full: $d"; rm -rf "$d"; }}; done; '
            f'latest=$(echo "$keep" | tail -n 1 | sed "s/full_//"); '
            f'for d in $(ls -d incr_* 2>/dev/null); do t=${{d#incr_}}; [ "$t" -lt "$latest" ] && {{ echo "pruned incr (older than latest full): $d"; rm -rf "$d"; }}; done; '
            f'for d in $(ls -d incr_* 2>/dev/null | sort | head -n -{INCR_KEEP_DAYS_REMOTE}); do echo "pruned incr (keep {INCR_KEEP_DAYS_REMOTE}): $d"; rm -rf "$d"; done; '
            f'df -h {REMOTE_ROOT} | tail -1; du -sh {REMOTE_ROOT}/clickhouse\''
        ),
    )

    # 로컬(스테이징) 보존: Parquet 3일, 증분 3일, 전체는 최신 1개만 (base 로 필요).
    # ClickHouse(uid 101)가 750 으로 만드는 디렉터리를 Airflow(50000, 보조그룹 101)가 지우려면 그룹 쓰기가 필요하다.
    # 2026-10-07 (docs/51): 이 권한이 없어 prune 이 한 번도 성공한 적이 없었다. 호스트 cron 01:30 UTC 가 ClickHouse 컨테이너 root 로
    # `chmod -R g+w /backups` 를 한다(땜질. 맞는 방향은 ClickHouse 가 오브젝트 스토리지로 직접 백업하고 수명주기 정책이 지우는 것).
    prune_local = BashOperator(
        task_id="prune_local",
        bash_command=(
            f'set -euo pipefail; cd {BACKUP_ROOT}; '
            f'find parquet -name "orderbook_raw_*.parquet" -mtime +{PARQUET_KEEP_DAYS_LOCAL} -print -delete | sed "s/^/pruned local parquet: /"; '
            f'fulls=$(ls -d full_* | sort); for d in $(echo "$fulls" | head -n -1); do echo "pruned local full: $d"; rm -rf "$d"; done; '
            f'latest=$(echo "$fulls" | tail -n 1 | sed "s/full_//"); '
            f'for d in $(ls -d incr_* 2>/dev/null); do t=${{d#incr_}}; [ "$t" -lt "$latest" ] && {{ echo "pruned local incr (older than latest full): $d"; rm -rf "$d"; }}; done; '
            f'for d in $(ls -d incr_* 2>/dev/null | sort | head -n -{INCR_KEEP_LOCAL}); do echo "pruned local incr (keep {INCR_KEEP_LOCAL}): $d"; rm -rf "$d"; done; '
            f'du -sh {BACKUP_ROOT}'
        ),
    )

    # 검증: 원격이 로컬 스테이징을 전부 갖고 있는가 (dry-run 에서 전송 대상 0). 로컬 보존이 더 짧으니 원격 ⊇ 로컬 이어야 한다.
    verify_remote = BashOperator(
        task_id="verify_remote_in_sync",
        trigger_rule="all_done",   # 2026-10-08: prune 이 죽어도 검증은 돈다 (docs/51 2-4 의 재발 방지)
        bash_command=(
            f'set -euo pipefail; pending=$(rsync -a -n -i --chmod=ugo+rX -e "{SSH}" {BACKUP_ROOT}/ {REMOTE}:{REMOTE_ROOT}/clickhouse/ | grep -c "^<f" || true); '
            f'echo "pending files: $pending"; [ "$pending" -eq 0 ]'
        ),
    )

    # 2026-10-08 (docs/51 §8): 로컬 정리(원격과 같은 규칙) → 검증. 검증은 all_done 이라 정리가 죽어도 돈다
    [clickhouse_backup, export_parquet] >> sync_to_oracle >> apply_remote_retention >> prune_local >> verify_remote
