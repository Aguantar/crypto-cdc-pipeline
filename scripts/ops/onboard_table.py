#!/usr/bin/env python3
"""CDC 테이블 온보딩을 명령 하나로 (docs/53, 2026-10-23).

MySQL 표 하나를 Debezium → Kafka → ClickHouse(Kafka 엔진 + MV + ReplacingMergeTree) → dbt 소스까지 잇는다.
수동 5단계(DDL 확인, 커넥터 include.list, 토픽, ClickHouse 3개, dbt)를 실측한 뒤(docs/53 §1) 그 순서를 고쳐서 코드로 옮겼다.

순서가 수동 절차와 다른 이유:
  1. 토픽을 커넥터보다 먼저 만든다. 브로커가 auto.create 라 커넥터가 먼저 발행하면 3 파티션·72h 기본값으로 생기고 파티션은 줄일 수 없다(실측).
  2. ClickHouse Kafka 엔진 표를 커넥터보다 먼저 만든다. 컨슈머 그룹이 먼저 있어야 첫 메시지부터 읽는다.
  3. 커넥터에 표를 추가해도 기존 행은 안 온다(snapshot.mode=initial 은 오프셋이 있으면 SKIPPED, 실측). signal 표에
     execute-snapshot(incremental) 을 넣어 기존 행을 op='r' 로 받는다. 전제: 커넥터에 signal.data.collection 이 설정돼 있고
     그 표가 include.list 에 있다 (ledger-cdc-connector 는 10-23 에 설정).

사용:
  scripts/ops/onboard_table.py crypto_db.my_table [--connector ledger-cdc-connector] [--dry-run] [--no-snapshot] [--dml-check]
  scripts/ops/onboard_table.py crypto_db.my_table --remove        # 역순 제거 (실측용 표 정리)
--dml-check 는 그 표에 INSERT/UPDATE/DELETE 를 한 건씩 넣어 본다. 실측용 표에만 쓴다.
"""
import argparse, json, subprocess, sys, time, urllib.request, re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONNECT = "http://localhost:8083"
CH_DB = "cdc_pipeline"
TOPIC_SPEC = "partitions=1 compression.type=zstd retention.ms=2592000000 min.insync.replicas=1"  # 2층 원장 기본 (kafka-topics.sh)
META_COLS = "is_deleted UInt8, op LowCardinality(String), source_ts_ms Int64, kafka_ts_ms Int64, ch_inserted_at DateTime64(3) DEFAULT now64(3)"


def sh(cmd, check=True, inp=None):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, input=inp)
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd}\n{r.stderr.strip()}")
    return r.stdout


def mysql(sql, db="crypto_db"):
    # root 비밀번호는 컨테이너 환경에만 있다 - 호스트로 꺼내지 않는다.
    return sh(f"docker exec -i cdc-mysql sh -c 'mysql -uroot -p\"$MYSQL_ROOT_PASSWORD\" -N {db}' 2>/dev/null", inp=sql)


def ch(sql, admin=False):
    if admin:
        return sh(f"docker exec -i cdc-clickhouse sh -c 'clickhouse-client --user default --password \"$CLICKHOUSE_PASSWORD\" --multiquery'", inp=sql)
    return sh("docker exec -i cdc-clickhouse clickhouse-client --multiquery", inp=sql)


def http(method, path, body=None):
    req = urllib.request.Request(CONNECT + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read() or b"null")


# ── 타입 매핑: MySQL(information_schema) → Debezium JSON(connect 모드, decimal=string) → ClickHouse ──
# Debezium 이 JSON 에 넣는 형태가 기준이다. 실측(docs/53 §2): TIMESTAMP 는 ISO 문자열, DATETIME 은 epoch ms 정수,
# DECIMAL 은 문자열(decimal.handling.mode=string), ENUM·JSON 은 문자열, BOOLEAN(tinyint(1)) 은 정수.
def map_column(name, column_type, data_type, nullable):
    t = data_type.lower(); ct = column_type.lower()
    unsigned = "unsigned" in ct
    if t in ("tinyint",) and ct.startswith("tinyint(1)"):
        ch_t, expr = "UInt8", f"toUInt8(JSONExtractInt(r, '{name}'))"
    elif t in ("tinyint", "smallint", "mediumint", "int", "integer"):
        ch_t, expr = ("UInt32", f"JSONExtractUInt(r, '{name}')") if unsigned else ("Int32", f"JSONExtractInt(r, '{name}')")
    elif t == "bigint":
        ch_t, expr = ("UInt64", f"JSONExtractUInt(r, '{name}')") if unsigned else ("Int64", f"JSONExtractInt(r, '{name}')")
    elif t in ("decimal", "numeric"):
        m = re.search(r"\((\d+),(\d+)\)", ct); p, s = (m.group(1), m.group(2)) if m else ("18", "8")
        ch_t, expr = f"Decimal({p}, {s})", f"toDecimal128OrZero(JSONExtractString(r, '{name}'), {s})"
    elif t in ("float", "double", "real"):
        ch_t, expr = "Float64", f"JSONExtractFloat(r, '{name}')"
    elif t == "timestamp":
        ch_t, expr = "DateTime64(3)", f"parseDateTime64BestEffortOrZero(JSONExtractString(r, '{name}'), 3)"
    elif t == "datetime":
        ch_t, expr = "DateTime64(3)", f"fromUnixTimestamp64Milli(JSONExtractInt(r, '{name}'))"
    elif t == "date":
        ch_t, expr = "Date", f"toDate(JSONExtractInt(r, '{name}'))"
    elif t in ("enum", "set") or (t in ("varchar", "char") and int(re.search(r"\((\d+)\)", ct).group(1)) <= 40):
        ch_t, expr = "LowCardinality(String)", f"JSONExtractString(r, '{name}')"
    else:  # varchar(>40), text, json, blob 등
        ch_t, expr = "String", f"JSONExtractString(r, '{name}')"
    if nullable and not ch_t.startswith("LowCardinality"):
        ch_t, expr = f"Nullable({ch_t})", f"if(JSONType(r, '{name}') = 'Null', NULL, {expr})"
    elif nullable:
        expr = f"if(JSONType(r, '{name}') = 'Null', '', {expr})"
    return ch_t, expr


def read_columns(db, table):
    out = mysql(f"SELECT column_name, column_type, data_type, column_key, is_nullable FROM information_schema.columns "
                f"WHERE table_schema='{db}' AND table_name='{table}' ORDER BY ordinal_position;")
    cols = [l.split("\t") for l in out.strip().splitlines() if l.strip()]
    return [{"name": c[0], "column_type": c[1], "data_type": c[2], "pk": c[3] == "PRI", "nullable": c[4] == "YES"} for c in cols]


def names(prefix, db, table):
    return {"topic": f"{prefix}.{db}.{table}", "target": f"{CH_DB}.{table}", "queue": f"{CH_DB}.{prefix}_{table}_queue",
            "mv": f"{CH_DB}.mv_{prefix}_{table}", "group": f"clickhouse-{prefix}-{table}"}


def generate_ddl(prefix, db, table, cols):
    n = names(prefix, db, table)
    pk = [c["name"] for c in cols if c["pk"]]
    mapped = [(c["name"], *map_column(c["name"], c["column_type"], c["data_type"], c["nullable"])) for c in cols]
    has_version = any(c["name"] == "version" and "int" in c["data_type"] for c in cols)
    # version 컬럼이 있으면 그것(2층 원장 규약, 앱이 UPDATE 마다 올린다). 없으면 binlog 시각(source_ts_ms)으로 최신을 가린다.
    version_col = "version" if has_version else "source_ts_ms"
    version_expr = "toUInt32(JSONExtractUInt(r, 'version') + if(o = 'd', 1, 0))" if has_version else None
    col_defs = ",\n    ".join(f"{name} {ch_t}" for name, ch_t, _ in mapped)
    selects = []
    for name, _, expr in mapped:
        selects.append(f"{version_expr} AS version" if name == "version" and has_version else f"{expr} AS {name}")
    target = (f"CREATE TABLE IF NOT EXISTS {n['target']}\n(\n    {col_defs},\n    {META_COLS}\n)\n"
              f"ENGINE = ReplacingMergeTree({version_col}, is_deleted)\nORDER BY ({', '.join(pk)});")
    queue = (f"CREATE TABLE IF NOT EXISTS {n['queue']} (raw String)\nENGINE = Kafka SETTINGS kafka_broker_list = 'kafka-1:29092', "
             f"kafka_topic_list = '{n['topic']}', kafka_group_name = '{n['group']}', kafka_format = 'JSONAsString', kafka_num_consumers = 1;")
    mv = (f"CREATE MATERIALIZED VIEW IF NOT EXISTS {n['mv']} TO {n['target']} AS\n"
          f"WITH JSONExtractString(raw, 'op') AS o,\n     if(o = 'd', JSONExtractRaw(raw, 'before'), JSONExtractRaw(raw, 'after')) AS r\nSELECT\n    "
          + ",\n    ".join(selects)
          + ",\n    if(o = 'd', 1, 0) AS is_deleted, o AS op, JSONExtractInt(raw, 'source', 'ts_ms') AS source_ts_ms, toUnixTimestamp64Milli(_timestamp_ms) AS kafka_ts_ms\n"
          f"FROM {n['queue']} WHERE o IN ('c', 'u', 'd', 'r');")
    return target, queue, mv


def dbt_block(table, pk):
    tests = "\n".join(f"          - name: {c}\n            tests: [not_null]" for c in pk)
    if len(pk) == 1:
        tests = f"          - name: {pk[0]}\n            tests: [not_null, unique]"
    return (f"      - name: {table}\n        description: \"onboard_table.py 가 추가 (docs/53). RMT - FINAL 로 최신, is_deleted=1 은 원본 DELETE\"\n"
            f"        columns:\n{tests}\n")


def topics_line(topic):
    return f'  ["{topic}"]="{TOPIC_SPEC}"\n'


def wait_connector(name, timeout=60):
    for _ in range(timeout // 3):
        st = http("GET", f"/connectors/{name}/status")
        if st["connector"]["state"] == "RUNNING" and all(t["state"] == "RUNNING" for t in st["tasks"]):
            return True
        time.sleep(3)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("table", help="db.table")
    ap.add_argument("--connector", default="ledger-cdc-connector")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-snapshot", action="store_true", help="기존 행 증분 스냅샷을 생략")
    ap.add_argument("--dml-check", action="store_true", help="INSERT/UPDATE/DELETE 한 건씩 넣어 e2e 확인 (실측용 표만)")
    ap.add_argument("--remove", action="store_true")
    a = ap.parse_args()
    db, table = a.table.split(".")
    t0 = time.time()
    say = lambda m: print(f"[{time.strftime('%H:%M:%S')} +{time.time() - t0:5.1f}s] {m}", flush=True)

    cfg = http("GET", f"/connectors/{a.connector}/config")
    prefix = cfg["topic.prefix"]
    n = names(prefix, db, table)
    include = cfg["table.include.list"].split(",")
    topics_sh = ROOT / "scripts/ops/kafka-topics.sh"
    sources_yml = ROOT / "dbt_cdc_pipeline/models/staging/sources.yml"

    if a.remove:
        say(f"제거: {a.table} (역순)")
        if a.dry_run:
            say("dry-run: 커넥터 include.list 에서 제거, ClickHouse MV·queue·표 DROP, 토픽 삭제, kafka-topics.sh·sources.yml 되돌림"); return
        if a.table in include:
            cfg["table.include.list"] = ",".join(t for t in include if t != a.table)
            http("PUT", f"/connectors/{a.connector}/config", cfg); say(f"  커넥터 include.list 에서 제거, RUNNING={wait_connector(a.connector)}")
        ch(f"DROP VIEW IF EXISTS {n['mv']}; DROP TABLE IF EXISTS {n['queue']}; DROP TABLE IF EXISTS {n['target']};", admin=True); say("  ClickHouse 3개 DROP")
        sh(f"docker exec cdc-kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --delete --topic {n['topic']}", check=False); say("  토픽 삭제")
        s = topics_sh.read_text(); topics_sh.write_text(s.replace(topics_line(n["topic"]), ""))
        s = sources_yml.read_text(); s2 = re.sub(rf"      - name: {table}\n(?:        .*\n|          .*\n)*", "", s); sources_yml.write_text(s2)
        say("  kafka-topics.sh·sources.yml 되돌림. MySQL 표는 건드리지 않는다"); return

    # 1. 검증
    say(f"1. 검증 {a.table} → 커넥터 {a.connector} (prefix {prefix})")
    cols = read_columns(db, table)
    if not cols: sys.exit(f"MySQL 에 {a.table} 이 없다")
    pk = [c["name"] for c in cols if c["pk"]]
    if not pk: sys.exit("PK 가 없다 - ReplacingMergeTree 정렬 키를 정할 수 없다")
    fmt = mysql("SELECT @@binlog_format;").strip()
    if fmt != "ROW": sys.exit(f"binlog_format={fmt} (ROW 여야 한다)")
    if a.table in include: sys.exit(f"{a.table} 은 이미 include.list 에 있다")
    signal = cfg.get("signal.data.collection")
    if not signal and not a.no_snapshot: sys.exit("커넥터에 signal.data.collection 이 없다 - 기존 행을 가져올 수 없다. --no-snapshot 이거나 signal 표를 먼저 설정")
    rows_src = int(mysql(f"SELECT count(*) FROM {table};").strip())
    say(f"   컬럼 {len(cols)}, PK {pk}, binlog ROW, 원본 행 {rows_src}, signal {signal}")

    target, queue, mv = generate_ddl(prefix, db, table, cols)
    if a.dry_run:
        print("\n-- 토픽 (kafka-topics.sh 에 추가)\n" + topics_line(n["topic"]).rstrip())
        print(f"\n-- ClickHouse\n{target}\n\n{queue}\n\n{mv}")
        print(f"\n-- 커넥터 table.include.list += {a.table}")
        print(f"-- signal: INSERT INTO {signal} ... execute-snapshot incremental [{a.table}]")
        print("\n-- dbt sources.yml (raw 소스, binance_trades 앞 - 생성되는 테스트: source_not_null_raw_<표>_<pk>, source_unique_raw_<표>_<pk>)\n" + dbt_block(table, pk).rstrip())
        print("\n-- 감시: 커넥터 단위는 check_kafka_connect 가 본다. 표 단위 적재 감시는 없다(표마다 유입 주기가 달라 신선도 임계를 못 정한다) - 대조표가 생기면 ledger_reconcile 처럼 항목을 추가한다")
        say("dry-run 끝 - 아무것도 바꾸지 않았다"); return

    # 2. 토픽 (커넥터보다 먼저)
    say("2. 토픽 정의 + 생성")
    s = topics_sh.read_text()
    if topics_line(n["topic"]) not in s:
        anchor = '  ["ledger.crypto_db.debezium_signal"]'
        i = s.index(anchor); j = s.index("\n", i) + 1
        topics_sh.write_text(s[:j] + topics_line(n["topic"]) + s[j:])
    cfgs = " ".join(f"--config {kv}" for kv in TOPIC_SPEC.split()[1:])
    out = sh(f"docker exec cdc-kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists --topic {n['topic']} --partitions 1 --replication-factor 1 {cfgs}")
    say(f"   {out.strip() or '이미 있음'}")

    # 3. ClickHouse (커넥터보다 먼저 - 컨슈머 그룹이 첫 메시지 전에 있어야 한다)
    say("3. ClickHouse 표·Kafka 엔진·MV")
    ch(f"{target}\n{queue}\n{mv}", admin=True); say(f"   {n['target']}, {n['queue']}, {n['mv']}")

    # 4. 커넥터
    say("4. 커넥터 include.list 추가 + PUT")
    cfg["table.include.list"] = ",".join(include + [a.table])
    http("PUT", f"/connectors/{a.connector}/config", cfg)
    ok = wait_connector(a.connector); say(f"   RUNNING={ok}")
    if not ok: sys.exit("커넥터가 RUNNING 으로 돌아오지 않았다")

    # 5. 기존 행 증분 스냅샷
    if not a.no_snapshot:
        say(f"5. 증분 스냅샷 신호 ({rows_src}행)")
        sig_table = signal.split(".")[1]
        mysql(f"INSERT INTO {sig_table} (id, type, data) VALUES ('onboard-{table}-{int(t0)}', 'execute-snapshot', "
              f"'{{\"data-collections\": [\"{a.table}\"], \"type\": \"incremental\"}}');")
        got = 0
        for _ in range(60):
            got = int(ch(f"SELECT countIf(op = 'r') FROM {n['target']}").strip() or 0)
            if got >= rows_src: break
            time.sleep(2)
        say(f"   ClickHouse op='r' {got}/{rows_src}" + ("" if got >= rows_src else " - 120초 안에 다 안 왔다"))
    else:
        say("5. 스냅샷 생략 (--no-snapshot)")

    # 6. dbt
    say("6. dbt sources.yml")
    s = sources_yml.read_text()
    if f"      - name: {table}\n" not in s:
        anchor = "      - name: binance_trades\n"
        sources_yml.write_text(s.replace(anchor, dbt_block(table, pk) + anchor, 1))
    say("   raw 소스 + PK not_null/unique 테스트 추가 (dbt ls --select source:raw.<표> 로 보인다)")

    # 7. 검증
    say("7. 검증")
    final = int(ch(f"SELECT count() FROM {n['target']} FINAL WHERE is_deleted = 0").strip())
    src = int(mysql(f"SELECT count(*) FROM {table};").strip())
    say(f"   MySQL {src} 행 = ClickHouse FINAL {final} 행 → {'일치' if src == final else '불일치'}")
    if a.dml_check:
        tag = f"onboard-check-{int(t0)}"
        before = int(ch(f"SELECT count() FROM {n['target']}").strip())
        mysql(f"INSERT INTO {table} (label, amount, updated_ms) VALUES ('{tag}', 1, UNIX_TIMESTAMP(NOW(3))*1000);"
              f"UPDATE {table} SET version=version+1, updated_ms=UNIX_TIMESTAMP(NOW(3))*1000 WHERE label='{tag}';"
              f"DELETE FROM {table} WHERE label='{tag}';")
        # c→u→d 를 행 세 개로 세면 안 된다. RMT 백그라운드 머지가 초 단위로 낮은 version 을 접는다(실측 - 34초 뒤 'd' 만 남았다).
        # 대신 결과를 본다: DELETE 가 도착했으면 version 3(c=1, u=2, d=+1) 에 is_deleted=1 이다.
        s0 = time.time(); got = ""
        for _ in range(30):
            got = ch(f"SELECT concat(toString(max(version)), '/', toString(max(is_deleted))) FROM {n['target']} WHERE label = '{tag}'").strip()
            if got == "3/1": break
            time.sleep(1)
        say(f"   DML e2e INSERT→UPDATE→DELETE: version/is_deleted = {got} ({'통과' if got == '3/1' else '실패'}, {time.time() - s0:.1f}초)")
    say("8. 감시: 커넥터 단위 check_kafka_connect 가 본다. 표 단위 적재 감시 항목은 없다 - 대조표를 두면 ledger_reconcile 처럼 추가")
    say(f"끝. 손댄 파일: {topics_sh.relative_to(ROOT)}, {sources_yml.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
