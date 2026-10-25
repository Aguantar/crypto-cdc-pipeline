"""onboard_table.py 의 순수 부분(타입 매핑·DDL 생성) 스냅샷 테스트. 도커 없이 돈다: python3 -m pytest scripts/ops/tests -q"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import onboard_table as ot

COLS = [
    {"name": "id", "column_type": "bigint unsigned", "data_type": "bigint", "pk": True, "nullable": False},
    {"name": "label", "column_type": "varchar(40)", "data_type": "varchar", "pk": False, "nullable": False},
    {"name": "amount", "column_type": "decimal(18,8)", "data_type": "decimal", "pk": False, "nullable": False},
    {"name": "status", "column_type": "enum('open','closed')", "data_type": "enum", "pk": False, "nullable": False},
    {"name": "note", "column_type": "varchar(200)", "data_type": "varchar", "pk": False, "nullable": True},
    {"name": "version", "column_type": "int unsigned", "data_type": "int", "pk": False, "nullable": False},
    {"name": "updated_ms", "column_type": "bigint", "data_type": "bigint", "pk": False, "nullable": False},
    {"name": "created_at", "column_type": "timestamp(3)", "data_type": "timestamp", "pk": False, "nullable": False},
]


def test_type_mapping_follows_debezium_json_shapes():
    # 실측(docs/53 §2): TIMESTAMP 는 ISO 문자열, DECIMAL 은 문자열, ENUM 은 문자열, DATETIME 은 epoch ms
    assert ot.map_column("created_at", "timestamp(3)", "timestamp", False) == ("DateTime64(3)", "parseDateTime64BestEffortOrZero(JSONExtractString(r, 'created_at'), 3)")
    assert ot.map_column("seen_at", "datetime(3)", "datetime", False) == ("DateTime64(3)", "fromUnixTimestamp64Milli(JSONExtractInt(r, 'seen_at'))")
    assert ot.map_column("amount", "decimal(18,8)", "decimal", False) == ("Decimal(18, 8)", "toDecimal128OrZero(JSONExtractString(r, 'amount'), 8)")
    assert ot.map_column("id", "bigint unsigned", "bigint", False)[0] == "UInt64"
    assert ot.map_column("flag", "tinyint(1)", "tinyint", False)[0] == "UInt8"
    assert ot.map_column("note", "varchar(200)", "varchar", True)[0] == "Nullable(String)"
    assert ot.map_column("side", "enum('BUY','SELL')", "enum", False)[0] == "LowCardinality(String)"


def test_ddl_snapshot():
    target, queue, mv = ot.generate_ddl("ledger", "crypto_db", "pipeline_onboard_probe", COLS)
    assert "ENGINE = ReplacingMergeTree(version, is_deleted)" in target and "ORDER BY (id)" in target
    assert "note Nullable(String)" in target and "is_deleted UInt8, op LowCardinality(String), source_ts_ms Int64" in target
    assert "kafka_topic_list = 'ledger.crypto_db.pipeline_onboard_probe', kafka_group_name = 'clickhouse-ledger-pipeline_onboard_probe'" in queue
    assert "toUInt32(JSONExtractUInt(r, 'version') + if(o = 'd', 1, 0)) AS version" in mv
    assert "if(o = 'd', JSONExtractRaw(raw, 'before'), JSONExtractRaw(raw, 'after')) AS r" in mv
    assert mv.rstrip().endswith("FROM cdc_pipeline.ledger_pipeline_onboard_probe_queue WHERE o IN ('c', 'u', 'd', 'r');")


def test_without_version_column_uses_binlog_ts():
    cols = [c for c in COLS if c["name"] != "version"]
    target, _, mv = ot.generate_ddl("ledger", "crypto_db", "t", cols)
    assert "ReplacingMergeTree(source_ts_ms, is_deleted)" in target
    assert "AS version" not in mv


def test_dbt_block_and_topic_line():
    assert ot.dbt_block("t", ["id"]) .endswith("          - name: id\n            tests: [not_null, unique]\n")
    assert ot.topics_line("ledger.crypto_db.t") == '  ["ledger.crypto_db.t"]="partitions=1 compression.type=zstd retention.ms=2592000000 min.insync.replicas=1"\n'
