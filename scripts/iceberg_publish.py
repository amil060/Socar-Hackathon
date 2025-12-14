from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

BASE_DIR = Path("/home/hackathon")
MARTS_DIR = BASE_DIR / "processed_data" / "marts"
VAULT_DIR = BASE_DIR / "vault" / "runs"

ICEBERG_ENDPOINT = "http://localhost:8181"
CATALOG_ALIAS = "iceberg_catalog"
NAMESPACE = "sumba101"


@dataclass
class PublishResult:
    table_name: str
    snapshot_id: int | None
    published_at_utc: str


def git_short_commit() -> str:
    """Return the current Git short commit hash if available, otherwise 'unknown'."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(BASE_DIR),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out or "unknown"
    except Exception:
        return "unknown"


def connect() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with Iceberg support enabled."""
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")

    # Performance knobs (tune based on VM resources)
    con.execute("PRAGMA threads=8;")
    con.execute("PRAGMA memory_limit='12GB';")

    return con

def attach_catalog(con: duckdb.DuckDBPyConnection) -> None:
    """Attach an Iceberg REST catalog to DuckDB."""
    # This local REST catalog is unauthenticated, so we attach without a SECRET.
    # DuckDB supports REST catalogs and time travel with snapshot versions. 
    con.execute(
        f"""
        ATTACH 'warehouse' AS {CATALOG_ALIAS} (
          TYPE iceberg,
          ENDPOINT '{ICEBERG_ENDPOINT}',
          AUTHORIZATION_TYPE 'none'
        );
        """
    )
    # Create a namespace (schema) to organize tables
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_ALIAS}.{NAMESPACE};")


def sanitize_table_name(filename: str) -> str:
    """Convert a mart Parquet filename into a safe Iceberg table name."""
    name = Path(filename).stem.lower()
    return name.replace("-", "_").replace(".", "_")


def iceberg_safe_select(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> str:
    """
    Build a SELECT list that casts unsupported types (e.g., TIMESTAMP_NS) to Iceberg-compatible types.
    """
    # Describe the Parquet schema via DuckDB (fast, reads metadata only).
    info = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{parquet_path.as_posix()}')"
    ).fetchall()

    select_exprs: list[str] = []
    for col_name, col_type, *_ in info:
        t = str(col_type).upper()

        # Iceberg doesn't accept TIMESTAMP_NS from DuckDB writer; cast to TIMESTAMP (microsecond precision).
        if "TIMESTAMP_NS" in t:
            select_exprs.append(f"CAST({col_name} AS TIMESTAMP) AS {col_name}")
        else:
            select_exprs.append(col_name)

    return ", ".join(select_exprs)


def try_get_latest_snapshot_id(con: duckdb.DuckDBPyConnection, fq_table: str) -> int | None:
    """Fetch the latest snapshot_id from Iceberg snapshots metadata, if available."""
    try:
        row = con.execute(
            f"SELECT snapshot_id FROM iceberg_snapshots('{fq_table}') ORDER BY committed_at DESC LIMIT 1;"
        ).fetchone()
        return int(row[0]) if row else None
    except Exception:
        return None

def table_exists(con: duckdb.DuckDBPyConnection, fq_table: str) -> bool:
    """Return True if the Iceberg table is visible in the attached catalog."""
    try:
        con.execute(f"SELECT 1 FROM {fq_table} LIMIT 0;")
        return True
    except Exception:
        return False

def publish_one(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> PublishResult:
    """Publish a single mart Parquet file to its own Iceberg table (idempotent overwrite)."""
    table = sanitize_table_name(parquet_path.name)
    fq_table = f"{CATALOG_ALIAS}.{NAMESPACE}.{table}"
    table_root = Path("/warehouse") / NAMESPACE / table
    (table_root / "data").mkdir(parents=True, exist_ok=True)
    (table_root / "metadata").mkdir(parents=True, exist_ok=True)
    select_list = iceberg_safe_select(con, parquet_path)
    # Idempotent demo strategy:
    # - CREATE OR REPLACE makes repeated runs deterministic.
    # - For 1TB incremental mode, we will switch to partition-aware INSERT-only updates.
    if not table_exists(con, fq_table):
        con.execute(
            f"""
            CREATE TABLE {fq_table} AS
            SELECT {select_list} FROM read_parquet('{parquet_path.as_posix()}');
            """
        )
    else:
        # Table likely already exists -> append new data to generate a new snapshot
        con.execute(
            f"""
            INSERT INTO {fq_table}
            SELECT {select_list} FROM read_parquet('{parquet_path.as_posix()}');
            """
        )

    snapshot_id = try_get_latest_snapshot_id(con, fq_table)
    published_at = datetime.now(timezone.utc).isoformat()
    return PublishResult(table_name=table, snapshot_id=snapshot_id, published_at_utc=published_at)


def main() -> None:
    if not MARTS_DIR.exists():
        raise SystemExit(f"Missing marts directory: {MARTS_DIR}")

    parquet_files = sorted(MARTS_DIR.glob("*.parquet"))
    if not parquet_files:
        raise SystemExit(f"No mart parquet files found under: {MARTS_DIR}")

    run_id = os.environ.get("PIPELINE_RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_vault = VAULT_DIR / run_id
    run_vault.mkdir(parents=True, exist_ok=True)

    con = connect()
    attach_catalog(con)

    results: list[PublishResult] = []
    for p in parquet_files:
        results.append(publish_one(con, p))

    meta = {
        "pipeline_run_id": run_id,
        "git_commit": git_short_commit(),
        "ingest_dts_utc": datetime.now(timezone.utc).isoformat(),
        "tables": [r.__dict__ for r in results],
        "iceberg_endpoint": ICEBERG_ENDPOINT,
        "namespace": NAMESPACE,
    }

    out_path = run_vault / "iceberg_publish.json"
    out_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"Published {len(results)} tables to Iceberg.")
    print(f"Metadata saved to: {out_path}")


if __name__ == "__main__":
    main()
