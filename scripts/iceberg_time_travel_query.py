from __future__ import annotations

import json
from pathlib import Path

import duckdb

BASE_DIR = Path("/home/hackathon")
VAULT_RUNS = BASE_DIR / "vault" / "runs"

ICEBERG_ENDPOINT = "http://localhost:8181"
CATALOG_ALIAS = "iceberg_catalog"
NAMESPACE = "sumba101"


def main() -> None:
    runs = sorted([p for p in VAULT_RUNS.glob("*") if p.is_dir()])
    if not runs:
        raise SystemExit(f"No runs found under: {VAULT_RUNS}")

    # Pick the latest run that has publish metadata
    meta_path = None
    for r in reversed(runs):
        candidate = r / "iceberg_publish.json"
        if candidate.exists():
            meta_path = candidate
            break
    if meta_path is None:
        raise SystemExit("No iceberg_publish.json found in vault runs.")

    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")

    # Attach REST catalog (local filesystem warehouse is managed by the REST server).
    con.execute(
        f"""
        ATTACH 'warehouse' AS {CATALOG_ALIAS} (
          TYPE iceberg,
          ENDPOINT '{ICEBERG_ENDPOINT}',
          AUTHORIZATION_TYPE 'none'
        );
        """
    )

    # Pick the first published table name
    if not meta.get("tables"):
        raise SystemExit("Publish metadata contains no tables.")

    table = meta["tables"][0]["table_name"]
    fq = f"{CATALOG_ALIAS}.{NAMESPACE}.{table}"

    # Fetch snapshots directly from Iceberg metadata (do not rely on the publish JSON).
    snaps = con.execute(
        f"SELECT snapshot_id, timestamp_ms FROM iceberg_snapshots('{fq}') ORDER BY timestamp_ms DESC"
    ).fetchall()

    if len(snaps) < 2:
        raise SystemExit("Not enough snapshots for time travel. Run publish at least twice.")

    previous_snapshot_id = int(snaps[1][0])

    # Current version
    now_count = con.execute(f"SELECT count(*) FROM {fq};").fetchone()[0]

    # Time travel: query the same table at the previous snapshot
    old_count = con.execute(
        f"SELECT count(*) FROM {fq} AT (VERSION => {previous_snapshot_id});"
    ).fetchone()[0]

    print(f"Table: {fq}")
    print(f"Current rows: {now_count}")
    print(f"Previous snapshot id: {previous_snapshot_id}")
    print(f"Rows at previous snapshot: {old_count}")
    print("Time travel query executed successfully.")


if __name__ == "__main__":
    main()
