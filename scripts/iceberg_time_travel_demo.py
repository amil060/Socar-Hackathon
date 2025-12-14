from pathlib import Path
import duckdb
import time

BASE = Path("/home/hackathon")
MARTS = BASE / "processed_data" / "marts"
ICEBERG_DIR = BASE / "lakehouse" / "iceberg" / "demo_table"  # local warehouse
ICEBERG_DIR.parent.mkdir(parents=True, exist_ok=True)

def main():
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("PRAGMA threads=8;")          # perf
    con.execute("PRAGMA memory_limit='12GB';")

    # 1) Create/refresh a SMALL demo iceberg table from marts parquet (fast demo for jury)
    parquet_glob = str(MARTS / "*.parquet")
    if not list(MARTS.glob("*.parquet")):
        raise SystemExit(f"No marts parquet found in {MARTS}. Run pipeline first.")


    # --- Create a “versioned” iceberg-like folder layout by copying to v1/v2 and use iceberg_scan ---
    v1 = ICEBERG_DIR.parent / "demo_table_v1"
    v2 = ICEBERG_DIR.parent / "demo_table_v2"
    v1.mkdir(parents=True, exist_ok=True)
    v2.mkdir(parents=True, exist_ok=True)

    # Build two versions (today/yesterday) quickly: write parquet outputs into v1 and v2
    con.execute(
        f"CREATE OR REPLACE TABLE tmp AS "
        f"SELECT * FROM read_parquet('{parquet_glob}', union_by_name=True) "
    f"LIMIT 200000;"
    )
    con.execute(f"COPY tmp TO '{v1}/data.parquet' (FORMAT PARQUET);")
    time.sleep(1)
    con.execute(f"CREATE OR REPLACE TABLE tmp AS SELECT * FROM tmp WHERE 1=1;")  # pretend “new run”
    con.execute(f"COPY tmp TO '{v2}/data.parquet' (FORMAT PARQUET);")

    # Show “time travel” effect in UI: query two versions
    now_count = con.execute(f"SELECT count(*) FROM read_parquet('{v2}/data.parquet');").fetchone()[0]
    old_count = con.execute(f"SELECT count(*) FROM read_parquet('{v1}/data.parquet');").fetchone()[0]

    print("=== DEMO RESULT ===")
    print("Yesterday (v1) rows:", old_count)
    print("Today (v2) rows:", now_count)
    print("Next step: switch from v1/v2 folders to real Iceberg snapshots (REST catalog)")

if __name__ == "__main__":
    main()
