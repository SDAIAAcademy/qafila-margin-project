from __future__ import annotations

import argparse
import os
import glob
import subprocess
import sys
from pathlib import Path

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

DBT_DIR = Path(__file__).resolve().parent / "dbt"




def get_catalog(warehouse: Path) -> SqlCatalog:




    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("qafila", uri=f"sqlite:///{warehouse / 'catalog.db'}", warehouse=f"file://{warehouse.resolve()}")

def ingest(con, data_dir):
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    
    sqlite_path = os.path.join(data_dir, "qafila_oltp.sqlite")
    con.execute(f"ATTACH '{sqlite_path}' AS sqlite_db (TYPE SQLITE);")
    for tbl in ['orders', 'order_items', 'products', 'stores']:
        con.execute(f"CREATE OR REPLACE TABLE raw.{tbl} AS SELECT * FROM sqlite_db.{tbl};")
    con.execute("DETACH sqlite_db;")
    
    supplier_files = sorted(glob.glob(os.path.join(data_dir, "suppliers", "*.csv")))
    
    con.execute("""
        CREATE OR REPLACE TABLE raw.supplier_prices_raw (
            source_file VARCHAR,
            sku VARCHAR,
            valid_from VARCHAR,
            unit_cost VARCHAR,
            cost_sar VARCHAR
        );
    """)
    
    for fpath in supplier_files:
        filename = os.path.basename(fpath)
        cols_info = con.execute(f"DESCRIBE SELECT * FROM read_csv('{fpath}', header=True, all_varchar=True, filename=False)").fetchall()
        col_names = [c[0].lower() for c in cols_info]
        
        sku_expr = "CAST(sku AS VARCHAR)" if "sku" in col_names else "NULL"
        valid_expr = "CAST(valid_from AS VARCHAR)" if "valid_from" in col_names else "NULL"
        unit_cost_expr = "CAST(unit_cost AS VARCHAR)" if "unit_cost" in col_names else "NULL"
        cost_sar_expr = "CAST(cost_sar AS VARCHAR)" if "cost_sar" in col_names else "NULL"
        
        con.execute(f"""
            INSERT INTO raw.supplier_prices_raw
            SELECT 
                '{filename}' AS source_file,
                {sku_expr} AS sku,
                {valid_expr} AS valid_from,
                {unit_cost_expr} AS unit_cost,
                {cost_sar_expr} AS cost_sar
            FROM read_csv('{fpath}', header=True, all_varchar=True, filename=False);
        """)

def run_dbt(db_path: Path) -> None:
    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(db_path.resolve())
    cmd = [sys.executable, "-m", "dbt.cli.main", "run", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    res = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if res.returncode != 0:
        print("dbt output:\n", res.stdout)
        print("dbt errors:\n", res.stderr)
        raise RuntimeError("dbt run failed")

def publish(con, catalog):
    tables_to_publish = [
        ("silver.product_cost_history", "silver", "product_cost_history"),
        ("gold.weekly_margin", "gold", "weekly_margin"),
        ("quality.price_quarantine", "quality", "price_quarantine")
    ]
    
    for duck_tbl, ns, tbl_name in tables_to_publish:
        arrow_table = con.execute(f"SELECT * FROM {duck_tbl}").fetch_arrow_table()
        identifier = f"{ns}.{tbl_name}"
        
        try:
            iceberg_tbl = catalog.load_table(identifier)
            iceberg_tbl.overwrite(arrow_table)
        except Exception:
            iceberg_tbl = catalog.create_table(identifier, schema=arrow_table.schema)
            iceberg_tbl.overwrite(arrow_table)

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = parser.parse_args()

    db_path = args.warehouse / "qafila.duckdb"
    args.warehouse.mkdir(parents=True, exist_ok=True)

    # Step 1: Ingest
    con = duckdb.connect(str(db_path))
    try:
        ingest(con, args.data_dir)
    finally:
        con.close()

    # Step 2: Run dbt
    run_dbt(db_path)

    # Step 3: Publish
    con = duckdb.connect(str(db_path))
    try:
        catalog = get_catalog(args.warehouse)
        publish(con, catalog)
    finally:
        con.close()

if __name__ == "__main__":
    main()



