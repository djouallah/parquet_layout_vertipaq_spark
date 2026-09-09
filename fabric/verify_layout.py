"""Verify the parquet layout of a mirrored Databricks schema as Fabric sees it.

Reads the LIVE files of each Delta table (delta-rs over the mirrored item's OneLake path) and
inspects their footers with DuckDB's parquet_metadata(). Per table it reports what the duckrun
parquet-layout profile promises and what Direct Lake cares about:

  files, row groups, rows per row group (min / avg / max) and the share of row groups inside
  Direct Lake's 1M..16M window (the paper's Table 9.3.2.1 metric), compression, per-column
  encodings (is a dictionary page there?), and whether the sort key's row-group ranges are
  disjoint and monotonic -- the proof of a global sort rather than a per-file one.

Usage (after `az login` into the Fabric tenant):

  python fabric/verify_layout.py --workspace dhr-mimoune --item databricks_ne --schema tpcds_sf10_layout
  python fabric/verify_layout.py --path abfss://<ws>@onelake.dfs.fabric.microsoft.com/<item>/Tables/<schema>   # any Delta folder, e.g. the V-Order reference

Needs: duckrun (auth + item lookup), deltalake, duckdb.
"""
from __future__ import annotations

import argparse
import json
import sys

import duckdb
from deltalake import DeltaTable

TABLES = ["store_sales", "catalog_sales", "catalog_page", "customer_address", "customer_demographics",
          "date_dim", "item", "promotion", "ship_mode", "store"]
SORT_KEY = {  # first column of the build's ORDER BY -- what the disjoint-range check runs on
    "store_sales": "ss_sold_date_sk", "catalog_sales": "cs_sold_date_sk",
    "catalog_page": "cp_catalog_page_sk", "customer_address": "ca_address_sk",
    "customer_demographics": "cd_demo_sk", "date_dim": "d_date_sk_1", "item": "i_item_sk",
    "promotion": "p_promo_sk", "ship_mode": "sm_ship_mode_sk", "store": "s_store_sk",
}
RG_LO, RG_HI = 1_000_000, 16_000_000
MIRRORED_TYPES = ("MirroredAzureDatabricksCatalog", "MirroredDatabase", "Lakehouse", "Warehouse")


def resolve_root(args) -> str:
    """abfss://<ws-id>@onelake.dfs.fabric.microsoft.com/<item-id>/Tables/<schema>"""
    if args.path:
        return args.path.rstrip("/")
    import duckrun
    ws = duckrun.workspace(args.workspace)
    items = [it for it in ws.list_items() if it.get("displayName") == args.item]
    if not items:
        sys.exit(f"item {args.item!r} not found in workspace {args.workspace!r}")
    item = next((it for it in items if it.get("type") in MIRRORED_TYPES), items[0])
    print(f"item {args.item!r}: type={item.get('type')} id={item['id']}")
    return f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{item['id']}/Tables/{args.schema}"


def live_files(table_uri: str, token: str) -> list[str]:
    dt = DeltaTable(table_uri, storage_options={"bearer_token": token, "use_fabric_endpoint": "true"})
    return sorted(dt.file_uris())


def inspect(con: duckdb.DuckDBPyConnection, table: str, files: list[str]) -> dict:
    con.execute("CREATE OR REPLACE TEMP TABLE m AS SELECT * FROM parquet_metadata($files)", {"files": files})
    rg = con.execute("""
        SELECT file_name, row_group_id, max(row_group_num_rows) AS rows,
               sum(total_compressed_size) AS comp_bytes
        FROM m GROUP BY 1, 2
    """).fetchall()
    n_files = len({r[0] for r in rg})
    rows = [r[2] for r in rg]
    in_window = sum(1 for r in rows if RG_LO <= r <= RG_HI)
    comp_mb = sum(r[3] for r in rg) / 1024 / 1024

    cols = con.execute("""
        SELECT path_in_schema,
               string_agg(DISTINCT compression, ',' ORDER BY compression)       AS compression,
               string_agg(DISTINCT encodings,   ',' ORDER BY encodings)         AS encodings,
               count(dictionary_page_offset)                                     AS dict_chunks,
               count(*)                                                          AS chunks,
               round(sum(total_compressed_size) / 1024 / 1024, 1)                AS mb
        FROM m GROUP BY 1 ORDER BY mb DESC
    """).fetchall()

    key = SORT_KEY.get(table)
    sortedness = None
    if key:
        ranges = con.execute("""
            SELECT file_name, row_group_id,
                   TRY_CAST(stats_min_value AS BIGINT) AS lo, TRY_CAST(stats_max_value AS BIGINT) AS hi
            FROM m WHERE path_in_schema = $key AND stats_min_value IS NOT NULL
            ORDER BY lo, hi
        """, {"key": key}).fetchall()
        if ranges and all(r[2] is not None and r[3] is not None for r in ranges):
            overlaps = sum(1 for a, b in zip(ranges, ranges[1:]) if a[3] > b[2])
            sortedness = {"key": key, "row_groups": len(ranges), "overlapping_neighbours": overlaps,
                          "globally_sorted": overlaps == 0}
    return {
        "table": table, "files": n_files, "row_groups": len(rows),
        "rows": sum(rows), "rg_min": min(rows), "rg_avg": int(sum(rows) / len(rows)), "rg_max": max(rows),
        "pct_rg_in_1m_16m": round(100 * in_window / len(rows), 1),
        "compressed_mb": round(comp_mb, 1), "avg_file_mb": round(comp_mb / n_files, 1),
        "compression": sorted({c[1] for c in cols}),
        "columns_with_dictionary": sum(1 for c in cols if c[3] == c[4]),
        "columns_without_dictionary": [c[0] for c in cols if c[3] < c[4]],
        "sortedness": sortedness,
        "columns": [{"column": c[0], "compression": c[1], "encodings": c[2],
                     "dict_chunks": c[3], "chunks": c[4], "mb": c[5]} for c in cols],
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workspace", help="Fabric workspace name or GUID")
    ap.add_argument("--item", help="mirrored item display name (defaults to the catalog name)")
    ap.add_argument("--schema", help="schema under Tables/, e.g. tpcds_sf10_layout")
    ap.add_argument("--path", help="explicit abfss://...<item>/Tables/<schema> root; skips the lookup")
    ap.add_argument("--tables", default=",".join(TABLES), help="comma-separated subset")
    ap.add_argument("--json", help="also write the full result (incl. per-column detail) to this file")
    args = ap.parse_args()
    if not args.path and not (args.workspace and args.item and args.schema):
        ap.error("--path, or all of --workspace --item --schema")

    from duckrun.auth import get_onelake_token
    token = get_onelake_token()
    root = resolve_root(args)
    print("root:", root)

    con = duckdb.connect()
    con.execute("INSTALL azure; LOAD azure;")
    con.execute("CREATE OR REPLACE SECRET onelake (TYPE azure, PROVIDER access_token, ACCESS_TOKEN $t)", {"t": token})

    results = []
    for t in [x.strip() for x in args.tables.split(",") if x.strip()]:
        try:
            files = live_files(f"{root}/{t}", token)
        except Exception as e:  # noqa: BLE001
            print(f"  {t}: cannot read Delta log -- {e}")
            continue
        r = inspect(con, t, files)
        results.append(r)
        s = r["sortedness"]
        print(f"  {t:<22} files {r['files']:>5}  row groups {r['row_groups']:>5}  rows/RG "
              f"{r['rg_min']:>10,} / {r['rg_avg']:>10,} / {r['rg_max']:>10,}  in 1M..16M {r['pct_rg_in_1m_16m']:>5}%  "
              f"{r['avg_file_mb']:>7} MB/file  {','.join(r['compression'])}  dict cols "
              f"{r['columns_with_dictionary']}/{len(r['columns'])}  "
              + (f"sorted={s['globally_sorted']} ({s['overlapping_neighbours']} overlaps on {s['key']})" if s else "sortedness n/a"))

    print("\n| table | files | row groups | rows/RG min/avg/max | % RG in 1M..16M | MB/file | compression | dict cols | globally sorted |")
    print("|---|---:|---:|---|---:|---:|---|---:|---|")
    for r in results:
        s = r["sortedness"]
        print(f"| {r['table']} | {r['files']} | {r['row_groups']} | {r['rg_min']:,} / {r['rg_avg']:,} / {r['rg_max']:,} | "
              f"{r['pct_rg_in_1m_16m']} | {r['avg_file_mb']} | {','.join(r['compression'])} | "
              f"{r['columns_with_dictionary']}/{len(r['columns'])} | {s['globally_sorted'] if s else 'n/a'} |")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump({"root": root, "tables": results}, fh, indent=2)
        print("written", args.json)


if __name__ == "__main__":
    main()
