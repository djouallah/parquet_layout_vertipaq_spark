"""Pull what `layout_stats` wrote to the tpcds_bench lakehouse Files section and print it.

The notebook stages its results under `Files/layout_stats/sf{sf}`:

  chunks_<arm>.parquet   raw parquet_metadata rows, one per column chunk per row group
  enc.parquet            the same chunks classified: dict_state = dictionary | fell_back | plain
  files.csv              arm, table, file_name, num_rows, num_row_groups
  summary_dict.csv       % bytes dictionary / fell_back / plain per arm x table
  columns.csv            per arm x table x column: dict_state, chunks, MB, encodings
  row_groups.csv         files, row groups, rg_per_file, min/avg/max, % in 1M..16M
  ordering.csv           overlaps on the first key

Usage (after `az login` into the Fabric tenant):

  python fabric/pull_layout_stats.py --sf 1000 [--workspace <ws>] [--out scratch/layout_stats]
"""
from __future__ import annotations

import argparse
import os

import duckdb

WORKSPACE = "51650f82-6bb5-4023-b0ab-db197d32e0be"   # mimbenchmarking, as deploy_bench.py
BENCH_LAKEHOUSE = "tpcds_bench"
FACTS = ("store_sales", "catalog_sales")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sf", type=int, required=True)
    ap.add_argument("--workspace", default=WORKSPACE, help="Fabric workspace name or GUID")
    ap.add_argument("--out", default="scratch/layout_stats", help="local folder to download into")
    ap.add_argument("--no-download", action="store_true", help="only print what is already local")
    args = ap.parse_args()

    local = os.path.join(args.out, f"sf{args.sf}")
    if not args.no_download:
        import duckrun
        ws = duckrun.workspace(args.workspace)
        lh = next((it for it in ws.list_items("lakehouses") if it.get("displayName") == BENCH_LAKEHOUSE), None)
        if not lh:
            raise SystemExit(f"lakehouse {BENCH_LAKEHOUSE!r} not found in {args.workspace!r}")
        # GUIDs, never friendly names: this tenant has OneLake friendly-name support disabled.
        duckrun.connect(f"{ws.id}/{lh['id']}", name="bench").download(
            f"layout_stats/sf{args.sf}", local, overwrite=True)

    have = sorted(os.listdir(local)) if os.path.isdir(local) else []
    if not have:
        raise SystemExit(f"nothing under {local}")
    print("files:", ", ".join(have))

    con = duckdb.connect()
    p = lambda n: os.path.join(local, n).replace("\\", "/")  # noqa: E731

    print("\n--- summary_dict")
    con.sql(f"SELECT * FROM read_csv('{p('summary_dict.csv')}')").show(max_rows=100, max_width=250)

    print("\n--- columns on the facts, non-dictionary first, by MB")
    con.sql(f"""
        SELECT * FROM read_csv('{p('columns.csv')}')
        WHERE "table" IN {FACTS}
        ORDER BY (dict_state <> 'dictionary') DESC, arm, "table", mb DESC
    """).show(max_rows=200, max_width=250)

    print("\n--- row_groups")
    con.sql(f"SELECT * FROM read_csv('{p('row_groups.csv')}')").show(max_rows=100, max_width=250)

    print("\n--- files per arm x table")
    con.sql(f"""
        SELECT arm, "table", count(*) AS files, sum(num_rows) AS rows,
               round(avg(num_rows)) AS avg_rows_per_file, max(num_row_groups) AS max_rg_per_file
        FROM read_csv('{p('files.csv')}') GROUP BY ALL ORDER BY 1, 2
    """).show(max_rows=100, max_width=250)

    print("\n--- ordering")
    con.sql(f"SELECT * FROM read_csv('{p('ordering.csv')}')").show(max_rows=100, max_width=250)


if __name__ == "__main__":
    main()
