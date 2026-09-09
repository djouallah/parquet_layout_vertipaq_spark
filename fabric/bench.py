"""Benchmark the Direct Lake models the paper's way, without DAX Studio.

Methodology ported from djouallah/direct-lake-parquet-layout (`benchmark/xmla_compare.py`):

  the semantic model is DELETED and recreated before every measurement, then the whole suite runs
  `--passes` times and the PASS NUMBER is the tier —

    pass 1     cold   first visit; pays the whole Delta -> memory transcode, once
    pass 2     warm   second visit, segments resident
    pass 3..N  hot    settled; reported as a median over N-2 samples

Nothing touches the model between its readiness probe and pass 1, so pass 1 is a true cold
transcode. The readiness probe reads a tiny dimension (`ship_mode`, 20 rows) rather than a fact,
so it cannot pre-warm anything the suite measures. `directLakeOnly` means a query Direct Lake
cannot serve fails rather than silently falling back to the SQL endpoint and logging a pushdown
time that would read as a slow layout.

The DAX is executed inside Fabric by a deployed notebook (semantic-link's `evaluate_dax`, which
speaks XMLA from within the capacity), because a laptop REST round trip costs ~2.5 s and would
bury every engine time under transport. This laptop drives it: delete + deploy each model, run the
notebook, collect its JSON.

  python fabric/bench.py --workspace <ws> --item <mirrored item> --schemas tpcds_sf10_layout,tpcds_sf100_layout
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import textwrap
import time

HERE = os.path.dirname(os.path.abspath(__file__))

# The paper's five queries (section 6.3) under its scenario 1 (no slicer selected), plus per-column
# probes that isolate one column's transcode, plus scenario 3 (filters on all slicers). Query text
# is shared with djouallah/direct-lake-parquet-layout's TPCDS suite so numbers are comparable.
QUERIES = [
    # --- per-column probes: one column each, so a cold number is that column's transcode ---
    ("probe", "probe_ext_sales_price", 'EVALUATE ROW("x", SUM(store_sales[ss_ext_sales_price]))'),
    ("probe", "probe_quantity", 'EVALUATE ROW("x", SUM(store_sales[ss_quantity]))'),
    ("probe", "probe_customer", 'EVALUATE ROW("x", DISTINCTCOUNT(store_sales[ss_customer_sk]))'),
    ("probe", "probe_rowcount", 'EVALUATE ROW("x", COUNTROWS(store_sales))'),

    # --- the paper's five, scenario 1 (no filter) ---
    ("paper_s1", "q1_distinct_count", 'EVALUATE ROW("Customer Count", [Store Distinct Customers])'),
    ("paper_s1", "q2_pct_share",
     "EVALUATE SUMMARIZECOLUMNS('item'[i_category], "
     '"Share", [Store Profit % by Item Category], "Profit", [Store Net Profit])'),
    ("paper_s1", "q3_rank_by_sum",
     'EVALUATE TOPN(1001, SUMMARIZECOLUMNS(promotion[p_promo_name], '
     '"Catalog Revenue", [Catalog Revenue]), [Catalog Revenue], DESC)'),
    ("paper_s1", "q4_yoy_second_fact",
     'EVALUATE SUMMARIZECOLUMNS(date_dim[d_quarter_name], '
     '"Catalog Sales Quantity", [Catalog Sales Quantity], '
     '"Catalog Sales Same Period LY", [Catalog Sales Same Period LY], '
     '"Catalog Sales YoY", [Catalog Sales YoY])'),
    ("paper_s1", "q5_yoy_ytd_large_fact",
     "EVALUATE SUMMARIZECOLUMNS('item'[i_category], date_dim[d_quarter_name], "
     '"Store Revenue", [Store Revenue], "Store Revenue YoY", [Store Revenue YoY], '
     '"Store Revenue YTD", [Store Revenue YTD])'),

    # --- the same five under scenario 3, filters on all slicers ---
    ("paper_s3", "q1_filtered_distinct_count",
     'EVALUATE CALCULATETABLE(ROW("Customer Count", [Store Distinct Customers]), '
     'date_dim[d_year] = 2022, customer_address[ca_state] = "CA", '
     'customer_demographics[cd_education_status] = "College")'),
    ("paper_s3", "q2_filtered_pct_share",
     "EVALUATE CALCULATETABLE(SUMMARIZECOLUMNS('item'[i_category], "
     '"Share", [Store Profit % by Item Category]), '
     'date_dim[d_year] = 2022, customer_address[ca_state] = "CA", '
     'customer_demographics[cd_education_status] = "College")'),
    ("paper_s3", "q3_filtered_rank_by_sum",
     'EVALUATE CALCULATETABLE(TOPN(1001, SUMMARIZECOLUMNS(promotion[p_promo_name], '
     '"Catalog Revenue", [Catalog Revenue]), [Catalog Revenue], DESC), '
     'date_dim[d_year] = 2022, ship_mode[sm_carrier] = "UPS", catalog_page[cp_type] = "bi-annual")'),
    ("paper_s3", "q4_filtered_yoy_second_fact",
     'EVALUATE CALCULATETABLE(SUMMARIZECOLUMNS(date_dim[d_quarter_name], '
     '"Catalog Sales Quantity", [Catalog Sales Quantity], '
     '"Catalog Sales YoY", [Catalog Sales YoY]), '
     'ship_mode[sm_carrier] = "UPS", catalog_page[cp_type] = "bi-annual")'),
    ("paper_s3", "q5_filtered_yoy_ytd_large_fact",
     "EVALUATE CALCULATETABLE(SUMMARIZECOLUMNS('item'[i_category], date_dim[d_quarter_name], "
     '"Store Revenue", [Store Revenue], "Store Revenue YTD", [Store Revenue YTD]), '
     'customer_address[ca_state] = "CA", '
     'customer_demographics[cd_education_status] = "College")'),
]

# 20 rows, and no query in the suite touches it -- so proving the model can read OneLake cannot
# pre-warm anything measured.
READY = 'EVALUATE ROW("n", COUNTROWS(ship_mode))'


def notebook_source(models: list[str], passes: int, think: int) -> dict:
    """A Fabric notebook that runs the suite against each model in `models`, in order.

    Models are benchmarked one at a time and none is touched before its own pass 1, so a model
    later in the list stays cold while an earlier one is measured.
    """
    body = textwrap.dedent(f'''
        import json, statistics, time
        import sempy.fabric as fabric

        MODELS  = {models!r}
        QUERIES = {QUERIES!r}
        READY   = {READY!r}
        PASSES  = {passes}
        THINK   = {think}

        def run(model, dax):
            t0 = time.perf_counter()
            df = fabric.evaluate_dax(model, dax)
            return (time.perf_counter() - t0) * 1000.0, len(df)

        def ready(model, tries=20, delay=15):
            """A freshly created Direct Lake model cannot read OneLake until security propagates."""
            for i in range(1, tries + 1):
                try:
                    run(model, READY)
                    print(f"  {{model}}: queryable after {{i}} attempt(s)", flush=True)
                    return True
                except Exception as e:
                    print(f"  {{model}}: not ready ({{str(e).splitlines()[0][:110]}})", flush=True)
                    if i < tries:
                        time.sleep(delay)
            return False

        out = {{}}
        for model in MODELS:
            print(f"=== {{model}}", flush=True)
            if not ready(model):
                out[model] = {{"error": "never became queryable"}}
                continue
            samples = {{}}
            for p in range(1, PASSES + 1):
                for tier, name, dax in QUERIES:
                    try:
                        ms, rows = run(model, dax)
                    except Exception as e:
                        samples.setdefault(name, {{}})[p] = None
                        print(f"  pass {{p}} {{name}}: FAILED {{str(e).splitlines()[0][:120]}}", flush=True)
                        continue
                    samples.setdefault(name, {{}})[p] = ms
                    if p == 1:
                        samples[name]["rows"] = rows
                        samples[name]["tier"] = tier
                    time.sleep(THINK)
                print(f"  pass {{p}} done", flush=True)
            res = {{}}
            for name, by_pass in samples.items():
                hot = [v for p, v in by_pass.items()
                       if isinstance(p, int) and p >= 3 and v is not None]
                res[name] = {{
                    "tier": by_pass.get("tier"),
                    "rows": by_pass.get("rows"),
                    "cold_ms": by_pass.get(1),
                    "warm_ms": by_pass.get(2),
                    "hot_ms": statistics.median(hot) if hot else None,
                    "hot_n": len(hot),
                    "ms_by_pass": {{str(p): v for p, v in by_pass.items() if isinstance(p, int)}},
                }}
            out[model] = res
            print(json.dumps({{model: res}}, indent=1), flush=True)

        mssparkutils.notebook.exit(json.dumps(out))
    ''').strip()
    return {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {"language_info": {"name": "python"},
                     "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"}},
        "cells": [{"cell_type": "code", "source": body.splitlines(keepends=True),
                   "metadata": {}, "execution_count": None, "outputs": []}],
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workspace", required=True)
    ap.add_argument("--item", required=True, help="mirrored item name or GUID")
    ap.add_argument("--schemas", required=True, help="comma-separated schemas, one model each")
    ap.add_argument("--passes", type=int, default=5, help="passes over the suite: 1 cold, 2 warm, rest hot")
    ap.add_argument("--think", type=int, default=1, help="seconds between queries (outside every timed region)")
    ap.add_argument("--out", default="bench_results.json")
    ap.add_argument("--skip-deploy", action="store_true", help="benchmark the models already deployed")
    args = ap.parse_args()

    schemas = [s.strip() for s in args.schemas.split(",") if s.strip()]

    import duckrun
    from duckrun.fabric_remote import _create_notebook, _http_request, _FABRIC_API
    ws = duckrun.workspace(args.workspace)

    # Delete and recreate every model BEFORE any of them is benchmarked, so no model is warm when
    # its own pass 1 runs. A reframe is part of deploy; nothing else touches them.
    if not args.skip_deploy:
        for schema in schemas:
            existing = [it for it in ws.list_items("semanticModels") if it.get("displayName") == schema]
            for it in existing:
                r = _http_request("DELETE", f"{_FABRIC_API}/workspaces/{ws.id}/semanticModels/{it['id']}",
                                  token=ws._token)
                print(f"deleted semantic model {schema!r} ({it['id']}) -> {r.status_code}")
            cmd = [sys.executable, os.path.join(HERE, "build_semantic_model.py"),
                   "--workspace", args.workspace, "--item", args.item,
                   "--schema", schema, "--name", schema]
            print("$", " ".join(cmd), flush=True)
            subprocess.run(cmd, check=True)

    nb_name = "bench_direct_lake"
    src = notebook_source(schemas, args.passes, args.think)
    existing = next((it for it in ws.list_items("notebooks") if it.get("displayName") == nb_name), None)
    nb_id = _create_notebook(ws._token, ws.id, nb_name, src, item_id=existing["id"] if existing else None)
    print(f"{'updated' if existing else 'created'} notebook {nb_name!r} ({nb_id})")

    t0 = time.time()
    print(f"running {len(schemas)} model(s) x {args.passes} passes x {len(QUERIES)} queries...", flush=True)
    result = ws.run(nb_name)
    print(f"notebook finished in {time.time() - t0:,.0f}s")

    payload = result if isinstance(result, str) else json.dumps(result)
    try:
        data = json.loads(payload)
    except Exception:
        print(payload[:4000]); sys.exit("could not parse the notebook's exit value")
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    print("written", args.out)

    for model, res in data.items():
        if "error" in res:
            print(f"\n{model}: {res['error']}"); continue
        print(f"\n=== {model}   (ms; cold = first touch after a fresh deploy)")
        print(f"{'query':<34}{'tier':<10}{'cold':>10}{'warm':>10}{'hot med':>10}")
        for tier, name, _ in QUERIES:
            r = res.get(name)
            if not r:
                continue
            f = lambda v: "-" if v is None else f"{v:,.0f}"
            print(f"{name:<34}{tier:<10}{f(r['cold_ms']):>10}{f(r['warm_ms']):>10}{f(r['hot_ms']):>10}")


if __name__ == "__main__":
    main()
