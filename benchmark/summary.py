"""Regenerate summary.csv and the two chart PNGs, on the laptop.

Everything here runs the RESULTS NOTEBOOK'S OWN CELLS, verbatim: `build_notebooks.py` holds them
as strings and this script `exec`s them in one namespace, so a number or a line on a chart cannot
disagree with what the notebook would draw in Fabric. Nothing is reimplemented -- the only things
this file adds are a OneLake token (a notebook gets one for free), a no-op `display`, and a
`plt.show` that writes a PNG instead of opening a window.

  python benchmark/summary.py [--out .] [--no-charts]

Writes:
  summary.csv                the `layout` table: one row per scale factor x arm x fact
  chart_sf100.png            the recipe against the best each other writer reached, SF100
  chart_sf1000.png           the same four arms at SF1000

There is no markdown twin. `summary.md` was the same table rendered a second way, and a second
copy of a number is a second thing that can go stale. Read `summary.csv`.

The charts are `draw()` from the notebook's chart cell, called with a chosen arm list instead of
`ARMS`. Same `suite_s` out of `summary`, same log axis, same labels and colours.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys

import duckdb
import duckrun

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKSPACE = "51650f82-6bb5-4023-b0ab-db197d32e0be"   # mimbenchmarking, as deploy_bench.py
BENCH_LAKEHOUSE = "tpcds_bench"

# The four arms worth putting side by side: the recipe, and the best each other writer reached.
# Ordered baseline-first so the legend reads recipe -> ordered -> other writers.
def chart_arms(ns):
    return [ns["ARM_DEFAULT"], ns["ARM_CLUSTER"], ns["ARM_DUCKSORT"], ns["ARM_VORDER"]]

CHART_TITLE = "the recipe against the best each writer reached"


def onelake_token() -> str:
    """duckrun signs in through the browser; fall back to the az CLI, which is what CI/WSL has."""
    try:
        from duckrun.auth import get_onelake_token
        return get_onelake_token()
    except Exception:
        out = subprocess.run(["az", "account", "get-access-token", "--scope",
                              "https://storage.azure.com/.default", "-o", "json"],
                             capture_output=True, text=True, check=True,
                             shell=(os.name == "nt"))
        return json.loads(out.stdout)["accessToken"]


def load_cells():
    spec = importlib.util.spec_from_file_location(
        "bn", os.path.join(REPO, "notebooks", "build_notebooks.py"))
    bn = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bn)
    return bn


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workspace", default=WORKSPACE)
    ap.add_argument("--lakehouse", default=BENCH_LAKEHOUSE)
    ap.add_argument("--out", default=REPO, help="where summary.* and the PNGs go")
    ap.add_argument("--no-charts", action="store_true")
    args = ap.parse_args()

    bn = load_cells()

    # A notebook reads OneLake with its own identity; here the token is explicit. Patching the
    # class rather than every call site keeps the notebook cells untouched.
    import deltalake
    _DT = deltalake.DeltaTable
    tok = onelake_token()

    class DT(_DT):
        def __init__(self, path, *a, **kw):
            if str(path).startswith("abfss://") and "storage_options" not in kw:
                kw["storage_options"] = {"bearer_token": tok, "use_fabric_endpoint": "true"}
            super().__init__(path, *a, **kw)

    deltalake.DeltaTable = DT

    ws = duckrun.workspace(args.workspace)
    lh = next((it for it in ws.list_items("lakehouses")
               if it.get("displayName") == args.lakehouse), None)
    if not lh:
        raise SystemExit(f"lakehouse {args.lakehouse!r} not found in {args.workspace!r}")

    ns = {
        "duckdb": duckdb, "duckrun": duckrun,
        "ws_id": ws.id, "lh_id": lh["id"], "lh_name": args.lakehouse,
        "N_QUERIES": 24, "SLICER": (3, 7, 8, 11, 15, 16, 17, 18, 21),
        "delta_path": (f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/"
                       f"{lh['id']}/Tables/dbo/perfresults3"),
        "display": lambda *a, **k: None,
    }
    for cell in ("RES_RUNS", "RES_HEADLINE", "RES_LAYOUT"):
        exec(getattr(bn, cell), ns)

    os.makedirs(args.out, exist_ok=True)
    df = duckdb.sql("SELECT * FROM layout").df()
    csv_path = os.path.join(args.out, "summary.csv")
    df.to_csv(csv_path, index=False)
    print(f"wrote {csv_path}: {len(df)} rows")

    if args.no_charts:
        return

    # The chart cell ends by drawing every arm; suppress that, then call its own `draw` with the
    # arm list we want and save what it produced. `draw` itself is not touched.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.show = lambda *a, **k: None
    exec(getattr(bn, "RES_CHART"), ns)

    saved = []
    for sf_ in ns["SFS"]:
        plt.close("all")
        ns["draw"](sf_, chart_arms(ns), CHART_TITLE)
        if not plt.get_fignums():
            continue
        png = os.path.join(args.out, f"chart_sf{sf_}.png")
        plt.gcf().savefig(png, dpi=140)
        plt.close("all")
        saved.append(png)
    print("wrote " + ", ".join(saved) if saved else "no charts: no measured arms")


if __name__ == "__main__":
    main()
