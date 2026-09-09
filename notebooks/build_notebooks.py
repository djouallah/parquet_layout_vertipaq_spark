"""Emit the two Fabric notebooks the concurrency ladder needs.

The notebooks are generated rather than hand-edited so their Python stays reviewable as Python:
`.ipynb` JSON with escaped newlines is not a diffable format. The `.ipynb` files are BUILD OUTPUT:
a fix made in one of them is silently overwritten by the next build (the 2026-09-05 layout_stats
fix -- one footer read per arm -- was lost exactly that way and came back as a double sweep), so
change the cell source HERE and re-run:

    python notebooks/build_notebooks.py

  RunPerfScenario.ipynb   one virtual user: opens its own XMLA connection, runs the suite,
                          appends its rows to `perfresults3`. `runMultiple` starts N of these.
  run_benchmark.ipynb     one rung: for each arm, DELETES and recreates its semantic model from
                          the paper's TMDL (embedded here), reframes it, then runs the 24-query
                          suite `runs` (3) times back to back at `concurrent_threads` against that
                          ONE model -- the paper's own protocol, where Run 1 is the first touch --
                          and deletes it. Rows land in `perfresults3`; the old `perfresults` holds
                          the previous probe + one-warm-pass protocol and is never mixed in.

Ported from c:\\direct_query/notebooks/, which is the working reference for this pattern. The
cell metadata below (the `parameters` tag, the microsoft language keys, the jupyter kernelspec)
is what Fabric needs to accept the notebook and to let `runMultiple` override the first cell.
"""
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SUITE = os.path.join(HERE, os.pardir, "paper", "PowerBIPerformanceData.json")
PAPER_MODEL = os.path.join(HERE, os.pardir, "paper", "model")

# One definition of the workspace, in deploy_bench.py, imported rather than copied: it is baked
# into the embedded model definitions, so a second copy that drifted would ship a notebook that
# creates its models against the wrong workspace.
sys.path.insert(0, os.path.join(HERE, os.pardir))
from deploy_bench import WORKSPACE                                          # noqa: E402

NB_META = {
    "dependencies": {"lakehouse": {}},
    # Pin the interpreter. Unpinned, Fabric picks whatever its default runtime is today and the
    # deltalake / pyarrow pair moves underneath the notebook -- which surfaces as opaque Delta
    # errors on a notebook that ran fine last week. c:\direct_query pins the same way.
    "kernel_info": {"name": "jupyter", "jupyter_kernel_name": "python3.12"},
    "kernelspec": {"display_name": "Jupyter", "language": "Jupyter", "name": "jupyter"},
    "language_info": {"name": "python"},
    "microsoft": {"language": "python", "language_group": "jupyter_python"},
}
CELL_META = {"microsoft": {"language": "python", "language_group": "jupyter_python"}}
PARAM_META = dict(CELL_META, tags=["parameters"])


def code(src, meta=None):
    return {"cell_type": "code", "source": src.strip("\n").splitlines(keepends=True),
            "metadata": meta or CELL_META, "execution_count": None, "outputs": []}


def markdown(src):
    return {"cell_type": "markdown", "source": src.strip("\n").splitlines(keepends=True),
            "metadata": {}}


# Scale factor is applied as a substitution over the finished cells rather than threaded through
# every constant: `sf100` appears only inside schema and model names (tpcds_sf100_layout,
# tpcds_sf100_vorder, tpcds_sf100), so one rewrite covers the RB_PARAMS defaults, the PATTERN map,
# VORDER_SCHEMA and the prose, and none of them can be missed. The negative lookahead
# stops sf100 matching inside sf1000.
# The vendored TMDL names ONE scale factor's schemas. Rather than freeze that at build time, the
# embedded definitions carry a token in its place and the notebook fills it in from its own `sf`
# parameter -- so one build serves every scale factor and nothing has to be regenerated to move
# between them.
SF_TOKEN = "sf__SF__"
_SF_RE = re.compile("sf[0-9]+(?![0-9])")


def write(name, cells):
    path = os.path.join(HERE, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"nbformat": 4, "nbformat_minor": 5, "metadata": NB_META, "cells": cells},
                  f, indent=1)
    print(f"written {path} ({len(cells)} cells)")


# ===========================================================================================
# RunPerfScenario — one virtual user
# ===========================================================================================

RPS_PARAMS = '''
# This is a parameters cell. runMultiple overrides every one of these per thread.

xmla_endpoint = None          # None = this workspace's XMLA endpoint
perf_analyzer_filename = ""   # OneLake https URL of tpcds_queries.json
model = ""                    # semantic model DISPLAY NAME, e.g. tpcds_sf100_layout
roles = None
customdata = None
effective_username = None
iterations = 1                # passes over the suite within this thread
delay_sec = 4                 # think time between queries, outside every timed region
loadtestId = "localtesting"
threadId = 0                  # this virtual user's id
concurrent_threads = 1        # the rung's concurrency; logged, not enforced here
run_index = 1                 # WHICH of the load tests over this model this is -- the paper's
                              # `Run` column. 1 is the first touch of a model created seconds ago;
                              # 2 and 3 are the same suite again on the same model.
# 0 on run 1, 1 on runs 2-3. Kept because the old table has it and the shape is familiar, but
# `run_index` is what the analysis groups by. Nothing is flushed here -- what makes run 1 cold is
# that the driver created the model moments before it.
cache = 0
delta_path = ""               # abfss path of the perfresults3 Delta table
nbr_queries = 24              # NEVER 0: the suite is sliced [:nbr_queries], so 0 runs nothing
single_query = ""             # when set, run ONLY this DAX (the cold transcode probe), not the suite
model_id = ""                 # the semantic model's item GUID, as created for THIS run
model_created_utc = ""        # when the driver created it -- what makes run 1 provably a first touch
totalrows = 0                 # store_sales rows: MEASURED off the model below, not passed in
scale_factor = 0              # DERIVED from totalrows below, not passed in
'''

RPS_HELPERS = '''
import decimal
import json
import random
import time
from datetime import datetime
from typing import Iterable

import requests
import sempy.fabric as fabric

# create_tom_server() initializes the coreclr .NET runtime and registers the AnalysisServices
# assembly resolver. Its workspace discover can fail intermittently -- swallow it; the CLR loads
# before the failing Connect(). This MUST run before importing AdomdClient, otherwise pythonnet
# defaults to mono and raises "Could not find libmono".
try:
    fabric.create_tom_server()
except Exception:
    pass

from Microsoft.AnalysisServices.AdomdClient import AdomdConnection
from System.Globalization import CultureInfo

_INV = CultureInfo.InvariantCulture

# store_sales rows per scale factor AFTER the paper's section 4.5 customisation (rows with any
# null dropped). SF10 = 26,206,837 and SF100 = 262,082,396, linear to 5 parts in 100,000, so
# dividing by this recovers the scale factor. It is the post-customisation number on purpose:
# the raw dsdgen count (28,800,991 at SF10) is not what any model here holds.
SF1_STORE_SALES = 2_620_684


def _jsonable(v):
    """.NET value -> JSON-safe Python. Numbers become STRINGS, not floats: the Store Revenue
    grand total at SF100 needs more significant digits than a double holds, and the golden check
    compares at decimal precision. Never bare str() on a .NET Decimal/DateTime -- that formats
    with the current culture."""
    if v is None:
        return None
    if isinstance(v, bool):          # bool before int: bool subclasses int
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, decimal.Decimal):
        # normalize() drops trailing zeros so a scale-4 and a scale-6 rendering of the same
        # number match; "f" forces plain notation (normalize can emit 2.3E+13).
        return format(v.normalize(), "f") if v.is_finite() else str(v)
    if isinstance(v, float):
        return repr(v)               # shortest round-trip repr of the double
    if isinstance(v, str):
        return v
    tn = type(v).__name__            # still a .NET object
    if tn == "DBNull":
        return None
    if tn == "DateTime":
        return v.ToString("o", _INV)
    if tn == "Decimal":
        return v.ToString(_INV)
    return str(v)


def run_query(con: AdomdConnection, query: str):
    """(column names, rows of raw values). Reading the cells costs one GetValue per cell inside
    the loop the reader already runs -- these queries return 1-50 rows, so it is microseconds.
    Serializing to JSON is the expensive part and the caller does it AFTER stopping the clock,
    so `duration` stays comparable across runs."""
    cmd = con.CreateCommand()
    cmd.CommandText = query
    rdr = cmd.ExecuteReader()
    n = rdr.FieldCount
    cols = [rdr.GetName(i) for i in range(n)]
    out = []
    while rdr.Read():
        out.append([None if rdr.IsDBNull(i) else rdr.GetValue(i) for i in range(n)])
    rdr.Close()
    return cols, out


BUSTER = "cache_buster] < 2"


def bust(query_text: str, n: int) -> str:
    """The paper's cache-defeating substitution, verbatim (its section 7.3).

    Every captured query filters BOTH facts on `cache_buster < 2`, and cache_buster is 1 in every
    row, so any value above 1 selects every row and the result is unchanged -- but the query TEXT
    differs, so the semantic model's query cache cannot serve one virtual user's query out of
    another's result. Their code is a literal replace of the substring "< 2":

        random_number = random.randint(2, 1000)
        query_text = query_text.replace("< 2", f"< {random_number}")

    Kept exactly, INCLUDING the fact that `n` is drawn once per load_queries() call rather than per
    execution -- so a thread's own repeat iterations share a number and it is concurrency ACROSS
    threads that defeats the cache. Changing that would make our numbers mean something different
    from theirs.

    The one thing added is the guard in load_queries(): their replace is unanchored, so a query
    containing `< 2025` anywhere would be silently corrupted into `< <random>025`."""
    return query_text.replace("< 2", f"< {n}")


def measure_scale(con: AdomdConnection):
    """(store_sales rows, scale factor) read off the model itself.

    Derived rather than passed in, because a hand-entered scale factor is another copy of a
    constant that already lives in the build notebooks, and a stale copy silently mislabels rows.
    Reading it off the model also makes two arms built at different scale factors show up as two
    scale_factor values instead of a mystery value mismatch."""
    _, out = run_query(con, 'EVALUATE ROW("n", COUNTROWS(store_sales))')
    rows = int(out[0][0])
    sf = round(rows / SF1_STORE_SALES)
    # A guard, not a fallback: a half-built star lands on a count that is not a scale factor at
    # all, and must fail here rather than be rounded into a plausible-looking label.
    assert sf > 0 and abs(rows - sf * SF1_STORE_SALES) < 0.01 * rows, \\
        f"{rows:,} store_sales rows is not a scale factor of this dataset"
    return rows, sf


def load_queries(fn: str) -> list:
    """Read the Performance Analyzer shaped suite. The title comes from the LAST Visual Container
    Lifecycle event seen, so the strict alternation is load-bearing.

    `fn` empty (the default) reads the suite EMBEDDED in this notebook by
    notebooks/build_notebooks.py. Embedding removes the upload-to-OneLake-and-read-it-back path
    entirely: one fewer thing that can go stale, no storage token needed inside a child session,
    and the notebook is a record of exactly the DAX it ran. Pass a path or an https URL to
    override."""
    if not fn:
        json_string = EMBEDDED_SUITE
    elif fn.startswith("http://") or fn.startswith("https://"):
        hdrs = {}
        if "onelake" in fn:
            import notebookutils
            hdrs["Authorization"] = f"Bearer {notebookutils.credentials.getToken('storage')}"
        resp = requests.get(fn, headers=hdrs)
        resp.raise_for_status()
        json_string = resp.content.decode("utf-8-sig")   # BOM-safe
    else:
        with open(fn, encoding="utf-8-sig") as f:
            json_string = f.read()

    d = json.loads(json_string)
    events = d.get("events", [])
    all_text = "".join(e["metrics"].get("QueryText", "")
                       for e in events if e.get("name") == "Execute DAX Query")
    # Their replace is unanchored. On THIS capture every "< 2" is a cache_buster predicate, so it
    # is safe; on a re-capture a stray `< 2025` would be corrupted into `< <random>025`. Fail
    # loudly rather than time a mangled query.
    n_buster, n_lt2 = all_text.count(BUSTER), all_text.count("< 2")
    assert n_buster == n_lt2, (
        f"{n_lt2 - n_buster} occurrence(s) of '< 2' are NOT cache_buster predicates; the "
        f"unanchored replace would corrupt them")

    n = random.randint(2, 1000)          # once per virtual user, as theirs is
    print(f"  cache_buster: {n_buster} predicates, filtering < {n} this run", flush=True)

    queries, visual_name = [], ""
    for e in events:
        if e.get("name") == "Visual Container Lifecycle":
            visual_name = e["metrics"].get("visualTitle", "")
        if e.get("name") == "Execute DAX Query":
            queries.append({"visual_name": visual_name,
                            "query_text": bust(e["metrics"].get("QueryText", ""), n)})
    return queries


# The XMLA connection can be dropped mid-suite -- a query that outruns the server's patience takes
# the whole connection with it. Without recovery every LATER query then fails with "the connection
# is not open", so one slow query reports as a total failure: at SF1000 that turned 1 timeout into
# 19 errors out of 24 and destroyed the run's data. The connection string is kept here so a dropped
# connection can be rebuilt in place.
CONSTR = ""

_CONN_ERR = ("connection is not open", "connection either timed out", "transport connection",
             "forcibly closed", "adomdconnectionexception")


def _is_conn_error(err) -> bool:
    s = f"{type(err).__name__}: {err}".lower()
    return any(k in s for k in _CONN_ERR)


def _reopen() -> AdomdConnection:
    """A fresh connection. Reopening the dropped object is not reliable, so build a new one."""
    con = AdomdConnection(CONSTR)
    con.Open()
    return con


def run_perf_scenario(con: AdomdConnection, queries: Iterable[dict], i: int) -> list:
    results = []
    for qn, q in enumerate(queries, start=1):
        visual_name = q["visual_name"]
        query_text = q["query_text"]
        # A failing query is recorded, not raised. Propagating would unwind past the Delta write
        # below and take the whole load test with it -- every query that DID work discarded
        # along with the one that did not.
        #
        # A CONNECTION failure is different from a query failure: the connection stays broken, so
        # without rebuilding it every remaining query in the suite fails too and the run reports as
        # a wipeout. Rebuild once and retry the query, with the clock restarted so the reconnect
        # never lands inside a duration.
        duration = rows = result_json = None
        error = None
        for attempt in (1, 2):
            start = time.time()
            try:
                cols, out = run_query(con, query_text)
                duration = time.time() - start    # clock stops BEFORE serializing
                rows = len(out)
                error = None
                result_json = json.dumps(
                    {"columns": cols, "rows": [[_jsonable(v) for v in r] for r in out]},
                    separators=(",", ":"),
                )
                break
            except Exception as query_err:
                # duration stays NULL rather than time-to-failure: an error is not a query
                # duration, and recording one would drag medians toward whatever a fast failure
                # costs. MEDIAN ignores NULL, which is the behaviour we want.
                duration, rows, result_json = None, None, None
                error = f"{type(query_err).__name__}: {query_err}"[:500]
                if attempt == 1 and _is_conn_error(query_err) and CONSTR:
                    print(f"  q{qn} '{visual_name}' lost the connection; reconnecting", flush=True)
                    try:
                        con = _reopen()
                        continue
                    except Exception as reopen_err:
                        error = f"reconnect failed: {type(reopen_err).__name__}: {reopen_err}"[:500]
                print(f"  q{qn} '{visual_name}' FAILED: {error[:160]}", flush=True)
                break

        results.append({
            "loadtest_id": loadtestId,
            "model": model,
            "visual_name": visual_name,
            "result_json": result_json,
            "concurrent_threads": concurrent_threads,
            "iterations": iterations,
            "delay_sec": delay_sec,
            "query_number": qn,
            "iteration": i,
            "rows": rows,
            "duration": duration,
            "start_time": start,
            "start_time_dt": datetime.fromtimestamp(start),
            "thread_id": threadId,
            # The paper's Run: 1, 2 or 3 over ONE model lifetime. A COLUMN, not something to be
            # recovered by re-parsing loadtest_id -- every table below groups by it.
            "run_index": run_index,
            "cache": cache,
            # The model this row was measured against, and when it was created. Written per row so
            # "cold" is a fact in the table -- results.py checks start_time against it -- rather
            # than a claim in a comment. It is also the join key a CU ledger needs: one GUID
            # belongs to exactly one arm and one run.
            "model_id": model_id,
            "model_created_utc": model_created_utc,
            "totalrows": totalrows,
            "scale_factor": scale_factor,
            "nbr_queries": nbr_queries,
            "error": error,           # NULL on success; the message on failure
        })
        time.sleep(delay_sec)
    return results
'''

RPS_MAIN = '''
import time

import notebookutils
import pandas as pd
import sempy.fabric as fabric
from deltalake.writer import write_deltalake

# Load the AnalysisServices CLR via sempy (coreclr) before using AdomdClient. No bare
# `import clr` -- that defaults pythonnet to mono and raises "Could not find libmono".
try:
    fabric.create_tom_server()
except Exception:
    pass
from Microsoft.AnalysisServices.AdomdClient import AdomdConnection

assert model, "model (the semantic model display name) is required"
assert delta_path, "delta_path is required"
assert nbr_queries > 0, "nbr_queries must be > 0: the suite is sliced [:nbr_queries]"

token = notebookutils.credentials.getToken("pbi")
workspace_name = notebookutils.runtime.context["currentWorkspaceName"]
if xmla_endpoint is None:
    xmla_endpoint = f"powerbi://api.powerbi.com/v1.0/myorg/{workspace_name}"

constr = f"Data Source={xmla_endpoint};Initial Catalog={model};password={token};Timeout=7200;"
if effective_username is not None:
    constr += f"EffectiveUserName={effective_username};"
if customdata is not None:
    constr += f"CustomData={customdata};"
if roles is not None:
    constr += f"Roles={roles};"

# A model that was just reframed can refuse the first connect with "database <model> does not
# exist" for a short window. Retry until it opens.
# Hand the helpers the connection string so a dropped connection can be rebuilt mid-suite.
CONSTR = constr
con = AdomdConnection(constr)
for attempt in range(30):          # up to ~5 minutes
    try:
        con.Open()
        print(f"  connected to '{model}' (attempt {attempt + 1})", flush=True)
        break
    except Exception as open_err:
        if attempt == 29:
            raise
        print(f"  connect not ready (attempt {attempt + 1}): {str(open_err)[:120]}", flush=True)
        time.sleep(10)

# Asked ONCE, before the clock ever starts, so it cannot land in any duration -- and asked of the
# model rather than taken as a parameter, so a run can never be stamped with a scale factor it
# did not query.
totalrows, scale_factor = measure_scale(con)
print(f"  store_sales: {totalrows:,} rows -> SF{scale_factor}", flush=True)

try:
    if single_query:
        # The cold pass: ONE query that pages in every column the warm suite reads. Its duration
        # is the transcode, with no warm tail mixed into it. `bust` is not applied -- the probe
        # carries no cache_buster predicate, and there is nothing to defeat on a model created
        # seconds ago.
        queries = [{"visual_name": "transcode (all columns warm reads)",
                    "query_text": single_query}]
        print("running the cold transcode probe (1 query)", flush=True)
    else:
        queries = load_queries(perf_analyzer_filename)[:nbr_queries]
        assert len(queries) == nbr_queries, \
            f"suite has {len(queries)} queries, expected {nbr_queries}"
        print(f"starting {iterations} iteration(s) over {len(queries)} queries, "
              f"{delay_sec}s think time", flush=True)

    all_results = []
    for i in range(iterations):
        all_results += run_perf_scenario(con, queries, i)
        time.sleep(delay_sec)
    con.Close()

    n_failed = sum(1 for r in all_results if r["error"])
    if n_failed:
        failed_qs = sorted({r["query_number"] for r in all_results if r["error"]})
        print(f"WARNING: {n_failed} of {len(all_results)} executions failed (queries "
              f"{failed_qs}) - recorded with a NULL duration, not dropped. results.py drops "
              f"any (loadtest, thread, iteration) that did not complete the whole suite.",
              flush=True)

    df = pd.DataFrame(all_results)
    # Pin the string columns: pyarrow infers a `null` type from an all-None column, which
    # produces an unusable Delta column.
    for c in ("result_json", "visual_name", "error"):
        df[c] = df[c].astype("string")

    MAX_RETRIES, RETRY_DELAY_SEC = 5, 2
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # schema_mode="merge" so a newly added column can land in a table written
            # before that column existed, instead of failing the append.
            write_deltalake(delta_path, df, mode="append", schema_mode="merge")
            print(f"Delta write successful on attempt {attempt}", flush=True)
            break
        except Exception as write_err:
            print(f"Delta write failed on attempt {attempt}: {write_err}", flush=True)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY_SEC)
except Exception as e:
    print(e, flush=True)
    raise
'''

# ===========================================================================================
# run_benchmark — one rung
# ===========================================================================================

RB_CONFIGURE = '''
%%configure
{ "vCores": { "parameterName": "pipelinecore", "defaultValue": 2 }}
'''

RB_DOC = '''
# TPC-DS concurrency ladder — Direct Lake over mirrored Databricks vs Direct Lake on OneLake

The arms, over the **same rows**, written different ways:

| model | arm |
|---|---|
| `tpcds_sf{sf}_default` | Direct Lake over the **mirrored Databricks** catalog; the same rows written by Databricks with a plain `saveAsTable` under the configuration-only recipe -- Optimized Writes at a fixed bin (one ~6M-row row group per file), dictionary kept, **no sort** |
| `tpcds_sf{sf}_vorder` | Direct Lake on OneLake; the same rows rewritten by **Fabric Spark** with V-Order, ZSTD, Optimize Write at 1 GB, partitioned by date and Z-ordered |

All are `directLakeOnly`, so a query Direct Lake cannot serve fails rather than quietly falling
back to the SQL endpoint and logging a pushdown time that would read as a slow layout.

The protocol is the **white paper's own**: ONE model, `runs` (3) consecutive load tests over it.
The arm's semantic model is deleted, recreated from the paper's TMDL (embedded below) and
reframed; then the 24-query suite runs three times at `concurrent_threads` readers, back to back:

| run | `cache` | readers | what it is |
|---|---|---|---|
| 1 | `0` | `concurrent_threads` | the model is seconds old, so nothing is resident: this run **pays the transcode**, query by query, exactly as their Run 1 does |
| 2 | `1` | `concurrent_threads` | warm |
| 3 | `1` | `concurrent_threads` | warm — and run 1 → run 3 is the **warming curve**, which is the whole point |

Their published per-query P50s (`paper/loadtest_p50_per_run.csv`) carry the same `Run` column, so
ours sit beside theirs run by run. At SF100/20 users their V-Order arm warms (99.5 / 5.0 / 6.2 s)
while their mirrored arm never does (114.8 / 142.7 / 166.9 s, degrading) — a fixed mirroring tax
cannot produce that shape, but layout can, and that is what this measures.

The ONE deviation from their protocol: **the model is deleted after run 3**, so nothing holds
capacity memory into the next arm. A run that FAILS keeps its model, so there is something left to
inspect.

There is no separate transcode probe any more. Running one first would page in every column the
suite reads and flatten run 1 into run 2, destroying the curve. `TRANSCODE_DAX` is still embedded
below and can be run by hand against a model nothing else has touched.

Every query filters its fact on a randomised `cache_buster` value, so no thread is served another
thread's cached result.

Parameters below are overridden per rung by `run_benchmark_pipeline`.
'''

RB_PARAMS = '''
ws_name = ""                     # Fabric workspace display name; resolved from context if blank
lh_name = "tpcds_bench"          # lakehouse holding the results table and the query file
results_table = "perfresults3"   # Delta table under Tables/dbo/. `perfresults` is the OLD probe +
                                 # one-warm-pass protocol and is deliberately left alone; point a
                                 # rehearsal at e.g. perfresults3_smoke to keep it out of the real
                                 # table
sf = 100                         # scale factor: picks the tpcds_sf{sf}_* schemas AND the schema
                                 # baked into the model definition created below
arms = "cluster"                 # which arms to measure: default, default, defaultf8, cluster,
                                 # clustersn, partition, vorder, vonly, duckdb, ducksort, or a
                                 # comma list.
                                 # Not every arm exists at every sf, so naming one is the normal case
models = ""                      # explicit model names override `arms` entirely; a pipeline
                                 # passes scalars. A pipeline whose `models` parameter is the
                                 # empty string delivers it as None, hence the `or ""` below
iterations = 1                   # passes over the suite INSIDE one thread, inside one load test.
                                 # NOT `runs`, which is separate load tests. Stays 1: the paper's
                                 # Run column is load tests, not repeats within a thread
concurrent_threads = 2
nbr_queries = 24                 # the whole captured suite; never 0
delay_sec = 4
sf_label = 0                     # recorded in loadtest_id only; 0 -> sf. The real SF is measured
                                 # per thread regardless
runs = 3                         # load tests over ONE model -- their Run column: 1, 2, 3
'''

RB_BOOTSTRAP = '''
import time

import notebookutils
from notebookutils.common import configs

configs.tokenCacheEnabled = False

sf = int(sf)
sf_label = int(sf_label) or sf
runs = int(runs)
# Everything scale-factor-shaped is derived from `sf`, never hardcoded: this notebook runs at any
# scale factor without being regenerated. Naming the models explicitly still wins, so a single arm
# can be measured (the V-Order arm does not exist at every sf).
SCHEMA = {"default": f"tpcds_sf{sf}_default",
          # 8M rows per file, one row group each: the same unordered write as `default` at 6M,
          # to test whether segment SIZE is a lever at all with no ordering to eliminate on.
          "defaultf8": f"tpcds_sf{sf}_defaultf8",
          # WITHDRAWN. The two-row-groups-per-file geometry, from the fortnight the recipe carried
          # parquet.block.row.count.limit. It tied `default` and the key left the recipe.
          "default2rg": f"tpcds_sf{sf}_default2rg", "cluster": f"tpcds_sf{sf}_cluster",
          "partition": f"tpcds_sf{sf}_partition", "vorder": f"tpcds_sf{sf}_vorder",
          "vonly": f"tpcds_sf{sf}_vonly",
          # The clustered arm in snappy, with the 128 MB byte target the first one predates.
          "clustersn": f"tpcds_sf{sf}_clustersn",
          # The delta_rs / delta-rs arms (build_duckdb.ipynb). Third writer, two variants:
          # `duckdb` sorts on a key duckrun picked itself, `ducksort` on the date key.
          "duckdb": f"tpcds_sf{sf}_duckdb", "ducksort": f"tpcds_sf{sf}_ducksort"}
_arms = [a.strip() for a in (arms or "default,cluster,vorder").split(",") if a.strip()]
_bad = [a for a in _arms if a not in SCHEMA]
if _bad:
    raise ValueError(f"unknown arm(s) {_bad}; expected any of {list(SCHEMA)}")
model_to_test = ([m.strip() for m in (models or "").split(",") if m.strip()]
                 or [SCHEMA[a] for a in _arms])
# semantic model name -> the Pattern label recorded in loadtest_id. By SUFFIX, one entry per arm:
# a binary "vorder or else the rest" would stamp every Databricks arm with one label and merge
# their rows together.
# Internal only -- these land in loadtest_id and never on a chart, so the existing values stay as
# they are rather than being renamed under rows already in perfresults3.
_SUFFIX = {"_defaultf8": "dbxdefaultf8", "_default": "dbxdefault", "_default2rg": "dbxdefault2rg", "_cluster": "dbxcluster", "_partition": "dbxpartition",
           "_clustersn": "dbxclustersn",         # endswith: does not match "_cluster"
           "_vorder": "fabvorder", "_vonly": "fabvonly",
           "_duckdb": "duckauto", "_ducksort": "ducksort"}
PATTERN = {m: next((p for s, p in _SUFFIX.items() if m.endswith(s)), m) for m in model_to_test}
_unlabelled = [m for m, p in PATTERN.items() if p == m]
if _unlabelled:
    raise ValueError(f"model(s) {_unlabelled} end in none of {list(_SUFFIX)}; the Pattern label would be the model name")
print(f"sf={sf}  models={model_to_test}")
if not ws_name:
    ws_name = notebookutils.runtime.context.get("currentWorkspaceName", "")

workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
lakehouse_id = notebookutils.lakehouse.get(lh_name)["id"]

# GUIDs, not friendly names. This tenant has OneLake friendly-name support DISABLED, so a
# `<workspace>/<lakehouse>.Lakehouse/...` path is refused outright with
# `FriendlyNameSupportDisabled: WorkspaceId and ArtifactId should be either valid Guids or valid
# Names`. The GUID form works everywhere, so it is the one to use regardless of tenant setting.
# perfresults3, NOT perfresults: under this protocol run 1 is a full 24-query load test on a
# fresh model, where in the old table the first pass was a single probe query. The two tables
# answer differently-defined questions and must never be unioned, so the new one starts empty.
delta_path = (f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
              f"{lakehouse_id}/Tables/dbo/{results_table}")
# Empty: RunPerfScenario reads the suite embedded in itself. Set an https URL or a path here to
# override it with a file.
queryfile = ""

def _results_rows() -> int:
    """Rows currently in the results table, or 0 before it exists. Used to prove a pass actually
    recorded something rather than trusting that runMultiple raised on failure -- it does not
    always, and a swallowed failure is indistinguishable from success without this.

    A table that CANNOT BE READ is not an empty table. Returning 0 for both once turned an
    unreadable path into "every virtual user failed identically" and sent an hour of debugging
    at the wrong thing, so only the genuine not-created-yet case returns 0.
    """
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError
    try:
        return DeltaTable(delta_path).to_pyarrow_dataset().count_rows()
    except TableNotFoundError:
        return 0


print(f"workspace : {ws_name}")
print(f"models    : {model_to_test}")
print(f"threads   : {concurrent_threads}   runs: {runs}   iterations: {iterations}   "
      f"queries: {nbr_queries}")
print(f"results   : {delta_path}")
print(f"queries   : {queryfile or '<embedded in RunPerfScenario>'}")
'''

RB_RUNDAX = '''

def _report_children(outcome):
    """Print each child's own failure. `runMultiple` returns a per-activity dict and does NOT
    always raise, so without this a pass where all 20 users died the same way reports only that
    nothing was written -- never what went wrong inside them."""
    if not isinstance(outcome, dict):
        return
    items = outcome.get("results", outcome) if "results" in outcome else outcome
    if not isinstance(items, dict):
        return
    failed = 0
    for act, res in items.items():
        if not isinstance(res, dict):
            continue
        err = res.get("exception") or res.get("error") or res.get("errorMessage")
        if err:
            failed += 1
            if failed <= 3:      # they fail identically; three samples is the diagnosis
                print(f"  {act}: {str(err).splitlines()[0][:300]}", flush=True)
    if failed > 3:
        print(f"  ... and {failed - 3} more child(ren) failed the same way", flush=True)


def run_dax(workspace, dataset, run_index, threads, model_id="", model_created_utc="",
            single_query=""):
    """One load test over the suite at `threads` concurrency -- one of the three.

    `threads` is an argument, not the global: every run goes at the rung's concurrency, and it is
    what each result row records as its concurrent_threads.

    `run_index` is an ARGUMENT too, not the notebook parameter it used to be: the three runs happen
    inside one notebook invocation now, over one model, so it moves per call.

    `model_id` / `model_created_utc` describe the model these three runs share, and ride down to
    every row, so run 1 can be verified against the moment its model came into existence.

    Every activity is an independent child notebook session, so each virtual user opens its own
    XMLA connection -- which is what makes this a concurrency test rather than a loop.
    """
    # `cache` is DERIVED from run_index, never passed: two arguments that must agree is one that
    # can disagree, and a row claiming to be a warm run 1 would be undetectable afterwards.
    cache = int(run_index > 1)
    ts = time.strftime("%Y%m%d-%H%M%S")
    # Their analysis notebook parses loadtest_id with a Pattern_SF_Run-timestamp regex into
    # Pattern, SF, Run -- so keep that shape and our results drop straight into their comparison.
    # The pattern names are OURS (dbxcluster / fabvorder), not their db_dq / fab_dl, so nothing
    # masquerades as their data.
    loadtest_id = f"{PATTERN.get(dataset, dataset)}_{sf_label}_{run_index:02d}-{ts}"
    print(f"  load test {loadtest_id}: run {run_index}, {threads} thread(s), cache={cache}",
          flush=True)

    args = {
        "xmla_endpoint": f"powerbi://api.powerbi.com/v1.0/myorg/{workspace}",
        "perf_analyzer_filename": queryfile,
        "model": dataset,
        "roles": None,
        "customdata": None,
        "effective_username": None,
        "iterations": iterations,
        "delay_sec": delay_sec,
        "loadtestId": loadtest_id,
        "threadId": 0,
        "concurrent_threads": threads,
        "run_index": run_index,
        "useRootDefaultLakehouse": True,
        "cache": cache,
        "delta_path": delta_path,
        "nbr_queries": nbr_queries,
        "model_id": model_id,
        "model_created_utc": model_created_utc,
        "single_query": single_query,
    }
    activity = {
        "name": "RunPerfScenario",
        "path": "RunPerfScenario",
        "timeoutPerCellInSeconds": 90000,
        "args": {},
        "workspace": None,
        "retry": 0,                 # a dead thread stays dead; results.py drops the short rung
        "retryIntervalInSeconds": 0,
        "dependencies": [],         # no ordering: they all start together
    }
    DAG = {"activities": [], "timeoutInSeconds": 43200, "concurrency": threads}
    for i in range(threads):
        a = dict(activity)
        a["name"] = f"RunPerfScenario_{i}"
        a["args"] = {**args, "threadId": i}
        DAG["activities"].append(a)

    before = _results_rows()
    outcome = None
    try:
        outcome = notebookutils.notebook.runMultiple(DAG)
    except Exception as e:
        # Not fatal here: a thread that dies writes nothing and is simply absent from the table,
        # which results.py turns into a dropped short rung rather than a load test at a
        # concurrency that never happened. But it IS printed, and so is every child's own
        # exception below -- "see the snapshots" is useless advice when the snapshots are three
        # clicks deep in the monitoring UI and the job has already moved on.
        print(f"  load test error: {e}", flush=True)
    _report_children(outcome)
    after = _results_rows()
    print(f"  load test complete: {after - before:,} rows written "
          f"({before:,} -> {after:,})", flush=True)
    # A pass that wrote NOTHING is not a degraded rung, it is a broken harness -- every thread
    # failed the same way. Swallowing runMultiple's exception once turned exactly that into a
    # Completed job with an empty table, so the row count is the thing that decides.
    if after == before:
        raise RuntimeError(
            f"{dataset} run {run_index} at {threads} thread(s) wrote no rows to {results_table}. "
            f"Every virtual user failed identically -- see the RunPerfScenario snapshots above.")
    return loadtest_id
'''

RB_REFRAME = '''
from datetime import datetime, timezone

import requests

FABRIC_API = "https://api.fabric.microsoft.com"
POWERBI_API = "https://api.powerbi.com/v1.0/myorg"
MODEL_FOLDER = "dbx"       # workspace folder the recreated models are parked in


def _headers():
    return {"Authorization": f"Bearer {notebookutils.credentials.getToken('pbi')}",
            "Content-Type": "application/json"}


def _dataset_id(name):
    r = requests.get(f"{FABRIC_API}/v1/workspaces/{workspace_id}/semanticModels",
                     headers=_headers())
    r.raise_for_status()
    return next((i["id"] for i in r.json().get("value", []) if i["displayName"] == name), None)


def _reframe(dataset_id):
    """Full refresh so the Direct Lake tables are loaded. Everything here is ASYNC and that is the
    whole difficulty:

      * the POST itself can be REFUSED while a just-created model is still provisioning, so it is
        retried rather than raised on -- a 4xx here used to kill the run seconds after a
        successful create;
      * the refresh then runs in the background, so poll the specific request id from this POST.
        Do NOT trust `$top=1`: it can return a previous run's 'Completed' and hand back a model
        that has loaded nothing.

    Only when the poll says Completed has anything been loaded -- and even then the XMLA catalog
    lags, which is what `_wait_queryable` is for.
    """
    h = _headers()
    base = f"{POWERBI_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    r = None
    for attempt in range(40):                 # up to ~5 min of "not ready to refresh yet"
        r = requests.post(base, headers=h, json={"type": "full"})
        if r.status_code in (200, 201, 202):
            break
        print(f"  refresh not accepted yet (attempt {attempt + 1}): "
              f"HTTP {r.status_code} {r.text[:140]}", flush=True)
        time.sleep(8)
        h = _headers()                        # the token can also have been the problem
    else:
        raise Exception(f"refresh never accepted: HTTP {r.status_code} {r.text[:200]}")
    req_id = r.headers.get("Location", "").rstrip("/").split("/")[-1] or r.headers.get("RequestId", "")
    for _ in range(240):
        time.sleep(5)
        if req_id:
            st = requests.get(f"{base}/{req_id}", headers=h).json().get("status")
        else:
            st = requests.get(f"{base}?$top=1", headers=h).json().get("value", [{}])[0].get("status")
        if st == "Completed":
            # A reframe reports Completed before the XMLA catalog is reliably queryable, so
            # settle before the first connect.
            time.sleep(30)
            return
        if st in ("Failed", "Disabled"):
            raise Exception(f"reframe {st}")
    raise Exception("reframe timed out")


def _delete_model(ds_id, name):
    r = requests.delete(f"{FABRIC_API}/v1/workspaces/{workspace_id}/semanticModels/{ds_id}",
                        headers=_headers())
    print(f"  deleted existing '{name}' ({ds_id}) -> HTTP {r.status_code}", flush=True)
    if r.status_code not in (200, 202, 204, 404):
        raise Exception(f"could not delete '{name}': HTTP {r.status_code} {r.text[:200]}")
    # The name has to be free before the create, and a delete is not instant.
    for _ in range(60):
        if _dataset_id(name) is None:
            return
        time.sleep(5)
    raise Exception(f"'{name}' still present 5 minutes after delete")


def _create_model(name, parts):
    """Create the semantic model from the paper's TMDL, in the `dbx` workspace folder.

    Inline on purpose -- NO duckrun import. This notebook is the one that writes the results, and
    installing duckrun here would drag a different duckdb/deltalake pair into the session doing the
    Delta write, which is the class of problem the pinned python3.12 kernel exists to avoid.

    The whole difficulty is the 202. A create that returns one has NOT applied the definition yet,
    and the item name resolves before it has: reading the id back at that point produced a HOLLOW
    model -- item present, refresh Completed in nine seconds, `store_sales` and `Measures 1` not
    resolving, and every virtual user dying on its first real query. So a 202 is polled to
    `Succeeded` and the id is taken from the operation result, never from a name lookup.
    """
    body = {"displayName": name, "definition": {"parts": parts}}
    if FOLDER_ID:
        body["folderId"] = FOLDER_ID
    url = f"{FABRIC_API}/v1/workspaces/{workspace_id}/semanticModels"
    last = None
    for fmt in (None, "TMDL"):          # TMSL goes in with no format; TMDL may want one
        b = dict(body)
        if fmt:
            b["definition"] = dict(body["definition"], format=fmt)
        r = requests.post(url, headers=_headers(), json=b)
        if r.status_code in (200, 201):
            return r.json()["id"]
        if r.status_code == 202:
            return _await_created_item(r)
        last = f"HTTP {r.status_code} {r.text[:200]}"
        print(f"  create rejected (format={fmt}): {last}", flush=True)
    raise Exception(f"could not create semantic model '{name}': {last}")


def _await_created_item(resp):
    """Poll a create long-running-operation to Succeeded and return the id it reports."""
    location = resp.headers.get("Location")
    if not location:
        raise Exception("create returned 202 with no Location to poll")
    for _ in range(120):                # 10 minutes
        time.sleep(5)
        r = requests.get(location, headers=_headers())
        r.raise_for_status()
        body = r.json()
        status = body.get("status")
        if status == "Succeeded":
            if body.get("id"):
                return body["id"]
            # Some tenants return the item from a /result sub-url rather than inline.
            rr = requests.get(location.rstrip("/") + "/result", headers=_headers())
            rr.raise_for_status()
            return rr.json()["id"]
        if status in ("Failed", "Undetermined"):
            raise Exception(f"item create failed: {body}")
    raise Exception("timed out creating the semantic model")


def _folder_id(name):
    """The id of the root-level workspace folder `name`, or "" if it cannot be resolved. Cosmetic:
    a model in the workspace root measures exactly the same, so this never fails the run."""
    try:
        r = requests.get(f"{FABRIC_API}/v1/workspaces/{workspace_id}/folders", headers=_headers())
        for f in r.json().get("value", []):
            if f.get("displayName") == name and not f.get("parentFolderId"):
                return f["id"]
    except Exception as e:                                          # noqa: BLE001
        print(f"  folder lookup failed ({e}); creating in the workspace root", flush=True)
    return ""



def _wait_queryable(name, minutes=15):
    """Block until the DRIVER can open an XMLA connection to `name` and get an answer.

    A model that was just CREATED is not immediately on the XMLA endpoint -- the item exists, the
    refresh reports Completed, and `Initial Catalog=<name>` still fails for a while. Without this
    gate the 20 children each hit that window, each retries for five minutes, and each dies having
    written nothing: the pass reports "every virtual user failed identically" and names no cause.
    One connection from the driver is the cheap way to find out, and it fails HERE, with the
    reason, instead of twenty times in child sessions whose logs are hard to reach.

    Deliberately the SAME mechanism the children use (AdomdConnection over the workspace XMLA
    endpoint), not the executeQueries REST API: REST can answer while XMLA is still catching up,
    and the children speak XMLA.
    """
    import sempy.fabric as fabric
    try:
        fabric.create_tom_server()
    except Exception:
        pass
    from Microsoft.AnalysisServices.AdomdClient import AdomdConnection

    endpoint = f"powerbi://api.powerbi.com/v1.0/myorg/{ws_name}"
    deadline = time.time() + minutes * 60
    attempt, last = 0, None
    while time.time() < deadline:
        attempt += 1
        con = AdomdConnection(f"Data Source={endpoint};Initial Catalog={name};"
                              f"password={notebookutils.credentials.getToken('pbi')};Timeout=600;")
        try:
            con.Open()
            cmd = con.CreateCommand()
            # NOT `ROW("n", 1)`: that resolves against nothing and happily passes on a model
            # with no tables in it, which is exactly the failure this gate exists to catch. Query
            # what a child needs -- a fact table and a measure off `Measures 1` -- so a hollow
            # model keeps failing here instead of killing every virtual user five minutes later.
            cmd.CommandText = ('EVALUATE ROW("rows", COUNTROWS(store_sales), '
                               '"rev", [Store Revenue])')
            rdr = cmd.ExecuteReader()
            rdr.Close()
            con.Close()
            print(f"  '{name}' answers over XMLA (attempt {attempt})", flush=True)
            return
        except Exception as e:
            last = str(e)
            if attempt == 1 or attempt % 5 == 0:
                print(f"  not queryable yet (attempt {attempt}): {last[:140]}", flush=True)
            time.sleep(15)
    raise Exception(f"'{name}' never became queryable over {endpoint} within {minutes} min. "
                    f"Last error: {last}")


def prepare_model(name):
    """DELETE the model, recreate it from the embedded TMDL, reframe it. Returns (id, created_utc).

    Recreating is what makes the first pass genuinely cold: a reframe alone leaves whatever the
    previous run paged in, so pass 1 would measure a half-warm model and call it a transcode. The
    definition is the paper's own TMDL, embedded by build_notebooks.py, so what is measured here
    and what `fabric/deploy_paper_model.py` deploys cannot drift apart.
    """
    # ARM_OF is keyed on the TOKENISED model name, so resolve the arm by shape at this sf
    # rather than by literal -- the definitions are scale-factor agnostic now.
    arm = next((a for a in EMBEDDED_MODELS if model_name(a, sf) == name), None)
    if arm is None:
        raise Exception(f"no embedded definition for '{name}' at sf={sf} - expected one of "
                        f"{[model_name(a, sf) for a in EMBEDDED_MODELS]}")
    existing = _dataset_id(name)
    if existing:
        _delete_model(existing, name)
    print(f"  creating '{name}' from the paper's TMDL...", flush=True)
    created_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    ds_id = _create_model(name, model_parts(arm, sf))
    print(f"  created '{name}' ({ds_id}) at {created_utc}", flush=True)
    print(f"  reframing '{name}'...", flush=True)
    _reframe(ds_id)
    print("  reframe complete", flush=True)
    _wait_queryable(name)
    return ds_id, created_utc


FOLDER_ID = ""             # resolved once, in run_test


def run_test():
    global FOLDER_ID
    FOLDER_ID = _folder_id(MODEL_FOLDER)
    print(f"models will be created in folder {MODEL_FOLDER!r} ({FOLDER_ID or 'workspace root'})",
          flush=True)
    for dataset in model_to_test:
        print(f"\\n=== {dataset}", flush=True)
        # Recreating the model is FATAL if it fails, never skipped. Skipping produced a rung that
        # finished cleanly with one arm in it -- and a one-armed rung is indistinguishable,
        # downstream, from an arm that was never asked to run.
        model_id, created_utc = prepare_model(dataset)

        # THE PAPER'S PROTOCOL: three load tests over this ONE model, back to back. Run 1 is
        # cold in the only sense that matters -- the model came into existence moments ago, so
        # every column is transcoded inside the run by whichever query touches it first. That is
        # exactly what their Run 1 measures, which is why ours can be set beside theirs.
        #
        # NO sleep between runs. The gap is already runMultiple tearing down N child sessions and
        # starting N more; adding to it would only give the capacity time to evict what run 1 paid
        # for, and run 2 would come back part-cold.
        #
        # There is deliberately no transcode probe before run 1. It would page in every column the
        # suite reads and flatten run 1 into run 2, which destroys the warming curve -- the one
        # thing this protocol exists to produce.
        for r in range(1, runs + 1):
            print(f"  run {r} of {runs}: {concurrent_threads} thread(s), "
                  f"model created {created_utc}", flush=True)
            run_dax(ws_name, dataset, run_index=r, threads=concurrent_threads,
                    model_id=model_id, model_created_utc=created_utc)

        # All three runs are recorded, so the model has no further use -- drop it. This is the ONE
        # deviation from the paper's protocol (they left theirs standing): nothing then holds
        # capacity memory through the cool-down, and the next arm creates into an empty workspace
        # instead of racing a delete. The delete at the START of prepare_model stays as the safety
        # net: a run that died mid-pass leaves its model behind on purpose, for inspection, and the
        # next run clears it.
        # Deliberately NOT in a finally: a failed run keeps its model so there is something to
        # look at.
        _delete_model(model_id, dataset)

        time.sleep(300)   # cool-down after each model so back-to-back rungs start rested
    return "done"
'''

RB_RUN = '''
run_test()
'''



def transcode_query():
    """One DAX query that pages in EXACTLY the columns the warm suite reads, and nothing else.

    This is what the cold pass runs. Running the 24-query suite cold measured the wrong thing: the
    transcode is paid once, by whichever query first touches each column, so queries 5-24 ran warm
    inside the "cold" pass and dragged its median down to near the warm one (268 ms against 138 ms)
    while the suite behind it took 45 s against 3 s. One query, one duration, and that duration IS
    the transcode.

    The column set is DERIVED, never hand-listed, from three places -- miss any one and the probe
    silently under-transcodes:
      * literal column references in the 24 captured queries;
      * column references inside `Measures 1`, where every fact measure actually lives (the
        captured DAX names only 11 columns; the heavy ones are all inside the measures);
      * every relationship key, which the engine needs to resolve the joins.
    Types come from the TMDL, so numerics get SUM -- which reads every value -- and text gets MIN.
    """
    import re
    base = os.path.join(PAPER_MODEL, "fabric_mirrored")
    with open(SUITE, encoding="utf-8") as f:
        txt = "\n".join(e["metrics"]["QueryText"]
                        for e in json.load(f)["events"] if e.get("name") == "Execute DAX Query")
    with open(os.path.join(base, "tables", "Measures 1.tmdl"), encoding="utf-8") as f:
        txt += f.read()
    refs = (set(re.findall(r"'([^']+)'\[([^\]]+)\]", txt))
            | set(re.findall(r"(?<!['\w])(\w+)\[([^\]]+)\]", txt)))
    with open(os.path.join(base, "relationships.tmdl"), encoding="utf-8") as f:
        refs |= set(re.findall(r"(?:from|to)Column:\s*([\w ]+)\.([\w]+)", f.read()))
    refs = {(t.strip(), c.strip()) for t, c in refs
            if t.strip() not in {"Measures 1", "Time Unit"} and not t.startswith("__")}

    types = {}
    for fn in os.listdir(os.path.join(base, "tables")):
        with open(os.path.join(base, "tables", fn), encoding="utf-8") as f:
            body = f.read()
        for m in re.finditer(r"column\s+'?([\w ]+?)'?\n(.*?)(?=\n\tcolumn |\n\tpartition |\Z)",
                             body, re.S):
            dt = re.search(r"dataType:\s*(\w+)", m.group(2))
            if dt:
                types[(fn[:-5], m.group(1).strip())] = dt.group(1)

    cols = sorted(c for c in refs if c in types)
    unresolved = sorted(c for c in refs if c not in types)
    assert not unresolved, f"columns with no dataType in the TMDL: {unresolved}"
    NUMERIC = {"int64", "double", "decimal"}
    body = ",\n    ".join(
        f'"c{i}", {"SUM" if types[c] in NUMERIC else "MIN"}(\'{c[0]}\'[{c[1]}])'
        for i, c in enumerate(cols))
    print(f"  transcode probe: {len(cols)} columns across {len({t for t, _ in cols})} tables")
    return "EVALUATE ROW(\n    " + body + "\n)"


def embedded_suite_cell():
    """The DAX suite as a literal in the notebook, so the runner needs no OneLake read."""
    with open(SUITE, encoding="utf-8") as f:
        suite = f.read()
    n = sum(1 for e in json.loads(suite)["events"] if e.get("name") == "Execute DAX Query")
    assert "'''" not in suite, "the suite would break the triple-quoted literal"
    print(f"  embedding {n} queries from {SUITE}")
    return ("# The DAX suite, inlined VERBATIM from paper/PowerBIPerformanceData.json -- the white\n"
            "# paper's own Performance Analyzer capture, unedited: github.com/lipinght/DB-DQ-Whitepaper (MIT), commit 99f9904d.\n"
            "# Regenerate after touching that file: python notebooks/build_notebooks.py\n"
            "EMBEDDED_SUITE = r'''" + suite + "'''\n")


# ===========================================================================================
# results — read perfresults3 in Fabric and answer the one question
# ===========================================================================================

RES_DOC = '''
# Can a Databricks-written table be good enough without V-Order?

Direct Lake models over **the same TPC-DS rows**, differing only in who wrote the parquet
and how:

| arm | writer | V-Order | layout |
|---|---|---|---|
| `tpcds_sf{sf}_cluster` | **Databricks**, parquet-mr, Photon off | **no** | the SAME config as `default` plus `CLUSTER BY` on the date key, declared on an empty table and then appended, so the rows are PLACED by the key on write. This is how an ordering reaches the files on this write path: a `df.orderBy()` does not, because Optimized Writes plans a repartition above it and `EliminateSorts` deletes the sort |
| `tpcds_sf{sf}_default` | **Databricks**, parquet-mr, Photon off | **no** | **configuration only**: plain `saveAsTable` -- optimize-write bin 4096 MiB, ~6M-row row groups (one per file), dictionary kept, ZSTD, no ordering, no `OPTIMIZE` |
| `tpcds_sf{sf}_partition` | **Databricks**, parquet-mr, Photon off | **no** | the paper's Fabric arm reproduced minus V-Order: `partitionBy` the date key and nothing else. One file per date, ~143k-row row groups, both recipe row caps inert far above it. `partition` vs `vorder` is V-Order ALONE at that geometry |
| `tpcds_sf{sf}_vorder` | **Fabric Spark** | **yes** | the paper's arm, four things at once: V-Order, ZSTD, optimize write, **partitioned by the date key** and **Z-ordered on the address key**. The partition lands ONE FILE PER PARTITION -- 1,823 files of ~143k rows at SF100, so ~1,800 Direct Lake segments |
| `tpcds_sf{sf}_vonly` | **Fabric Spark** | **yes** | V-Order and nothing else: **no partition, no sort, no `OPTIMIZE`**. Its geometry lands on `default`'s (measured 5.8M rows/file against 5.6M, 45 files against 46), so `vonly` vs `default` is V-Order encoding ALONE, with everything else held equal |
| `tpcds_sf{sf}_duckdb` | **delta_rs** (delta-rs, via duckrun) | **no** | the third writer, reading the `default` rows back and rewriting them under duckrun's fixed 0.4.68 profile: a 4M-row row-group CEILING inside a 256 MB file roll (so every file ends on a truncated group), a 32 MB dictionary page limit, **SNAPPY**. `SORTED BY AUTO` -- duckrun profiles the data and picks the ORDER BY itself, to minimise modelled in-memory bytes rather than to prune. The key it chose is in the build notebook's output |

V-Order cannot be produced outside Fabric. The question is whether the row-group geometry, the
dictionary and the ordering get close enough that it does not matter.

The `duck*` arms are SNAPPY where every other arm is ZSTD -- duckrun's notebook API cannot set the
codec -- so their **bytes** are not comparable to any other arm's. Their **encodings** are, because
dictionary coverage is classified from the parquet footer rather than from size.

The pair that settles it is **`default` vs `vonly`**: same rows, same geometry, no ordering and no
partition on either side, so the only thing left between them is the writer's encoding. The second
chart below draws exactly those two. `vorder` answers a different question -- what the paper
measured -- and `cluster` answers a third, whether an ordering that reaches the files pays.

The protocol is the **paper's own**: each arm's model is created once, measured by **three
back-to-back load tests**, then deleted. `run_index` 1 is the first touch of a model created
minutes earlier -- it pays the transcode inside the suite, exactly as their Run 1 does; 2 and 3 are
the same suite again on the same model.

So there are two comparisons here, and they answer different questions:

* **steady state (runs 2-3)** -- what the parquet layout costs a resident model. This is the arm
  vs arm number.
* **run 1 -> 2 -> 3** -- whether an arm warms up at all. That curve is the paper's own headline:
  at SF100/20 users its V-Order arm went 99.5 / 5.0 / 6.2 s while its mirrored arm went
  114.8 / 142.7 / 166.9 s and never warmed. A fixed mirroring tax cannot produce those two shapes;
  layout can.

The suite number quoted throughout is theirs: the **sum of the per-query medians over the 15
visual queries**, medians taken per query first (a pooled median would weight a query by how many
readers finished it).

One asymmetry to keep in mind on run 1: the mirrored arm's files live in the Databricks metastore's
storage (North Europe) while this capacity is in West Central US; the V-Order arm reads OneLake in
region. That costs the mirrored arm a transatlantic round trip on every first read, and nothing at
steady state, where no storage is on the path.
'''

RES_INSTALL = '''
!pip install -q duckrun --upgrade
notebookutils.session.restartPython()
'''

RES_BOOTSTRAP = '''
import duckdb
import notebookutils

lh_name = "tpcds_bench"          # holds perfresults3
results_table = "perfresults3"   # the THREE-RUN protocol's table. `perfresults` is the old
                                 # probe + one-warm-pass protocol: different definition of a first
                                 # pass, never to be unioned with this one. Point this at a smoke
                                 # table to read a rehearsal run instead.
MIRROR_ITEM = "01b539f3-4a9d-45ef-b1ef-0ba59552eb21"   # mirrored Azure Databricks catalog
VORDER_LH = "tpcds_vorder"       # the Fabric Spark V-Order copy
# Scale factor is a PARAMETER, not a constant: nothing here is regenerated to move between
# scale factors. Override `sf` in the parameters cell (or from a pipeline) and the schemas,
# the model names and the stats targets all follow.
sf = 100
VORDER_SCHEMA = f"tpcds_sf{sf}"
N_QUERIES = 24                   # all-24-or-out; a partial suite is not a run
RUNS = 3                         # load tests per model lifetime -- the paper's Run column
SLICER = (3, 7, 8, 11, 15, 16, 17, 18, 21)   # the 9 slicer queries, reported apart

ws_id = notebookutils.runtime.context["currentWorkspaceId"]
lh_id = notebookutils.lakehouse.get(lh_name)["id"]
# GUIDs, never friendly names: this tenant has OneLake friendly-name support disabled, so
# `<ws>/tpcds_vorder.Lakehouse` is refused outright while `<ws-guid>/<item-guid>` works everywhere.
vorder_id = notebookutils.lakehouse.get(VORDER_LH)["id"]
delta_path = (f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}/"
              f"Tables/dbo/{results_table}")
print(delta_path)
'''

RES_COMPACT = '''
# Every virtual user appends its own small file, so 20 users x 3 runs x N arms leaves the table a
# heap of tiny files. Compacting is housekeeping, not analysis -- it changes no number below.
#
# The vacuum keeps a WEEK. It used to keep nothing (retention_hours=0,
# enforce_retention_duration=False) and that destroyed the table on 2026-09-08: vacuum deletes every
# parquet under the folder that the CURRENT snapshot does not reference, which includes a file a
# concurrent `write_deltalake(mode="append")` has already uploaded but not yet committed. The
# appender then commits an `add` for a file that is gone, and from that moment every full read of
# perfresults3 -- including the `count_rows()` guard that every run notebook opens with -- fails
# with a 404 on the missing blob. That is what the 7-day default and `enforce_retention_duration`
# exist to prevent, so both are back on. Old files now linger for a week instead of being reclaimed
# at once; the table is a few hundred KB of results, so that costs nothing.
#
# If the table is ALREADY broken, `DeltaTable(delta_path).repair(dry_run=False)` drops the dangling
# references (FSCK) and the rows in the deleted file are lost -- the all-24-or-out filter below
# discards the affected run anyway.
from deltalake import DeltaTable
_dt = DeltaTable(delta_path)
_dt.optimize.compact()
_dt.vacuum(retention_hours=168, dry_run=False)
print("compacted")
'''

RES_RUNS = '''
# The comparable result set. Two filters, both applied to EVERY run including run 1:
#   all-24-or-out  a run is one (loadtest_id, thread_id, iteration) and counts only if all 24
#                  queries SUCCEEDED -- a failed query still writes its row, so counting rows would
#                  let a run that errored on three queries pass as complete.
#   short rung     a rung that asked for 20 users and got 8 measured 8-way concurrency.
import pyarrow as pa
from deltalake import DeltaTable

perf = pa.table(DeltaTable(delta_path).to_pyarrow_table())
duckdb.register("perf", perf)

# Arm from the last '_' token of the model name -- exact equality, no pattern matching.
# Every one of these names a LAYOUT. Which engine wrote the files is not a variable in this
# experiment -- VertiPaq reads parquet from any producer -- and labelling the arms "dbx" vs "Fabric"
# made the charts read as a producer comparison, which is the exact misreading the whole thing
# exists to prevent. V-Order stays in the names because it IS a layout property (an encoding plus an
# in-row-group sort), not an engine.
# A REMOVED arm's rows are filtered out below rather than relabelled: nothing is ever deleted from
# perfresults3, so without the filter those rows reach the charts through ELSE as a model name
# pretending to be an arm. Removed from the repo on
# 2026-09-08. Its rows stay in perfresults3 -- nothing is deleted from the results table -- and are
# excluded here so they cannot be read as a fifth layout.
ARM_DEFAULT = "Databricks: the recipe (6M rows per file)"
# The same unordered write at 8M. `default` is 6M with one row group per file and this is 8M with
# one per file, so segment SIZE is the only thing between them -- the test of whether size is a
# lever without an ordering. Prediction on the record, and MEASURED: within noise of `default`.
ARM_DEFAULTF8 = "Databricks: 8M rows per file"
# WITHDRAWN 2026-09-09, and it is the arm that earned its own withdrawal. It held 6M row groups TWO
# to a 12M-row file, which only `parquet.block.row.count.limit` can express, and it measured
# identical to one group per file. So the key bought nothing -- and it needs parquet-java 1.16,
# where every older runtime accepts it and ignores it. The key left the recipe and this arm left
# with it. Its rows stay in perfresults3 and its chunks in the footer export, filtered by name.
ARM_DEFAULT2RG = "Databricks: 6M groups, 2 per file (withdrawn)"
# The clustered arm again, in SNAPPY and with the 128 MB byte target the first clustered build
# predates -- the two things the delta_rs arm had and ours did not. Its pair is `cluster`, and
# the question is COLD: delta_rs transcodes in 17.7 s where our clustered arm takes 36.9.
ARM_CLUSTERSN = "Databricks: clustered by date, snappy"
ARM_CLUSTER, ARM_VORDER = "Databricks: clustered by date", "Fabric: partition per date + Z-order + V-Order"
ARM_VONLY = "Fabric: V-Order"
ARM_PARTITION = "Databricks: partition per date"
# The delta_rs arms name the WRITER first, unlike every label above. On the other arms the writer is
# a constant within its cloud and the layout is the variable; here the layout is deliberately held
# at what the Databricks arms already ran and the WRITER is what changed, so a label that said only
# "4M groups" would hide the one thing the arm exists to vary.
ARM_DUCKDB = "delta_rs: auto sort key"
ARM_DUCKSORT = "delta_rs: sorted per date"

# A removed arm leaves rows in `perfresults3` AND chunks in the `layout_stats` footer export --
# nothing is ever deleted from either -- so it has to be filtered by NAME in BOTH places, and this
# is the one list. Drop an arm from the repo without adding it here and it comes back through the
# ELSE above as a model name pretending to be an arm, or as a timing-less row in the layout table.
# Never re-use a token that has been here.
WITHDRAWN_ARMS = ("sort", "nosort4m", "default2rg")
_WITHDRAWN = ", ".join(f"'{a}'" for a in WITHDRAWN_ARMS)
duckdb.sql(f"""
    CREATE OR REPLACE TABLE scanned AS
    SELECT *,
           CASE str_split(model, '_')[-1]
                WHEN 'default' THEN '{ARM_DEFAULT}'
                WHEN 'defaultf8' THEN '{ARM_DEFAULTF8}'
                WHEN 'default2rg' THEN '{ARM_DEFAULT2RG}'
                WHEN 'cluster' THEN '{ARM_CLUSTER}'
                WHEN 'clustersn' THEN '{ARM_CLUSTERSN}'
                WHEN 'vorder' THEN '{ARM_VORDER}'
                WHEN 'vonly' THEN '{ARM_VONLY}'
                WHEN 'partition' THEN '{ARM_PARTITION}'
                WHEN 'duckdb' THEN '{ARM_DUCKDB}'
                WHEN 'ducksort' THEN '{ARM_DUCKSORT}'
                ELSE model END AS arm,
           CAST(regexp_extract(model, 'sf([0-9]+)', 1) AS INTEGER) AS sf
    FROM perf
    WHERE str_split(model, '_')[-1] NOT IN ({_WITHDRAWN})   -- removed arms; rows stay in the table
""")
duckdb.sql(f"""
    CREATE OR REPLACE TABLE runs AS
    WITH complete AS (
        SELECT loadtest_id, thread_id, iteration FROM scanned
        GROUP BY 1,2,3
        HAVING COUNT(DISTINCT CASE WHEN error IS NULL THEN query_number END) = {N_QUERIES}
    ), full_rung AS (
        SELECT s.loadtest_id FROM scanned s JOIN complete USING (loadtest_id, thread_id, iteration)
        GROUP BY s.loadtest_id
        HAVING COUNT(DISTINCT s.thread_id) = MAX(s.concurrent_threads)
    )
    SELECT s.* FROM scanned s
    JOIN complete USING (loadtest_id, thread_id, iteration)
    JOIN full_rung USING (loadtest_id)
    WHERE s.error IS NULL
""")
kept = duckdb.sql("SELECT count(*) FROM runs").fetchone()[0]
total = duckdb.sql("SELECT count(*) FROM scanned").fetchone()[0]
print(f"{kept:,} of {total:,} rows are comparable")
'''

# ===========================================================================================
# layout_stats — the parquet geometry of both arms. EXPENSIVE, and needed once.
# ===========================================================================================

STATS_CONFIGURE = '''%%configure
{ "vCores": {  "parameterName": "pipelinecore",  "defaultValue": 8 }}
'''

# Parameters, alone in their own cell so the arm list and the scale factor can be edited without
# scrolling into the body -- and so a pipeline can override them.
STATS_PARAMS = '''
sf = 100
# Which arms to read. Drop names to make a run cheaper: every arm costs a full footer sweep of both
# facts. `default` is the control for the ordering question and is worth keeping in.
arms = "default,partition,cluster,vorder,vonly,duckdb"
'''

STATS_DOC = '''
# What the arms are actually sitting on

`duckrun.get_stats(detailed=True)` reads the Delta log for the live file list and then every parquet
**footer**, returning one row per **column chunk per row group** -- `encodings`,
`dictionary_page_offset`, `total_compressed_size` and the min/max stats. That per-chunk detail is
the point: the aggregated form (`detailed=False`) reports `avg_row_group` and a compression string
and cannot answer either question this notebook exists for. It is also the only footer read here --
file counts, rows per file and row groups per file are all derived from those chunks.

**How much of the data is dictionary-encoded?** One number, by BYTES, off the `encodings` in the
footer: the chunk lists a dictionary encoding, or it does not. Nothing more is read into the list --
a bare `PLAIN` beside a dictionary means different things per writer (delta_rs puts one on every
dictionary chunk, because the dictionary page is itself PLAIN-encoded), so reading it as a fallback
signal scored that writer at zero. Whatever is not, VertiPaq has to build a
dictionary for at transcode -- time and memory on every cold read -- and that is the whole reason
the number is here. WHY a given column missed out is the writer's business and is not reported:
reading a reason out of an encoding list is guesswork.

**Did the ordering reach the files?** Row-group min/max on the first key: disjoint, monotonic ranges
mean whole row groups can be eliminated, overlapping ones mean they cannot, whatever the writer was
asked to do. The unsorted arm is expected to overlap on every neighbour -- it is the control.

**Everything computed here is written to the `tpcds_bench` lakehouse, Files section,
`Files/layout_stats/sf{sf}`** (raw chunks per arm as parquet, the classified chunks, and the summary
tables as CSV), so the numbers can be read outside Fabric: `python fabric/pull_layout_stats.py`.

**This is expensive** -- it opens every file of every arm -- and the layout only changes when the
tables are rebuilt. Run it once after a build, not on every results pass.
'''

STATS_BODY = '''
import os
import tempfile

import duckdb
import duckrun
import notebookutils
import pandas as pd

MIRROR_ITEM = "01b539f3-4a9d-45ef-b1ef-0ba59552eb21"   # mirrored Azure Databricks catalog
VORDER_LH = "tpcds_vorder"
BENCH_LH = "tpcds_bench"                                # results land in its Files section
# Scale factor is a PARAMETER, not a constant. Override `sf` in the parameters cell (or from a
# pipeline) and the schemas follow.
# `sf` and `arms` come from the parameters cell above.
RG_LO, RG_HI = 1_000_000, 16_000_000                   # Direct Lake's usable row-group window
FACTS = ("store_sales", "catalog_sales")
# First key of each arm's ordering -- the column whose row-group ranges say whether the ordering
# reached the files. `default` is expected to overlap on every neighbour; it is the control.
KEY = {"store_sales": "ss_sold_date_sk", "catalog_sales": "cs_sold_date_sk"}

ws_id = notebookutils.runtime.context["currentWorkspaceId"]
# GUIDs, never friendly names: this tenant has OneLake friendly-name support disabled.
vorder_id = notebookutils.lakehouse.get(VORDER_LH)["id"]
bench_id = notebookutils.lakehouse.get(BENCH_LH)["id"]
# arm -> (item holding it, schema). The mirrored Databricks arms all live in one item; the
# Fabric-written arms live in the tpcds_vorder lakehouse.
#
# The `duck*` arms are in tpcds_vorder too, and that is a name the lakehouse does not describe:
# they are written by delta_rs (delta-rs, via duckrun), NOT by the V-Order writer. The arm is the
# schema suffix, as everywhere here; the item is just where it is parked.
ALL_SESSIONS = {"default":    (MIRROR_ITEM, f"tpcds_sf{sf}_default"),
                "defaultf8": (MIRROR_ITEM, f"tpcds_sf{sf}_defaultf8"),
                "default2rg": (MIRROR_ITEM, f"tpcds_sf{sf}_default2rg"),
                "partition": (MIRROR_ITEM, f"tpcds_sf{sf}_partition"),
                "cluster":   (MIRROR_ITEM, f"tpcds_sf{sf}_cluster"),
                "clustersn": (MIRROR_ITEM, f"tpcds_sf{sf}_clustersn"),
                "vorder":    (vorder_id,   f"tpcds_sf{sf}"),
                "vonly":     (vorder_id,   f"tpcds_sf{sf}_vonly"),
                "duckdb":    (vorder_id,   f"tpcds_sf{sf}_duckdb"),
                "ducksort":  (vorder_id,   f"tpcds_sf{sf}_ducksort")}
_want = [a.strip() for a in arms.split(",") if a.strip()]
_bad = [a for a in _want if a not in ALL_SESSIONS]
if _bad:
    raise ValueError(f"unknown arm(s) {_bad}; expected any of {list(ALL_SESSIONS)}")
SESSIONS = tuple((a, *ALL_SESSIONS[a]) for a in _want)
print(f"sf{sf}, reading: " + ", ".join(f"{a} ({sch})" for a, _, sch in SESSIONS))

# Everything is staged on local disk first, then uploaded in one call at the end. The raw footer
# read goes to parquet straight from the DuckDB relation (exact types, no pandas round trip) --
# and `get_stats` is the ONE footer read per arm: the notebook then works off the local file, and
# everything below (file counts, rows per file, row groups per file) is derived from those chunks
# rather than re-opening the footers. No `parquet_file_metadata` pass: it re-opens every footer a
# second time over OneLake, one file at a time, and the only thing it adds is `created_by`, which
# says nothing the arm's name does not.
OUT_DIR = tempfile.mkdtemp(prefix=f"layout_stats_sf{sf}_")
OUT = f"layout_stats/sf{sf}"                            # relative to the lakehouse Files section

# Not every arm exists at every sf, so an arm whose schema is absent is reported and skipped rather
# than killing the notebook.
got = []
for arm, item, schema in SESSIONS:
    path = os.path.join(OUT_DIR, f"chunks_{arm}.parquet")
    try:
        sess = duckrun.connect(f"{ws_id}/{item}", schema=schema, name=arm)
        sess.get_stats(detailed=True).project(f"'{arm}' AS arm, *").write_parquet(path)
    except Exception as e:                                          # noqa: BLE001
        print(f"{arm:<8} no stats for schema {schema} ({str(e)[:110]})")
        continue
    got.append(arm)
    n, nf = duckdb.sql(f"SELECT count(*), count(DISTINCT file_name) "
                       f"FROM read_parquet('{path}')").fetchone()
    print(f"{arm:<8} {n:,} column chunks over {nf:,} files")
assert got, "no arm produced stats at this sf"

duckdb.sql(f"""
    CREATE OR REPLACE TABLE chunks AS
    SELECT * FROM read_parquet('{OUT_DIR}/chunks_*.parquet', union_by_name = true)
""")
# Per-file shape, straight off the chunks. A row group contributes one row per column, so the
# distinct (file, row_group_id) pairs are taken first -- summing row_group_num_rows over the raw
# chunks would multiply every count by the column count.
files_df = duckdb.sql("""
    WITH rg AS (SELECT DISTINCT arm, "table", file_name, row_group_id, row_group_num_rows AS rows
                FROM chunks)
    SELECT arm, "table", file_name, sum(rows) AS num_rows, count(*) AS num_row_groups
    FROM rg GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
""").df()
'''

STATS_DICT = '''
# One row per column chunk, classified. `encodings` is a comma-separated list, so it is split and
# trimmed rather than matched with LIKE: a bare LIKE '%PLAIN%' also matches PLAIN_DICTIONARY and
# would report every dictionary-encoded chunk as a fallback.
duckdb.sql("""
    CREATE OR REPLACE TABLE enc AS
    WITH e AS (
        SELECT arm, "table" AS tbl, path_in_schema AS col, file_name, row_group_id,
               row_group_num_rows AS rg_rows, total_compressed_size AS bytes,
               list_transform(str_split(coalesce(encodings, ''), ','), x -> trim(x)) AS encs
        FROM chunks
    )
    SELECT *,
           -- Dictionary-encoded, or not. A dictionary encoding in the list is the whole test, and
           -- it has to be, because a bare PLAIN alongside it means different things per writer:
           -- delta_rs lists PLAIN on EVERY dictionary chunk (the dictionary page itself is
           -- PLAIN-encoded), so excluding those scored the whole delta_rs family at zero. Measured
           -- on this project's exports, no parquet-mr chunk has ever carried both, so this is the
           -- same number as the old rule everywhere it was not simply wrong.
           list_contains(encs, 'RLE_DICTIONARY') OR list_contains(encs, 'PLAIN_DICTIONARY') AS is_dict
    FROM e
""")

print("--- Dictionary, by BYTES. Whatever is not dictionary-encoded gets its dictionary rebuilt by")
print("    VertiPaq on every cold read.")
summary_dict = duckdb.sql("""
    SELECT arm, tbl AS "table",
           round(100.0 * sum(bytes) FILTER (WHERE is_dict) / sum(bytes), 1) AS pct_bytes_dict,
           count(*) FILTER (WHERE NOT is_dict)              AS chunks_not_dict,
           count(*)                                         AS chunks,
           count(DISTINCT col) FILTER (WHERE NOT is_dict)   AS cols_not_dict,
           count(DISTINCT col)                              AS cols
    FROM enc GROUP BY 1, 2 ORDER BY 1, 2
""").df()
display(summary_dict)

# Every column of every table, facts first, ranked by MB -- this is what gets written out. The
# display below cuts it to what costs: the facts' non-dictionary columns, biggest first. A wide
# column missing its dictionary is what costs; a narrow one is noise.
columns_df = duckdb.sql(f"""
    SELECT arm, tbl AS "table", col, is_dict,
           count(*) AS chunks, round(sum(bytes) / 1048576.0, 1) AS mb,
           any_value(list_aggregate(encs, 'string_agg', ', ')) AS encodings
    FROM enc
    GROUP BY 1, 2, 3, 4 ORDER BY (tbl IN {FACTS}) DESC, arm, mb DESC
""").df()
print("--- The fact columns that are NOT dictionary-encoded.")
display(columns_df[columns_df["table"].isin(FACTS) & ~columns_df["is_dict"]])
'''

STATS_GEOMETRY = '''
# Row-group geometry, from the same footer read. One row group per file is the shape the recipe
# targets; `pct_in_window` is the paper's own Table 9.3.2.1 metric. `spread` (max/min over the
# groups that are not the remainder) is the UNIFORMITY criterion -- ragged groups are the failure
# mode, and the cluster arm is the one at risk of them: it hits its row target with
# maxRecordsPerFile, which leaves one short tail per task.
print(f"--- Row groups. Direct Lake's window is {RG_LO:,}..{RG_HI:,} rows.")
row_groups = duckdb.sql(f"""
    WITH rg AS (SELECT DISTINCT arm, tbl, file_name, row_group_id, rg_rows FROM enc),
         r AS (SELECT *, count(*) OVER (PARTITION BY arm, tbl) AS n,
                      row_number() OVER (PARTITION BY arm, tbl ORDER BY rg_rows) AS smallest
               FROM rg)
    SELECT arm, tbl AS "table", count(DISTINCT file_name) AS files, count(*) AS row_groups,
           round(count(*)::DOUBLE / count(DISTINCT file_name), 2) AS rg_per_file,
           min(rg_rows) AS rg_min, round(avg(rg_rows)) AS rg_avg, max(rg_rows) AS rg_max,
           -- the single smallest group is the remainder bin and is judged apart, as in rowgroup_probe
           round(max(rg_rows)::DOUBLE / nullif(min(rg_rows) FILTER (WHERE smallest > 1 OR n = 1), 0), 3)
               AS spread,
           count(*) FILTER (WHERE rg_rows < {RG_LO}) AS groups_under_1m,
           round(100.0 * count(*) FILTER (WHERE rg_rows BETWEEN {RG_LO} AND {RG_HI}) / count(*), 1)
               AS pct_in_window
    FROM r GROUP BY 1, 2 ORDER BY 1, 2
""").df()
display(row_groups)

# Did the ordering reach the files? Row groups sorted by their low bound on the first key: if any
# range starts before its predecessor ends, that pair overlaps and a filter landing in the overlap
# eliminates neither. `default` is the control and should overlap nearly everywhere; `cluster` is
# the arm this measures -- 0 overlaps means clustering on write placed the rows, a high count means
# it did not and the arm measures nothing.
#
# A key that is a HIVE PARTITION column is not in the parquet file at all (the V-Order arm is
# partitioned by the date key), so there are no chunks and no stats to read. That is reported as
# `partitioned` -- a stronger form of elimination, not a failure -- and never as "0 row groups,
# not eliminable", which is what it used to look like.
print("--- Ordering on the first key. overlaps = neighbouring row-group ranges that intersect.")
key_rows = []
for arm, tbl in duckdb.sql(
        f"SELECT DISTINCT arm, tbl FROM enc WHERE tbl IN {FACTS} ORDER BY 1, 2").fetchall():
    key = KEY[tbl]
    present = duckdb.sql("""
        SELECT count(*) FROM chunks WHERE arm = $arm AND "table" = $tbl AND path_in_schema = $key
    """, params={"arm": arm, "tbl": tbl, "key": key}).fetchone()[0]
    if not present:
        key_rows.append({"arm": arm, "table": tbl, "key": key, "row_groups": 0, "overlaps": None,
                         "verdict": "partitioned (key not in the file)"})
        continue
    r = duckdb.sql("""
        WITH r AS (
            SELECT TRY_CAST(any_value(stats_min_value) AS BIGINT) AS lo,
                   TRY_CAST(any_value(stats_max_value) AS BIGINT) AS hi
            FROM chunks
            WHERE arm = $arm AND "table" = $tbl AND path_in_schema = $key
              AND stats_min_value IS NOT NULL
            GROUP BY file_name, row_group_id
        ), o AS (SELECT lo, hi, lag(hi) OVER (ORDER BY lo, hi) AS prev_hi FROM r)
        SELECT count(*), count(*) FILTER (WHERE prev_hi IS NOT NULL AND prev_hi > lo) FROM o
    """, params={"arm": arm, "tbl": tbl, "key": key}).fetchone()
    verdict = ("no min/max stats on the key" if r[0] == 0 else
               "eliminable" if r[1] == 0 else f"{round(100.0 * r[1] / r[0])}% of neighbours overlap")
    key_rows.append({"arm": arm, "table": tbl, "key": key, "row_groups": r[0],
                     "overlaps": r[1], "verdict": verdict})
ordering = pd.DataFrame(key_rows)
display(ordering)
'''


STATS_WRITE = '''
# Write everything to the tpcds_bench lakehouse, Files section, so it can be read outside Fabric
# (`python fabric/pull_layout_stats.py --sf <sf>`). The raw per-arm footer reads are already on
# local disk as chunks_<arm>.parquet; the classified chunks and the summary tables join them, and
# duckrun uploads the folder in one call. overwrite=True: a re-run at the same sf replaces the set.
duckdb.sql(f"COPY enc TO '{OUT_DIR}/enc.parquet' (FORMAT PARQUET)")
for name, df in {"files": files_df, "summary_dict": summary_dict, "columns": columns_df,
                 "row_groups": row_groups, "ordering": ordering}.items():
    df.to_csv(os.path.join(OUT_DIR, f"{name}.csv"), index=False)
print("staged:", sorted(os.listdir(OUT_DIR)))

bench = duckrun.connect(f"{ws_id}/{bench_id}", name="bench")
bench.copy(OUT_DIR, OUT, overwrite=True)
print(f"written to lakehouse {BENCH_LH}: Files/{OUT}")
'''


RES_HEADLINE = '''
# THE table. One row per (sf, arm, run) -- filter it however you like.
#
# TWO-STEP MEDIAN, as the paper's analysis does it: median across readers WITHIN a query first,
# then combine queries. `suite_s` is the SUM of those per-query medians over the 15 visual queries,
# so it is the query time ONE reader spends making a full pass -- not an average, and not the load
# test's wall clock (readers run concurrently and there is `delay_sec` think time between queries).
# It is the paper's own metric: its 99.5 / 5.0 / 6.2 (V-Order) and 114.8 / 142.7 / 166.9 (mirrored)
# are exactly this number.
#
# Read ACROSS the runs: run 1 is the first touch of a model created minutes earlier and pays the
# transcode inside the suite; runs 2-3 are the same suite on the same resident model.
duckdb.sql("""
    CREATE OR REPLACE TABLE tops AS
    SELECT sf, max(concurrent_threads) AS top FROM runs GROUP BY 1
""")
SFS = [r[0] for r in duckdb.sql("SELECT sf FROM tops ORDER BY sf").fetchall()]
RUN_INDEXES = [r[0] for r in duckdb.sql("SELECT DISTINCT run_index FROM runs ORDER BY 1").fetchall()]

# Step one of the two-step, and the base the chart draws from.
duckdb.sql("""
    CREATE OR REPLACE TABLE per_q AS
    SELECT r.sf, r.arm, r.run_index, r.query_number,
           any_value(r.visual_name) AS visual_name,
           median(r.duration) AS d,
           count(*) AS execs
    FROM runs r JOIN tops t ON r.sf = t.sf AND r.concurrent_threads = t.top
    GROUP BY ALL
""")

# `summary` is THE table, and the chart below plots straight out of it -- one number cannot then
# disagree with the other.
#
# suite_s is the two-step per-query statistic (the paper's metric). p50 / p95 / max are NOT: they
# come straight off the RAW executions -- every reader's every visual query in the run, one pool of
# 20 x 15 durations -- so the three of them describe one distribution: typical, tail, worst.
#
# They used to be computed over the 15 per-query medians instead (`median(d)`, `median(d95)`), and
# `p95_ms` in particular was then a median-of-p95s, nobody's percentile: the V-Order cold run read
# 1,300 ms while 5% of its executions were over 24,100 ms and its worst was 26,518 ms.
#
# load_tests / models come from a SEPARATE aggregate, joined at group level. Joining row-level
# `runs` into the per-query query fanned every row out by its 480 executions and multiplied
# suite_s by 480 (it read 33,047 where the answer was 68.8).
#
# load_tests > 1 means the arm was benchmarked more than once and this row POOLS those sessions.
# Fine when they agree; check RES_PERQUERY before trusting a row where they might not.
duckdb.sql(f"""
    CREATE OR REPLACE TABLE summary AS
    WITH q AS (
        SELECT sf, arm, run_index,
               round(sum(d), 1) AS suite_s,
               count(*) AS queries
        FROM per_q WHERE query_number NOT IN {SLICER}
        GROUP BY ALL
    ), e AS (
        SELECT r.sf, r.arm, r.run_index, max(r.concurrent_threads) AS users,
               round(1000 * quantile_cont(r.duration, 0.5)) AS p50_ms,
               round(1000 * quantile_cont(r.duration, 0.95)) AS p95_ms,
               -- The single worst execution anywhere in the run: one query, one reader, the whole
               -- transcode landing on it.
               round(1000 * max(r.duration)) AS max_ms,
               count(*) AS execs,
               count(DISTINCT r.loadtest_id) AS load_tests,
               count(DISTINCT r.model_id) AS models
        FROM runs r JOIN tops t ON r.sf = t.sf AND r.concurrent_threads = t.top
        WHERE r.query_number NOT IN {SLICER}
        GROUP BY ALL
    )
    SELECT q.sf, q.arm, e.users, q.run_index, q.suite_s, e.p50_ms, e.p95_ms, e.max_ms,
           q.queries, e.execs, e.load_tests, e.models
    FROM q JOIN e USING (sf, arm, run_index)
    ORDER BY sf, arm, run_index
""")
display(duckdb.sql("SELECT * FROM summary").df())
'''

RES_PERQUERY = '''
# Per query. Two tables, because the protocol now answers two questions per query.
#
# A: THE ARMS, at steady state (runs 2-3) -- what the layout costs a resident model, per query, so
#    a single slow shape cannot hide inside a median.
# B: THE WARMING CURVE per query, per arm -- run 1 / run 2 / run 3, sorted worst first. This is the
#    direct replay of paper/loadtest_p50_per_run.csv (SF, Pattern, query_number, Run, p50): their
#    SF10 q1 goes 28,258 ms -> 184 ms -> 160 ms, and this is the same table for our rows.
for sf_ in SFS:
    users = duckdb.sql(f"SELECT top FROM tops WHERE sf = {sf_}").fetchone()[0]
    print(f"--- SF{sf_}: arms at steady state (runs 2-3), {users} reader(s), median ms per query")
    display(duckdb.sql(f"""
        SELECT query_number,
               CASE WHEN query_number IN {SLICER} THEN '(slicer) ' ELSE '' END
                 || any_value(visual_name) AS visual,
               round(1000 * median(d) FILTER (WHERE arm = '{ARM_DEFAULT}')) AS default_ms,
               round(1000 * median(d) FILTER (WHERE arm = '{ARM_CLUSTER}')) AS cluster_ms,
               round(1000 * median(d) FILTER (WHERE arm = '{ARM_VORDER}')) AS vorder_ms,
               round(median(d) FILTER (WHERE arm = '{ARM_DEFAULT}')
                     / nullif(median(d) FILTER (WHERE arm = '{ARM_VORDER}'), 0), 2) AS default_over_vorder,
               round(median(d) FILTER (WHERE arm = '{ARM_PARTITION}')
                     / nullif(median(d) FILTER (WHERE arm = '{ARM_VORDER}'), 0), 2) AS partition_over_vorder,
               round(median(d) FILTER (WHERE arm = '{ARM_CLUSTER}')
                     / nullif(median(d) FILTER (WHERE arm = '{ARM_DEFAULT}'), 0), 2) AS cluster_over_default
        FROM per_q WHERE sf = {sf_} AND run_index > 1
        GROUP BY query_number ORDER BY query_number
    """).df())

    for arm_ in [r[0] for r in duckdb.sql(
            f"SELECT DISTINCT arm FROM per_q WHERE sf = {sf_} ORDER BY 1").fetchall()]:
        print(f"--- SF{sf_}, {arm_}: per query across the three runs, slowest to warm first")
        display(duckdb.sql(f"""
            SELECT query_number,
                   CASE WHEN query_number IN {SLICER} THEN '(slicer) ' ELSE '' END
                     || any_value(visual_name) AS visual,
                   round(1000 * max(d) FILTER (WHERE run_index = 1)) AS run1_ms,
                   round(1000 * max(d) FILTER (WHERE run_index = 2)) AS run2_ms,
                   round(1000 * max(d) FILTER (WHERE run_index = 3)) AS run3_ms,
                   round(max(d) FILTER (WHERE run_index = 1)
                         / nullif(max(d) FILTER (WHERE run_index = 2), 0), 1) AS warmup_x
            FROM per_q WHERE sf = {sf_} AND arm = '{arm_}'
            GROUP BY query_number ORDER BY warmup_x DESC NULLS LAST
        """).df())
'''

RES_CHART = '''
# TWO CHARTS PER SCALE FACTOR, both plotting `suite_s` straight out of `summary` -- the same column
# the table shows, so a number on a picture is always a number in that table.
#
#   1. this cell: every arm measured -- the whole warming curve
#   2. next cell: `default` against `vonly`, the pair that isolates V-Order
#
# LOG y, and it has to be: run 1 carries the whole transcode and is 10-30x runs 2-3, so on a linear
# axis every warm point collapses onto the baseline. The point labels carry the real numbers, so
# nothing depends on reading a gridline.
import matplotlib.pyplot as plt
import numpy as np

d = duckdb.sql("SELECT sf, arm, run_index, suite_s FROM summary").df()

ARMS = [ARM_DEFAULT, ARM_DEFAULTF8,
        ARM_CLUSTER, ARM_CLUSTERSN, ARM_PARTITION, ARM_VORDER, ARM_VONLY,
        ARM_DUCKDB, ARM_DUCKSORT]
# The legend names the LAYOUT, not the engine -- that is what these charts are about. Naming them
# "dbx" vs "Fabric" would read as a producer comparison, which is the misreading the whole
# experiment exists to prevent.
# Row-group size leads every label where the recipe SETS it, because that is the variable: one
# parquet row group is one VertiPaq segment, so "6M groups" and "~143k groups" are ~46 segments per
# fact against ~1,800.
#
# The cluster arm's label deliberately carries NO row-group size. It used to say "6M-row groups,
# clustered by date" and that was a claim nobody had checked; measured at SF100 the clustered write
# produced 2.05M-row groups on store_sales and 1.11M on catalog_sales. The row caps never fire on a
# clustered write -- the clustering exchange picks the file, and therefore the group -- so any
# number in this label would be a number the recipe did not choose and does not hold across tables
# or scale factors. See LEARNING.md, *The clustered SF100 arm, measured both ways*.
SHORT = {ARM_DEFAULT: "Databricks: the recipe -- 6M rows per file, 1 row group",
         ARM_DEFAULTF8: "Databricks: 8M rows per file, 1 row group",
         ARM_DEFAULT2RG: "Databricks: 6M x2 per file (withdrawn)",
         ARM_CLUSTER: "Databricks: clustered by date (write sizes the groups)",
         ARM_CLUSTERSN: "Databricks: clustered by date, 128 MB files, snappy",
         ARM_VONLY: "Fabric: V-Order",
         ARM_PARTITION: "Databricks: partition per date",
         ARM_VORDER: "Fabric: partition per date + Z-order + V-Order",
         ARM_DUCKDB: "delta_rs: auto sort key",
         ARM_DUCKSORT: "delta_rs: sorted per date"}
# The two V-Order pairs share a hue so the comparison is visible before the legend is read:
# orange/purple is the recipe's geometry without/with V-Order, teal/red is partition-per-date.
# The three delta_rs arms share a blue, for the same reason: on those the WRITER is the variable,
# so the family has to read as one before anyone gets to the legend. The four unordered Databricks
# unordered Databricks arms share an orange: one write, two geometries.
COLOR = {ARM_DEFAULT: "#f58518", ARM_DEFAULTF8: "#c2571a", ARM_DEFAULT2RG: "#ffb266",
         ARM_CLUSTER: "#54a24b", ARM_CLUSTERSN: "#2f6b28",
         ARM_VONLY: "#b279a2", ARM_PARTITION: "#72b7b2", ARM_VORDER: "#e45756",
         ARM_DUCKDB: "#4c78a8", ARM_DUCKSORT: "#7fa8c9"}


def suite_for(sf_, arm, run_):
    hit = d[(d.sf == sf_) & (d.arm == arm) & (d.run_index == run_)]
    return float(hit.suite_s.iloc[0]) if len(hit) else np.nan


def draw(sf_, arms, title):
    users = duckdb.sql(f"SELECT top FROM tops WHERE sf = {sf_}").fetchone()[0]
    vals = {(a, r): suite_for(sf_, a, r) for a in arms for r in RUN_INDEXES}
    # Only the arms actually measured get a line. A flat line through absent runs would claim a
    # measurement that does not exist.
    shown = [a for a in arms if any(not np.isnan(vals[(a, r)]) for r in RUN_INDEXES)]
    if not shown:
        print(f"SF{sf_}: nothing to chart for {title}")
        return
    present = [v for v in vals.values() if not np.isnan(v)]
    lo, hi = min(present), max(present)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.set_yscale("log")
    ax.set_ylim(lo / 2.2, hi * 4)      # room for the point labels
    for arm in shown:
        xs = [i for i, r in enumerate(RUN_INDEXES) if not np.isnan(vals[(arm, r)])]
        ys = [vals[(arm, RUN_INDEXES[i])] for i in xs]
        ax.plot(xs, ys, marker="o", markersize=8, linewidth=2.5, color=COLOR[arm],
                label=SHORT[arm])
        for x, y in zip(xs, ys):
            ax.annotate(f"{y:,.1f}s", (x, y), textcoords="offset points", xytext=(0, 9),
                        ha="center", fontsize=9, color=COLOR[arm], fontweight="bold")

    ax.set_xticks(range(len(RUN_INDEXES)))
    ax.set_xticklabels([f"run {r}" for r in RUN_INDEXES], fontsize=12)
    ax.set_xlim(-0.35, len(RUN_INDEXES) - 0.65)
    ax.set_ylabel("suite_s -- one reader's full pass, 15 visual queries, log scale")
    ax.set_title(f"SF{sf_}: {title} -- three load tests over ONE model, {users} readers")
    ax.set_xlabel("run 1 = first touch of a model created minutes earlier  |  "
                  "runs 2-3 = the same suite again, same model")
    ax.legend(fontsize=10)
    ax.grid(axis="y", alpha=0.3, which="both")
    plt.tight_layout()
    plt.show()


# `sf_`, not `sf`: `sf` is this notebook's parameter and a loop variable would overwrite it.
for sf_ in SFS:
    draw(sf_, ARMS, "every arm")
'''

RES_CHART2 = '''
# BEST AGAINST BEST, and nothing else on the axes.
#
# `clustered by date` is the best layout Databricks can reach from CONFIGURATION plus one CLUSTER BY
# -- no V-Order, because Databricks has none. `partition per date + Z-order + V-Order` is the best
# Fabric produces, and it is the paper's own arm. That pair is the question this whole repo exists
# to answer: can a Databricks team, writing Delta with a pasted config, land a table that Direct Lake
# reads as well as one Fabric wrote for itself?
#
# `delta_rs sorted per date` is the third writer's best: delta-rs ordering the facts on the SAME
# date key the clustered arm uses. Three engines, each at the best layout it can reach.
#
# READ IT AS THREE LAYOUTS, NOT THREE PRODUCTS. Each line is the best LAYOUT one writer reached, and
# the chart below it is what stops this from being a league table: `vonly` is Fabric's own writer,
# V-Order and all, with NO layout work, and it is the WORST arm here -- ~11.7 s in steady state
# against these three at ~2.3-2.5. So no writer is good on its own and none is disqualified by its
# encoding; what separates the lines is the layout each was asked for.
#
# `default` and `partition` stay off: they are mechanism controls that isolate one variable each, and
# they are drawn below so neither chart is read as the other.
for sf_ in SFS:
    draw(sf_, [ARM_CLUSTER, ARM_VORDER, ARM_DUCKSORT],
         "best per writer: Databricks, Fabric, delta-rs")
# The control, on its own axes so it cannot be mistaken for a fourth contender.
for sf_ in SFS:
    draw(sf_, [ARM_VONLY, ARM_VORDER], "V-Order with no layout work vs V-Order with it")
'''

RES_CHART3 = '''
# The mechanism pairs. Each holds everything constant but ONE thing, which is what makes them
# readable at all -- and which is why they are not on the best-against-best chart above.
for sf_ in SFS:
    # Same rows, same writer settings in every respect that shapes the files -- no ordering, no
    # partition, no sort, no OPTIMIZE on either side -- and, measured, the same geometry: 45 files
    # of 5.8M rows against 46 of 5.6M. So the ONLY thing left between these two lines is whether the
    # parquet was written with V-Order.
    #
    # `vorder` is deliberately absent HERE: it partitions the facts by the date key, landing 1,823
    # files of ~143k rows, ~1,800 segments against these two arms' ~45 -- 40x. On this chart it would
    # answer a question about partitioning while looking like an answer about V-Order.
    draw(sf_, [ARM_DEFAULT, ARM_VONLY], "V-Order or not, everything else equal")
    # The same question at the other geometry, and THE decisive pair: partitioned by the date key
    # partitioned by the date key on BOTH sides, so the file count and the elimination are the
    # same. TWO things differ, not one: V-Order, and the row order INSIDE each file -- Fabric's
    # OPTIMIZE ZORDER interleaves (measured 11 runs per distinct address key), the Databricks arm
    # does not order at all (1.02, i.e. what the source already was). An attempt to match that with
    # `sortWithinPartitions` never reached the parquet and was removed on 2026-09-08; see
    # LEARNING.md, *The within-partition sort never reached the files*. So this pair BOUNDS the
    # V-Order effect, it does not isolate it.
    draw(sf_, [ARM_PARTITION, ARM_VORDER], "partition per date on both: V-Order and in-file order differ")
    # And the geometry question with the encoding held out of it entirely.
    draw(sf_, [ARM_DEFAULT, ARM_PARTITION], "6M-row groups vs partition per date, neither V-Ordered")
    # SEGMENT SIZE WITH NOTHING TO ELIMINATE. Same writer, same config but one number, no ordering on
    # either side, one row group per file on both: 6M against 8M. Every arm in this project with
    # small groups and no useful ordering is slow (the withdrawn 4M arm was no better than this 6M,
    # duckdb at 2.4M is 14.2 s, vonly at 1.5-2.9M is 11.7 s) while every arm with small groups AND
    # an ordering is fast (cluster 2.0M, ducksort 2.1-2.8M, both ~2.5 s). That says the ordering is
    # the lever and the size is not. This pair tests the other direction: if size were the lever,
    # going BIGGER should hurt. If the two lines sit together, it is not.
    # The withdrawn `default2rg` was the third line here: 6M groups TWO per 12M-row file, which
    # isolated groups-per-file at a fixed segment. It tied, the row-group key left the recipe with
    # it, and it is filtered out by name -- so this pair is now the whole geometry question, and
    # `default` is what every other arm should be read against.
    draw(sf_, [ARM_DEFAULT, ARM_DEFAULTF8],
         "unordered segment size, one row group per file: 6M against 8M")
    # WHO PICKS THE SORT KEY. Both arms are ordered and neither is V-Ordered; what differs is who
    # chose the key. `cluster` sorts on the date surrogate because 23 of the 24 captured queries
    # filter on it -- a key chosen for PRUNING. `duckdb` sorts on whatever duckrun's recommender
    # picked, which optimises modelled in-memory BYTES and is free to take up to 4 columns. Since
    # only the FIRST key eliminates row groups, the interesting outcome is a split: `duckdb` smaller
    # and cheaper cold, `cluster` faster on the date-filtered queries.
    #
    # NOT a single-variable pair, and it must not be read as one -- the writers, the row-group sizes
    # and the compression all differ too (delta_rs writes SNAPPY where every other arm is ZSTD, so
    # BYTES between these lines are not comparable). Read it with the key that `build_duckdb`
    # printed and with the layout table below, never on its own.
    draw(sf_, [ARM_CLUSTER, ARM_DUCKDB], "a key chosen for pruning vs a key duckrun picked")
    # THE ORDERING PAIR, and the cleanest one the delta_rs family gives: `ducksort` orders on the
    # SAME date key `cluster` clusters on, so the key is held and the WRITER is what changes -- a
    # Databricks clustering exchange against a delta-rs sort. Still not single-variable (row-group
    # size and SNAPPY vs ZSTD differ), but the ordering is finally the same on both sides, which
    # `duckdb` could never give: AUTO picked a key chosen to shrink memory, not to prune.
    draw(sf_, [ARM_CLUSTER, ARM_DUCKSORT], "the same date key, two writers")
    # WHAT delta_rs HAD THAT WE DID NOT. `ducksort` is the best COLD arm in the project -- 17.7 s
    # against the clustered arm's 36.9 -- and the two differences that could account for it are the
    # CODEC (snappy decompresses far faster than zstd) and the DICTIONARY, which our clustered
    # catalog_sales lost 19.6 % of its bytes to at 1.11M rows per group. `clustersn` is the same
    # CLUSTER BY with both addressed: snappy, and `delta.targetFileSize` = 128 MB to lift the groups
    # into the 2-5M band where the dictionary holds. Both are TABLE PROPERTIES, so the write is
    # still `df.write.saveAsTable()` and the recipe is still the config plus one CLUSTER BY.
    draw(sf_, [ARM_CLUSTER, ARM_CLUSTERSN, ARM_DUCKSORT], "same date key: zstd, snappy, delta_rs")
    # Partition per date, two writers: parquet-mr holds ~50% of fact bytes as dictionary at that
    # ~143k-row geometry and V-Order holds 100%. That gap is the dictionary bracket, and it is the
    # one thing V-Order was measured to be the only answer to.
    draw(sf_, [ARM_PARTITION, ARM_VORDER],
         "partition per date, two writers: parquet-mr, V-Order")
'''

RES_LAYOUT = '''
# WHAT THE ARMS ARE SITTING ON -- one table, every scale factor, next to the charts it explains.
#
# Read straight off the RAW footer export. `layout_stats` writes `chunks_<arm>.parquet` per scale
# factor -- one row per column chunk per row group, exactly what `parquet_metadata` returned -- and
# this cell recomputes geometry, dictionary and ordering from it. Its summary CSVs are deliberately
# not read: they are one arm-list and one sf per run, already aggregated, and a second copy of a
# number is a second thing that can be stale.
#
# EVERY COLUMN, for a reader who has seen none of this before. An `arm` is one way of writing the
# SAME TPC-DS data: same rows, same schema, same queries -- only the parquet layout differs. The
# charts compare how fast Power BI reads each one. This table is what they are sitting on.
#
#   WHAT IT IS
#   sf              TPC-DS scale factor. 100 is roughly 100 GB of source data, 1000 ten times that.
#   arm             the layout, named exactly as the chart legends name it -- and prefixed with the
#                   engine that wrote it, which is why there is no separate writer column. The
#                   engine is NOT the variable under test: VertiPaq reads parquet from any producer,
#                   what the charts measure is LAYOUT, and every writer here appears at both the
#                   fast and the slow end. Read a row by its layout columns, not by its prefix.
#   table           the two fact tables. Everything else is a small dimension and is left out.
#
#   HOW BIG
#   rows            rows in the table. Identical across arms at one sf -- same data, laid out
#                   differently -- so a difference here means an arm was built wrong.
#   table_gb        compressed column bytes on disk. Not the same as memory: VertiPaq re-encodes.
#   files           parquet files. More, smaller files is not automatically worse; see row_groups.
#   file_mb         average file. The recipe aims at 128 MB on a clustered write.
#
#   HOW IT IS SHAPED  -- this is the part that moves the charts
#   row_groups      row groups across the whole table. ONE PARQUET ROW GROUP BECOMES ONE VERTIPAQ
#                   SEGMENT, so this is the segment count Direct Lake ends up with.
#   rows_per_group  rows in the average one. Direct Lake wants 1M..16M: below it there are too many
#                   segments to manage, above it a segment is too coarse to skip. When a row lands
#                   outside that window the `note` column says so.
#
#   HOW IT IS ENCODED
#   dict_pct        % of bytes the parquet writer kept dictionary-encoded. Whatever is NOT
#                   dictionary gets its dictionary REBUILT by VertiPaq the first time the column is
#                   read -- time and memory on every cold read, which is what run 1 pays for.
#
#   HOW IT IS ORDERED
#   key             the column the arm was ordered or partitioned on, if any.
#   overlaps        neighbouring row groups whose key ranges intersect. Where they overlap, a filter
#                   cannot rule either one out, so the engine reads both.
#   sorting         the verdict in words. `eliminable` = disjoint ranges, whole row groups can be
#                   skipped. `partitioned on the key` is STRONGER still: one key value per file, so
#                   nothing can overlap at all, and it outranks any measured overlap count.
#
#   WHAT IS SURPRISING
#   note            ONLY what the other columns cannot tell you, or where an arm did not do what its
#                   name says. Not a description of the arm. An ordinary row has an EMPTY note, so a
#                   note means look here. Three things can appear: a CLUSTER BY that did not sort
#                   the rows at this scale, a Z-order that cannot help elimination because the
#                   partition already put one date in each file, and a SORTED BY AUTO that ordered
#                   the table on a key no query filters on. Anything a reader can get by reading
#                   another column stays OUT -- `rows_per_group` against the window above, `files`
#                   against `row_groups`, and
#                   `dict_pct`, are theirs to read.
#
#   WHAT IT COST  -- THREE load tests are run back to back over ONE model, then it is deleted. Run 1
#                    is the first touch of a model created minutes earlier, so it pays the whole
#                    Delta-to-memory transcode inside the run. Runs 2 and 3 are the same suite again
#                    on the same resident model. Reading ACROSS the runs is the point: whether an arm
#                    warms up at all is the paper's own headline.
#   full_runs       how many COMPLETE load tests feed this row, over all its runs: a load test counts
#                   only if every reader finished all 24 queries and the rung got the users it asked
#                   for. 3 is one model lifetime; 9 is three lifetimes pooled into the same medians.
#   run<N>_total_s  TOTAL SECONDS for the whole 15-query suite, for ONE reader, on that run. Built
#                   per query as the median across the 20 readers, then summed over the 15 queries.
#                   So it is a total, not a per-query average, and not the load test's wall clock,
#                   which is shorter because the readers run concurrently with think time between
#                   queries. These are exactly the points plotted on the charts above. Compare arms
#                   on the LAST run; that is steady state.
#   cold_*/warm_*   the same two ends, for the per-EXECUTION statistics below: cold is run 1, warm is
#                   the last run. These are MILLISECONDS and describe ONE query, not the suite.
#   *_p50_ms        the typical single execution, over every reader's every query in that run.
#   *_p95_ms        95th percentile of a SINGLE query execution, over the same pool. The tail a user
#                   actually waits on when the model is busy.
#   *_max_ms        the single worst execution anywhere in the run -- one query, one reader, the
#                   whole transcode landing on it. Cold max is usually the ugliest number here.
#                   All of these come from the SAME `summary` table the charts plot, so a number here
#                   can never disagree with one up there. Blank means the arm was not benchmarked.
#
# Dimensions are excluded: every one is a single file and a single row group at every arm, so they
# say nothing about the charts and would triple the row count.
import duckrun

RG_LO, RG_HI = 1_000_000, 16_000_000            # Direct Lake's usable row-group window
# The first ordering key of each fact -- the column whose row-group ranges say whether the ordering
# reached the files. Same map as layout_stats.
KEY = {"store_sales": "ss_sold_date_sk", "catalog_sales": "cs_sold_date_sk"}
_KV = ", ".join(f"('{t}', '{c}')" for t, c in KEY.items())
_KCOLS = ", ".join(f"'{c}'" for c in KEY.values())

# Same lakehouse layout_stats writes to -- lh_name is already tpcds_bench, so ws_id and lh_id from
# the bootstrap cell are the address and no GUID lookup is needed. `bench.con` is duckrun's own
# DuckDB connection and carries the OneLake credential, so the footers are read in place: nothing
# is downloaded and no local temp folder is left behind.
bench = duckrun.connect(f"{ws_id}/{lh_id}", name="bench")
CHUNKS = (f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}/"
          "Files/layout_stats/sf*/chunks_*.parquet")

# The scale factor is the FOLDER, which is what makes one glob cover every sf at once: layout_stats
# runs one sf at a time and each run drops its own sf<n>/ directory. Projected down here, on the
# remote connection, so what crosses into this notebook is the columns below and nothing else --
# min/max are cast and kept for the KEY columns only, which is the whole of what they are read for.
# `.arrow()` hands back a RecordBatchReader on some DuckDB builds and a Table on others, and a
# reader is consumed by the first query that touches it. to_arrow_table / fetch_arrow_table is a
# materialised Table on every build.
#
# WITHDRAWN_ARMS is applied HERE as well as on the results table. A removed arm keeps its chunks in
# the export just as it keeps its rows in perfresults3, and this cell does not join to the timings
# -- it LEFT JOINs them -- so without the filter a withdrawn arm reaches the layout table anyway,
# as a row with real geometry and empty timings. Filtering at the read rather than in the CTE also
# keeps the chunk count printed below honest.
try:
    _rel = bench.con.sql(f"""
        SELECT CAST(regexp_extract(filename, 'sf([0-9]+)', 1) AS INTEGER) AS sf,
               arm, "table" AS tbl, file_name, row_group_id,
               row_group_num_rows AS rg_rows, path_in_schema AS col,
               total_compressed_size AS bytes, encodings,
               CASE WHEN path_in_schema IN ({_KCOLS})
                    THEN TRY_CAST(stats_min_value AS BIGINT) END AS lo,
               CASE WHEN path_in_schema IN ({_KCOLS})
                    THEN TRY_CAST(stats_max_value AS BIGINT) END AS hi
        FROM read_parquet('{CHUNKS}', filename = true, union_by_name = true)
        WHERE arm NOT IN ({_WITHDRAWN})
    """)
    _raw = (_rel.to_arrow_table() if hasattr(_rel, "to_arrow_table") else _rel.fetch_arrow_table())
except Exception as e:                                                      # noqa: BLE001
    _raw = None
    print(f"no footer export under {lh_name}: Files/layout_stats ({str(e)[:140]}).")
    print("Run the `layout_stats` notebook once per scale factor, then re-run this cell.")

if _raw is not None:
    duckdb.register("lay_chunks", _raw)
    print(f"{_raw.num_rows:,} column chunks over "
          f"{duckdb.sql('SELECT count(DISTINCT (sf, arm)) FROM lay_chunks').fetchone()[0]} "
          "scale-factor x arm combinations")

    # Cold and warm off `summary` -- the SAME suite_s the charts plot, so a number here can never
    # disagree with a number up there. min_by/max_by over run_index rather than a hardcoded 1 and 3:
    # a session that ran two runs, or four, still reads correctly.
    # One suite_s column PER RUN, so a row carries the arm's whole warming curve and the columns line
    # up one-for-one with the points on its chart line. Built from RUN_INDEXES, which the headline
    # cell discovered from the data -- a session that ran two runs, or four, gets two or four columns
    # rather than three that are half empty.
    # `run<N>_total_s`, spelled out: it is a TOTAL in SECONDS -- the whole 15-query suite for ONE
    # reader, per-query medians across the readers then summed. Not a per-query average, and not the
    # load test's wall clock, which is shorter because readers run concurrently with think time.
    _RUNS = ", ".join(f"max(suite_s) FILTER (WHERE run_index = {r}) AS run{r}_total_s"
                      for r in RUN_INDEXES) or "NULL::DOUBLE AS run_total_s"
    _RUNCOLS = ", ".join(f"t.run{r}_total_s" for r in RUN_INDEXES) or "t.run_total_s"
    _LAST = f"t.run{RUN_INDEXES[-1]}_total_s" if RUN_INDEXES else "t.run_total_s"
    duckdb.sql(f"""
        CREATE OR REPLACE TABLE lay_time AS
        SELECT sf, arm,
               -- Complete load tests pooled into this row, over every run index. `load_tests` in
               -- `summary` is per run, so the sum is the count of distinct surviving loadtest_ids.
               CAST(sum(load_tests) AS INTEGER) AS full_runs,
               {_RUNS},
               -- p95 and max are per-EXECUTION, off the raw pool of every reader's every query, so
               -- they only make sense at the two ends: the run that pays the transcode and the run
               -- that does not. min_by/max_by over run_index rather than a hardcoded 1 and 3.
               min_by(p50_ms, run_index) AS cold_p50_ms,
               min_by(p95_ms, run_index) AS cold_p95_ms,
               min_by(max_ms, run_index) AS cold_max_ms,
               max_by(p50_ms, run_index) AS warm_p50_ms,
               max_by(p95_ms, run_index) AS warm_p95_ms,
               max_by(max_ms, run_index) AS warm_max_ms
        FROM summary GROUP BY 1, 2
    """)

    # The chunks carry the RAW arm names; the charts carry the layout labels. Relabel or a row
    # cannot be matched to the line it explains. ELSE arm, so a name this build does not know
    # (a withdrawn arm, or the pre-rename `layout`) still shows rather than becoming NULL.
    LAY_ARM = f"""CASE arm WHEN 'default' THEN '{ARM_DEFAULT}'
                           WHEN 'defaultf8' THEN '{ARM_DEFAULTF8}'
                           WHEN 'default2rg' THEN '{ARM_DEFAULT2RG}'
                           WHEN 'cluster' THEN '{ARM_CLUSTER}'
                           WHEN 'clustersn' THEN '{ARM_CLUSTERSN}'
                           WHEN 'partition' THEN '{ARM_PARTITION}'
                           WHEN 'vorder' THEN '{ARM_VORDER}'
                           WHEN 'vonly' THEN '{ARM_VONLY}' WHEN 'duckdb' THEN '{ARM_DUCKDB}'
                           WHEN 'ducksort' THEN '{ARM_DUCKSORT}'
                           ELSE arm END"""
    # `note` carries ONLY what is surprising -- what the row's own columns cannot tell you, or where
    # the arm did not do what its name says. It is NOT a description of the arm: `dict_pct`,
    # `overlaps` and `sorting` are right there and a reader can read them. An unremarkable row gets
    # an EMPTY note, and that is the point: a note means look here.
    #
    # Everything below is derived from this row, so nothing can go stale except the one design fact
    # -- that an ordering inside a one-file-per-date partition cannot help elimination, since the
    # partition already gives it -- and even that only fires on a row measured to be partitioned.
    _INPART_SORT = "'%Z-order%'"

    duckdb.sql(f"""
        CREATE OR REPLACE TABLE layout AS
        WITH c AS (SELECT * REPLACE ({LAY_ARM} AS arm) FROM lay_chunks),
             k(tbl, key) AS (VALUES {_KV}),
             -- A row group contributes one row per COLUMN, so the distinct (file, group) pairs come
             -- first: summing rg_rows over the raw chunks multiplies every count by the column count.
             rg AS (SELECT DISTINCT sf, arm, tbl, file_name, row_group_id, rg_rows FROM c),
             -- PARTITIONED ON THE KEY is the strongest elimination there is, and it is read off
             -- the PATH, not guessed: Hive writes the value into the directory name
             -- (`.../ss_sold_date_sk=2451742/part-0000...`), so one partition holds exactly one key
             -- value and no two files can overlap. Whether the column is ALSO inside the parquet is
             -- a writer detail and nothing else -- Fabric drops it, Databricks keeps it -- so the
             -- two partitioned arms used to get different verdicts for the same layout.
             part AS (
                 SELECT r.sf, r.arm, r.tbl,
                        max(CASE WHEN r.file_name LIKE '%/' || k.key || '=%' THEN 1 ELSE 0 END) = 1
                            AS partitioned
                 FROM rg r JOIN k ON r.tbl = k.tbl GROUP BY 1, 2, 3
             ),
             geom AS (
                 SELECT sf, arm, tbl, CAST(sum(rg_rows) AS BIGINT) AS "rows",
                        count(DISTINCT file_name) AS files,
                        count(*) AS row_groups,
                        CAST(round(avg(rg_rows)) AS BIGINT) AS rows_per_group
                 FROM rg GROUP BY 1, 2, 3
             ),
             -- On-disk size, from the column chunks rather than the row groups: bytes are a
             -- per-column-chunk figure, so they must be summed before the distinct-row-group step
             -- that `rg` does. This is compressed COLUMN data -- footers and page headers are not
             -- in it -- so it reads a little under what the Files listing shows.
             size AS (
                 SELECT sf, arm, tbl, sum(bytes) AS total_bytes,
                        sum(bytes) / count(DISTINCT file_name) AS bytes_per_file
                 FROM c GROUP BY 1, 2, 3
             ),
             -- Dictionary BY BYTES. `encodings` is a comma-separated list, so it is split and
             -- trimmed rather than matched with LIKE: a bare LIKE '%PLAIN%' also matches
             -- PLAIN_DICTIONARY and would report every dictionary chunk as a fallback.
             e AS (SELECT sf, arm, tbl, bytes,
                          list_transform(str_split(coalesce(encodings, ''), ','), x -> trim(x)) AS encs
                   FROM c),
             dict AS (
                 -- The same rule layout_stats uses: a dictionary encoding in the list, full stop.
                 -- Not "and no bare PLAIN" -- delta_rs lists PLAIN on every dictionary chunk, since
                 -- the dictionary page is PLAIN-encoded, and excluding those scored that writer at
                 -- zero. No parquet-mr chunk in this project has ever carried both.
                 SELECT sf, arm, tbl,
                        round(100.0 * sum(bytes) FILTER (
                            WHERE list_contains(encs, 'RLE_DICTIONARY')
                               OR list_contains(encs, 'PLAIN_DICTIONARY')) / sum(bytes), 1) AS dict_pct
                 FROM e GROUP BY 1, 2, 3
             ),
             -- One [lo, hi] per row group on the first key, for the arms that are NOT partitioned on
             -- it. Fabric's V-Order arm has no chunks here at all -- it drops the partition column
             -- from the file -- and does not need them: `part` above already settled it.
             kr AS (
                 SELECT c.sf, c.arm, c.tbl, any_value(c.lo) AS lo, any_value(c.hi) AS hi
                 FROM c JOIN k ON c.tbl = k.tbl AND c.col = k.key
                 WHERE c.lo IS NOT NULL
                 GROUP BY c.sf, c.arm, c.tbl, c.file_name, c.row_group_id
             ),
             -- Sorted by low bound: if a range starts before its predecessor ends, that pair overlaps
             -- and a filter landing in the overlap eliminates neither group.
             ov AS (
                 SELECT sf, arm, tbl, count(*) AS key_groups,
                        count(*) FILTER (WHERE prev_hi IS NOT NULL AND prev_hi > lo) AS overlaps
                 FROM (SELECT *, lag(hi) OVER (PARTITION BY sf, arm, tbl ORDER BY lo, hi) AS prev_hi
                       FROM kr)
                 GROUP BY 1, 2, 3
             )
        SELECT g.sf, g.arm, g.tbl AS "table",
               g."rows", round(z.total_bytes / 1073741824.0, 2) AS table_gb,
               g.files, round(z.bytes_per_file / 1048576.0, 1) AS file_mb,
               g.row_groups, g.rows_per_group,
               d.dict_pct, k.key,
               -- Zero by construction when the key is the partition column, not by measurement:
               -- every file in a partition carries the one value the directory names.
               CASE WHEN p.partitioned THEN 0 ELSE o.overlaps END AS overlaps,
               CASE WHEN p.partitioned      THEN 'partitioned on the key (one value per file)'
                    WHEN o.key_groups IS NULL THEN 'no min/max stats on the key'
                    WHEN o.overlaps = 0       THEN 'eliminable'
                    ELSE CAST(round(100.0 * o.overlaps / o.key_groups) AS INTEGER)
                         || '% of neighbours overlap' END AS sorting,
               t.full_runs, {_RUNCOLS},
               t.cold_p50_ms, t.cold_p95_ms, t.cold_max_ms,
               t.warm_p50_ms, t.warm_p95_ms, t.warm_max_ms,
               concat_ws('; ',
                   -- The arm is named for an ordering it did not deliver. At SF100 the clustered
                   -- write placed the rows and this is silent; at SF1000 it did not.
                   CASE WHEN g.arm = '{ARM_CLUSTER}' AND o.overlaps > 0
                        THEN 'CLUSTER BY did not sort the rows at this scale -- '
                             || CAST(round(100.0 * o.overlaps / o.key_groups) AS INTEGER)
                             || '% of neighbouring row groups still overlap' END,
                   -- The arm DID sort -- AUTO just sorted on a key nothing filters on, so the
                   -- overlap count beside it reads like a failed sort and is not one. Unconditional
                   -- for the arm: it is what `SORTED BY AUTO` is, not something that went wrong.
                   CASE WHEN g.arm = '{ARM_DUCKDB}'
                        THEN 'SORTED BY AUTO picked its own key -- it minimises modelled memory, '
                             || 'not pruning -- so the date key the queries filter on is unordered' END,
                   -- The Fabric arm's OPTIMIZE ZORDER did rewrite every file (measured: rows
                   -- interleaved on the address key), but it cannot buy elimination -- one date per
                   -- file already gives that. The Databricks partition arm has no in-file ordering
                   -- at all, so this note is the Fabric one's alone.
                   CASE WHEN p.partitioned AND g.arm LIKE {_INPART_SORT}
                        THEN 'the Z-order reorders rows inside each file; it cannot help '
                             || 'elimination -- one date per file already does' END
               ) AS note
        FROM geom g
        JOIN k ON g.tbl = k.tbl                       -- facts only, and it supplies the key
        LEFT JOIN dict d ON d.sf = g.sf AND d.arm = g.arm AND d.tbl = g.tbl
        LEFT JOIN ov   o ON o.sf = g.sf AND o.arm = g.arm AND o.tbl = g.tbl
        LEFT JOIN part p ON p.sf = g.sf AND p.arm = g.arm AND p.tbl = g.tbl
        LEFT JOIN size z ON z.sf = g.sf AND z.arm = g.arm AND z.tbl = g.tbl
        LEFT JOIN lay_time t ON t.sf = g.sf AND t.arm = g.arm
        ORDER BY g.sf, coalesce({_LAST}, 1e9), g.tbl
    """)

    # Coverage, said out loud. layout_stats takes an `arms` list and is run per sf, so PARTIAL is the
    # normal state -- and a join that quietly drops an arm would let an absent measurement read as an
    # absent difference.
    _gap = duckdb.sql("""
        SELECT sf, arm, 'charted, no footer export' AS missing FROM lay_time
        WHERE (sf, arm) NOT IN (SELECT sf, arm FROM layout)
        UNION ALL
        SELECT DISTINCT sf, arm, 'footer export, not charted' FROM layout WHERE warm_max_ms IS NULL
        ORDER BY 1, 3, 2
    """).df()
    if len(_gap):
        print("--- gaps. Run `layout_stats` at that sf with the arm in its `arms` list to fill one in.")
        display(_gap)

    print("--- one row per scale factor x arm x fact, best steady state first. Row groups of")
    print(f"    {RG_LO:,}..{RG_HI:,} rows are the ones Direct Lake wants; `sorting` is whether the")
    print("    ordering reached the files, so row groups can be eliminated at all.")
    display(duckdb.sql("SELECT * FROM layout").df())
'''

RES_MEMORY = '''
# What each arm WEIGHS in VertiPaq, from whatever modelsegments / modelcolumns hold. Segments are
# Direct Lake's row groups.
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

base = delta_path.rsplit("/", 1)[0]
try:
    seg = pa.table(DeltaTable(f"{base}/modelsegments").to_pyarrow_table())
    col = pa.table(DeltaTable(f"{base}/modelcolumns").to_pyarrow_table())
except TableNotFoundError:
    seg = col = None
    print("no memory capture (modelsegments / modelcolumns absent). Nothing else here needs them.")

if seg is not None:
    duckdb.register("seg", seg)
    duckdb.register("col", col)
    ARM_SQL = (f"CASE pattern WHEN 'dbxdefaultf8' THEN '{ARM_DEFAULTF8}' "
               f"WHEN 'dbxdefault2rg' THEN '{ARM_DEFAULT2RG}' "
               f"WHEN 'dbxdefault' THEN '{ARM_DEFAULT}' "
               f"WHEN 'dbxclustersn' THEN '{ARM_CLUSTERSN}' "
               f"WHEN 'dbxcluster' THEN '{ARM_CLUSTER}' WHEN 'dbxpartition' THEN '{ARM_PARTITION}' "
               f"WHEN 'fabvonly' THEN '{ARM_VONLY}' "
               f"WHEN 'fabvorder' THEN '{ARM_VORDER}' "
               f"WHEN 'duckauto' THEN '{ARM_DUCKDB}' "
               f"WHEN 'ducksort' THEN '{ARM_DUCKSORT}' "
               f"ELSE pattern END")
    # TABLE_ID / COLUMN_ID carry an object id suffix ("store_sales (132)"); DIMENSION_NAME, when the
    # DMV has it, is the plain table name.
    TBL = "DIMENSION_NAME" if "DIMENSION_NAME" in seg.schema.names else "split_part(TABLE_ID, ' (', 1)"

    print("--- per run: total vs resident, GB (USED_SIZE over every segment, hierarchies and "
          "relationship indexes included -- it is all capacity memory)")
    display(duckdb.sql(f"""
        SELECT sf, {ARM_SQL} AS arm, run_index, concurrent_threads AS users, captured_utc,
               round(sum(USED_SIZE) / 1e9, 2) AS total_gb,
               round(sum(USED_SIZE) FILTER (WHERE ISRESIDENT) / 1e9, 2) AS resident_gb,
               count(*) AS segments, count(*) FILTER (WHERE ISRESIDENT) AS resident_segments
        FROM seg GROUP BY ALL ORDER BY sf, arm, run_index
    """).df())

    # Per fact column at the LATEST capture of each (sf, arm): segments (= row groups), rows per
    # segment, bytes, residency. Hierarchies (H$), relationships (R$) and row numbers excluded here.
    duckdb.sql(f"""
        CREATE OR REPLACE TABLE factcols AS
        WITH latest AS (SELECT sf, pattern, max(captured_utc) AS captured_utc FROM seg GROUP BY 1, 2)
        SELECT s.sf, {ARM_SQL.replace("pattern", "s.pattern")} AS arm,
               {TBL.replace("TABLE_ID", "s.TABLE_ID").replace("DIMENSION_NAME", "s.DIMENSION_NAME")} AS tbl,
               split_part(s.COLUMN_ID, ' (', 1) AS col,
               count(*) AS segments, round(avg(s.RECORDS_COUNT)) AS rows_per_segment,
               round(sum(s.USED_SIZE) / 1e6, 1) AS used_mb,
               round(100.0 * count(*) FILTER (WHERE s.ISRESIDENT) / count(*)) AS resident_pct
        FROM seg s JOIN latest l USING (sf, pattern, captured_utc)
        WHERE s.TABLE_ID NOT LIKE 'H$%' AND s.TABLE_ID NOT LIKE 'R$%' AND s.TABLE_ID NOT LIKE 'U$%'
          AND s.COLUMN_ID NOT LIKE 'RowNumber%'
          AND (s.TABLE_ID LIKE 'store_sales%' OR s.TABLE_ID LIKE 'catalog_sales%')
        GROUP BY ALL
    """)
    enc = duckdb.sql(f"""
        WITH latest AS (SELECT sf, pattern, max(captured_utc) AS captured_utc FROM col GROUP BY 1, 2)
        SELECT c.sf, {ARM_SQL.replace("pattern", "c.pattern")} AS arm,
               split_part(c.TABLE_ID, ' (', 1) AS tbl, split_part(c.COLUMN_ID, ' (', 1) AS col,
               any_value(CASE c.COLUMN_ENCODING WHEN 1 THEN 'hash (dictionary)' WHEN 2 THEN 'value'
                         ELSE CAST(c.COLUMN_ENCODING AS VARCHAR) END) AS encoding,
               round(max(c.DICTIONARY_SIZE) / 1e6, 1) AS dictionary_mb
        FROM col c JOIN latest l USING (sf, pattern, captured_utc)
        WHERE c.TABLE_ID LIKE 'store_sales%' OR c.TABLE_ID LIKE 'catalog_sales%'
        GROUP BY ALL
    """)
    duckdb.register("enc", enc.arrow())
    print("--- fact columns, latest capture per arm")
    display(duckdb.sql("""
        SELECT f.*, e.encoding, e.dictionary_mb
        FROM factcols f LEFT JOIN enc e USING (sf, arm, tbl, col)
        ORDER BY sf, tbl, col, arm
    """).df())

    # The ablation in bytes, per column. This used to be sorted vs unsorted; there is no sorted arm
    # -- the ORDER BY never reached the files -- so it is the geometry pair instead: 6M-row groups
    # against one file per date, neither V-Ordered.
    print("--- 6M groups / one file per date, MB per fact column (>1 = the 6M column is bigger)")
    display(duckdb.sql(f"""
        SELECT sf, tbl, col,
               max(used_mb) FILTER (WHERE arm = '{ARM_PARTITION}') AS partition_mb,
               max(used_mb) FILTER (WHERE arm = '{ARM_DEFAULT}') AS default_mb,
               round(max(used_mb) FILTER (WHERE arm = '{ARM_DEFAULT}')
                     / nullif(max(used_mb) FILTER (WHERE arm = '{ARM_PARTITION}'), 0), 2)
                 AS default_over_partition
        FROM factcols GROUP BY ALL
        HAVING partition_mb IS NOT NULL AND default_mb IS NOT NULL
        ORDER BY sf, tbl, default_over_partition DESC
    """).df())
'''


MODEL_HELPERS = '''
def model_name(arm, sf):
    """This arm's semantic model name at this scale factor."""
    return EMBEDDED_MODELS[arm]["model"].replace("__SF__", str(sf))


def model_parts(arm, sf):
    """The arm's TMDL as definition parts, with this run's scale factor filled in.

    The definitions are embedded DECODED with the scale factor left as a token, so one build of
    this notebook runs at any sf. A token rather than str.format() because TMDL is full of braces
    (DAX table constructors, `Measures 1`).
    """
    return [{"path": p["path"], "payloadType": "InlineBase64",
             "payload": base64.b64encode(
                 p["text"].replace("__SF__", str(sf)).encode()).decode()}
            for p in EMBEDDED_MODELS[arm]["parts"]]
'''


def embedded_models_cell(ws_id):
    """The two arms' semantic models as TMDL definition parts, inlined.

    The driver recreates its arm's model before measuring it, and it runs inside Fabric where
    `paper/model/**` is not on disk -- so the definition travels with the notebook. It is built by
    the SAME `parts_for()` that `fabric/deploy_paper_model.py` deploys with, which is what stops
    the measured model and the deployed one from drifting apart.

    The substitutions bake in workspace and item GUIDs, hence `--workspace`.
    """
    import sys
    sys.path.insert(0, os.path.join(HERE, os.pardir, "fabric"))
    import deploy_paper_model as dpm

    # Embed the TMDL DECODED with the scale factor replaced by a token; model_parts() re-encodes
    # per run. Freezing a concrete schema here is what made the notebook single-scale-factor.
    import base64

    models = {}
    for arm in dpm.ARMS:
        parts, name = dpm.parts_for(arm, ws_id, show_diff=False)
        models[arm] = {
            "model": _SF_RE.sub(SF_TOKEN, name),
            "parts": [{"path": q["path"],
                       "text": _SF_RE.sub(SF_TOKEN, base64.b64decode(q["payload"]).decode())}
                      for q in parts],
        }
    arm_of = {v["model"]: a for a, v in models.items()}
    blob = json.dumps(models, separators=(",", ":"))
    assert "'''" not in blob, "the definitions would break the triple-quoted literal"
    print(f"  embedding {len(models)} model definition(s), {len(blob):,} chars, for workspace {ws_id}")
    return ("# The paper's own TMDL for both arms, inlined by notebooks/build_notebooks.py from\n"
            "# paper/model/** (lipinght/DB-DQ-Whitepaper, MIT, 99f9904d) via deploy_paper_model.py:parts_for(). Workspace and item\n"
            "# GUIDs are already substituted, so REBUILD THESE NOTEBOOKS if either changes:\n"
            "#     python notebooks/build_notebooks.py --workspace <ws>\n"
            "import base64, json\n"
            "EMBEDDED_MODELS = json.loads(r'''" + blob + "''')\n"
            "ARM_OF = " + repr(arm_of) + "\n"
            + MODEL_HELPERS +
            "# NOT part of the protocol any more: the three runs ARE the measurement, and a\n"
            "# probe before run 1 would page in every column the suite reads and flatten run 1\n"
            "# into run 2. Kept because it is still the honest way to time a transcode BY\n"
            "# ITSELF -- every column the suite reads, generated from the capture, `Measures 1`\n"
            "# and the relationships -- run by hand against a model nothing has touched.\n"
            "TRANSCODE_DAX = " + repr(transcode_query()) + "\n")


def main():
    import argparse
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workspace", default=WORKSPACE,
                    help="workspace id baked into the embedded model definitions")
    args = ap.parse_args()

    # paper/ is gitignored (it was vendored until 2026-09-07): the built notebooks carry everything
    # they need, but REBUILDING them reads the capture and the TMDL from disk.
    if not (os.path.exists(SUITE) and os.path.isdir(PAPER_MODEL)):
        sys.exit("paper/ is not on disk. Fetch github.com/lipinght/DB-DQ-Whitepaper (MIT, commit "
                 "99f9904d): load_test_json/PowerBIPerformanceData.json -> paper/ and "
                 "test_model_report/ -> paper/model/, then rerun.")

    write("RunPerfScenario.ipynb", [
        code(RPS_PARAMS, PARAM_META),
        code(embedded_suite_cell()),
        code(RPS_HELPERS),
        code(RPS_MAIN),
    ])
    write("run_benchmark.ipynb", [
        code(RB_CONFIGURE),
        markdown(RB_DOC),
        code(RB_PARAMS, PARAM_META),
        code(RB_BOOTSTRAP),
        code(embedded_models_cell(args.workspace)),
        code(RB_RUNDAX),
        code(RB_REFRAME),
        code(RB_RUN),
    ])
    write("layout_stats.ipynb", [
        code(STATS_CONFIGURE),  # must be the FIRST cell for Fabric to apply it
        markdown(STATS_DOC),
        code(RES_INSTALL),      # same duckrun install + restart
        # sf + arms on their own, so a run can be narrowed in one edit. AFTER the install cell, not
        # before it: restartPython() restarts the interpreter and anything set above it is gone.
        code(STATS_PARAMS),
        code(STATS_BODY),
        code(STATS_DICT),
        code(STATS_GEOMETRY),
        code(STATS_WRITE),
    ])
    write("results.ipynb", [
        markdown(RES_DOC),
        code(RES_INSTALL),
        code(RES_BOOTSTRAP),
        code(RES_COMPACT),
        code(RES_RUNS),
        code(RES_HEADLINE),
        code(RES_CHART),
        code(RES_CHART2),
        code(RES_CHART3),
        # The layout table goes UNDER the charts: it is what they are read against, not a
        # preamble. It reads what `layout_stats` already wrote to the lakehouse -- no footer
        # sweep happens here, so it costs a download of a few CSVs.
        code(RES_LAYOUT),
        # RES_MEMORY (the modelsegments / modelcolumns scan) is deliberately NOT emitted: reading
        # every segment row for every arm takes minutes and nothing above depends on it. The cell
        # source is kept in RES_MEMORY above so it can be pasted back if the memory question returns.
    ])


if __name__ == "__main__":
    main()
