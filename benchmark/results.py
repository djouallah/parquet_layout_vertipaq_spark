"""Read `perfresults3`, drop the runs that are not comparable, check the two arms agree, chart.

Runs on the laptop (Delta over OneLake via `deltalake` -> Arrow -> DuckDB), so the analysis can be
re-run without spending Fabric capacity. Ported from `c:\\direct_query/notebooks/results.ipynb`;
the filters and the golden check are its logic, the labels and thresholds are this dataset's.

  python benchmark/results.py --workspace <ws> --lakehouse tpcds_bench [--chart out.png]

Two filters decide what counts, and both exist because of failures seen in the reference:

  all-24-or-out  a "run" is one (loadtest_id, thread_id, iteration). It counts only if all 24
                 queries SUCCEEDED. Counting rows instead would let a run that errored on three
                 queries still total 10 and pass, because a failed query still writes its row.

  short rung     a rung that asked for 20 threads and only got 8 measured 8-way concurrency.
                 Plotting it at x=20 draws a load test that never happened -- and typically as a
                 median BELOW the rung beneath it, because the survivors ran against a capacity
                 the other 12 threads had vacated.
"""
from __future__ import annotations

import argparse
import decimal
import os
import json

import duckdb
import pyarrow as pa

N_QUERIES = 24          # the paper's whole capture; results are all-24-or-out, never a subset
# The 9 slicer queries. The paper's reported results exclude slicer interactions, but its
# load test still ran them, so we replay all 24 and split them here.
SLICER_QUERIES = (3, 7, 8, 11, 15, 16, 17, 18, 21)

# Anchored on the WHOLE name, never a prefix LIKE: a prefix match would quietly fold two arms into
# one engine, which is the single mistake that would make this whole comparison meaningless. The
# regexp accepts any scale factor (the old literal list silently dropped SF1000 into ELSE).
# EXACT equality on the arm token, never a pattern. Every model is named tpcds_sf<N>_<arm>, so the
# last '_' segment IS the arm. `LIKE '%_sort'` used to be here and was wrong: `_` is SQL's
# single-character wildcard, so it matched tpcds_sf<N>_default too and folded two arms into one --
# the single mistake that would make this whole comparison meaningless.
ARM = "str_split(model, '_')[-1]"
# REMOVED ARMS keep their rows -- nothing is ever deleted from perfresults3 -- so they have to be
# filtered by name here or they reach the charts through ELSE as a model name pretending to be an
# arm. Add a token here whenever an arm is removed from the repo, and never re-use a token.
WITHDRAWN = f"{ARM} NOT IN ('sort', 'nosort4m', 'default2rg')"

ENGINE = f"""CASE {ARM} WHEN 'default' THEN 'dbx_mirrored_default'
                  WHEN 'defaultf8' THEN 'dbx_mirrored_defaultf8'
                  WHEN 'default2rg' THEN 'dbx_mirrored_default2rg'
                  WHEN 'cluster' THEN 'dbx_mirrored_cluster'
                  WHEN 'clustersn' THEN 'dbx_mirrored_clustersn'
                  WHEN 'partition' THEN 'dbx_mirrored_partition'
                  WHEN 'vorder' THEN 'fabric_vorder'
                  WHEN 'vonly' THEN 'fabric_vonly'
                  ELSE model END"""

# Our Pattern label, as it appears in loadtest_id and in the paper-shaped output. Their four are
# db_cm / db_dq / fab_dl / fab_mirror; ours are deliberately different so nothing masquerades as
# their data when the two tables sit side by side.
PATTERN = f"""CASE {ARM} WHEN 'default' THEN 'dbxdefault'
                   WHEN 'defaultf8' THEN 'dbxdefaultf8'
                   WHEN 'default2rg' THEN 'dbxdefault2rg'
                   WHEN 'cluster' THEN 'dbxcluster'
                   WHEN 'clustersn' THEN 'dbxclustersn'
                   WHEN 'partition' THEN 'dbxpartition'
                   WHEN 'vorder' THEN 'fabvorder'
                   WHEN 'vonly' THEN 'fabvonly'
                   ELSE model END"""

# The two arms are the SAME ROWS written two ways, so every measure must agree exactly. These are
# loose enough for decimal-vs-double rendering and nothing else; a real difference means the arms
# hold different data and every timing below is void.
RTOL = decimal.Decimal("1e-9")
ATOL = decimal.Decimal("0.01")


def load(workspace: str, lakehouse: str, table: str) -> pa.Table:
    from deltalake import DeltaTable
    from duckrun.auth import get_onelake_token
    import duckrun

    ws = duckrun.workspace(workspace)
    lh = next((it for it in ws.list_items("lakehouses") if it.get("displayName") == lakehouse), None)
    if not lh:
        raise SystemExit(f"lakehouse {lakehouse!r} not found in {workspace!r}")
    path = (f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lh['id']}/Tables/{table}")
    print(f"reading {path}")
    dt = DeltaTable(path, storage_options={"bearer_token": get_onelake_token(),
                                           "use_fabric_endpoint": "true"})
    return pa.table(dt.to_pyarrow_table())


def golden_check(con) -> int:
    """Compare every arm's values against one baseline per scale factor.

    Per query: the row count, then the SUM of each column. Sums are order-insensitive, so a TOPN
    tie cannot fabricate a mismatch. Numbers arrive as strings (RunPerfScenario serializes them
    that way on purpose) so they compare at decimal precision rather than through a double.

    The baseline is drawn from ANY concurrency. It used to require `concurrent_threads = 1`, which
    was fine while every rung ran a 1-thread warm-up; now that both passes run at the rung's
    concurrency there may be no such rung at all, and the check would quietly compare nothing. A
    value does not depend on how many users asked for it, so the restriction bought nothing.
    """
    decimal.getcontext().prec = 60
    D = decimal.Decimal

    golden = con.sql("""
        SELECT scale_factor, loadtest_id, engine
        FROM runs
        WHERE result_json IS NOT NULL
        GROUP BY scale_factor, loadtest_id, engine
        QUALIFY row_number() OVER (
            PARTITION BY scale_factor
            -- The engine rank MUST be the first sort key: loadtest_id is tpcds-{model}-{ts}, so
            -- ordering by it alone is newest-first only within one model's prefix.
            ORDER BY CASE engine WHEN 'dbx_mirrored' THEN 0 ELSE 1 END, loadtest_id DESC
        ) = 1
    """).df()
    if golden.empty:
        print("\nNo run carries result_json -- value comparison skipped.")
        return 0
    print("\nGolden baselines:")
    print(golden.to_string(index=False))

    def sums(row_json):
        d = json.loads(row_json)
        cols, rows = d["columns"], d["rows"]
        out = {}
        for i, c in enumerate(cols):
            tot = D(0)
            numeric = False
            for r in rows:
                v = r[i]
                if v is None:
                    continue
                try:
                    tot += D(str(v))
                    numeric = True
                except Exception:
                    numeric = False
                    break
            if numeric:
                out[c] = tot
        return len(rows), out

    problems = []
    for sf, g in golden.groupby("scale_factor"):
        gid = g.loadtest_id.iloc[0]
        base = con.sql(f"""
            SELECT query_number, visual_name, any_value(result_json) AS rj
            FROM runs WHERE loadtest_id = '{gid}' AND result_json IS NOT NULL
            GROUP BY query_number, visual_name
        """).df()
        gmap = {r.query_number: sums(r.rj) for r in base.itertuples()}
        others = con.sql(f"""
            SELECT engine, loadtest_id, query_number, any_value(result_json) AS rj
            FROM runs
            WHERE scale_factor = {sf} AND loadtest_id <> '{gid}' AND result_json IS NOT NULL
            GROUP BY engine, loadtest_id, query_number
        """).df()
        for r in others.itertuples():
            if r.query_number not in gmap:
                continue
            grows, gsums = gmap[r.query_number]
            nrows, nsums = sums(r.rj)
            if nrows != grows:
                problems.append((sf, r.engine, r.query_number, "ROWS_DIFF", f"{nrows} vs {grows}"))
                continue
            for c, gv in gsums.items():
                if c not in nsums:
                    problems.append((sf, r.engine, r.query_number, "MISSING_COLUMN", c))
                    continue
                nv = nsums[c]
                diff = abs(nv - gv)
                scale = max(abs(nv), abs(gv))
                if not (diff <= ATOL or (scale and diff <= RTOL * scale)):
                    rel = float(diff / scale) if scale else 0.0
                    problems.append((sf, r.engine, r.query_number, "SUM_DIFF", f"{c} rel={rel:.3e}"))
    if problems:
        print(f"\n!! {len(problems)} VALUE MISMATCHES -- the arms do not hold the same data, "
              f"so every timing below is void:")
        for p in problems[:40]:
            print("   ", p)
    else:
        print("\nValue check: every arm matches its baseline on every query. The arms hold the "
              "same rows, so the timings compare like with like.")
    return len(problems)


def paper_table(con, paper_csv: str) -> None:
    """Our arms beside the paper's published per-query P50s, at their concurrency.

    Their `DL_Mirror` is the CONTROL: the same TPC-DS subset, the same 24 queries, the same model
    and the same F128, written by Databricks the ordinary way (partitioned by date, no global sort,
    no row-group cap). Rebuilding it ourselves would only add noise, so the comparison is against
    their numbers directly.

    Two ratios carry the result:

      dbxcluster / DL_Mirror  what the layout bought on the mirrored path -- the answer
      fabvorder / fab_dl      the calibration: how comparable our environment is to theirs.

    This table is STEADY STATE only (runs 2-3), and that is why the comparison is legitimate across
    environments at all: a warm pass is a VertiPaq scan over resident compressed columns on the
    same F128 SKU, with no storage on the path, so the REGION cannot move it. A calibration far
    from 1.0 therefore does not mean "different geography" -- look at engine version drift over
    the months between the two runs, at capacity contention, or at segments being evicted and
    re-paged mid-pass under 20 users on a 12.9 GB fact (which shows up as a fat p95/max rather
    than a moved median).

    Run 1 is a different matter and is NOT reported against the paper here: our two arms
    do not read from the same place. The mirrored arm's files live in the Databricks metastore's
    North Europe storage while the capacity is in West Central US; the V-Order arm reads OneLake
    in the capacity's own region. That asymmetry is worth a transatlantic round trip on every cold
    read and it penalises the LAYOUT arm, so a cold win there is won despite it.
    """
    if not os.path.exists(paper_csv):
        print(f"\n(paper comparison skipped: {paper_csv} not found)")
        return

    # Their concurrency. Rungs 1/4/16 have no counterpart in their data and are reported on their
    # own as the scaling curve, never against the paper.
    THEIR_THREADS = 20
    ours = con.sql(f"""
        SELECT scale_factor AS "SF", {PATTERN} AS "Pattern", query_number, visual_name,
               round(1000 * median(duration)) AS p50
        FROM runs
        WHERE run_index > 1 AND concurrent_threads = {THEIR_THREADS}
        GROUP BY 1,2,3,4
    """).df()
    if ours.empty:
        print(f"\n(paper comparison skipped: no measured rows at {THEIR_THREADS} threads yet)")
        return

    con.execute("CREATE OR REPLACE TABLE paper AS SELECT * FROM read_csv_auto($f, header=true)",
                {"f": paper_csv})
    con.register("ours", ours)

    print(f"\n--- Per query at {THEIR_THREADS} users, SF100: ours beside the paper's (ms)")
    cmp_df = con.sql(f"""
        WITH theirs AS (
            SELECT CAST(SF AS INT) AS sf, Pattern, CAST(query_number AS INT) AS qn,
                   visual_name, median(CAST(p50 AS DOUBLE)) AS p50
            FROM paper WHERE CAST(SF AS INT) = 100 GROUP BY 1,2,3,4
        ), o AS (
            SELECT CAST("SF" AS INT) AS sf, "Pattern" AS Pattern, query_number AS qn, p50 FROM ours
        )
        SELECT t.qn AS query_number,
               CASE WHEN t.qn IN {SLICER_QUERIES} THEN '(slicer) ' ELSE '' END || any_value(t.visual_name) AS visual,
               round(max(CASE WHEN t.Pattern = 'DL_Mirror' THEN t.p50 END))       AS "DL_Mirror(control)",
               round(max(CASE WHEN o.Pattern = 'dbxdefault' THEN o.p50 END))       AS "ours dbxdefault",
               round(max(CASE WHEN o.Pattern = 'dbxcluster' THEN o.p50 END))      AS "ours dbxcluster",
               round(max(CASE WHEN t.Pattern = 'fab_dl'    THEN t.p50 END))       AS "fab_dl(target)",
               round(max(CASE WHEN o.Pattern = 'fabvorder' THEN o.p50 END))       AS "ours fabvorder"
        FROM theirs t LEFT JOIN o ON o.sf = t.sf AND o.qn = t.qn
        GROUP BY t.qn ORDER BY t.qn
    """).df()
    print(cmp_df.to_string(index=False))

    # RUN BY RUN -- what replaying their protocol unlocked. Their CSV carries a Run column and now
    # so do our rows, so the two warming curves go side by side directly instead of being averaged
    # into one "warm" number. Suite seconds = the sum of the per-query p50s over the 15 visual
    # queries, which is what their 99.5 / 5.0 / 6.2 and 114.8 / 142.7 / 166.9 figures are.
    print(f"\n--- The warming curve, ours beside theirs: suite seconds per run, SF100, "
          f"{THEIR_THREADS} users")
    curve = con.sql(f"""
        WITH theirs AS (
            SELECT Pattern, CAST(Run AS INT) AS run_index,
                   sum(CAST(p50 AS DOUBLE)) / 1000 AS suite_s
            FROM paper
            WHERE CAST(SF AS INT) = 100 AND CAST(query_number AS INT) NOT IN {SLICER_QUERIES}
            GROUP BY 1, 2
        ), ours_pq AS (
            -- Two-step again: median per (query, run) across the readers, then summed.
            SELECT {PATTERN} AS Pattern, run_index, query_number, median(duration) AS d
            FROM runs
            WHERE scale_factor = 100 AND concurrent_threads = {THEIR_THREADS}
              AND query_number NOT IN {SLICER_QUERIES}
            GROUP BY 1, 2, 3
        ), ours_suite AS (
            SELECT Pattern, run_index, sum(d) AS suite_s FROM ours_pq GROUP BY 1, 2
        ), sides AS (
            SELECT 'theirs: ' || Pattern AS arm, run_index, suite_s FROM theirs
            UNION ALL SELECT 'ours:   ' || Pattern, run_index, suite_s FROM ours_suite
        )
        SELECT arm,
               round(max(suite_s) FILTER (WHERE run_index = 1), 1) AS run1_s,
               round(max(suite_s) FILTER (WHERE run_index = 2), 1) AS run2_s,
               round(max(suite_s) FILTER (WHERE run_index = 3), 1) AS run3_s,
               round(max(suite_s) FILTER (WHERE run_index = 1)
                     / nullif(max(suite_s) FILTER (WHERE run_index = 2), 0), 1) AS warmup_x
        FROM sides GROUP BY arm ORDER BY arm
    """).df()
    print(curve.to_string(index=False))
    print("    warmup_x is the whole comparison: their fab_dl warms ~20x, their DL_Mirror does not "
          "warm at all")
    print(f"\n--- The result: median over the 15 VISUAL queries at {THEIR_THREADS} users, SF100")
    summary = con.sql(f"""
        -- Median ACROSS RUNS per query first, then across the 15 queries -- the same two-step the
        -- per-query table above uses. Taking one median over all (query, run) rows instead would
        -- weight a query by how many runs it happens to have and print a different control value
        -- than the table right above it.
        WITH per_q AS (
            SELECT Pattern, CAST(query_number AS INT) AS qn, median(CAST(p50 AS DOUBLE)) AS p50
            FROM paper
            WHERE CAST(SF AS INT) = 100 AND CAST(query_number AS INT) NOT IN {SLICER_QUERIES}
            GROUP BY 1, 2
        ), theirs AS (
            SELECT Pattern, median(p50) AS p50 FROM per_q GROUP BY 1
        ), o AS (
            -- SF filter is NOT optional: `paper` is filtered to SF100 above, so without the same
            -- filter here an SF1000 arm's 96 s queries fold into the SF100 median and every ratio
            -- below is arithmetic between two scale factors.
            SELECT "Pattern" AS Pattern, median(p50) AS p50
            FROM ours WHERE CAST("SF" AS INT) = 100 AND query_number NOT IN {SLICER_QUERIES}
            GROUP BY 1
        )
        SELECT * FROM (
            SELECT 'DL_Mirror (theirs, CONTROL)' AS arm,
                   round((SELECT p50 FROM theirs WHERE Pattern = 'DL_Mirror')) AS median_ms
            UNION ALL SELECT 'dbxdefault (ours, config-only TREATMENT)',
                   round((SELECT p50 FROM o WHERE Pattern = 'dbxdefault'))
            UNION ALL SELECT 'dbxcluster (ours, clustered TREATMENT)',
                   round((SELECT p50 FROM o WHERE Pattern = 'dbxcluster'))
            UNION ALL SELECT 'fab_dl (theirs, target)',
                   round((SELECT p50 FROM theirs WHERE Pattern = 'fab_dl'))
            UNION ALL SELECT 'fabvorder (ours, CALIBRATION)',
                   round((SELECT p50 FROM o WHERE Pattern = 'fabvorder'))
        )
    """).df()
    print(summary.to_string(index=False))

    def val(df, name):
        m = df[df.arm.str.startswith(name)]
        return None if m.empty or m.median_ms.isna().all() else float(m.median_ms.iloc[0])

    # `dbxcluster`, not the withdrawn `dbxsort`: CLUSTER BY is what actually places rows by the
    # key on this write path. The sort arm's orderBy never reached the writer, so the ratio it used
    # to print was an unordered arm against an unordered control.
    ctrl, treat = val(summary, "DL_Mirror"), val(summary, "dbxcluster")
    targ, calib = val(summary, "fab_dl"), val(summary, "fabvorder")
    if ctrl and treat:
        print(f"\n  the layout is worth  {ctrl / treat:.1f}x  vs the untuned mirrored control "
              f"({ctrl:,.0f} ms -> {treat:,.0f} ms)")
    if targ and calib:
        print(f"  calibration          {calib / targ:.2f}x  our V-Order vs theirs "
              f"({targ:,.0f} ms -> {calib:,.0f} ms); far from 1.0 means read the first ratio with "
              f"that correction")
        if ctrl and treat:
            print(f"  ... corrected        {(ctrl / treat) * (calib / targ):.0f}x  the same ratio "
                  f"with our environment's speed discounted out")
    # Layout against V-Order IN OUR OWN ENVIRONMENT: no calibration, no cross-environment
    # arithmetic, both arms the same rows on the same capacity at the same moment. It is the only
    # fully controlled number here, so it is the one to lead with.
    if treat and calib:
        print(f"\n  layout vs V-Order    {treat / calib:.2f}x  ({treat:,.0f} ms -> {calib:,.0f} ms), "
              f"ours vs ours -- the only comparison needing no correction")
    if ctrl and treat:
        # The gap is measured to OUR V-Order when we have one: mixing their target into a
        # percentage compares two environments and can read over 100%, which says nothing.
        target, whose = (calib, "our") if calib else (targ, "their")
        if target and ctrl != target:
            print(f"  gap closed           {(ctrl - treat) / (ctrl - target) * 100:.0f}% of the "
                  f"distance from the control to {whose} V-Order")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workspace", required=True)
    ap.add_argument("--paper-csv",
                    default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                         os.pardir, "paper", "loadtest_p50_per_run.csv"),
                    help="the paper's published per-query P50s: lipinght/DB-DQ-Whitepaper "
                         "metrics_analysis_notebooks_report/loadtest_p50_per_run.csv, fetched into paper/")
    ap.add_argument("--lakehouse", default="tpcds_bench")
    # perfresults3 is the three-run protocol's table; `dbo/perfresults` still reads the old
    # probe + one-warm-pass results, whose run 1 means something different.
    ap.add_argument("--table", default="dbo/perfresults3")
    ap.add_argument("--chart", help="write the concurrency chart here (PNG)")
    ap.add_argument("--csv", help="write the summary table here")
    args = ap.parse_args()

    perf = load(args.workspace, args.lakehouse, args.table)     # noqa: F841 - used by duckdb
    con = duckdb.connect()
    con.register("perf", perf)
    # `model_id` / `model_created_utc` arrived with the fresh-model protocol and are absent from
    # every row written before it. The writer merges the schema, so they exist as NULL once one
    # new row lands -- but a table holding only old rows has no such column at all, and every
    # query below would fail on a missing name rather than report an empty new column.
    have = set(perf.schema.names)
    shim = "".join(f", CAST(NULL AS VARCHAR) AS {c}"
                   for c in ("model_id", "model_created_utc") if c not in have)
    # `run_index` became a column with the three-run protocol. Rows written before it still carry
    # it INSIDE loadtest_id (Pattern_SF_Run-timestamp), so derive it rather than NULL it: a NULL
    # would make every per-run table below silently empty on the historical table instead of
    # reporting the three load tests it does have.
    if "run_index" not in have:
        shim += (", TRY_CAST(regexp_extract(loadtest_id, '_([0-9]+)-[0-9]{8}-[0-9]{6}$', 1) "
                 "AS INTEGER) AS run_index")
    # One filter point for the withdrawn arm: everything downstream derives from `scanned`.
    con.sql(f"CREATE OR REPLACE TABLE scanned AS SELECT *, {ENGINE} AS engine{shim} "
            f"FROM perf WHERE {WITHDRAWN}")
    total = con.sql("SELECT count(*) FROM scanned").fetchone()[0]
    print(f"{total:,} rows")

    con.sql(f"""
        CREATE OR REPLACE TABLE runs AS
        WITH complete AS (
            SELECT loadtest_id, thread_id, iteration
            FROM scanned
            GROUP BY loadtest_id, thread_id, iteration
            HAVING COUNT(DISTINCT CASE WHEN error IS NULL THEN query_number END) = {N_QUERIES}
        ),
        full_rung AS (
            SELECT s.loadtest_id
            FROM scanned s JOIN complete USING (loadtest_id, thread_id, iteration)
            GROUP BY s.loadtest_id
            HAVING COUNT(DISTINCT s.thread_id) = MAX(s.concurrent_threads)
        )
        SELECT s.* FROM scanned s
        JOIN complete  USING (loadtest_id, thread_id, iteration)
        JOIN full_rung USING (loadtest_id)
        WHERE s.error IS NULL
    """)
    kept = con.sql("SELECT count(*) FROM runs").fetchone()[0]
    print(f"{kept:,} rows survive all-{N_QUERIES}-or-out and the short-rung filter")

    # A load test that ran a SHORT SUITE can never satisfy all-N-or-out, so without this it just
    # vanishes and the report says nothing. The pipeline shipped `nbr_queries = 10` for a while,
    # which silently discarded every row it wrote -- name the cause instead of printing an empty
    # table.
    wrong_n = con.sql(f"""
        SELECT loadtest_id, any_value(engine) AS engine, MAX(nbr_queries) AS suite_size,
               count(*) AS rows_dropped
        FROM scanned GROUP BY loadtest_id
        HAVING MAX(nbr_queries) <> {N_QUERIES} ORDER BY 1
    """).df()
    if not wrong_n.empty:
        print(f"\n!! these load tests ran a suite of other than {N_QUERIES} queries, so every one "
              f"of their rows is dropped by all-{N_QUERIES}-or-out:")
        print(wrong_n.to_string(index=False))

    print("\n--- Failures per rung (a rate that climbs with concurrency is a connection ceiling)")
    flaky = con.sql("""
        SELECT engine, scale_factor, concurrent_threads, cache,
               count(*) AS execs,
               count(*) FILTER (WHERE error IS NOT NULL) AS failures,
               round(100.0 * count(*) FILTER (WHERE error IS NOT NULL) / count(*), 1) AS fail_pct,
               any_value(error) FILTER (WHERE error IS NOT NULL) AS sample_error
        FROM scanned GROUP BY 1,2,3,4 HAVING failures > 0 ORDER BY 1,2,3
    """).df()
    print("none" if flaky.empty else flaky.to_string(index=False))

    print("\n--- Rungs dropped as short (asked for N threads, fewer finished the suite)")
    short = con.sql(f"""
        WITH complete AS (
            SELECT loadtest_id, thread_id, iteration FROM scanned
            GROUP BY 1,2,3
            HAVING COUNT(DISTINCT CASE WHEN error IS NULL THEN query_number END) = {N_QUERIES}
        )
        SELECT s.loadtest_id, any_value(s.engine) AS engine, MAX(s.concurrent_threads) AS asked,
               COUNT(DISTINCT c.thread_id) AS intact
        FROM scanned s LEFT JOIN complete c USING (loadtest_id, thread_id, iteration)
        GROUP BY s.loadtest_id
        HAVING intact <> MAX(s.concurrent_threads)
        ORDER BY asked
    """).df()
    print("none" if short.empty else short.to_string(index=False))

    print("\n--- One model per lifetime? (all three runs must share the model created for them)")
    fresh = con.sql("""
        SELECT scale_factor, engine, model_id,
               any_value(model_created_utc) AS created_utc,
               count(DISTINCT run_index) AS runs_seen,
               count(DISTINCT loadtest_id) AS load_tests,
               min(start_time_dt) AS first_query,
               CASE WHEN any_value(model_id) IS NULL OR any_value(model_id) = ''
                         THEN 'no model_id: pre-dates the fresh-model protocol'
                    WHEN any_value(model_created_utc) IS NULL OR any_value(model_created_utc) = ''
                         THEN 'no creation stamp'
                    WHEN min(start_time_dt) < CAST(any_value(model_created_utc) AS TIMESTAMP)
                         THEN '!! ran BEFORE its model was created'
                    WHEN count(DISTINCT run_index) <> 3
                         THEN '!! not three runs against this model'
                    ELSE 'ok' END AS verdict
        FROM runs GROUP BY scale_factor, engine, model_id ORDER BY first_query
    """).df()
    if fresh.empty:
        print("no rows")
    else:
        # Only the exceptions are worth screen space; a clean protocol prints one line. A model
        # carrying fewer than three runs is the failure that matters most: it means the warming
        # curve for that arm is between different objects, or a run was dropped by all-24-or-out.
        bad = fresh[fresh.verdict != "ok"]
        print(f"{len(fresh) - len(bad)} of {len(fresh)} model lifetime(s) carried all three runs "
              f"against the model created for them")
        if not bad.empty:
            print(bad.to_string(index=False))

    n_bad = golden_check(con)

    print("\n--- The warming curve: suite time per run (sum of the per-query medians, 15 visual)")
    print("    run 1 = first touch of a model created minutes earlier, so it PAYS the transcode "
          "inside the suite;\n    runs 2-3 = the same suite again on the same model. This is the "
          "paper's own metric and its own\n    protocol: at SF100/20 users theirs went 99.5 / 5.0 "
          "/ 6.2 s (V-Order) and 114.8 / 142.7 / 166.9 s\n    (mirrored, never warming).")
    print("    NB run 1 is not a like-for-like arm comparison: the mirrored arm reads North "
          "Europe storage\n       from a West Central US capacity, the V-Order arm reads OneLake "
          "in region. Steady state has no\n       storage on the path and is the fair one.")
    tiers = con.sql(f"""
        -- Two-step, as everywhere else here: median per (query, run) first, summed over the 15
        -- visual queries. A pooled median would weight a query by how many readers finished it.
        WITH per_q AS (
            SELECT scale_factor, concurrent_threads, engine, run_index, query_number,
                   median(duration) AS d
            FROM runs WHERE query_number NOT IN {SLICER_QUERIES}
            GROUP BY 1,2,3,4,5
        ), s AS (
            SELECT scale_factor, concurrent_threads, engine, run_index, sum(d) AS suite_s
            FROM per_q GROUP BY 1,2,3,4
        )
        SELECT scale_factor, concurrent_threads, engine,
               round(max(suite_s) FILTER (WHERE run_index = 1), 1) AS run1_s,
               round(max(suite_s) FILTER (WHERE run_index = 2), 1) AS run2_s,
               round(max(suite_s) FILTER (WHERE run_index = 3), 1) AS run3_s,
               round(max(suite_s) FILTER (WHERE run_index = 1)
                     / nullif(max(suite_s) FILTER (WHERE run_index = 2), 0), 1) AS warmup_x,
               round(max(suite_s) FILTER (WHERE run_index = 3)
                     / nullif(max(suite_s) FILTER (WHERE run_index = 2), 0), 2) AS drift_x
        FROM s GROUP BY 1,2,3 ORDER BY 1,2,3
    """).df()
    print(tiers.to_string(index=False) if not tiers.empty else "no measured rows yet")
    print("    warmup_x >> 1 = the arm warmed up; drift_x > 1 = it got slower run over run")

    print("\n--- Steady-state concurrency ladder (runs 2-3; duration in seconds)")
    df = con.sql("""
        SELECT scale_factor, engine, concurrent_threads,
               count(*) AS query_runs,
               round(median(duration), 3) AS median,
               round(avg(duration), 3)    AS avg,
               round(quantile_cont(duration, 0.95), 3) AS p95,
               round(max(duration), 3)    AS max
        FROM runs WHERE run_index > 1
        GROUP BY 1,2,3 ORDER BY 1,3,2
    """).df()
    print(df.to_string(index=False) if not df.empty else "no measured rows yet")

    print("\n--- Visual queries only (the 15 the paper reports on), vs slicer queries")
    split = con.sql(f"""
        SELECT scale_factor, engine, concurrent_threads,
               CASE WHEN query_number IN {SLICER_QUERIES} THEN 'slicer' ELSE 'visual' END AS kind,
               count(*) AS query_runs,
               round(1000 * median(duration)) AS median_ms
        FROM runs WHERE run_index > 1
        GROUP BY 1,2,3,4 ORDER BY 1,3,4,2
    """).df()
    print(split.to_string(index=False) if not split.empty else "no measured rows yet")

    paper_table(con, args.paper_csv)

    if args.csv and not df.empty:
        df.to_csv(args.csv, index=False)
        print(f"\nwritten {args.csv}")

    if args.chart and not df.empty:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        sfs = sorted(df.scale_factor.unique())
        fig, axes = plt.subplots(1, len(sfs), figsize=(6 * len(sfs), 4.5), squeeze=False)
        for ax, sf in zip(axes[0], sfs):
            sub = df[df.scale_factor == sf]
            for eng in sorted(sub.engine.unique()):
                e = sub[sub.engine == eng].sort_values("concurrent_threads")
                ax.plot(e.concurrent_threads, e["median"], marker="o", label=eng)
            # Ticks pinned to the rungs actually run: an interpolated tick would name a
            # concurrency nobody tested.
            ax.set_xticks(sorted(sub.concurrent_threads.unique()))
            ax.set_xlabel("concurrent users")
            ax.set_ylabel("median query duration (s)")
            ax.set_title(f"SF{sf}")
            ax.grid(alpha=0.3)
            ax.legend()
        fig.tight_layout()
        fig.savefig(args.chart, dpi=130)
        print(f"written {args.chart}")

    raise SystemExit(1 if n_bad else 0)


if __name__ == "__main__":
    main()
