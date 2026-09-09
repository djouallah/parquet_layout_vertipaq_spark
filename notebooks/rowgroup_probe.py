# Databricks notebook source
# MAGIC %md
# MAGIC # How do you actually ask Spark for "one row group per file, N rows"?
# MAGIC
# MAGIC Three levers make a table VertiPaq-friendly: **uniform row groups**, **dictionary encoding**,
# MAGIC and a **global sort**. File size is not one of them — the 256 MB target in `build_layout.py`
# MAGIC is a hack, a way to steer row-group size indirectly through a bytes-per-row estimate
# MAGIC (`256 MB / bytes_per_row`, times a measured 1.2 fudge for parquet-mr's sorted output). The
# MAGIC geometry that matters ends up a side effect of arithmetic that had to be calibrated.
# MAGIC
# MAGIC This asks whether the row-group size can simply be *stated*. Uniformity is the pass criterion;
# MAGIC dictionary coverage and sort-range overlaps are measured alongside it, because a variant that
# MAGIC fixes the row groups and breaks either of the other two levers is a regression, not a win.
# MAGIC
# MAGIC Two direct levers exist. This measures which of them this runtime honours:
# MAGIC
# MAGIC | | partitions | row-group lever |
# MAGIC |---|---|---|
# MAGIC | **A** baseline | `repartitionByRange(ceil(rows/RG))` | none — the partition count IS the lever |
# MAGIC | **B** `maxRecordsPerFile` | coarse: ~k×RG rows each | Spark closes the file at RG rows; 1 GiB block means that file is one group |
# MAGIC | **C** `parquet.block.row.count.limit` | coarse | parquet-mr closes the ROW GROUP at RG rows |
# MAGIC | **D** both | coarse | belt and braces |
# MAGIC | **E** configuration only | none: plain `df.write.saveAsTable()` | Delta Optimized Writes bin-packs the shuffle into `binSize` MiB bins, one file per bin; 1 GiB block = one group |
# MAGIC | **F** configuration only, **C instead of B** | none | file cap OFF, `parquet.block.row.count.limit` ON: one file holds SEVERAL groups of exactly RG rows |
# MAGIC
# MAGIC **F is the one the recipe hangs on.** A–E were all written while "one row group per file" was
# MAGIC assumed to be the goal, so B was adopted and C was rejected for firing *inside* a file. Direct
# MAGIC Lake's reviewers say the opposite — several row groups in one file is what they prefer — which
# MAGIC removes C's only disqualification and makes the ragged tail cheaper: the same one ragged unit
# MAGIC per task, but a ragged row GROUP inside a file rather than a whole ragged FILE.
# MAGIC
# MAGIC **"Coarse" has to mean bigger than RG.** The first run of this notebook used `defaultParallelism`
# MAGIC (8) partitions at SF10, i.e. 3.6M rows each against a 6M cap — so no cap ever fired and B, C and
# MAGIC D merely re-measured A's sampling skew. Partitions are now sized as *k full groups each*, which
# MAGIC forces the split and makes `parts_for(k)` the variable worth studying.
# MAGIC
# MAGIC **C only exists in parquet-java >= 1.16.0 (released 2025-09-03).** On anything older the key is
# MAGIC accepted and silently ignored — no error, wrong geometry — so cell 2 prints the parquet version
# MAGIC actually loaded and checks the constant resolves before any of this is believed.
# MAGIC
# MAGIC B failed once before and was blamed unfairly: it was set equal to the *range-partition* target,
# MAGIC so a partition that sampling made oversized spilled its remainder into a second tiny file (row
# MAGIC groups of 10,195 rows at SF100). Coarse partitions + `maxRecordsPerFile` is the combination
# MAGIC that was never tried.
# MAGIC
# MAGIC **E is the "nosort" recipe** -- no repartition, no cap, no sort: cluster config alone decides the
# MAGIC geometry. Nothing in it is table-specific, so it writes REAL Unity Catalog managed tables (same
# MAGIC table names, new schemas `tpcds_sf{sf}_nosort_b<bin>`, never overwritten) for three bin sizes and
# MAGIC both facts, and measures rows per file through `_metadata.file_path`. That answers the two things
# MAGIC the docs do not: how many rows a MiB of bin buys per table, and whether Unity Catalog's file-size
# MAGIC autotuning overrides `binSize` on managed tables (rows per file would then not move with the bin).
# MAGIC
# MAGIC A-D write plain Parquet to a scratch Volume folder, NOT a managed table: Unity Catalog blocks path
# MAGIC reads of managed-table files, and those variants have to read their own footers.

# COMMAND ----------

import json
import math
import os
import shutil
import time

import pyarrow.parquet as pq
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "databricks_ne")
dbutils.widgets.text("raw_schema", "tpcds_raw")
dbutils.widgets.text("scale_factor", "10")
dbutils.widgets.text("table", "store_sales")
dbutils.widgets.text("rows_per_group", "")          # empty -> 6M, or 16M at SF1000
dbutils.widgets.text("keep_output", "false")        # true = leave the written files for inspection

catalog    = dbutils.widgets.get("catalog")
raw_schema = dbutils.widgets.get("raw_schema")
sf         = int(dbutils.widgets.get("scale_factor"))
table      = dbutils.widgets.get("table")
# Direct Lake's window is 1M..16M. 6M is right for tens/hundreds of millions of rows; at SF1000
# (2.6B) a 6M target is ~440 groups per fact, so the target moves to the top of the window. Same
# rule as build_layout.py, so the probe tests the size that scale factor would really be built at.
RG         = int(dbutils.widgets.get("rows_per_group") or (16_000_000 if sf >= 1000 else 6_000_000))
KEEP       = dbutils.widgets.get("keep_output").lower() == "true"

RAW  = f"/Volumes/{catalog}/{raw_schema}/landing/sf{sf}/{table}"
OUT  = f"/Volumes/{catalog}/{raw_schema}/landing/_rowgroup_probe"
# Must stay in step with DEFAULT_SORT_KEYS in build_layout.py -- the probe is only meaningful if it
# writes the same shape the build does. Date leads (the one filter with contiguous keys), then a
# low-cardinality key so VertiPaq gets RLE runs (see the rationale in build_layout.py).
SORT = {"store_sales":   ["ss_sold_date_sk", "ss_store_sk", "ss_promo_sk", "ss_item_sk"],
        "catalog_sales": ["cs_sold_date_sk", "cs_ship_mode_sk", "cs_catalog_page_sk", "cs_item_sk"]}[table]

print(f"input  {RAW}")
print(f"output {OUT}")
print(f"target {RG:,} rows per row group, sorted by {SORT}")

# COMMAND ----------

# MAGIC %md ## Capability probe — before anything is written
# MAGIC
# MAGIC The one thing that must not be assumed. A missing `BLOCK_ROW_COUNT_LIMIT` means variant C is
# MAGIC not being tested at all, however good its numbers look.

# COMMAND ----------

jvm = spark.sparkContext._jvm


def conf_or(key, fallback="unset"):
    """spark.conf.get with a fallback that cannot blow up.

    Passing a default to spark.conf.get type-checks it against the CONFIG's declared type, so a
    string fallback on an int-typed key raises INVALID_CONF_VALUE.TYPE_MISMATCH rather than
    returning the string. That killed a whole probe run at the last print of the last cell.
    """
    try:
        return spark.conf.get(key)
    except Exception:                                           # noqa: BLE001
        return fallback


REPORT_TABLE = f"{catalog}.{raw_schema}.rowgroup_probe_report"


def persist(block, records):
    """Append one block of results the moment it exists.

    This notebook has twice run for twenty minutes and then died in its own reporting -- once on an
    undefined name, once on a typed spark.conf default -- losing every measurement because the exit
    payload never ran. Measurements are expensive and printing is not, so the measurements land
    first and the verdict is decoration.
    """
    if not records:
        return
    try:
        import pandas as _pd
        df = _pd.DataFrame(records).astype(str)
        (spark.createDataFrame(df)
              .withColumn("block", F.lit(block))
              .withColumn("scale_factor", F.lit(sf))
              .withColumn("rows_per_group_target", F.lit(RG))
              .withColumn("run_at", F.current_timestamp())
              .write.format("delta").mode("append").option("mergeSchema", "true")
              .saveAsTable(REPORT_TABLE))
        print(f"  [{block}] {len(records)} rows appended to {REPORT_TABLE}")
    except Exception as e:                                      # noqa: BLE001
        print(f"  [{block}] could NOT be persisted: {type(e).__name__}: {str(e)[:200]}")

try:
    parquet_version = jvm.org.apache.parquet.Version.FULL_VERSION
except Exception as e:                                          # noqa: BLE001
    parquet_version = f"<unreadable: {e}>"

try:
    block_row_count_key = jvm.org.apache.parquet.hadoop.ParquetOutputFormat.BLOCK_ROW_COUNT_LIMIT
    HAS_C = True
except Exception as e:                                          # noqa: BLE001
    block_row_count_key, HAS_C = None, False
    print(f"BLOCK_ROW_COUNT_LIMIT does not resolve: {type(e).__name__}")

print(f"spark            {spark.version}")
print(f"parquet-mr       {parquet_version}")
print(f"photon           {conf_or('spark.databricks.photon.enabled')}  (must be false)")
print(f"BLOCK_ROW_COUNT_LIMIT  {'present -> ' + str(block_row_count_key) if HAS_C else 'ABSENT: variant C cannot work on this runtime'}")

# The key name is hardcoded rather than read from the JVM constant, so C is still *attempted* on a
# runtime that lacks it -- an ignored key and an absent key look identical in the output otherwise,
# and knowing which one happened is the point of this notebook.
C_KEY = "parquet.block.row.count.limit"

# COMMAND ----------

# MAGIC %md ## What is each recipe line actually worth?
# MAGIC
# MAGIC This cluster carries **none** of the recipe (see `databricks.yml`, job `tpcds_rowgroup_probe`:
# MAGIC its `spark_conf` is `spark.master` and the single-node flag, nothing else). So whatever these
# MAGIC keys read here IS the platform default on this runtime.
# MAGIC
# MAGIC A line whose recipe value equals the default is **not** a line to delete. It is a GUARD: the
# MAGIC recipe gets pasted into estates where a cluster policy, a workspace default or a previous
# MAGIC engineer may already have set it the other way, and a default is only a default until someone
# MAGIC changes it. What this cell buys is knowing WHICH KIND each line is -- one that moves the
# MAGIC platform, or one that pins it -- so the comment next to it can say so honestly.
# MAGIC Run before anything below sets a single key.

# COMMAND ----------

# The recipe as it stands, in databricks.yml order. Value None = "we do not set it".
RECIPE = [
    ("spark.databricks.delta.optimizeWrite.enabled",                          "true",       "spark"),
    ("spark.databricks.delta.optimizeWrite.binSize",                          "4096",       "spark"),
    ("spark.sql.files.maxRecordsPerFile",                                     "12000000",   "spark"),
    ("parquet.block.row.count.limit",                                         "6000000",    "hadoop"),
    ("parquet.block.size",                                                    "2147483648", "hadoop"),
    ("spark.databricks.delta.autoCompact.enabled",                            "false",      "spark"),
    ("parquet.page.row.count.limit",                                          "16000000",   "hadoop"),
    ("parquet.page.size",                                                     "67108864",   "hadoop"),
    ("parquet.dictionary.page.size",                                          "67108864",   "hadoop"),
    ("parquet.enable.dictionary",                                             "true",       "hadoop"),
    ("spark.databricks.delta.properties.defaults.checkpointPolicy",           "classic",    "spark"),
    ("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true",       "spark"),
    ("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",   "false",      "spark"),
    ("spark.task.cpus",                                                       "4",          "spark"),
]

_h = spark.sparkContext._jsc.hadoopConfiguration()
defaults = []
for key, want, where in RECIPE:
    got = conf_or(key, None) if where == "spark" else _h.get(key)
    same = got is not None and str(got).lower() == want.lower()
    defaults.append({"key": key, "recipe": want, "default_here": got,
                     "changes_anything": not same,
                     # A Hadoop key that reads None is simply ABSENT from the conf -- parquet-mr
                     # then uses its own built-in default, which this cannot see. So "absent" says
                     # the recipe inherits nothing from the cluster, NOT that the effective default
                     # differs from the recipe value. Only a key that reads back a DIFFERENT value
                     # is known to move anything.
                     "kind": ("guard -- already this value here" if same else
                              "absent from the conf (library default applies, not visible here)"
                              if got is None else "moves it: " + str(got))})

import pandas as pd
report_d = pd.DataFrame(defaults)
display(report_d)
persist("defaults", defaults)

print()
print("Already this value on a cluster carrying none of the recipe -- pure guards:")
for d in defaults:
    if not d["changes_anything"]:
        print(f"  {d['key']:<70} = {d['default_here']}")
print("They stay. A default is only a default until a cluster policy changes it, and this block is")
print("meant to be pasted into estates where somebody already has.")
print()
print("Not set on a bare cluster. For spark.* that means the Spark default applies; for the")
print("parquet.* keys it means parquet-mr's own built-in default applies, which is documented but")
print("NOT observable from here -- so this list is 'the recipe inherits nothing', not 'all of these")
print("change something':")
for d in defaults:
    if d["changes_anything"]:
        print(f"  {d['key']:<70} {d['default_here']} -> {d['recipe']}")

# COMMAND ----------

# MAGIC %md ## The writer profile, minus the row-group lever
# MAGIC
# MAGIC Everything `build_layout.py` sets except what decides row-group size. `parquet.block.size` is
# MAGIC deliberately 1 GiB: its job is to never fire, so that whatever DOES close the group is the thing
# MAGIC being measured.

# COMMAND ----------

hconf = spark.sparkContext._jsc.hadoopConfiguration()

BASE = {
    "spark.databricks.delta.optimizeWrite.enabled": "false",
    "spark.databricks.delta.autoCompact.enabled": "false",
}
PARQUET = {
    "parquet.block.size": str(1024 ** 3),                # 1 GiB: must never be the thing that fires
    "parquet.page.size": str(1024 ** 2),
    "parquet.page.row.count.limit": "1000000",
    "parquet.dictionary.page.size": str(32 * 1024 ** 2),
    "parquet.enable.dictionary": "true",
    "parquet.statistics.truncate.length": "64",
}
for k, v in BASE.items():
    spark.conf.set(k, v)
# Hadoop keys go on BOTH: a parquet.* key set only on the Spark conf does not always reach the
# writer, and build_layout.py has always set them twice for that reason.
for k, v in PARQUET.items():
    spark.conf.set(k, v)
    hconf.set(k, v)

src = spark.read.parquet(RAW)
n_rows = src.count()
cores = max(1, spark.sparkContext.defaultParallelism)


def parts_for(k):
    """Partition count that puts ~k FULL row groups in every partition.

    The first version of this notebook used `cores` partitions and learned nothing: at SF10 that
    is 3.6M rows each, under a 6M cap, so neither cap ever fired and B/C/D just re-measured A's
    sampling skew. A cap can only be tested by partitions LARGER than the cap, and k is how much
    larger. k also decides the tail: each partition emits k full groups plus one remainder, so a
    bigger k means fewer tails but coarser parallelism.
    """
    return max(1, round(n_rows / (k * RG)))


print(f"{n_rows:,} rows, defaultParallelism={cores}")
print(f"A: {math.ceil(n_rows / RG)} range partitions of ~{RG:,} rows (the geometry IS the partitioning)")
for k in (2, 4):
    p = parts_for(k)
    print(f"k={k}: {p} partitions of ~{n_rows // p:,} rows -> ~{(n_rows // p) // RG} full groups "
          f"+ a {(n_rows // p) % RG:,}-row tail each")

# COMMAND ----------

# MAGIC %md ## Write the four variants

# COMMAND ----------

def write_variant(name, n_parts, max_records_per_file=None, block_row_count=None):
    """One write, with only the row-group lever varying. Returns wall-clock seconds.

    AQE is off for the write: it coalesces the range partitions back together, which silently
    changes the file count and therefore the geometry being measured.
    """
    path = f"{OUT}/{name}"
    if os.path.exists(path):
        shutil.rmtree(path)

    spark.conf.set("spark.sql.adaptive.enabled", "false")
    # 0 disables the Spark-side file cap; INT_MAX is parquet's own default for the row-group cap.
    # Both are set explicitly on every variant rather than unset, so no variant can inherit a
    # lever from the one before it.
    spark.conf.set("spark.sql.files.maxRecordsPerFile", str(max_records_per_file or 0))
    brc = str(block_row_count or 2147483647)
    spark.conf.set(C_KEY, brc)
    hconf.set(C_KEY, brc)

    t0 = time.time()
    (src.repartitionByRange(n_parts, *[F.col(c) for c in SORT])
        .sortWithinPartitions(*SORT)
        .write.mode("overwrite").parquet(path))
    took = time.time() - t0

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.files.maxRecordsPerFile", "0")
    print(f"  {name}: written in {took:,.0f}s")
    return took


# Each cap variant is run at two partition coarsenesses, because with a cap the partition count no
# longer sets the row-group size -- it sets how many TAILS there are. k=2 is the least coarse
# partitioning that still exercises the cap; k=4 is the one a real build would use.
VARIANTS = [
    ("A_baseline",       math.ceil(n_rows / RG), None, None),
    ("B_maxRecords_k2",  parts_for(2),           RG,   None),
    ("B_maxRecords_k4",  parts_for(4),           RG,   None),
    ("C_blockRowCount",  parts_for(2),           None, RG),
    ("D_both",           parts_for(2),           RG,   RG),
]

timings = {}
for name, n_parts, mrpf, brc in VARIANTS:
    print(f"{name}: {n_parts} partitions, maxRecordsPerFile={mrpf}, {C_KEY}={brc}")
    timings[name] = write_variant(name, n_parts, mrpf, brc)

# COMMAND ----------

# MAGIC %md ## Read the footers — what was actually written

# COMMAND ----------

# The three levers that actually matter for VertiPaq, in order:
#   1. UNIFORM row groups -- a uniform ~6M is the goal. File size is NOT a goal: 256 MB was only
#      ever a hack for steering row-group size through a bytes-per-row estimate, so file MB is
#      reported here as information, never as a pass criterion.
#   2. Dictionary encoding retained, and declared in the footer.
#   3. The global sort surviving the write -- adjacent row groups must not overlap on the sort key.
# A variant that gives perfect row groups while breaking 2 or 3 is a regression, not a win, so all
# three are measured for every variant.
def geometry(path, key=None):
    files = sorted(f for f in os.listdir(path) if f.endswith(".parquet"))
    groups, per_file, sizes, created = [], [], [], set()
    dict_cols, total_cols, ranges = 0, 0, []
    key = key or SORT[0]                     # variant F sweeps both facts, so the key is per table
    for f in files:
        pf = pq.ParquetFile(os.path.join(path, f))
        md = pf.metadata
        per_file.append(md.num_row_groups)
        sizes.append(os.path.getsize(os.path.join(path, f)) / 1024 / 1024)
        created.add(md.created_by)
        ki = md.schema.names.index(key)
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            groups.append(rg.num_rows)
            for c in range(rg.num_columns):
                col = rg.column(c)
                total_cols += 1
                # A dictionary page offset is the footer saying the chunk really is dictionary
                # encoded -- the same test verify_layout.py uses.
                dict_cols += 1 if col.dictionary_page_offset is not None else 0
            st = rg.column(ki).statistics
            if st is not None:
                ranges.append((st.min, st.max))
    overlaps = sum(1 for a, b in zip(ranges, ranges[1:]) if a[1] > b[0])
    return files, groups, per_file, sizes, created, dict_cols, total_cols, overlaps, len(ranges)


rows = []
for name, n_parts, mrpf, brc in VARIANTS:
    files, groups, per_file, sizes, created, dict_cols, total_cols, overlaps, n_ranges = \
        geometry(f"{OUT}/{name}")

    one_group_per_file = set(per_file) == {1}
    # Uniformity, which is the actual requirement. Spread is max/min: 1.0 is perfect. A cap makes
    # every full group EXACTLY RG rows and leaves one remainder per partition, so the tails are
    # judged separately -- they are allowed to be small, but not below Direct Lake's 1M floor.
    capped = mrpf is not None or brc is not None
    full = [g for g in groups if g >= RG * 0.95] if capped else list(groups)
    tails = [g for g in groups if g < RG * 0.95] if capped else []
    full = full or list(groups)              # a cap that never fired leaves nothing "full"
    spread = max(full) / min(full)
    tail_ok = all(g >= 1_000_000 for g in groups)
    in_window = all(1_000_000 <= g <= 16_000_000 for g in groups)

    rows.append({
        "variant": name,
        "lever": ("partition count" if name.startswith("A_") else
                  "maxRecordsPerFile" if name.startswith("B_") else
                  C_KEY if name.startswith("C_") else "both"),
        "parts": n_parts,
        "files": len(files),
        "row_groups": len(groups),
        "groups_per_file": f"{min(per_file)}..{max(per_file)}",
        "rows_min": min(groups),
        "rows_avg": round(sum(groups) / len(groups)),
        "rows_max": max(groups),
        "full_groups": len(full),
        "tail_min": min(tails) if tails else None,
        "spread_max_over_min": round(spread, 3),
        "dict_chunks_pct": round(100 * dict_cols / max(total_cols, 1)),
        "sort_overlaps": overlaps,
        "1_group_per_file": one_group_per_file,
        "uniform": spread <= 1.05,
        "no_group_under_1M": tail_ok,
        "PASS": one_group_per_file and spread <= 1.05 and in_window,
        "file_mb_avg": round(sum(sizes) / len(sizes), 1),      # information, not a target
        "write_s": round(timings[name]),
        "created_by": "; ".join(sorted(created))[:60],
    })

import pandas as pd
report = pd.DataFrame(rows)
display(report)
persist("AD", rows)

# COMMAND ----------

# MAGIC %md ## Variant E -- configuration only (the "nosort" recipe)
# MAGIC
# MAGIC Nothing in the write says how big a file is: `df.write.saveAsTable()` and these confs. Delta
# MAGIC Optimized Writes shuffles the rows and bin-packs the shuffle blocks into bins of `binSize` MiB,
# MAGIC one task and one file per bin; `parquet.block.size` at 1 GiB means that file is one row group.
# MAGIC `binSize` counts SHUFFLE bytes, so rows per file = bin / (shuffle bytes per row) and differs by
# MAGIC table width -- this measures that ratio for both facts at three bins, on managed tables.

# COMMAND ----------

E_BINS   = (512, 640, 768)                     # MiB of shuffle data per output file
E_TABLES = ("store_sales", "catalog_sales")
# The recipe, as in databricks.yml (job tpcds_build_nosort). Session level here, cluster level there.
E_CONF = {
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "false",
    "spark.databricks.delta.properties.defaults.checkpointPolicy": "classic",
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
    "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "false",
    # The recipe's one row rule is a CEILING at the top of Direct Lake's window: a bytes bin cannot
    # know its row count, and a 5-column table would land ~32M rows in one group. It never fires on
    # these facts. D left parquet.block.row.count.limit at RG, which WOULD fire inside a bin -- off.
    "spark.sql.files.maxRecordsPerFile": "16000000",
    C_KEY: "2147483647",
}
E_PARQUET = {
    "parquet.block.size": str(1024 ** 3),        # 1 GiB: one row group per file
    "parquet.enable.dictionary": "true",
    "parquet.dictionary.page.size": str(64 * 1024 ** 2),
}
for k, v in E_CONF.items():
    spark.conf.set(k, v)
for k, v in E_PARQUET.items():
    spark.conf.set(k, v)
    hconf.set(k, v)
hconf.set(C_KEY, "2147483647")


def e_schema(bin_mib):
    return f"tpcds_sf{sf}_nosort_b{bin_mib}"


def e_write(bin_mib, t):
    """Plain saveAsTable into a NEW schema; never overwrites. Returns wall-clock seconds, or None."""
    fq = f"{catalog}.{e_schema(bin_mib)}.{t}"
    if spark.catalog.tableExists(fq):
        print(f"  {fq}: exists from an earlier run -- measured as is, not rewritten")
        return None
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{e_schema(bin_mib)}")
    try:
        spark.sql(f"ALTER SCHEMA {catalog}.{e_schema(bin_mib)} DISABLE PREDICTIVE OPTIMIZATION")
    except Exception as e:                                       # noqa: BLE001
        print(f"  WARNING predictive optimization not disabled on {e_schema(bin_mib)}: {e}")
    # MiB: the conf is bytesConf(ByteUnit.MiB), default 512.
    spark.conf.set("spark.databricks.delta.optimizeWrite.binSize", str(bin_mib))
    t0 = time.time()
    (spark.read.parquet(f"/Volumes/{catalog}/{raw_schema}/landing/sf{sf}/{t}")
         .write.format("delta").saveAsTable(fq))                 # default mode: errorifexists
    took = time.time() - t0
    print(f"  {fq}: written in {took:,.0f}s")
    return took


def e_geometry(bin_mib, t):
    """Rows per FILE, from the table itself. UC blocks path reads of managed-table files, so row
    groups are not counted here: with a 1 GiB block and files a fraction of that, one file is one
    row group -- confirmed over the mirror (fabric/verify_layout.py), as for every other arm."""
    fq = f"{catalog}.{e_schema(bin_mib)}.{t}"
    per_file = (spark.table(fq)
                .groupBy(F.col("_metadata.file_path").alias("f"))
                .agg(F.count("*").alias("n"), F.max("_metadata.file_size").alias("b"))
                .collect())
    # The whole DESCRIBE DETAIL row, not just its properties map: the protocol columns
    # (minReaderVersion, tableFeatures) live on the row, not inside `properties`.
    d = spark.sql(f"DESCRIBE DETAIL {fq}").collect()[0].asDict()
    props = d.get("properties") or {}
    n = sorted(r["n"] for r in per_file)
    mb = [r["b"] / 1024 / 1024 for r in per_file]
    full = n[1:] if len(n) > 1 else n            # the smallest file is the remainder bin, judged apart
    return {
        "variant": f"E_b{bin_mib}_{t}", "lever": f"optimizeWrite.binSize={bin_mib} MiB",
        "table": t, "bin_mib": bin_mib,
        "files": len(n), "rows_min": n[0], "rows_avg": round(sum(n) / len(n)), "rows_max": n[-1],
        "remainder_rows": n[0] if len(n) > 1 else None,
        "spread_max_over_min": round(max(full) / min(full), 3),
        "uniform": max(full) / min(full) <= 1.05,
        "in_window": all(1_000_000 <= g <= 16_000_000 for g in full),
        "file_mb_avg": round(sum(mb) / len(mb), 1),
        "shuffle_B_per_row": round(bin_mib * 1024 ** 2 / (sum(full) / len(full))),
        # minReaderVersion, not tableFeatures membership: `deletionVectors` in the feature list means
        # the FEATURE is enabled, not that a deletion vector exists. Reader 3 is now expected.
        "min_reader": d.get("minReaderVersion"),
        "checkpoint_policy": props.get("delta.checkpointPolicy"),
        "optimize_write_prop": props.get("delta.autoOptimize.optimizeWrite"),
    }


e_rows = []
for b in E_BINS:
    for t in E_TABLES:
        print(f"E: bin {b} MiB, {t}")
        took = e_write(b, t)
        g = e_geometry(b, t)
        g["write_s"] = None if took is None else round(took)
        e_rows.append(g)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")

report_e = pd.DataFrame(e_rows)
display(report_e)
persist("E", e_rows)

# Autotune check: rows per file must MOVE with the bin. If Unity Catalog's file-size autotuning
# overrides binSize on managed tables, all three bins land on the same ~256 MB files.
for t in E_TABLES:
    r = report_e[report_e.table == t].sort_values("bin_mib")
    lo, hi = int(r.rows_avg.iloc[0]), int(r.rows_avg.iloc[-1])
    print(f"E {t}: rows/file {lo:,} at {E_BINS[0]} MiB -> {hi:,} at {E_BINS[-1]} MiB: "
          + ("binSize is honoured" if hi > lo * 1.2 else
             "WARNING does NOT scale with binSize -- UC autotune decides file size on managed tables"))

# COMMAND ----------

# MAGIC %md ## Variant F — configuration only, row groups closed by ROW COUNT
# MAGIC
# MAGIC The reviewers' preference is **several row groups inside one file**, and the answer given was
# MAGIC that Spark cannot reliably be asked for that. It can: `parquet.block.row.count.limit` states
# MAGIC the maximum rows per ROW GROUP (parquet-java >= 1.16.0, default `Integer.MAX_VALUE`), which is
# MAGIC the same row-denominated promise `maxRecordsPerFile` makes, one level down.
# MAGIC
# MAGIC F is the recipe with the two swapped: **no file cap, a row-group cap instead.** Optimized
# MAGIC Writes still decides file size, so a file now holds `floor(bin_rows / RG)` full groups plus one
# MAGIC remainder. The ragged unit per task is unchanged — it is a ragged row GROUP instead of a ragged
# MAGIC FILE — and the file count drops by roughly the number of groups per file.
# MAGIC
# MAGIC Delta to a **Volume path**, not a managed table: UC blocks path reads of managed-table files and
# MAGIC F's whole claim is in the footers. Both facts, because a row-denominated knob only earns the
# MAGIC word if 6M means 6M on the 23-column fact and the 34-column one alike.
# MAGIC
# MAGIC The failure to watch for is not a crash. An ignored key writes ONE huge group per file and
# MAGIC looks like a clean result, which is what `f_ignored` tests.

# COMMAND ----------

F_BINS   = (1024, 2048, 4096)                  # MiB of shuffle per task -> how many groups per file
F_TABLES = ("store_sales", "catalog_sales")
F_KEY    = {"store_sales": "ss_sold_date_sk", "catalog_sales": "cs_sold_date_sk"}

# The recipe as it would stand after the swap. The only differences from databricks.yml are the two
# geometry keys; every page and dictionary setting is the shipped one, because a variant that fixes
# the row groups and loses the dictionary is a regression.
F_CONF = {
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "false",
    "spark.sql.files.maxRecordsPerFile": "0",              # the file cap is OFF -- this is the change
}
F_PARQUET = {
    "parquet.block.size": str(2 * 1024 ** 3),   # 2 GiB, inert: bytes must not close a group first
    "parquet.page.row.count.limit": "16000000",
    "parquet.page.size": str(64 * 1024 ** 2),
    "parquet.dictionary.page.size": str(64 * 1024 ** 2),
    "parquet.enable.dictionary": "true",
}
for k, v in F_CONF.items():
    spark.conf.set(k, v)
for k, v in F_PARQUET.items():
    spark.conf.set(k, v)
    hconf.set(k, v)
# THE lever under test, on both confs like every other parquet key here.
spark.conf.set(C_KEY, str(RG))
hconf.set(C_KEY, str(RG))

f_rows = []
for b in F_BINS:
    for t in F_TABLES:
        path = f"{OUT}/F_b{b}_{t}"
        if os.path.exists(path):
            shutil.rmtree(path)
        spark.conf.set("spark.databricks.delta.optimizeWrite.binSize", str(b))
        print(f"F: bin {b} MiB, {t}")
        t0 = time.time()
        (spark.read.parquet(f"/Volumes/{catalog}/{raw_schema}/landing/sf{sf}/{t}")
             .write.format("delta").mode("overwrite").save(path))
        took = time.time() - t0

        files, groups, per_file, sizes, created, dict_cols, total_cols, overlaps, n_ranges = \
            geometry(path, F_KEY[t])
        # "Full" means EXACTLY the cap. A row-group cap does not approximate: it closes the group on
        # the RG-th row. Anything else in `full` would mean bytes closed the group first.
        full  = [g for g in groups if g == RG]
        tails = [g for g in groups if g != RG]
        f_rows.append({
            "variant": f"F_b{b}_{t}", "table": t, "bin_mib": b,
            "files": len(files), "row_groups": len(groups),
            "groups_per_file": f"{min(per_file)}..{max(per_file)}",
            "groups_per_file_min": min(per_file),
            "exactly_RG": len(full), "tails": len(tails),
            "tail_min": min(tails) if tails else None,
            "rows_min": min(groups), "rows_max": max(groups),
            "rows_per_file_avg": round(sum(groups) / len(files)),
            "dict_chunks_pct": round(100 * dict_cols / max(total_cols, 1)),
            "file_mb_avg": round(sum(sizes) / len(sizes), 1),
            "sort_overlaps": overlaps,                    # unsorted input: reported, not a criterion
            # PASS: the cap fired exactly, every file holds several groups, one remainder per file at
            # most, and nothing escaped Direct Lake's ceiling.
            "PASS": (len(full) > 0 and min(per_file) >= 2 and len(tails) <= len(files)
                     and max(groups) <= 16_000_000),
            "write_s": round(took),
            "created_by": "; ".join(sorted(created))[:60],
        })

report_f = pd.DataFrame(f_rows)
display(report_f)
persist("F", f_rows)

# An ABSENT key and an IGNORED key look identical in a table of geometry; only this separates them.
f_ignored = not HAS_C or any(r["rows_max"] > RG * 1.05 for r in f_rows)
print(f"\n{C_KEY} = {RG:,}: "
      + ("ABSENT from this runtime -- F measures nothing" if not HAS_C else
         "IGNORED -- a group came back larger than the cap, so the key did not reach the writer"
         if f_ignored else
         "HONOURED -- every row group closed on the row count, not on bytes"))

# The point of the sweep: with the file cap gone the bin sets FILE size only, so this is the number
# that decides how many 6M groups share a file. It goes into databricks.yml as optimizeWrite.binSize.
for t in F_TABLES:
    r = report_f[report_f.table == t].sort_values("bin_mib")
    print(f"F {t}: groups/file "
          + " -> ".join(f"{int(x)} at {int(b)} MiB" for x, b in zip(r.groups_per_file_min, r.bin_mib))
          + f", files {int(r.files.iloc[0])} -> {int(r.files.iloc[-1])}")

# COMMAND ----------

# MAGIC %md ## Variant G — does `autoCompact` fire under the recipe, and does it ignore it?
# MAGIC
# MAGIC The recipe carries `spark.databricks.delta.autoCompact.enabled false` on the claim that a
# MAGIC background compaction would undo the geometry. Two things had never been checked:
# MAGIC
# MAGIC 1. **Does it even trigger?** It needs `autoCompact.minNumFiles` small files (default 50). The
# MAGIC    recipe's own shape leaves ~1 remainder file per TASK -- 46 at SF1000 store_sales, 1 at SF10.
# MAGIC    If it never fires under the recipe, the line is guarding against nothing.
# MAGIC 2. **Does it ignore the parquet settings?** Documented: auto compaction runs synchronously on
# MAGIC    the cluster that performed the write, so it should INHERIT them -- unlike the `OPTIMIZE`
# MAGIC    that predictive optimization runs on serverless, which is where the measured damage came
# MAGIC    from. If so its only weapon is `autoCompact.maxFileSize` (128 MB, in BYTES).
# MAGIC
# MAGIC G1 is the recipe's real shape. G2 forces the trigger with a tiny bin so there are >50 small
# MAGIC files, which is the case the line exists for. Both write Delta to a Volume path so the footers
# MAGIC can be read; compacted-away files stay on disk until VACUUM, so only the files the Delta log
# MAGIC still references are measured.

# COMMAND ----------

def active_files(path):
    """Names of the files the Delta log currently references. os.listdir would also return the
    tombstoned inputs of a compaction, which would make a compacted table look untouched."""
    rows = (spark.read.format("delta").load(path)
                 .select(F.col("_metadata.file_path").alias("f")).distinct().collect())
    return sorted(r["f"].rsplit("/", 1)[-1] for r in rows)


def footers(path, names):
    groups, per_file, sizes, dict_cols, total_cols = [], [], [], 0, 0
    for n in names:
        pf = pq.ParquetFile(os.path.join(path, n))
        md = pf.metadata
        per_file.append(md.num_row_groups)
        sizes.append(os.path.getsize(os.path.join(path, n)) / 1024 / 1024)
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            groups.append(rg.num_rows)
            for c in range(rg.num_columns):
                total_cols += 1
                dict_cols += 1 if rg.column(c).dictionary_page_offset is not None else 0
    return {"files": len(names), "row_groups": len(groups),
            "groups_per_file": f"{min(per_file)}..{max(per_file)}",
            "rows_min": min(groups), "rows_max": max(groups),
            "in_window_pct": round(100 * sum(1 for g in groups if 1_000_000 <= g <= 16_000_000) / len(groups)),
            "dict_chunks_pct": round(100 * dict_cols / max(total_cols, 1)),
            "file_mb_avg": round(sum(sizes) / len(sizes), 1)}


# The recipe, verbatim, minus the one key under test.
G_CONF = {"spark.databricks.delta.optimizeWrite.enabled": "true",
          "spark.sql.files.maxRecordsPerFile": "12000000"}
G_PARQUET = {"parquet.block.size": str(2 * 1024 ** 3), C_KEY: str(RG),
             "parquet.page.row.count.limit": "16000000",
             "parquet.page.size": str(64 * 1024 ** 2),
             "parquet.dictionary.page.size": str(64 * 1024 ** 2),
             "parquet.enable.dictionary": "true"}

g_rows = []
# (label, binSize MiB, autoCompact)  -- G1 is the recipe's geometry, G2 forces >50 small files.
# (label, binSize MiB, maxRecordsPerFile, autoCompact). G1 is the recipe's own geometry. G2 has to
# cross autoCompact.minNumFiles (default 50) or it tests nothing -- the first attempt used a 32 MiB
# bin and got 12 files of 88 MB, because binSize does not shrink output the way it shrinks tasks. A
# small ROW cap does: 28.8M rows at 400k gives ~72 files.
for label, bin_mib, mrpf, auto in (("G1_recipe_shape_off", 4096, 12_000_000, "false"),
                                   ("G1_recipe_shape_ON",  4096, 12_000_000, "true"),
                                   ("G2_many_small_off",   4096,    400_000, "false"),
                                   ("G2_many_small_ON",    4096,    400_000, "true")):
    path = f"{OUT}/{label}"
    if os.path.exists(path):
        shutil.rmtree(path)
    for k, v in G_CONF.items():
        spark.conf.set(k, v)
    spark.conf.set("spark.sql.files.maxRecordsPerFile", str(mrpf))
    for k, v in G_PARQUET.items():
        spark.conf.set(k, v); hconf.set(k, v)
    spark.conf.set("spark.databricks.delta.optimizeWrite.binSize", str(bin_mib))
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", auto)

    t0 = time.time()
    (spark.read.parquet(f"/Volumes/{catalog}/{raw_schema}/landing/sf{sf}/store_sales")
         .write.format("delta").mode("overwrite").save(path))
    took = time.time() - t0

    hist = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").collect()
    ops = [h["operation"] for h in hist]
    # An auto compaction commits as OPTIMIZE with auto=true in its operationParameters.
    fired = any(h["operation"] == "OPTIMIZE"
                and str((h["operationParameters"] or {}).get("auto", "")).lower() == "true"
                for h in hist)
    g = {"variant": label, "bin_mib": bin_mib, "max_records_per_file": mrpf,
         "autoCompact": auto, "fired": fired,
         "history": ",".join(reversed(ops))}
    g.update(footers(path, active_files(path)))
    g["write_s"] = round(took)
    g_rows.append(g)
    print(f"  {label}: {g['files']} files, autoCompact "
          f"{'FIRED' if fired else 'did not fire'}, {took:,.0f}s"
          + ("" if fired or g["files"] >= 50 else
             f"  <-- only {g['files']} files, below minNumFiles: this variant tested NOTHING"))

spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
report_g = pd.DataFrame(g_rows)
display(report_g)
persist("G", g_rows)

_by = {r["variant"]: r for r in g_rows}
print()
print(f"autoCompact.minNumFiles on this runtime: "
      f"{conf_or('spark.databricks.delta.autoCompact.minNumFiles', 'unset (default 50)')}")
print(f"autoCompact.maxFileSize on this runtime: "
      f"{conf_or('spark.databricks.delta.autoCompact.maxFileSize', 'unset (default 128 MB)')}")
print()
print("G1, the recipe's own geometry: " + ("autoCompact FIRED -- the recipe line is load-bearing"
      if _by["G1_recipe_shape_ON"]["fired"] else
      "autoCompact did NOT fire -- too few small files, so the recipe line guards against nothing "
      "AT THIS SHAPE AND SCALE"))
print("G2, >50 small files: " + ("autoCompact FIRED -- compare the two G2 rows to see what it cost"
      if _by["G2_many_small_ON"]["fired"] else "autoCompact did NOT fire even here; the test is void"))
for pair in (("G1_recipe_shape_off", "G1_recipe_shape_ON"), ("G2_many_small_off", "G2_many_small_ON")):
    a, b = _by[pair[0]], _by[pair[1]]
    same = all(a[k] == b[k] for k in ("files", "row_groups", "dict_chunks_pct", "in_window_pct"))
    print(f"  {pair[0][:2]}: off vs ON -> "
          + ("IDENTICAL in files, row groups, dictionary % and window %"
             if same else
             f"files {a['files']}->{b['files']}, groups {a['row_groups']}->{b['row_groups']}, "
             f"dict {a['dict_chunks_pct']}%->{b['dict_chunks_pct']}%, "
             f"in-window {a['in_window_pct']}%->{b['in_window_pct']}%, "
             f"avg file {a['file_mb_avg']}->{b['file_mb_avg']} MB"))

# COMMAND ----------

# MAGIC %md ## Verdict

# COMMAND ----------

c_row = report[report.variant == "C_blockRowCount"].iloc[0]
# C is IGNORED, not failed, when it wrote the same geometry it would have written with no lever at
# all -- that is the silent case this notebook exists to catch. Only meaningful because the cap is
# now genuinely exercised: partitions are larger than RG, so a cap that does nothing shows up as a
# row group larger than RG.
c_ignored = not HAS_C or (c_row.rows_max > RG * 1.05)

lines = [
    f"parquet-mr {parquet_version} (spark {spark.version})",
    f"{C_KEY}: {'PRESENT' if HAS_C else 'ABSENT from this runtime'}"
    + (" -- and the geometry says it was IGNORED" if c_ignored and HAS_C else ""),
    "",
    "PASS = one row group per file, full groups UNIFORM (max/min <= 1.05), none under 1M rows.",
    "A cap makes every full group exactly RG and leaves ONE TAIL PER PARTITION, so tail_min is the",
    "number to watch as k rises: fewer partitions means fewer tails but a coarser write.",
    "File MB is reported but is not a criterion -- 256 MB was only ever a way to steer row-group",
    "size through a bytes-per-row estimate, which is the thing being replaced.",
    "Dictionary % and sort overlaps guard the other two levers: a variant that fixes the geometry",
    "and breaks either of those is a regression.",
    "",
]
for r in rows:
    tail = f"tail_min {r['tail_min']:,}" if r["tail_min"] else "no tail"
    lines.append(f"  {r['variant']:<18} {'PASS' if r['PASS'] else 'no  '}  "
                 f"{r['parts']:>3} parts -> {r['files']:>3} files, groups/file {r['groups_per_file']}, "
                 f"rows {r['rows_min']:,}..{r['rows_max']:,} "
                 f"({r['full_groups']} full, spread {r['spread_max_over_min']}, {tail}), "
                 f"dict {r['dict_chunks_pct']}%, overlaps {r['sort_overlaps']}, "
                 f"{r['file_mb_avg']} MB avg, {r['write_s']}s")
lines += ["", "E (configuration only, Optimized Writes; rows per FILE -- one group per file by the 1 GiB block).",
          "Pick the bin that puts store_sales in 6-7M rows with spread <= 1.05; that number goes into",
          "databricks.yml (tpcds_build_nosort) as spark.databricks.delta.optimizeWrite.binSize:"]
for r in e_rows:
    lines.append(f"  {r['variant']:<26} {'uniform' if r['uniform'] else 'RAGGED '}  {r['files']:>3} files, "
                 f"rows {r['rows_min']:,}..{r['rows_max']:,} (avg {r['rows_avg']:,}, "
                 f"spread {r['spread_max_over_min']}), {r['file_mb_avg']} MB avg, "
                 f"~{r['shuffle_B_per_row']} shuffle B/row"
                 + (f", {r['write_s']}s" if r['write_s'] else ""))

lines += ["", "F (configuration only, row groups closed by ROW COUNT; the file cap is off).",
          f"PASS = the cap fired EXACTLY at {RG:,}, every file holds >= 2 groups, at most one",
          "remainder per file, nothing over 16M. Pick the bin that gives the groups-per-file you",
          "want; it goes into databricks.yml as optimizeWrite.binSize, and it no longer has any say",
          "over row-group size:"]
for r in f_rows:
    tail = f"tail_min {r['tail_min']:,}" if r["tail_min"] else "no tail"
    lines.append(f"  {r['variant']:<26} {'PASS' if r['PASS'] else 'no  '}  {r['files']:>3} files, "
                 f"groups/file {r['groups_per_file']}, {r['exactly_RG']} groups exactly {RG:,}, "
                 f"{r['tails']} tails ({tail}), rows {r['rows_min']:,}..{r['rows_max']:,}, "
                 f"dict {r['dict_chunks_pct']}%, {r['file_mb_avg']} MB avg, {r['write_s']}s")
lines += ["", f"{C_KEY}: " + ("ABSENT -- F measured nothing" if not HAS_C else
                              "IGNORED -- a group exceeded the cap" if f_ignored else
                              "HONOURED")]
verdict = "\n".join(lines)
print(verdict)

if not KEEP:
    shutil.rmtree(OUT, ignore_errors=True)
    print(f"\nremoved {OUT} (pass keep_output=true to inspect the files)")
# E's managed tables are left in place on purpose: SF10, small, and the smoke-test schema for the arm.

dbutils.notebook.exit(json.dumps({"variants": rows, "E": e_rows, "F": f_rows,
                                  "G": g_rows, "defaults": defaults,
                                  "block_row_count_limit": "absent" if not HAS_C else
                                                           "ignored" if f_ignored else "honoured"},
                                 default=str))
