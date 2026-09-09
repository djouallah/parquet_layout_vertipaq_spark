# Databricks notebook source
# MAGIC %md
# MAGIC # What does `CLUSTER BY (one key)` actually do, and what does `OPTIMIZE` do to the recipe?
# MAGIC
# MAGIC Two things this project asserts but has never measured.
# MAGIC
# MAGIC **1. The `cluster` arm.** `build_spark.py` creates an empty table, pins
# MAGIC `checkpointPolicy=classic`, runs `ALTER TABLE ... CLUSTER BY (date_key)`, then appends -- under
# MAGIC the full recipe (`optimizeWrite.enabled=true`, `binSize=4096`, `maxRecordsPerFile=6000000`).
# MAGIC It asserts that `clusteringColumns` is non-empty and stops there. **`clusteringColumns` is a
# MAGIC declaration, not a measurement**: it says the key is registered on the table, not that a single
# MAGIC row moved. Nothing in the repo checks whether the rows landed in key order.
# MAGIC
# MAGIC **2. `OPTIMIZE`.** LEARNING.md's config block says "never running `OPTIMIZE`", with no
# MAGIC measurement behind it, and `build_spark.py` asserts it never ran. But the Databricks docs say
# MAGIC clustering is only made real *by* `OPTIMIZE` -- "because not all operations apply liquid
# MAGIC clustering, Databricks recommends frequently running `OPTIMIZE`". The rule and the feature are
# MAGIC in direct conflict, and the conflict has never been priced.
# MAGIC
# MAGIC ## What the docs claim, that this notebook is here to confirm on DBR 19
# MAGIC
# MAGIC - Clustering on write is **threshold-gated per transaction**. For ONE clustering key: 64 MB on a
# MAGIC   Unity Catalog managed table, 256 MB on any other Delta table. Under that, nothing clusters.
# MAGIC - Clustering on write is **best-effort**. A never-`OPTIMIZE`d clustered table is only as
# MAGIC   clustered as the write path happened to leave it.
# MAGIC - `OPTIMIZE` does the real clustering, incrementally, tagging files into **ZCubes** so later
# MAGIC   runs skip them. `OPTIMIZE FULL` reclusters everything.
# MAGIC - ~~On a UC managed table, `delta.targetFileSize` is respected by `OPTIMIZE` only, not by the
# MAGIC   write path.~~ **WRONG, corrected 2026-09-07 (`cluster_config_probe`): the clustered WRITE
# MAGIC   path on a managed table reads it, and a target coarse relative to the table collapses the
# MAGIC   write to one partition -- an unclustered table that still reports `clusteringColumns`.**
# MAGIC - Predictive optimization runs `OPTIMIZE` on **serverless compute**, carrying none of this
# MAGIC   cluster's `spark.hadoop.parquet.*` settings.
# MAGIC - Liquid clustering turns on **row tracking** by itself. Whether that materialises hidden row-id
# MAGIC   columns into the parquet files is the question -- measured here as 21 columns in, 21 out, so
# MAGIC   it does not. The recipe says nothing about row tracking, and no longer says anything about
# MAGIC   deletion vectors either (DBR 19's default, on, is what is wanted).
# MAGIC
# MAGIC ## Why synthetic data, not TPC-DS
# MAGIC
# MAGIC Every mechanism here is scale-free, and dsdgen output arrives partly date-ordered -- which would
# MAGIC make an unclustered write *look* clustered. This generates its own rows and **shuffles them**, so
# MAGIC the unclustered baseline is provably disordered and every clustering number has a known-correct
# MAGIC answer to be judged against. Geometry is scaled down (1M rows per group, not the recipe's 6M) so
# MAGIC 40M rows gives ~40 files and cross-file ordering is visible in minutes rather than hours.
# MAGIC
# MAGIC ## Two measurement surfaces
# MAGIC
# MAGIC Unity Catalog blocks path reads of managed-table files, and footers are where row groups and
# MAGIC dictionary encoding live. So every arm is written twice:
# MAGIC
# MAGIC | | what it is | what only it can measure |
# MAGIC |---|---|---|
# MAGIC | **A** | UC managed tables | the real deployment shape, and the 64 MB threshold that applies to it |
# MAGIC | **B** | path-based Delta under the Volume | parquet footers: row groups, rows per group, dictionary %, per-row-group key ranges, and the hidden columns the writer materialised |
# MAGIC
# MAGIC Whether a file was clustered is judged from **key ranges**, not from the Delta log's ZCube
# MAGIC tags. Those tags belong to the OSS Delta implementation; a Databricks engineer states they do
# MAGIC not appear in a liquid table written by DBR, which runs its own. They are recorded here as
# MAGIC information, and an absent tag is never read as evidence of anything.
# MAGIC
# MAGIC Surface B is capability-probed before anything is written. If Delta-in-a-Volume is refused here,
# MAGIC B is skipped with a warning and A still answers the ordering question.

# COMMAND ----------

import json
import os
import shutil
import time

from pyspark.sql import functions as F
from pyspark.sql import Window

dbutils.widgets.text("catalog", "databricks_ne")
dbutils.widgets.text("raw_schema", "tpcds_raw")
dbutils.widgets.text("rows", "40000000")            # ~3 GB at this width: clears the 256 MB threshold
dbutils.widgets.text("rows_per_group", "1000000")   # scaled down from the recipe's 6M -- see the header
dbutils.widgets.text("distinct_keys", "1800")       # ~5 years of dates, like ss_sold_date_sk
dbutils.widgets.text("keep_output", "false")        # true = leave the Volume files and probe schema
dbutils.widgets.text("bin_mib", "1024")             # scaled with rows_per_group -- see below
dbutils.widgets.text("run_part1", "true")
dbutils.widgets.text("run_part2", "true")

catalog    = dbutils.widgets.get("catalog")
raw_schema = dbutils.widgets.get("raw_schema")
N_ROWS     = int(dbutils.widgets.get("rows"))
RG         = int(dbutils.widgets.get("rows_per_group"))
N_KEYS     = int(dbutils.widgets.get("distinct_keys"))
KEEP       = dbutils.widgets.get("keep_output").lower() == "true"
BIN_MIB    = int(dbutils.widgets.get("bin_mib"))
RUN_P1     = dbutils.widgets.get("run_part1").lower() == "true"
RUN_P2     = dbutils.widgets.get("run_part2").lower() == "true"

KEY        = "dt"                                   # the one clustering / ordering key
PROBE      = f"clustering_probe_{int(time.time())}"  # fresh schema: never overwrite, never collide
VOL        = f"/Volumes/{catalog}/{raw_schema}/landing/_clustering_probe"

print(f"managed schema  {catalog}.{PROBE}")
print(f"volume path     {VOL}")
print(f"{N_ROWS:,} rows, {RG:,} per row group -> ~{N_ROWS // RG} files, {N_KEYS:,} distinct {KEY}")
print(f"optimizeWrite.binSize {BIN_MIB} MiB")
# The recipe ships binSize 4096 MiB against a 6M row cap, which at SF1000 puts ~9 full row groups in
# each writer task and gives a fact table ~50 tasks. Left at 4096 here it would put this whole 3 GB
# table in ONE bin -- one task, no shuffle to speak of, and the arms would be comparing writes that
# never had the shape the recipe has in production. It is scaled with rows_per_group to keep the
# invariant that matters: a task holds SEVERAL full row groups, not the whole table. The achieved
# bin count is printed per arm so a bad scaling shows up rather than hiding.

# COMMAND ----------

# MAGIC %md ## Capability probe -- before anything is written
# MAGIC
# MAGIC Three things that must not be assumed: that Photon is off (parquet-mr is the writer under test),
# MAGIC that the runtime supports `CLUSTER BY`, and that surface B is available at all. A silently
# MAGIC skipped surface and a working one look identical in the output otherwise.

# COMMAND ----------

jvm   = spark.sparkContext._jvm
hconf = spark.sparkContext._jsc.hadoopConfiguration()

try:
    parquet_version = jvm.org.apache.parquet.Version.FULL_VERSION
except Exception as e:                                          # noqa: BLE001
    parquet_version = f"<unreadable: {e}>"

photon = spark.conf.get("spark.databricks.photon.enabled", "unset")
print(f"spark       {spark.version}")
print(f"parquet-mr  {parquet_version}")
print(f"photon      {photon}  (must be false: parquet-mr is the writer under test)")
assert str(photon).lower() != "true", "Photon is ON -- the parquet writer under test is not the one running"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{PROBE}")
# A background OPTIMIZE would rewrite these tables with its own sizing, which is the one thing that
# would silently invalidate every number below -- including, in part 2, the numbers about OPTIMIZE.
try:
    spark.sql(f"ALTER SCHEMA {catalog}.{PROBE} DISABLE PREDICTIVE OPTIMIZATION")
    print(f"predictive optimization disabled on {PROBE}")
except Exception as e:                                          # noqa: BLE001
    print(f"WARNING could not disable predictive optimization on {PROBE}: {e}")

# Surface B: can a path-based Delta table live in a Volume on this workspace, and can it be clustered?
os.makedirs(VOL, exist_ok=True)
_probe_path = f"{VOL}/_capability"
shutil.rmtree(_probe_path, ignore_errors=True)
try:
    (spark.range(10).withColumn(KEY, F.col("id").cast("int"))
        .write.format("delta").clusterBy(KEY).mode("overwrite").save(_probe_path))
    d = spark.sql(f"DESCRIBE DETAIL delta.`{_probe_path}`").collect()[0].asDict()
    files_visible = any(f.endswith(".parquet") for f in os.listdir(_probe_path))
    HAS_B = bool(d.get("clusteringColumns")) and files_visible
    print(f"surface B   available (clusteringColumns={d.get('clusteringColumns')}, footers readable={files_visible})")
except Exception as e:                                          # noqa: BLE001
    HAS_B = False
    print(f"surface B   UNAVAILABLE: {type(e).__name__}: {str(e)[:300]}")
    print("            footers, dictionary % and ZCube tags cannot be measured; surface A still runs")
shutil.rmtree(_probe_path, ignore_errors=True)

# COMMAND ----------

# MAGIC %md ## The recipe, and the exact set of knobs each arm turns off
# MAGIC
# MAGIC `RECIPE` is `databricks.yml`'s `tpcds_build_spark` conf verbatim, minus `spark.task.cpus` (a
# MAGIC writer-memory setting, not a layout one) and with the row cap scaled to this notebook's `RG`.
# MAGIC `DEFAULTS` is what a session that never saw the recipe carries -- which is what predictive
# MAGIC optimization and a SQL warehouse bring to an `OPTIMIZE`.

# COMMAND ----------

# The layout recipe. Hadoop keys go on BOTH confs: a parquet.* key set only on the Spark conf does
# not always reach the writer, and every notebook here has set them twice for that reason.
RECIPE = {
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.optimizeWrite.binSize": str(BIN_MIB),
    "spark.sql.files.maxRecordsPerFile": str(RG),
    "spark.databricks.delta.autoCompact.enabled": "false",
    "spark.databricks.delta.properties.defaults.checkpointPolicy": "classic",
    # enableDeletionVectors is absent on purpose -- the recipe no longer states it and DBR 19's
    # default (on) is the intent. See databricks.yml.
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
    "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "false",
}
RECIPE_HADOOP = {
    "parquet.block.size": "2147483648",           # 2 GiB: set high enough to be inert
    "parquet.page.row.count.limit": "16000000",   # THE dictionary fix (default 20000)
    "parquet.page.size": "67108864",
    "parquet.dictionary.page.size": "67108864",
    "parquet.enable.dictionary": "true",
}
# Stock parquet-mr / Delta values. Not "the recipe with bits removed" -- the actual defaults, so an
# OPTIMIZE run under these is a faithful stand-in for one run by compute that never saw the recipe.
DEFAULTS = {
    "spark.databricks.delta.optimizeWrite.enabled": "false",
    "spark.databricks.delta.optimizeWrite.binSize": "512",
    "spark.sql.files.maxRecordsPerFile": "0",
    "spark.databricks.delta.autoCompact.enabled": "false",
}
DEFAULTS_HADOOP = {
    "parquet.block.size": "134217728",            # 128 MB
    "parquet.page.row.count.limit": "20000",
    "parquet.page.size": "1048576",
    "parquet.dictionary.page.size": "1048576",
    "parquet.enable.dictionary": "true",
}


# Every key either profile mentions. A key present in one profile and absent from the other must be
# UNSET when the other is applied, not left standing: `DEFAULTS` has no
# `properties.defaults.checkpointPolicy`, so without this the "bare" arm would silently inherit the
# recipe's `classic` and stop being a control. This is the failure mode that made the first
# rowgroup_probe run measure nothing.
PROFILE_KEYS = set(RECIPE) | set(DEFAULTS)


def apply_conf(conf, hadoop, overrides=None):
    """Apply a full conf profile plus named overrides, clearing anything the profile does not name."""
    merged = dict(conf)
    merged.update(overrides or {})
    for k in PROFILE_KEYS - set(merged):
        try:
            spark.conf.unset(k)
        except Exception:                                        # noqa: BLE001
            pass
    for k, v in merged.items():
        spark.conf.set(k, str(v))
    for k, v in hadoop.items():
        spark.conf.set(f"spark.hadoop.{k}", str(v))
        hconf.set(k, str(v))
    return merged


def recipe(overrides=None):
    return apply_conf(RECIPE, RECIPE_HADOOP, overrides)


def defaults():
    return apply_conf(DEFAULTS, DEFAULTS_HADOOP)


print("recipe:")
for k, v in {**RECIPE, **{f"spark.hadoop.{k}": v for k, v in RECIPE_HADOOP.items()}}.items():
    print(f"  {k:<70} {v}")

# COMMAND ----------

# MAGIC %md ## The data
# MAGIC
# MAGIC **The shuffle is the point.** An unclustered write of already-ordered input produces perfectly
# MAGIC disjoint files and looks exactly like successful clustering. `dt` is derived from a hash, so the
# MAGIC input arrives in no key order at all and `p0` is a real control.
# MAGIC
# MAGIC The column mix is chosen so the dictionary measurement means something: `id` is near-unique (the
# MAGIC shape parquet-mr's first-page cost test always loses on), `store` is cardinality 100 (RLE runs),
# MAGIC `cat` is mid-cardinality, and the padding is wide enough that a row costs realistic bytes.

# COMMAND ----------

def make_data():
    df = spark.range(0, N_ROWS, 1, 200)
    # dt from a hash of the id: uniformly spread over N_KEYS values and, crucially, in NO order.
    df = (df
          .withColumn(KEY,   (F.abs(F.hash(F.col("id"))) % F.lit(N_KEYS)).cast("int"))
          .withColumn("store", (F.abs(F.hash(F.col("id") + F.lit(7))) % F.lit(100)).cast("int"))
          .withColumn("cat",  F.concat(F.lit("c"), (F.abs(F.hash(F.col("id") + F.lit(13))) % F.lit(5000)).cast("string")))
          .withColumn("qty",  (F.abs(F.hash(F.col("id") + F.lit(3))) % F.lit(100)).cast("int")))
    for i in range(6):
        df = df.withColumn(f"m{i}", (F.abs(F.hash(F.col("id") + F.lit(100 + i))) % F.lit(1000000)).cast("double") / F.lit(100.0))
    for i in range(4):
        df = df.withColumn(f"s{i}", F.concat(F.lit(f"s{i}_"), (F.abs(F.hash(F.col("id") + F.lit(200 + i))) % F.lit(50000)).cast("string")))
    for i in range(6):
        df = df.withColumn(f"k{i}", (F.abs(F.hash(F.col("id") + F.lit(300 + i))) % F.lit(20000)).cast("int"))
    return df


DATA = make_data().cache()
n_cols = len(DATA.columns)
DATA.count()
# Measured once, not per arm: it is the denominator for keys_per_file and it cannot change.
TOTAL_KEYS = DATA.select(F.countDistinct(KEY)).collect()[0][0]
print(f"{N_ROWS:,} rows x {n_cols} columns, key `{KEY}` from a hash so the input is NOT in key order")
print(f"{TOTAL_KEYS:,} distinct {KEY} values -- a file holding all of them held its rows where they fell")

# COMMAND ----------

# MAGIC %md ## Measurement
# MAGIC
# MAGIC Four questions, asked identically of every arm.
# MAGIC
# MAGIC 1. **Geometry** -- rows per file, and how uniform.
# MAGIC 2. **Did clustering place the rows across files** -- `overlap_pct`, the share of files whose
# MAGIC    `[min(dt), max(dt)]` range overlaps another file's. 0 % is perfectly disjoint; ~100 % means
# MAGIC    every file spans the whole key range, i.e. nothing moved.
# MAGIC 3. **Did it order the rows inside a file** -- `sortedness`. Counts how often `dt` changes from
# MAGIC    one row to the next within a file. If the file is sorted that is ~the number of distinct
# MAGIC    values in it; if it is random it is ~the number of rows. Reported as a 0..1 score where 1 is
# MAGIC    perfectly sorted. This is the number VertiPaq's RLE actually consumes.
# MAGIC 4. **Protocol drift** -- what the write turned on behind your back.

# COMMAND ----------

def measure_managed(fq, label):
    """Surface A: everything measurable through Spark on a UC managed table."""
    t = spark.table(fq)
    fp = F.col("_metadata.file_path")

    per_file = (t.groupBy(fp.alias("f"))
                 .agg(F.count("*").alias("n"),
                      F.min(KEY).alias("lo"), F.max(KEY).alias("hi"),
                      F.countDistinct(KEY).alias("nk"),
                      F.max("_metadata.file_size").alias("bytes"))
                 .collect())
    n     = sorted(r["n"] for r in per_file)
    rng   = sorted(((r["lo"], r["hi"]) for r in per_file), key=lambda x: (x[0], x[1]))
    # A file overlaps if another file's range intersects it. Counted pairwise on the sorted list:
    # a file overlaps its predecessor when its low is at or below the running maximum high.
    overlapping, run_hi = 0, None
    for lo, hi in rng:
        if run_hi is not None and lo <= run_hi:
            overlapping += 1
        run_hi = hi if run_hi is None else max(run_hi, hi)

    # Sortedness: transitions in `dt` down each file, against the two reference points.
    # `_metadata.row_index` is per-file PHYSICAL order, which is exactly the order VertiPaq will
    # transcode -- an ORDER BY on any other column would measure a sort this notebook performed
    # rather than the one the writer left behind. It needs DBR 13.3+, so it is probed, not assumed.
    sortedness = None
    try:
        w = Window.partitionBy("_f").orderBy("_ri")
        st = (t.select(fp.alias("_f"), F.col("_metadata.row_index").alias("_ri"), F.col(KEY))
               .withColumn("_prev", F.lag(KEY).over(w))
               .groupBy("_f")
               .agg(F.sum(F.when(F.col("_prev").isNull() | (F.col("_prev") != F.col(KEY)), 1).otherwise(0)).alias("trans"),
                    F.countDistinct(KEY).alias("nk"),
                    F.count("*").alias("n"))
               .agg(F.sum("trans").alias("trans"), F.sum("nk").alias("nk"), F.sum("n").alias("n"))
               .collect()[0])
        # 1.0 = transitions equal the distinct count (sorted); 0.0 = one transition per row (random).
        floor_, ceil_ = st["nk"], st["n"]
        sortedness = 1.0 if ceil_ == floor_ else max(0.0, min(1.0, (ceil_ - st["trans"]) / (ceil_ - floor_)))
        sortedness = round(sortedness, 3)
    except Exception as e:                                       # noqa: BLE001
        print(f"  {label}: sortedness unavailable ({type(e).__name__}: {str(e)[:120]})")

    d = spark.sql(f"DESCRIBE DETAIL {fq}").collect()[0].asDict()
    props = d.get("properties") or {}
    hist = spark.sql(f"DESCRIBE HISTORY {fq}").select("operation", "operationMetrics").collect()

    return {
        "arm": label, "surface": "A_managed", "table": fq,
        "files": len(n), "rows_min": n[0], "rows_avg": round(sum(n) / len(n)), "rows_max": n[-1],
        "spread": round(n[-1] / max(n[0], 1), 2),
        "remainder_files": sum(1 for x in n if x < RG * 0.95),
        "overlap_pct": round(100 * overlapping / max(len(rng), 1)),
        "keys_per_file_avg": round(sum(r["nk"] for r in per_file) / len(per_file)),
        "keys_total": TOTAL_KEYS,
        "sortedness": sortedness,
        "file_mb_avg": round(sum(r["bytes"] for r in per_file) / len(per_file) / 1024 / 1024, 1),
        "cluster_cols": ",".join(d.get("clusteringColumns") or []),
        "min_reader": d.get("minReaderVersion"), "min_writer": d.get("minWriterVersion"),
        "features": ",".join(sorted(d.get("tableFeatures") or [])),
        "checkpoint_policy": props.get("delta.checkpointPolicy"),
        "row_tracking": props.get("delta.enableRowTracking"),
        "materialized_rowid": props.get("delta.rowTracking.materializedRowIdColumnName"),
        "deletion_vectors": props.get("delta.enableDeletionVectors"),
        "target_file_size": props.get("delta.targetFileSize"),
        "compression": props.get("delta.parquet.compression.codec"),
        "ops": ",".join(h["operation"] for h in hist),
    }

# COMMAND ----------

import pyarrow.parquet as pq


def live_files(path):
    """The files the table currently points at, replayed from the Delta log, with their tags.

    NOT `os.listdir`: OPTIMIZE leaves the files it replaced on disk until VACUUM, so after part 2's
    rewrite the directory holds both generations. Measuring the directory would report the old
    geometry and the new one mixed together -- which is exactly the number part 2 exists to get
    right. Replaying add/remove gives the live set."""
    live = {}
    logdir = os.path.join(path, "_delta_log")
    for lf in sorted(f for f in os.listdir(logdir) if f.endswith(".json")):
        with open(os.path.join(logdir, lf), encoding="utf-8") as fh:
            for line in fh:
                try:
                    act = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if "add" in act:
                    live[act["add"]["path"]] = act["add"].get("tags") or {}
                elif "remove" in act:
                    live.pop(act["remove"]["path"], None)
    return live


def measure_path(path, label):
    """Surface B: parquet footers plus the Delta log's ZCube tags. Returns None if B is unavailable.

    ZCube tags (`ZCUBE_ID` on an `add` action) are recorded but are INFORMATION ONLY: a Databricks
    engineer states on the community forum that they belong to the OSS Delta implementation and
    "you won't be able to find it in a liquid table written by DBR", which runs its own. An absent
    tag here proves nothing. Clustering is judged from the key ranges instead."""
    if not HAS_B:
        return None
    live = live_files(path)
    files = sorted(f for f in live if f.endswith(".parquet"))
    groups, per_file, sizes, created, schemas = [], [], [], set(), set()
    dict_cols = plain_cols = 0
    rg_ranges = []
    for f in files:
        md = pq.ParquetFile(os.path.join(path, f)).metadata
        per_file.append(md.num_row_groups)
        sizes.append(os.path.getsize(os.path.join(path, f)) / 1024 / 1024)
        created.add(md.created_by)
        schemas.add(tuple(md.schema.names))
        ki = md.schema.names.index(KEY) if KEY in md.schema.names else None
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            groups.append(rg.num_rows)
            for c in range(rg.num_columns):
                # A dictionary page offset is the footer stating the chunk really is dictionary
                # encoded -- the same test verify_layout.py and rowgroup_probe.py use.
                if rg.column(c).dictionary_page_offset is not None:
                    dict_cols += 1
                else:
                    plain_cols += 1
            if ki is not None:
                st = rg.column(ki).statistics
                if st is not None:
                    rg_ranges.append((st.min, st.max))

    rg_ranges.sort()
    ov, run_hi = 0, None
    for lo, hi in rg_ranges:
        if run_hi is not None and lo <= run_hi:
            ov += 1
        run_hi = hi if run_hi is None else max(run_hi, hi)

    # ZCube tags of the LIVE files only. Read as raw JSON lines rather than through Spark: the log
    # is small and the point is to see the `add` action exactly as the writer left it.
    zcubes, tagged, untagged = set(), 0, 0
    for f in files:
        zid = live[f].get("ZCUBE_ID")
        if zid:
            zcubes.add(zid)
            tagged += 1
        else:
            untagged += 1

    cols = sorted(schemas)[0] if len(schemas) == 1 else ("<schemas differ>",) if schemas else ()
    return {
        "arm": label, "surface": "B_path",
        "files": len(files), "row_groups": len(groups),
        "groups_per_file": f"{min(per_file)}..{max(per_file)}" if per_file else "-",
        "one_group_per_file": set(per_file) == {1},
        "rg_rows_min": min(groups) if groups else 0,
        "rg_rows_avg": round(sum(groups) / len(groups)) if groups else 0,
        "rg_rows_max": max(groups) if groups else 0,
        "rg_spread": round(max(groups) / max(min(groups), 1), 2) if groups else 0,
        "rg_in_1m_16m_pct": round(100 * sum(1 for g in groups if 1_000_000 <= g <= 16_000_000) / max(len(groups), 1)),
        "dict_chunk_pct": round(100 * dict_cols / max(dict_cols + plain_cols, 1)),
        "rg_overlap_pct": round(100 * ov / max(len(rg_ranges), 1)),
        "zcubes": len(zcubes), "files_zcube_tagged": tagged, "files_untagged": untagged,
        "hidden_cols": ",".join(c for c in cols if c.startswith("_")) or "-",
        "n_cols_in_file": len(cols),
        "file_mb_avg": round(sum(sizes) / max(len(sizes), 1), 1),
        "created_by": "; ".join(sorted(created))[:60],
    }

# COMMAND ----------

# MAGIC %md # Part 1 -- does `CLUSTER BY (one key)` place rows at write time?
# MAGIC
# MAGIC Eight arms over the same shuffled DataFrame. Only the named variable changes.
# MAGIC
# MAGIC | arm | what varies | what it isolates |
# MAGIC |---|---|---|
# MAGIC | `p0_plain_recipe` | no clustering | **the control.** Must show ~100 % overlap and ~0 sortedness, or the input was not shuffled and the whole run is void |
# MAGIC | `p1_cluster_recipe` | `CLUSTER BY` + the full recipe | exactly what `build_spark.py` does today |
# MAGIC | `p2_cluster_no_optimizewrite` | `optimizeWrite.enabled=false` | **leading hypothesis**: Optimized Writes' byte bin-packing shuffle running after the clustering shuffle |
# MAGIC | `p3_cluster_no_rowcap` | `maxRecordsPerFile` off | the row cap splitting clustered output |
# MAGIC | `p4_cluster_bare` | no recipe at all | control on the other side: does clustering-on-write work on DBR 19 for this data? If p4 clusters and p1 does not, the recipe is the cause |
# MAGIC | `p5_cluster_ctas` | `CREATE TABLE ... CLUSTER BY ... AS SELECT` | the documented CTAS path vs the arm's create-empty-then-append path |
# MAGIC | `p6_globalsort` | `orderBy(dt)` + recipe | **the known-good reference**, so every number above is interpretable rather than raw |
# MAGIC | `p7_threshold` | appends of 30 / 100 / 300 MB / 1 GB | where the 64 MB / 256 MB threshold actually bites |

# COMMAND ----------

def write_arm(label, mode, conf_overrides=None, use_defaults=False):
    """Write one arm to both surfaces. `mode` is what varies; everything else is held.

    plain   -- no clustering
    cluster -- create empty, pin classic checkpoints, ALTER ... CLUSTER BY, append. This is
               build_spark.py's exact sequence, reproduced rather than improved on.
    ctas    -- CREATE TABLE ... CLUSTER BY (...) AS SELECT
    sort    -- one global orderBy, to show it never reaches the writer
    """
    fq   = f"{catalog}.{PROBE}.{label}"
    path = f"{VOL}/{label}"
    if use_defaults:
        defaults()
    else:
        recipe(conf_overrides)

    t0 = time.time()
    if mode == "plain":
        DATA.write.format("delta").saveAsTable(fq)
    elif mode == "sort":
        DATA.orderBy(F.col(KEY)).write.format("delta").saveAsTable(fq)
    elif mode == "ctas":
        DATA.createOrReplaceTempView(f"src_{label}")
        spark.sql(f"CREATE TABLE {fq} CLUSTER BY ({KEY}) AS SELECT * FROM src_{label}")
    elif mode == "cluster":
        DATA.limit(0).write.format("delta").saveAsTable(fq)
        spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ('delta.checkpointPolicy' = 'classic')")
        spark.sql(f"ALTER TABLE {fq} CLUSTER BY ({KEY})")
        DATA.write.format("delta").mode("append").saveAsTable(fq)
    else:
        raise ValueError(mode)
    took_a = time.time() - t0

    row_a = measure_managed(fq, label)
    row_a["write_s"] = round(took_a)
    row_a["mode"] = mode

    row_b = None
    if HAS_B:
        shutil.rmtree(path, ignore_errors=True)
        t0 = time.time()
        try:
            if mode == "plain":
                DATA.write.format("delta").mode("overwrite").save(path)
            elif mode == "sort":
                DATA.orderBy(F.col(KEY)).write.format("delta").mode("overwrite").save(path)
            elif mode in ("cluster", "ctas"):
                # clusterBy on the DataFrameWriter is the path-table equivalent of both SQL forms;
                # the create-empty-then-ALTER dance has no path analogue worth reproducing.
                DATA.write.format("delta").clusterBy(KEY).mode("overwrite").save(path)
            row_b = measure_path(path, label)
            row_b["write_s"] = round(time.time() - t0)
            row_b["mode"] = mode
        except Exception as e:                                   # noqa: BLE001
            print(f"  {label}: surface B failed: {type(e).__name__}: {str(e)[:200]}")

    print(f"  {label:<28} {row_a['files']:>3} files ({row_a['remainder_files']} short = writer tasks), "
          f"rows {row_a['rows_min']:,}..{row_a['rows_max']:,}, "
          f"overlap {row_a['overlap_pct']}%, sortedness {row_a['sortedness']}, "
          f"keys/file {row_a['keys_per_file_avg']}/{row_a['keys_total']}, {row_a['write_s']}s"
          + (f" | B: {row_b['row_groups']} groups, dict {row_b['dict_chunk_pct']}%, "
             f"zcubes {row_b['zcubes']}, tagged {row_b['files_zcube_tagged']}/{row_b['files_zcube_tagged'] + row_b['files_untagged']}"
             if row_b else ""))
    return row_a, row_b


P1 = [
    ("p0_plain_recipe",            "plain",   None,                                                    False),
    ("p1_cluster_recipe",          "cluster", None,                                                    False),
    ("p2_cluster_no_optimizewrite", "cluster", {"spark.databricks.delta.optimizeWrite.enabled": "false"}, False),
    ("p3_cluster_no_rowcap",       "cluster", {"spark.sql.files.maxRecordsPerFile": "0"},              False),
    ("p4_cluster_bare",            "cluster", None,                                                    True),
    ("p5_cluster_ctas",            "ctas",    None,                                                    False),
    ("p6_globalsort",              "sort",    None,                                                    False),
]

rows_a, rows_b = [], []
if RUN_P1:
    for label, mode, ovr, dflt in P1:
        print(f"{label}: mode={mode}, {'DEFAULT conf' if dflt else 'recipe' + (f' + {ovr}' if ovr else '')}")
        ra, rb = write_arm(label, mode, ovr, dflt)
        rows_a.append(ra)
        if rb:
            rows_b.append(rb)
else:
    print("part 1 skipped (run_part1=false)")

# COMMAND ----------

# MAGIC %md ## p7 -- where the size threshold bites
# MAGIC
# MAGIC The docs put clustering-on-write behind a **per-transaction** size threshold: for one clustering
# MAGIC key, 64 MB on a UC managed table and 256 MB on any other Delta table. That is a claim about
# MAGIC commits, not about tables, so it is tested by appending commits of rising size to one clustered
# MAGIC table and reading the ZCube tags of each commit. A commit whose files carry no tag was not
# MAGIC clustered, however clustered the table declares itself to be.

# COMMAND ----------

threshold_rows = []
if RUN_P1:
    recipe()
    fq_t   = f"{catalog}.{PROBE}.p7_threshold"
    path_t = f"{VOL}/p7_threshold"

    # Bytes per row, measured rather than guessed, so the chunk sizes are real MB.
    bpr = max(1, round(rows_a[0]["file_mb_avg"] * 1024 * 1024 * rows_a[0]["files"] / N_ROWS)) if rows_a else 80
    CHUNKS_MB = [30, 100, 300, 1024]
    print(f"~{bpr} bytes/row measured; appending chunks of {CHUNKS_MB} MB")

    DATA.limit(0).write.format("delta").saveAsTable(fq_t)
    spark.sql(f"ALTER TABLE {fq_t} SET TBLPROPERTIES ('delta.checkpointPolicy' = 'classic')")
    spark.sql(f"ALTER TABLE {fq_t} CLUSTER BY ({KEY})")
    if HAS_B:
        shutil.rmtree(path_t, ignore_errors=True)
        DATA.limit(0).write.format("delta").clusterBy(KEY).mode("overwrite").save(path_t)

    offset = 0
    for mb in CHUNKS_MB:
        k = max(1, int(mb * 1024 * 1024 / bpr))
        chunk = DATA.where((F.col("id") >= offset) & (F.col("id") < offset + k))
        offset += k
        chunk.write.format("delta").mode("append").saveAsTable(fq_t)
        if HAS_B:
            chunk.write.format("delta").mode("append").save(path_t)
        # Whether THIS commit clustered, read off the data rather than off tags.
        #
        # The obvious test would be the ZCUBE_ID tag on the commit's `add` actions -- but a
        # Databricks engineer states on the community forum that ZCube tags are the OSS Delta
        # implementation and "you won't be able to find it in a liquid table written by DBR", which
        # runs its own. So an absent tag would be a false negative here, and tags are recorded as
        # information only. The decisive measure is the same one the arms use: the key spread of the
        # files this commit added. A clustered commit puts a narrow slice of the key range in each
        # file; an unclustered one leaves every file spanning the lot.
        tagged = spanning = n_new = None
        keys_per_file = None
        if HAS_B:
            logdir = os.path.join(path_t, "_delta_log")
            latest = sorted(f for f in os.listdir(logdir) if f.endswith(".json"))[-1]
            tagged, new_files = 0, []
            with open(os.path.join(logdir, latest), encoding="utf-8") as fh:
                for line in fh:
                    try:
                        a = json.loads(line).get("add")
                    except json.JSONDecodeError:
                        continue
                    if a:
                        new_files.append(a["path"])
                        if (a.get("tags") or {}).get("ZCUBE_ID"):
                            tagged += 1
            n_new = len(new_files)
            spans = []
            for f in new_files:
                md = pq.ParquetFile(os.path.join(path_t, f)).metadata
                if KEY not in md.schema.names:
                    continue
                ki = md.schema.names.index(KEY)
                lo, hi = None, None
                for i in range(md.num_row_groups):
                    st = md.row_group(i).column(ki).statistics
                    if st is None:
                        continue
                    lo = st.min if lo is None else min(lo, st.min)
                    hi = st.max if hi is None else max(hi, st.max)
                if lo is not None:
                    spans.append(hi - lo + 1)
            # A file spanning most of the key range held its rows where they fell.
            spanning = sum(1 for s in spans if s > N_KEYS * 0.5)
            keys_per_file = round(sum(spans) / len(spans)) if spans else None
        threshold_rows.append({
            "chunk_mb": mb, "rows": k, "files_added": n_new,
            "files_spanning_most_of_key_range": spanning,
            "key_span_per_file_avg": keys_per_file, "key_span_total": N_KEYS,
            "commit_files_zcube_tagged": tagged,      # information only -- see the comment above
            "clustered_on_write": None if n_new in (None, 0) else spanning < n_new,
        })
        print(f"  {mb:>5} MB / {k:,} rows -> {n_new} files, {spanning} spanning >half the key range, "
              f"avg key span {keys_per_file}/{N_KEYS}, zcube-tagged {tagged}")

    row_a = measure_managed(fq_t, "p7_threshold")
    row_a["mode"] = "threshold"
    row_a["write_s"] = None
    rows_a.append(row_a)
    if HAS_B:
        rb = measure_path(path_t, "p7_threshold")
        if rb:
            rb["mode"] = "threshold"
            rb["write_s"] = None
            rows_b.append(rb)

# COMMAND ----------

import pandas as pd

if rows_a:
    p1_a = pd.DataFrame(rows_a)
    display(p1_a[["arm", "mode", "files", "remainder_files", "rows_min", "rows_avg", "rows_max", "spread",
                  "overlap_pct", "sortedness", "keys_per_file_avg", "keys_total",
                  "cluster_cols", "min_reader", "min_writer", "row_tracking",
                  "materialized_rowid", "checkpoint_policy", "features", "write_s"]])
if rows_b:
    display(pd.DataFrame(rows_b))
if threshold_rows:
    display(pd.DataFrame(threshold_rows))

# COMMAND ----------

# MAGIC %md # Part 2 -- what does `OPTIMIZE` do to the recipe?
# MAGIC
# MAGIC Six variants, each on its **own fresh copy** of a base table, so before and after are both
# MAGIC measured and nothing is destroyed. Fresh writes rather than clones: at this size a rewrite costs
# MAGIC seconds and clone semantics are one more thing that would need its own verification.
# MAGIC
# MAGIC The variable is **the session config `OPTIMIZE` runs under**. `o3` and `o5` restore stock
# MAGIC parquet-mr and Delta defaults, which is what predictive optimization's serverless compute or
# MAGIC another team's SQL warehouse brings. Those two are the disaster cases, if there are any.
# MAGIC
# MAGIC | variant | base | conf at OPTIMIZE time | question |
# MAGIC |---|---|---|---|
# MAGIC | `o1_opt_recipe` | clustered | recipe | does OPTIMIZE preserve geometry when the session carries the recipe? |
# MAGIC | `o2_optfull_recipe` | clustered | recipe | does OPTIMIZE FULL cluster properly **and** keep geometry? |
# MAGIC | `o3_opt_default` | clustered | defaults | what predictive optimization actually does |
# MAGIC | `o4_optnosort_recipe` | unclustered | recipe | **the direct answer to "will OPTIMIZE hurt the existing config"** |
# MAGIC | `o5_optnosort_default` | unclustered | defaults | the "someone else ran OPTIMIZE" case |
# MAGIC | `o6_opt_targetfilesize` | clustered | recipe + `delta.targetFileSize` | can targetFileSize steer OPTIMIZE back onto the geometry? |

# COMMAND ----------

def optimize_arm(label, base_mode, full=False, use_defaults=False, target_file_size=None):
    """Write a fresh base table, measure it, run OPTIMIZE under the named conf, measure again."""
    fq   = f"{catalog}.{PROBE}.{label}"
    path = f"{VOL}/{label}"

    recipe()                                        # the base is ALWAYS written under the recipe
    if base_mode == "cluster":
        DATA.limit(0).write.format("delta").saveAsTable(fq)
        spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ('delta.checkpointPolicy' = 'classic')")
        spark.sql(f"ALTER TABLE {fq} CLUSTER BY ({KEY})")
        DATA.write.format("delta").mode("append").saveAsTable(fq)
    else:
        DATA.write.format("delta").saveAsTable(fq)
    if HAS_B:
        shutil.rmtree(path, ignore_errors=True)
        w = DATA.write.format("delta")
        if base_mode == "cluster":
            w = w.clusterBy(KEY)
        w.mode("overwrite").save(path)

    before_a = measure_managed(fq, f"{label}:before")
    before_b = measure_path(path, f"{label}:before") if HAS_B else None

    if target_file_size:
        spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ('delta.targetFileSize' = '{target_file_size}')")
        if HAS_B:
            spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.targetFileSize' = '{target_file_size}')")

    # THE variable: what the session carries when OPTIMIZE runs.
    if use_defaults:
        defaults()
    else:
        recipe()

    sql = f"OPTIMIZE {fq}" + (" FULL" if full else "")
    t0 = time.time()
    spark.sql(sql)
    took = time.time() - t0
    if HAS_B:
        try:
            spark.sql(f"OPTIMIZE delta.`{path}`" + (" FULL" if full else ""))
        except Exception as e:                                   # noqa: BLE001
            print(f"  {label}: surface B OPTIMIZE failed: {type(e).__name__}: {str(e)[:200]}")

    after_a = measure_managed(fq, f"{label}:after")
    after_b = measure_path(path, f"{label}:after") if HAS_B else None
    for r in (before_a, after_a):
        r["mode"] = f"{base_mode}{' FULL' if full else ''}{' default-conf' if use_defaults else ' recipe-conf'}"
        r["optimize_s"] = round(took)

    print(f"  {label}: {sql} under {'DEFAULTS' if use_defaults else 'recipe'} in {took:,.0f}s")
    print(f"    A files {before_a['files']}->{after_a['files']}, "
          f"rows/file {before_a['rows_avg']:,}->{after_a['rows_avg']:,}, "
          f"overlap {before_a['overlap_pct']}%->{after_a['overlap_pct']}%, "
          f"sortedness {before_a['sortedness']}->{after_a['sortedness']}")
    if before_b and after_b:
        for r in (before_b, after_b):
            r["mode"] = f"{base_mode}{' FULL' if full else ''}{' default-conf' if use_defaults else ' recipe-conf'}"
            r["optimize_s"] = round(took)
        print(f"    B groups/file {before_b['groups_per_file']}->{after_b['groups_per_file']}, "
              f"rows/group {before_b['rg_rows_avg']:,}->{after_b['rg_rows_avg']:,}, "
              f"dict {before_b['dict_chunk_pct']}%->{after_b['dict_chunk_pct']}%, "
              f"zcubes {before_b['zcubes']}->{after_b['zcubes']}, "
              f"in-window {before_b['rg_in_1m_16m_pct']}%->{after_b['rg_in_1m_16m_pct']}%")
    return [before_a, after_a], [r for r in (before_b, after_b) if r]


# One row group's worth of bytes, so targetFileSize is asked for exactly the geometry the recipe wants.
tfs = None
if rows_a:
    base = next((r for r in rows_a if r["arm"] == "p0_plain_recipe"), rows_a[0])
    tfs = int(base["file_mb_avg"] * 1024 * 1024 * RG / max(base["rows_avg"], 1))

P2 = [
    ("o1_opt_recipe",         "cluster", False, False, None),
    ("o2_optfull_recipe",     "cluster", True,  False, None),
    ("o3_opt_default",        "cluster", False, True,  None),
    ("o4_optnosort_recipe",   "plain",   False, False, None),
    ("o5_optnosort_default",  "plain",   False, True,  None),
    ("o6_opt_targetfilesize", "cluster", False, False, tfs),
]

opt_a, opt_b = [], []
if RUN_P2:
    for label, base_mode, full, dflt, t in P2:
        print(f"{label}:")
        aa, bb = optimize_arm(label, base_mode, full, dflt, t)
        opt_a += aa
        opt_b += bb
else:
    print("part 2 skipped (run_part2=false)")

# COMMAND ----------

if opt_a:
    display(pd.DataFrame(opt_a)[["arm", "mode", "files", "remainder_files", "rows_min", "rows_avg", "rows_max", "spread",
                                 "overlap_pct", "sortedness", "keys_per_file_avg",
                                 "cluster_cols", "min_reader", "features", "checkpoint_policy",
                                 "row_tracking", "target_file_size", "optimize_s", "ops"]])
if opt_b:
    display(pd.DataFrame(opt_b))

# COMMAND ----------

# MAGIC %md ## Verdict
# MAGIC
# MAGIC Read in this order. Nothing below is believable if the control failed.

# COMMAND ----------

def get(rows, arm):
    return next((r for r in rows if r["arm"] == arm), None)


def s_str(v):
    """sortedness for display. None means the measure was unavailable, not that it was zero."""
    return "n/a  " if v is None else f"{v:<5}"


lines = [
    f"spark {spark.version}, parquet-mr {parquet_version}, photon {photon}",
    f"surface B (footers, ZCube tags): {'available' if HAS_B else 'UNAVAILABLE -- footer numbers are missing'}",
    f"{N_ROWS:,} rows x {n_cols} cols, {RG:,} rows/group target, {N_KEYS:,} distinct {KEY}",
    "",
    "overlap_pct  share of files whose key range overlaps another's. ~100 = nothing moved, 0 = disjoint.",
    "sortedness   0 = key order is random inside each file, 1 = perfectly sorted. VertiPaq's RLE reads this.",
    "zcubes       INFORMATION ONLY. The ZCUBE_ID tag is the OSS Delta implementation; DBR runs its",
    "             own and may write none. Absent tags prove nothing -- key ranges are the evidence.",
    "keys/file    distinct key values per file, against the table total. Near the total means the",
    "             rows stayed where they fell. clusteringColumns is a declaration, not a measurement.",
    "",
]

ctl = get(rows_a, "p0_plain_recipe")
if ctl:
    ok = ctl["overlap_pct"] >= 80 and (ctl["sortedness"] is None or ctl["sortedness"] <= 0.2)
    lines.append(f"CONTROL p0_plain_recipe: overlap {ctl['overlap_pct']}%, sortedness {s_str(ctl['sortedness'])} -> "
                 + ("valid: the input really is unordered" if ok else
                    "INVALID -- the input was already ordered, or the measure is wrong. "
                    "Every clustering number in this run is void."))
    lines.append("")

if rows_a:
    lines.append("Part 1 -- did CLUSTER BY place the rows?")
    for r in rows_a:
        b = get(rows_b, r["arm"])
        z = f", dict {b['dict_chunk_pct']}%, groups/file {b['groups_per_file']}, zcube-tagged {b['files_zcube_tagged']}/{b['files']}" if b else ""
        lines.append(f"  {r['arm']:<28} overlap {r['overlap_pct']:>3}%  sortedness {s_str(r['sortedness'])} "
                     f"keys/file {r['keys_per_file_avg']:>5}/{r['keys_total']}  "
                     f"{r['files']:>3} files x ~{r['rows_avg']:,}{z}")
    p1, p4, ref = get(rows_a, "p1_cluster_recipe"), get(rows_a, "p4_cluster_bare"), get(rows_a, "p6_globalsort")
    if p1 and ref:
        worked = p1["overlap_pct"] < 50 or p1["keys_per_file_avg"] < p1["keys_total"] * 0.5
        lines.append("")
        lines.append(f"  The arm as built (p1) {'DID' if worked else 'DID NOT'} cluster. "
                     f"Reference: the global sort (p6) reaches overlap {ref['overlap_pct']}%, "
                     f"sortedness {s_str(ref['sortedness'])}.")
        if p4:
            lines.append(f"  Without the recipe (p4): overlap {p4['overlap_pct']}%, sortedness {s_str(p4['sortedness'])} -- "
                         + ("so the recipe is what suppresses clustering." if
                            (p4["overlap_pct"] < 50) and not worked else
                            "so the recipe is not the difference."))
    for name, hyp in (("p2_cluster_no_optimizewrite", "Optimized Writes"),
                      ("p3_cluster_no_rowcap", "the row cap"),
                      ("p5_cluster_ctas", "the create-empty-then-append path")):
        r = get(rows_a, name)
        if r and p1:
            sorted_better = (r["sortedness"] is not None and p1["sortedness"] is not None
                             and r["sortedness"] > p1["sortedness"] + 0.2)
            better = r["overlap_pct"] < p1["overlap_pct"] - 10 or sorted_better
            lines.append(f"  {hyp}: {'IS' if better else 'is NOT'} the cause "
                         f"({name} overlap {r['overlap_pct']}% vs p1 {p1['overlap_pct']}%, "
                         f"sortedness {s_str(r['sortedness'])} vs {s_str(p1['sortedness'])})")
    lines.append("")

if threshold_rows:
    lines.append("  Size threshold, per commit:")
    for r in threshold_rows:
        lines.append(f"    {r['chunk_mb']:>5} MB ({r['rows']:,} rows): "
                     + ("clustered on write" if r["clustered_on_write"] else
                        "NOT clustered" if r["clustered_on_write"] is False else "not measurable (no surface B)")
                     + (f" -- {r['files_spanning_most_of_key_range']}/{r['files_added']} files span "
                        f">half the key range, avg span {r['key_span_per_file_avg']}/{r['key_span_total']}"
                        if r.get("files_added") else ""))
    lines.append("")

rt = [r["arm"] for r in rows_a if str(r.get("row_tracking")).lower() == "true"]
mr = sorted({str(r["min_reader"]) for r in rows_a if r.get("cluster_cols")})
if rows_a:
    lines.append(f"  Protocol on clustered tables: minReaderVersion {mr or '-'}; "
                 f"row tracking on: {rt or 'none'}")
    hid = {r["arm"]: r["hidden_cols"] for r in rows_b if r.get("hidden_cols") not in (None, "-")}
    lines.append(f"  Hidden columns materialised into the parquet files: {hid or 'none'}")
    lines.append("")

if opt_a:
    lines.append("Part 2 -- what OPTIMIZE does to the recipe:")
    for label, *_ in P2:
        bef, aft = get(opt_a, f"{label}:before"), get(opt_a, f"{label}:after")
        bb, ab = get(opt_b, f"{label}:before"), get(opt_b, f"{label}:after")
        if not (bef and aft):
            continue
        # OPTIMIZE is judged against what it INHERITED, not against the recipe's target. Measuring
        # `after` against RG blames OPTIMIZE for geometry the write had already lost -- which is
        # exactly what happens on a clustered table, where the row cap wrecks the file sizes before
        # OPTIMIZE is ever called. "No change" is the honest verdict there, and it is the one that
        # answers the question being asked.
        rewrote = (aft["files"] != bef["files"] or aft["rows_avg"] != bef["rows_avg"])
        geom_kept = not rewrote or abs(aft["rows_avg"] - bef["rows_avg"]) / max(bef["rows_avg"], 1) < 0.1
        dict_kept = (ab["dict_chunk_pct"] >= bb["dict_chunk_pct"] - 5) if (bb and ab) else None
        win_kept = (ab["rg_in_1m_16m_pct"] >= 95) if ab else None
        lines.append(f"  {label:<24} rows/file {bef['rows_avg']:,}->{aft['rows_avg']:,} "
                     f"({'no rewrite' if not rewrote else 'geometry HELD' if geom_kept else 'geometry LOST'}), "
                     f"overlap {bef['overlap_pct']}%->{aft['overlap_pct']}%, "
                     f"sortedness {s_str(bef['sortedness'])}->{s_str(aft['sortedness'])}, {aft['optimize_s']}s"
                     + (f", dict {bb['dict_chunk_pct']}%->{ab['dict_chunk_pct']}% "
                        f"({'kept' if dict_kept else 'DESTROYED'}), "
                        f"groups/file {bb['groups_per_file']}->{ab['groups_per_file']}, "
                        f"in-window {ab['rg_in_1m_16m_pct']}%" if (bb and ab) else ""))
    lines.append("")
    o1, o3 = get(opt_a, "o1_opt_recipe:after"), get(opt_a, "o3_opt_default:after")
    o4, o5 = get(opt_a, "o4_optnosort_recipe:after"), get(opt_a, "o5_optnosort_default:after")
    o4b, o5b = get(opt_a, "o4_optnosort_recipe:before"), get(opt_a, "o5_optnosort_default:before")
    if o4 and o4b:
        safe = o4["files"] == o4b["files"] and o4["rows_avg"] == o4b["rows_avg"]
        lines.append(f"  ANSWER to 'will OPTIMIZE hurt the existing config': on an UNCLUSTERED recipe table,")
        lines.append(f"  OPTIMIZE under the recipe conf leaves rows/file at {o4['rows_avg']:,} "
                     f"(was {o4b['rows_avg']:,}) -- {'SAFE, it rewrote nothing' if safe else 'NOT SAFE'}.")
    if o5 and o5b:
        safe5 = o5["files"] == o5b["files"] and o5["rows_avg"] == o5b["rows_avg"]
        lines.append(f"  Under DEFAULT conf (a SQL warehouse, or predictive optimization's serverless")
        o5bb, o5ab = get(opt_b, "o5_optnosort_default:before"), get(opt_b, "o5_optnosort_default:after")
        lines.append(f"  compute) it leaves {o5['rows_avg']:,} rows/file -- "
                     + ("it rewrote nothing." if safe5 else "it REWROTE the files.")
                     + (f" In the footers: dictionary {o5bb['dict_chunk_pct']}%->{o5ab['dict_chunk_pct']}%, "
                        f"row groups per file {o5bb['groups_per_file']}->{o5ab['groups_per_file']}, "
                        f"in Direct Lake's window {o5bb['rg_in_1m_16m_pct']}%->{o5ab['rg_in_1m_16m_pct']}%."
                        if (o5bb and o5ab) else ""))
    if o1 and o3:
        lines.append(f"  On a CLUSTERED table: recipe conf -> {o1['rows_avg']:,} rows/file, "
                     f"overlap {o1['overlap_pct']}%; default conf -> {o3['rows_avg']:,}, overlap {o3['overlap_pct']}%.")

verdict = "\n".join(lines)
print(verdict)

# COMMAND ----------

# MAGIC %md ## Clean up
# MAGIC
# MAGIC The probe schema and the Volume folder both go. Unlike `rowgroup_probe`'s variant-E schemas
# MAGIC these have no later use -- the tables are synthetic and the findings are in the report.

# COMMAND ----------

payload = {
    "config": {"rows": N_ROWS, "rows_per_group": RG, "distinct_keys": N_KEYS, "columns": n_cols,
               "spark": spark.version, "parquet": str(parquet_version), "photon": str(photon),
               "surface_b": HAS_B, "schema": f"{catalog}.{PROBE}"},
    "part1_managed": rows_a, "part1_path": rows_b, "threshold": threshold_rows,
    "part2_managed": opt_a, "part2_path": opt_b,
    "verdict": verdict,
}

if not KEEP:
    shutil.rmtree(VOL, ignore_errors=True)
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{PROBE} CASCADE")
    print(f"dropped {catalog}.{PROBE} and removed {VOL} (pass keep_output=true to inspect)")
else:
    print(f"kept {catalog}.{PROBE} and {VOL}")

dbutils.notebook.exit(json.dumps(payload, default=str))
