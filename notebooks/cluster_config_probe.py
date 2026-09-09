# Databricks notebook source
# MAGIC %md
# MAGIC # cluster_config_probe: liquid clustering against the recipe, one knob at a time
# MAGIC No bundle job. Run it as a one-off `databricks jobs submit` with an explicit `new_cluster`
# MAGIC carrying the recipe's `spark_conf`. Never add it to `tpcds_layout`: deploying that bundle for
# MAGIC an experiment rewrites the shared build job under whoever is running a real arm.
# MAGIC
# MAGIC The `cluster` arm's exact write sequence (empty table, `checkpointPolicy=classic`,
# MAGIC `ALTER TABLE ... CLUSTER BY (date key)`, `append`) on the real SF{N} `store_sales` rows from the
# MAGIC raw Volume with the build's customisations, under the recipe's `spark_conf`. Each variant
# MAGIC changes ONE session-level knob before the append; the parquet-mr keys (`spark.hadoop.parquet.*`)
# MAGIC are cluster-level and identical throughout -- the row-group cap is the bundle's `rows_per_group`
# MAGIC variable and is asserted here.
# MAGIC
# MAGIC | variant | change |
# MAGIC |---|---|
# MAGIC | `recipe` | none: Optimized Writes on, binSize 4096, maxRecordsPerFile 12M |
# MAGIC | `nocap` | `spark.sql.files.maxRecordsPerFile = 0` |
# MAGIC | `noow` | `spark.databricks.delta.optimizeWrite.enabled = false` |
# MAGIC | `bin1024` | `spark.databricks.delta.optimizeWrite.binSize = 1024` |
# MAGIC | `tfs512` | table property `delta.targetFileSize = 512 MiB` set before the append |
# MAGIC | `ns_recipe` | **no `CLUSTER BY`** -- the default arm's write, recipe config, as the control |
# MAGIC | `ns_tfs128` | no `CLUSTER BY` + table property `delta.targetFileSize = 128 MB` |
# MAGIC | `ns_sess128` | no `CLUSTER BY` + session `...properties.defaults.targetFileSize = 128 MB` |
# MAGIC | `ac_off` | no `CLUSTER BY` + `maxRecordsPerFile = 500000` (~53 files), auto compaction OFF |
# MAGIC | `ac_on` | the same 53-file write with `autoCompact.enabled = true` |
# MAGIC | `ac_prop` | and with the TABLE property `delta.autoOptimize.autoCompact = true` too |
# MAGIC
# MAGIC `ac_off` / `ac_on` settle the auto-compaction question LEARNING.md has had open: the recipe's
# MAGIC own shape leaves ~1 remainder file per task, far under `autoCompact.minNumFiles` (50), so it
# MAGIC has never been seen to fire. A small `maxRecordsPerFile` forces >50 small files, which is the
# MAGIC lever the earlier attempt got wrong (`binSize` shrinks tasks, not output).
# MAGIC
# MAGIC The three `ns_*` variants answer one question: does a byte target reshape a NON-clustered
# MAGIC write, or do the row caps stay the only levers? If `ns_tfs128` and `ns_sess128` match
# MAGIC `ns_recipe` (12M-row files), a byte target is inert off the cluster arm and can live in the
# MAGIC shared config block. If they come out at ~3.7M rows, it cannot.
# MAGIC
# MAGIC For every variant: the executed physical plans (from the driver's SQL status store, the only
# MAGIC place the write's real plan is visible), per-file rows / bytes / key range / distinct keys,
# MAGIC overlap between files, in-file sortedness down `_metadata.row_index`, `DESCRIBE HISTORY`
# MAGIC metrics and `DESCRIBE DETAIL`. `tpcds_sf{N}_default.store_sales` is measured the same way as the
# MAGIC unclustered control. Row groups and dictionary coverage are NOT measured here: UC blocks footer
# MAGIC reads of managed tables, and the repo reads footers from Fabric over the mirror
# MAGIC (`layout_stats`, `fabric/verify_layout.py`). Mirror the variant's schema and read it there.
# MAGIC
# MAGIC Never overwrites: each variant is its own schema `tpcds_sf{N}_cluster_<variant>`, and an
# MAGIC existing table stops the run.

# COMMAND ----------

import json, re, time
from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog", "databricks_ne")
dbutils.widgets.text("raw_schema", "tpcds_raw")
dbutils.widgets.text("scale_factor", "10")
dbutils.widgets.text("variants", "recipe,nocap,noow,bin1024,tfs512")
dbutils.widgets.text("rows_per_group", "6000000")
CATALOG    = dbutils.widgets.get("catalog")
RAW_SCHEMA = dbutils.widgets.get("raw_schema")
SF         = int(dbutils.widgets.get("scale_factor"))
VARIANTS   = [v for v in re.split(r"[,+;\s]+", dbutils.widgets.get("variants")) if v]   # `+` from the CLI: --var splits on commas
ROWS_PER_GROUP = int(dbutils.widgets.get("rows_per_group"))
TABLE, KEY = "store_sales", "ss_sold_date_sk"
RAW = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/landing/sf{SF}"
CONTROL = f"{CATALOG}.tpcds_sf{SF}_default.{TABLE}"          # the unclustered control; must exist
print(f"sf{SF} variants={VARIANTS} rows_per_group={ROWS_PER_GROUP:,} control={CONTROL}")

KNOBS = {
    "recipe":     {},
    "nocap":      {"spark.sql.files.maxRecordsPerFile": "0"},
    "noow":       {"spark.databricks.delta.optimizeWrite.enabled": "false"},
    "bin1024":    {"spark.databricks.delta.optimizeWrite.binSize": "1024"},
    "tfs512":     {},
    # --- the default question: does a byte target reshape a NON-clustered write at all? ---
    # LEARNING.md asserted it does ("a second lever fighting the row caps") on reasoning alone.
    # These three settle it. `ns_recipe` is the like-for-like control written in this same session.
    "ns_recipe":  {},
    "ns_tfs128":  {},
    "ns_sess128": {"spark.databricks.delta.properties.defaults.targetFileSize": str(128 * 1024 * 1024)},
    # --- auto compaction: what does it do WHEN IT FIRES? ---
    # Open in LEARNING.md since 2026-09-07. It was probed once (rowgroup_probe G) and never
    # triggered, because the recipe's own shape leaves ~1 remainder file per task -- far under
    # autoCompact.minNumFiles (50). The earlier attempt to force it used optimizeWrite.binSize,
    # which shrinks TASKS not OUTPUT, so it still produced 12 big files.
    #
    # This forces it the way that actually works: a small maxRecordsPerFile. At SF10 store_sales
    # (26.2M rows) a 500k cap gives ~53 files of ~18 MB -- past minNumFiles, and every one of them
    # "small" against the 128 MB autoCompact.maxFileSize.
    #
    # `ac_off` is the control: same 53 files, compaction off. `ac_on` is the same write with
    # compaction on. What we are asking: does it fire, does it inherit the session's parquet
    # settings (it runs synchronously on the writing cluster, so it SHOULD close row groups at 6M),
    # and what file size does it impose. History shows the compaction commit if there is one.
    "ac_off":     {"spark.sql.files.maxRecordsPerFile": "500000",
                   "spark.databricks.delta.autoCompact.enabled": "false"},
    "ac_on":      {"spark.sql.files.maxRecordsPerFile": "500000",
                   "spark.databricks.delta.autoCompact.enabled": "true"},
    # MEASURED 2026-09-07: `ac_on` did NOT fire, and the reason is the recipe itself. The config's
    # `...properties.defaults.autoOptimize.autoCompact = false` stamps `delta.autoOptimize.autoCompact
    # = false` as a TABLE property on every new table, and the table property BEATS the session
    # switch. So `ac_on` and `ac_off` came out byte-identical. This variant sets the table property
    # true as well, which is the only way to actually make compaction run under this recipe.
    "ac_prop":    {"spark.sql.files.maxRecordsPerFile": "500000",
                   "spark.databricks.delta.autoCompact.enabled": "true"},
}
TBLPROPS = {"tfs512":    {"delta.targetFileSize": str(512 * 1024 * 1024)},
            "ns_tfs128": {"delta.targetFileSize": str(128 * 1024 * 1024)},
            "ac_prop":   {"delta.autoOptimize.autoCompact": "true"}}

# Variants that do NOT declare clustering keys -- the `default` arm's write, where the row caps are
# supposed to be the only levers. Everything else runs the cluster arm's sequence.
UNCLUSTERED = {"ns_recipe", "ns_tfs128", "ns_sess128", "ac_off", "ac_on", "ac_prop"}

# Variants written to a VOLUME as path tables instead of managed tables, because their question is
# about ROW GROUPS and UC will not let anything read a managed table's parquet footers. A Volume is
# FUSE-mounted on the driver, so the files are just files and pyarrow opens them directly. The repo's
# normal footer route (mirror to Fabric, run layout_stats) is right for an ARM; it is absurd overhead
# for a throwaway probe.
FOOTER_VARIANTS = {"ac_off", "ac_on", "ac_prop"}
PROBE_VOL = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/landing/probe_ac_sf{SF}"

RECIPE = {
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.optimizeWrite.binSize": "4096",
    "spark.sql.files.maxRecordsPerFile": "12000000",
}
MAX_RECORDS = int(RECIPE["spark.sql.files.maxRecordsPerFile"])
WATCH = list(RECIPE) + ["spark.databricks.delta.properties.defaults.targetFileSize",
                        "spark.databricks.delta.autoCompact.enabled",
                        "spark.databricks.delta.properties.defaults.checkpointPolicy",
                        "spark.sql.adaptive.enabled", "spark.sql.shuffle.partitions"]
hconf = spark.sparkContext._jsc.hadoopConfiguration()
HADOOP = {k: hconf.get(k) for k in ["parquet.block.row.count.limit", "parquet.block.size",
                                    "parquet.page.row.count.limit", "parquet.page.size",
                                    "parquet.dictionary.page.size", "parquet.enable.dictionary"]}
for k, want in RECIPE.items():
    got = spark.conf.get(k, None)
    assert str(got).lower() == want, f"cluster is not the recipe: {k}={got!r}, want {want!r}"
assert HADOOP["parquet.block.row.count.limit"] == str(ROWS_PER_GROUP), \
    f"row-group cap on the cluster is {HADOOP['parquet.block.row.count.limit']}, widget says {ROWS_PER_GROUP}"
assert HADOOP["parquet.page.row.count.limit"] == "16000000", HADOOP
print("recipe in force", HADOOP)

# COMMAND ----------

def executions():
    store = spark._jsparkSession.sharedState().statusStore()
    out, it = [], store.executionsList().iterator()
    while it.hasNext():
        e = it.next()
        out.append({"id": e.executionId(), "desc": str(e.description())[:200],
                    "plan": str(e.physicalPlanDescription())})
    return out


def measure(df):
    """Placement, geometry and in-file order through Spark: works on managed tables too."""
    fp = F.col("_metadata.file_path")
    per_file = (df.groupBy(fp.alias("f"))
                .agg(F.count("*").alias("n"), F.min(KEY).alias("lo"), F.max(KEY).alias("hi"),
                     F.countDistinct(KEY).alias("nk"), F.max("_metadata.file_size").alias("bytes"))
                .collect())
    n = sorted(r["n"] for r in per_file)
    rng = sorted(((r["lo"], r["hi"]) for r in per_file))
    overlapping, run_hi = 0, None
    for lo, hi in rng:
        if run_hi is not None and lo <= run_hi:
            overlapping += 1
        run_hi = hi if run_hi is None else max(run_hi, hi)
    total_keys = df.select(F.countDistinct(KEY)).collect()[0][0]
    w = Window.partitionBy("_f").orderBy("_ri")
    st = (df.select(fp.alias("_f"), F.col("_metadata.row_index").alias("_ri"), F.col(KEY))
          .withColumn("_prev", F.lag(KEY).over(w))
          .groupBy("_f")
          .agg(F.sum(F.when(F.col("_prev").isNull() | (F.col("_prev") != F.col(KEY)), 1)
                      .otherwise(0)).alias("trans"),
               F.countDistinct(KEY).alias("nk"), F.count("*").alias("n"))
          .agg(F.sum("trans").alias("trans"), F.sum("nk").alias("nk"), F.sum("n").alias("n"))
          .collect()[0])
    sortedness = 1.0 if st["n"] == st["nk"] else round((st["n"] - st["trans"]) / (st["n"] - st["nk"]), 3)
    return {
        "files": len(n), "rows": sum(n), "rows_min": n[0], "rows_avg": round(sum(n) / len(n)),
        "rows_max": n[-1], "rows_spread": round(n[-1] / max(n[0], 1), 2),
        "files_at_cap": sum(1 for x in n if x == MAX_RECORDS),
        "file_mb_min": round(min(r["bytes"] for r in per_file) / 2**20, 1),
        "file_mb_avg": round(sum(r["bytes"] for r in per_file) / len(per_file) / 2**20, 1),
        "file_mb_max": round(max(r["bytes"] for r in per_file) / 2**20, 1),
        "overlapping_files": overlapping, "overlap_pct": round(100 * overlapping / len(rng)),
        "keys_per_file_avg": round(sum(r["nk"] for r in per_file) / len(per_file)),
        "keys_total": total_keys, "sortedness": sortedness,
        "per_file": sorted([(r["lo"], r["hi"], r["nk"], r["n"], round(r["bytes"] / 2**20)) for r in per_file]),
    }


def history_and_detail(ref):
    h = spark.sql(f"DESCRIBE HISTORY {ref}").orderBy(F.col("version").desc()).limit(6).collect()
    d = spark.sql(f"DESCRIBE DETAIL {ref}").collect()[0].asDict()
    return {
        "history": [{"version": r["version"], "operation": r["operation"],
                     "parameters": dict(r["operationParameters"] or {}),
                     "metrics": dict(r["operationMetrics"] or {})} for r in h],
        "detail": {k: (list(v) if isinstance(v, (list, tuple)) else str(v)) for k, v in d.items()
                   if k in ("clusteringColumns", "minReaderVersion", "minWriterVersion", "tableFeatures",
                            "numFiles", "sizeInBytes", "properties")},
    }

def footers(loc):
    """Row groups and per-chunk encodings, read straight off the Volume with pyarrow.

    This is the whole reason the ac_* variants are path tables. Reports rows per row group, groups
    per file, and the share of column chunks carrying a dictionary page -- the two things a
    compaction could quietly change that a file listing cannot see.
    """
    import pyarrow.parquet as pq
    # ACTIVE files only, from the Delta log -- NOT a directory listing. A compaction tombstones the
    # files it replaces but leaves them on disk, so listing the directory would mix the before and
    # after and answer nothing.
    files = [r[0] for r in (spark.read.format("delta").load(loc)
                            .select(F.col("_metadata.file_path")).distinct().collect())]
    files = [p.replace("dbfs:", "").replace("file:", "") for p in files]
    groups, per_file, dict_chunks, total_chunks, dict_bytes, total_bytes = [], [], 0, 0, 0, 0
    for p in files:
        f = pq.ParquetFile(p)
        md = f.metadata
        per_file.append(md.num_row_groups)
        for g in range(md.num_row_groups):
            rg = md.row_group(g)
            groups.append(rg.num_rows)
            for c in range(rg.num_columns):
                col = rg.column(c)
                total_chunks += 1
                total_bytes += col.total_compressed_size
                if col.dictionary_page_offset is not None:
                    dict_chunks += 1
                    dict_bytes += col.total_compressed_size
    groups.sort()
    return {
        "files": len(files), "row_groups": len(groups),
        "groups_per_file_min": min(per_file), "groups_per_file_max": max(per_file),
        "rows_per_group_min": groups[0], "rows_per_group_med": groups[len(groups) // 2],
        "rows_per_group_max": groups[-1],
        "groups_in_1M_16M": sum(1 for g in groups if 1_000_000 <= g <= 16_000_000),
        "groups_under_1M": sum(1 for g in groups if g < 1_000_000),
        "dict_chunk_pct": round(100 * dict_chunks / total_chunks, 1),
        "dict_byte_pct": round(100 * dict_bytes / total_bytes, 1),
    }

# COMMAND ----------

src = (spark.read.parquet(f"{RAW}/{TABLE}")
       .na.drop(how="any").withColumn("cache_buster", F.lit(1).cast("int")))   # build_spark.customised

results = {"sf": SF, "rows_per_group": ROWS_PER_GROUP, "hadoop": HADOOP, "control": CONTROL, "variants": {}}
t0 = time.time()
results["control_measure"] = measure(spark.table(CONTROL))
print("control", json.dumps({k: v for k, v in results["control_measure"].items() if k != "per_file"}))

for v in VARIANTS:
    # PATH tables in the Volume, not managed tables, whenever the question needs FOOTERS. UC blocks
    # footer reads of a managed table's files, which is why this repo normally reads footers off the
    # Fabric mirror -- but mirroring is absurd overhead for a throwaway probe, and a Volume is
    # FUSE-mounted on the driver, so the parquet files are just files and pyarrow can open them.
    to_volume = v in FOOTER_VARIANTS
    if to_volume:
        loc = f"{PROBE_VOL}/{v}"
        ref = f"delta.`{loc}`"
        try:
            dbutils.fs.ls(loc)
            raise RuntimeError(f"{loc} exists; this probe never overwrites")
        except Exception as e:
            if "exists" in str(e):
                raise
    else:
        schema = f"tpcds_sf{SF}_probe_{v}" if v in UNCLUSTERED else f"tpcds_sf{SF}_cluster_{v}"
        fq = f"{CATALOG}.{schema}.{TABLE}"
        if spark.catalog.tableExists(fq):
            raise RuntimeError(f"{fq} exists; this probe never overwrites")
        ref = fq
    for k, want in RECIPE.items():                       # reset, then apply the one change
        spark.conf.set(k, want)
    spark.conf.unset("spark.databricks.delta.properties.defaults.targetFileSize")  # never leak across variants
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")          # recipe value; ac_on turns it back on
    for k, val in KNOBS[v].items():
        spark.conf.set(k, val)
    in_force = {k: spark.conf.get(k, None) for k in WATCH}
    if not to_volume:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
        spark.sql(f"ALTER SCHEMA {CATALOG}.{schema} DISABLE PREDICTIVE OPTIMIZATION")

    before = {e["id"] for e in executions()}
    t1 = time.time()
    if to_volume:
        src.limit(0).write.format("delta").mode("errorifexists").save(loc)
    else:
        src.limit(0).write.format("delta").saveAsTable(fq)                      # the arm, verbatim
    spark.sql(f"ALTER TABLE {ref} SET TBLPROPERTIES ('delta.checkpointPolicy' = 'classic')")
    for k, val in TBLPROPS.get(v, {}).items():
        spark.sql(f"ALTER TABLE {ref} SET TBLPROPERTIES ('{k}' = '{val}')")
    if v not in UNCLUSTERED:
        spark.sql(f"ALTER TABLE {ref} CLUSTER BY ({KEY})")
    t2 = time.time()
    if to_volume:
        src.write.format("delta").mode("append").save(loc)
    else:
        src.write.format("delta").mode("append").saveAsTable(fq)
    took = round(time.time() - t2, 1)
    new = [e for e in executions() if e["id"] not in before]
    plans = [{"id": e["id"], "desc": e["desc"], "plan": e["plan"][:80000]} for e in new
             if "WriteFiles" in e["plan"] or "Exchange" in e["plan"] or "Sort" in e["plan"]]
    r = {"target": ref, "knobs": KNOBS[v], "tblprops": TBLPROPS.get(v, {}), "in_force": in_force,
         "append_seconds": took, "setup_seconds": round(t2 - t1, 1), "plans": plans}
    r.update(history_and_detail(ref))
    r["measure"] = measure(spark.read.format("delta").load(loc) if to_volume else spark.table(fq))
    if to_volume:
        r["footers"] = footers(loc)
    results["variants"][v] = r
    print("=" * 100)
    print(v, took, "s", json.dumps({k: x for k, x in r["measure"].items() if k != "per_file"}))
    if to_volume:
        print("  FOOTERS", json.dumps(r["footers"]))
    for p in plans:
        print("-" * 100); print("EXECUTION", p["id"]); print(p["plan"][:20000])

results["total_seconds"] = round(time.time() - t0)
dbutils.notebook.exit(json.dumps(results))
