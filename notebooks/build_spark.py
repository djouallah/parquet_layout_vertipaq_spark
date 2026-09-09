# Databricks notebook source
# MAGIC %md
# MAGIC # One build, three arms: the config is identical, the ORDERING is the variable
# MAGIC
# MAGIC The question this repo exists to answer is what layout VertiPaq prefers and whether a
# MAGIC Databricks team can reach it from **configuration alone** -- a block pasted into a cluster
# MAGIC policy, with `df.write.saveAsTable()` unchanged. Answering it needs arms that differ in
# MAGIC exactly ONE thing. Three separate notebooks could not guarantee that: they drifted into
# MAGIC different row-group targets, different compression, different page settings, and the
# MAGIC "sorted vs unsorted" comparison was really measuring five config changes at once.
# MAGIC
# MAGIC So there is one notebook, one job, one `spark_conf`, and one parameter:
# MAGIC
# MAGIC | `ordering` | schema | what the write does |
# MAGIC |---|---|---|
# MAGIC | `none` | `tpcds_sf{N}_default` | `df.write.saveAsTable()`, and THE RECIPE: ask for nothing and this is what you get |
# MAGIC | `cluster` | `tpcds_sf{N}_cluster` | `CLUSTER BY (<date key>)` declared, then the same write |
# MAGIC
# MAGIC Everything else -- row-group size, dictionary settings, page limits, block size, compression,
# MAGIC protocol -- is byte-for-byte the same across the three. Any difference measured between them
# MAGIC is the ordering, or it is nothing.
# MAGIC
# MAGIC Two unordered-arm-only GEOMETRY variables make further arms, and the arm name carries the
# MAGIC number so no arm can be a silent variant of another. `rows_per_file` cuts the FILE and, since
# MAGIC the recipe sets no row-group cap, IS the segment: `rows_per_file=8000000` ->
# MAGIC `tpcds_sf{N}_defaultf8`. `rows_per_group` cuts the ROW GROUP directly and is 0 (unset) in the
# MAGIC recipe; it needs parquet-java 1.16 and is silently ignored below that, which is why it is not
# MAGIC the lever. Nothing else about the write changes: same `saveAsTable`, same `spark_conf` line
# MAGIC for line.
# MAGIC
# MAGIC **One key, the date.** It is the only key that eliminates row groups (23 of the paper's 24
# MAGIC captured queries filter `date_dim[d_year]`, and the date surrogate key is ordinal, so a
# MAGIC filtered year is a contiguous range). A second key matters too: with two or more keys liquid
# MAGIC clustering switches to a space-filling curve, and the arm would stop being one key's ordering.
# MAGIC
# MAGIC **There is no `orderBy` arm, and there cannot be a useful one.** Under this recipe Databricks
# MAGIC plans Optimized Writes as a repartition above the query, and Spark's `EliminateSorts` deletes a
# MAGIC global sort under a repartition, so a `df.orderBy(key)` never reaches the writer. `cluster` is
# MAGIC how an ordering is actually expressed here. See LEARNING.md, *How an ordering reaches a Delta
# MAGIC file*.
# MAGIC
# MAGIC Never overwrites: table names are the paper's, the arm lives in the schema suffix, and a
# MAGIC table that already exists stops the run.

# COMMAND ----------

import datetime as dt
import json
import time

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "databricks_ne")
dbutils.widgets.text("raw_schema", "tpcds_raw")
dbutils.widgets.text("scale_factor", "10")
dbutils.widgets.dropdown("ordering", "none", ["none", "cluster", "partition"])
dbutils.widgets.text("arm_schema", "")              # empty -> tpcds_sf{N}_{arm}
dbutils.widgets.text("order_keys", "")              # JSON {table: col}; empty -> DEFAULT_ORDER_KEYS
dbutils.widgets.text("rows_per_group", "0")         # rows per ROW GROUP; 0 = the recipe (key not set)
dbutils.widgets.text("rows_per_file", "6000000")    # rows per FILE, and THE geometry lever
dbutils.widgets.text("compression", "")             # "" = DBR's zstd; "snappy" -> a different arm

catalog    = dbutils.widgets.get("catalog")
raw_schema = dbutils.widgets.get("raw_schema")
sf         = int(dbutils.widgets.get("scale_factor"))
ORDERING   = dbutils.widgets.get("ordering")

# The geometry numbers. Two are widgets now, and that needs saying, because the rule here used to be
# "hardcoded, never parameters: a geometry value that can vary between runs is one that WILL, and
# then two arms are no longer comparable". The guarantee is kept a different way: the value goes
# into the ARM NAME, so a build at another cap or target is a DIFFERENT arm by construction and two
# builds of one arm can never differ silently. `default` means the recipe: a 6M file cap, no target.
#
# ONE number, and it is the OLD one. `parquet.block.row.count.limit` left the recipe 2026-09-09:
# The arm that used it (12M files of two 6M groups) measured identical to one 6M group per file,
# so the newer key's only capability -- several row groups inside one file -- buys nothing, and it
# costs portability. It needs parquet-java 1.16 (DBR 19, Fabric Runtime 2.0) and every older runtime
# ACCEPTS IT AND IGNORES IT, which reads as "row groups did not help". maxRecordsPerFile has been in
# Spark since 2.2. So the file boundary IS the row-group boundary again, deliberately: one file =
# one segment, denominated in rows. RECIPE_ROWS_PER_GROUP is 0 = the key is not set at all.
RECIPE_ROWS_PER_GROUP, RECIPE_ROWS_PER_FILE = 0, 6_000_000
ROWS_PER_GROUP = int(dbutils.widgets.get("rows_per_group"))   # parquet.block.row.count.limit
ROWS_PER_FILE  = int(dbutils.widgets.get("rows_per_file"))    # spark.sql.files.maxRecordsPerFile
# DBR 19 stamps delta.parquet.compression.codec = zstd itself, so "" IS the recipe. snappy exists
# to reproduce the delta_rs arm, which writes SNAPPY and cannot be told otherwise -- and whose
# bytes are therefore not comparable to a zstd arm's. A codec change is a DIFFERENT ARM, named.
COMPRESSION = dbutils.widgets.get("compression").strip().lower()
if COMPRESSION not in ("", "snappy", "zstd", "gzip", "lz4", "uncompressed"):
    raise ValueError(f"compression={COMPRESSION!r} is not a parquet codec Delta accepts")
# The group cap is OPTIONAL now -- 0 means the recipe, which does not set the key. Where it IS
# asked for it still has to divide the file cap, or a file boundary lands mid-group.
if ROWS_PER_GROUP and ROWS_PER_FILE % ROWS_PER_GROUP:
    raise ValueError(f"rows_per_group={ROWS_PER_GROUP:,} does not divide the {ROWS_PER_FILE:,}-row "
                     "file cap: a file boundary would land mid-group and every file would carry a ragged one")
if ROWS_PER_GROUP and (ROWS_PER_GROUP % 1_000_000
                       or not (1_000_000 <= ROWS_PER_GROUP <= 16_000_000)):
    raise ValueError(f"rows_per_group={ROWS_PER_GROUP:,}: whole millions only, inside Direct Lake's 1M-16M window")
# The file cap is the segment on the recipe, so it carries the window check the group cap used to.
if ROWS_PER_FILE % 1_000_000 or not (1_000_000 <= ROWS_PER_FILE <= 16_000_000):
    raise ValueError(f"rows_per_file={ROWS_PER_FILE:,}: whole millions only, inside Direct Lake's "
                     "1M-16M segment window -- with no group cap the file IS the row group")

# The arm name is derived, never passed separately: two parameters that must agree are two
# parameters that can disagree, and the schema suffix is what every downstream label keys off
# (benchmark/results.py, run_benchmark, deploy_paper_model). Base token from the ordering; a
# non-recipe GROUP cap appends "<n>m" and a non-recipe FILE cap appends "f<n>": none + an 8M file
# cap -> `defaultf8`. No '_' inside a token -- the results side reads the arm as the LAST '_'
# segment of the model name.
#
# `default` is the base token for `ordering=none`, so THE RECIPE NAMES ITSELF: ask the job for
# nothing and it writes tpcds_sf{N}_default. It used to be called `nosort`, with a separate
# `arm_name` widget existing solely to let the published config claim the name `default`. Renamed
# 2026-09-09, and the widget and its two guards deleted with it -- the recipe is the default, so it
# does not need a flag to say so.
BASE_ARM = {"none": "default", "cluster": "cluster", "partition": "partition"}[ORDERING]
ARM = (BASE_ARM
       + (f"{ROWS_PER_GROUP // 1_000_000}m" if ROWS_PER_GROUP != RECIPE_ROWS_PER_GROUP else "")
       + (f"f{ROWS_PER_FILE // 1_000_000}" if ROWS_PER_FILE != RECIPE_ROWS_PER_FILE else "")
       + {"": "", "snappy": "sn"}.get(COMPRESSION, COMPRESSION))
if ((ROWS_PER_GROUP != RECIPE_ROWS_PER_GROUP or ROWS_PER_FILE != RECIPE_ROWS_PER_FILE)
        and ORDERING != "none"):
    # THE ROW CAPS are what is restricted here, and only them. On a clustered write both are inert
    # (the clustering exchange sizes in bytes on its own); on a
    # partitioned write they sit far above a date's rows. A `clusterf4` would be a name claiming a
    # geometry the write cannot deliver. COMPRESSION is not restricted: it is a table property, it
    # reaches every write path, and a clustered arm in snappy is a perfectly buildable thing.
    raise ValueError(f"rows_per_group / rows_per_file are unordered-arm-only -- that is the only "
                     f"write the row caps reach: ordering={ORDERING} would build '{ARM}'")

arm_schema = dbutils.widgets.get("arm_schema").strip() or f"tpcds_sf{sf}_{ARM}"

RAW = f"/Volumes/{catalog}/{raw_schema}/landing/sf{sf}"
FQ  = lambda t: f"{catalog}.{arm_schema}.{t}"

FACTS = ["store_sales", "catalog_sales"]
DIMS  = ["catalog_page", "customer_address", "customer_demographics", "date_dim",
         "item", "promotion", "ship_mode", "store"]

# One key per fact, the date. Dimensions are one file and one row group each -- there is nothing to
# eliminate and nothing to order.
DEFAULT_ORDER_KEYS = {"store_sales": "ss_sold_date_sk", "catalog_sales": "cs_sold_date_sk"}
# Table 4.6.1's second key -- the paper's ZORDER column -- is NOT reproduced here, and that is a
# measured decision. `OPTIMIZE ... ZORDER BY` committed nothing on Databricks with one file per
# partition (cause unconfirmed), and the sort that stood in for it was measured never to reach the
# parquet. Why the sort was lost was never pinned down: the session switch that disabled optimized
# writes was not checked against the executed plan, and the docs say that switch should have won.
# Both are in LEARNING.md, *The within-partition sort never reached the files*. The arm is the
# PARTITION and nothing else.
ORDER_KEYS = dict(DEFAULT_ORDER_KEYS)
if dbutils.widgets.get("order_keys").strip():
    ORDER_KEYS.update(json.loads(dbutils.widgets.get("order_keys")))

# ROWS_PER_GROUP and ROWS_PER_FILE are defined with the arm name above -- the numbers ARE the arm.
# Both are asserted against the cluster's spark_conf below.
#
# NO byte target. A clustered write is sized in bytes by the clustering exchange itself (it replaces
# Optimized Writes; both row caps sit below its cut), and that unaided geometry -- 2.05M rows/file
# on store_sales, 1.11M on catalog_sales at SF100 -- is the arm that reached V-Order.
# `delta.targetFileSize` = 128 MB on the facts lifted catalog_sales to 2.23M rows and 96.8 %
# dictionary bytes and changed nothing else that was measured (`clustersn`, 2026-09-09), so it is
# not in the recipe. It is an option for a wider fact, as a TABLE property before the append; the
# session form overrides the row caps on every write (SF10, measured) and a coarse value collapses
# clustering to one partition (512 MiB on 900 MB, measured).

# The paper's post-customisation row counts (Table 4.3.1) -- the reproduction target.
PAPER_ROWS = {
    10:   {"store_sales": 26_206_837,    "catalog_sales": 14_257_451,    "catalog_page": 12_000, "customer_address": 250_000,   "customer_demographics": 1_920_800, "date_dim": 2_191, "item": 102_000, "promotion": 500,   "ship_mode": 20, "store": 102},
    100:  {"store_sales": 262_082_396,   "catalog_sales": 142_557_716,   "catalog_page": 20_400, "customer_address": 1_000_000, "customer_demographics": 1_920_800, "date_dim": 2_191, "item": 204_000, "promotion": 1_000, "ship_mode": 20, "store": 402},
    1000: {"store_sales": 2_620_785_279, "catalog_sales": 1_425_579_810, "catalog_page": 30_000, "customer_address": 6_000_000, "customer_demographics": 1_920_800, "date_dim": 2_191, "item": 300_000, "promotion": 1_500, "ship_mode": 20, "store": 1_002},
}

# TPC-DS sales span 1998-01-01..2003-12-31 (2,191 days); the paper reports the same 2,191 rows for
# 2021-01-01..2026-12-31. d_date_sk_1 is d_date_sk shifted back by the days between the two starts.
DATE_SHIFT_DAYS = (dt.date(2021, 1, 1) - dt.date(1998, 1, 1)).days        # 8,401
DATE_LO, DATE_HI = "2021-01-01", "2026-12-31"

print(f"catalog={catalog} raw={RAW}")
print(f"ordering={ORDERING}  arm={ARM}  target={catalog}.{arm_schema}")
print(f"rows_per_file={ROWS_PER_FILE:,}  "
      + (f"rows_per_group={ROWS_PER_GROUP:,} -> {ROWS_PER_FILE // ROWS_PER_GROUP} row group(s) per "
         "full file  " if ROWS_PER_GROUP else
         "row-group cap NOT set -> the file close ends the group, 1 per file  ")
      + f"compression={COMPRESSION or 'zstd (DBR default)'}")
if ORDERING != "none":
    for t, k in ORDER_KEYS.items():
        verb = {"cluster": "CLUSTER BY", "partition": "PARTITIONED BY"}[ORDERING]
        print(f"  {t}: {verb} ({k})")

# COMMAND ----------

# MAGIC %md ## Is the config in force? (checked, not set)

# COMMAND ----------

# The notebook sets NO layout knob. If the cluster does not carry the recipe the run stops here
# rather than writing a layout that is not the one being shared -- and, more importantly for the
# comparison, rather than letting one arm be built under different settings from another.


REQUIRED = {
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    # Rows per FILE, and on the recipe this is THE geometry lever: with no row-group cap the file
    # close ends the row group, so one file = one row group = one VertiPaq segment.
    "spark.sql.files.maxRecordsPerFile": str(ROWS_PER_FILE),
    # autoCompact.enabled is NOT checked: dropped from the recipe 2026-09-07. The table-property
    # twin below is the auto-compaction guard.
    # enableDeletionVectors is NOT checked: the recipe no longer states it, and DBR 19's default
    # (on) is what is wanted. See the comment in databricks.yml.
    "spark.databricks.delta.properties.defaults.checkpointPolicy": "classic",
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
    "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "false",
}
# What the parquet-mr writer reads -- `spark.hadoop.*` on the cluster lands here.
REQUIRED_HADOOP = {
    "parquet.block.size": "2147483648",
    "parquet.enable.dictionary": "true",
    "parquet.dictionary.page.size": "67108864",
    "parquet.page.size": "67108864",
    "parquet.page.row.count.limit": "16000000",
}
# The row-group cap is NOT in the recipe -- see the note on RECIPE_ROWS_PER_GROUP. An arm that
# asks for one pins the cluster to that value; the recipe instead requires the key to be ABSENT, so
# a cluster policy that sets it cannot quietly turn the recipe into a different geometry.
if ROWS_PER_GROUP:
    REQUIRED_HADOOP["parquet.block.row.count.limit"] = str(ROWS_PER_GROUP)

hconf = spark.sparkContext._jsc.hadoopConfiguration()

problems = []
for k, want in REQUIRED.items():
    got = spark.conf.get(k, None)
    print(f"  {k:<72} {got}")
    if str(got).lower() != want:
        problems.append(f"{k}={got!r}, want {want!r}")
for k, want in REQUIRED_HADOOP.items():
    got = hconf.get(k)
    print(f"  spark.hadoop.{k:<59} {got}")
    if str(got) != want:
        problems.append(f"spark.hadoop.{k}={got!r}, want {want!r}")

if not ROWS_PER_GROUP:
    _rgc = hconf.get("parquet.block.row.count.limit")
    print(f"  spark.hadoop.{'parquet.block.row.count.limit':<59} {_rgc}  (recipe: unset)")
    # Integer.MAX_VALUE is parquet-mr's own default and means the same as unset.
    if _rgc not in (None, "", "2147483647"):
        problems.append(f"parquet.block.row.count.limit={_rgc!r} is set, but this arm is built "
                        f"without a row-group cap -- the file cap is meant to be the only boundary")

BIN = spark.conf.get("spark.databricks.delta.optimizeWrite.binSize", None)
print(f"  {'spark.databricks.delta.optimizeWrite.binSize':<72} {BIN}  (MiB of shuffle per TASK)")
if not BIN:
    problems.append("spark.databricks.delta.optimizeWrite.binSize is not set")

# Where an arm asks for BOTH caps they have to be a whole-number multiple of each other, or a file
# boundary lands mid-group and every file carries a ragged one. On the recipe there is one cap and
# the answer is 1 by construction: the file close ends the group.
mrpf = int(REQUIRED["spark.sql.files.maxRecordsPerFile"])
rg   = int(REQUIRED_HADOOP.get("parquet.block.row.count.limit", 0))
if rg and mrpf % rg:
    problems.append(f"maxRecordsPerFile={mrpf:,} is not a multiple of the row-group cap {rg:,}")
GROUPS_PER_FILE = (mrpf // rg) if rg else 1

photon = spark.conf.get("spark.databricks.photon.enabled", "false")
print(f"  photon {photon}  (must be false: parquet-mr is the writer that honours the parquet.* keys)")
if str(photon).lower() == "true":
    problems.append("Photon is on")

if problems:
    raise RuntimeError("cluster config is not the recipe (databricks.yml, job tpcds_build_spark):\n  "
                       + "\n  ".join(problems))
print("config OK")

# COMMAND ----------

# MAGIC %md ## Schema, customisations (paper section 4.5)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{arm_schema}")
# A background OPTIMIZE would replace this geometry with its own -- and on a clustered table it is a
# full re-clustering, so the layout measured would not be the layout the benchmark reads.
try:
    spark.sql(f"ALTER SCHEMA {catalog}.{arm_schema} DISABLE PREDICTIVE OPTIMIZATION")
    print("predictive optimization disabled on", arm_schema)
except Exception as e:                                   # noqa: BLE001
    print(f"WARNING could not disable predictive optimization on {arm_schema}: {e}")


def customised(t):
    df = spark.read.parquet(f"{RAW}/{t}")
    if t in FACTS:
        # "All nulls in both Fact Tables were removed", then the cache_buster the load test randomises.
        df = df.na.drop(how="any").withColumn("cache_buster", F.lit(1).cast("int"))
    elif t == "date_dim":
        df = (df.withColumn("d_date_sk_1", (F.col("d_date_sk") - F.lit(DATE_SHIFT_DAYS)).cast("int"))
                .where(F.col("d_date").between(F.lit(DATE_LO).cast("date"), F.lit(DATE_HI).cast("date"))))
    return df

# COMMAND ----------

# MAGIC %md ## Write
# MAGIC
# MAGIC The same `df.write.format("delta").saveAsTable()` in all three arms. `cluster` declares the
# MAGIC key on the table first and appends; `partition` declares the partition. No repartition, no
# MAGIC coalesce, no count, no geometry arithmetic anywhere.

# COMMAND ----------

def write_table(t):
    fq = FQ(t)
    if spark.catalog.tableExists(fq):
        raise RuntimeError(f"{fq} already exists -- this build never overwrites; use another arm_schema")
    key = ORDER_KEYS.get(t)                       # None on every dimension
    df = customised(t)
    t0 = time.time()

    if ORDERING == "cluster" and key:
        # Declare the key on an empty table, then append: the rows are placed by clustering on
        # write. The checkpoint policy is pinned BEFORE the keys exist because liquid clustering
        # turns v2Checkpoint on by default from DBR 14.3 LTS and Direct Lake cannot read one at all.
        # No byte target: the exchange sizes its own files (see the note above PAPER_ROWS).
        df.limit(0).write.format("delta").saveAsTable(fq)
        spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ('delta.checkpointPolicy' = 'classic')")
        if COMPRESSION:
            # Before the append, like every other property here: DBR stamps zstd on the table at
            # creation and the codec a data file is written with is the one on the table at the time.
            spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES "
                      f"('delta.parquet.compression.codec' = '{COMPRESSION}')")
        spark.sql(f"ALTER TABLE {fq} CLUSTER BY ({key})")
        df.write.format("delta").mode("append").saveAsTable(fq)
    elif ORDERING == "partition" and key:
        # The Fabric arm's GEOMETRY without V-Order -- which Databricks has none of -- so
        # `partitionBy` on the date key IS the whole arm. Table 4.6.1's partition column, one file
        # per date. This arm DELIBERATELY ABANDONS the recipe's geometry: a date partition is ~120k
        # rows at SF100 and ~1.2M at SF1000, so both row caps sit inert far above it and the
        # partition decides everything. That is the point -- it reproduces the file count the Fabric
        # arm actually has (1,823 per fact).
        #
        # NO within-partition sort, removed 2026-09-08 after measuring that it never reached the
        # parquet (LEARNING.md, *The within-partition sort never reached the files*). It cost a
        # shuffle and a sort on every build and bought nothing. Nothing here reproduces the paper's
        # ZORDER, so the difference against the Fabric arm is V-Order AND the in-file row order --
        # two things, not one. Say so wherever the pair is quoted.
        df.write.format("delta").partitionBy(key).saveAsTable(fq)
    elif COMPRESSION:
        # The same plain write with the codec stated first -- a table property has to exist
        # before the rows do. This is the path a codec arm's DIMENSIONS take, so one schema is one
        # codec throughout.
        df.limit(0).write.format("delta").saveAsTable(fq)
        spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES "
                  f"('delta.parquet.compression.codec' = '{COMPRESSION}')")
        df.write.format("delta").mode("append").saveAsTable(fq)
    else:
        df.write.format("delta").saveAsTable(fq)          # default mode: errorifexists

    took = time.time() - t0
    d = spark.sql(f"DESCRIBE DETAIL {fq}").collect()[0].asDict()
    n = spark.table(fq).count()
    # rows_per_FILE, not per group: UC blocks path reads of managed-table files, so nothing here can
    # see a row group. A file now holds several, and calling this "rows_per_group" would be a lie by
    # a factor of GROUPS_PER_FILE. `layout_stats` over the mirror reports the real group sizes.
    props = {"layout.arm": ARM, "layout.ordering": ORDERING, "layout.order_key": key or "",
             "layout.rows_per_file": str(round(n / max(d["numFiles"], 1))),
             "layout.rows_per_file_target": str(ROWS_PER_FILE),
             "layout.rows_per_group_target": str(ROWS_PER_GROUP),
             "layout.groups_per_file_target": str(GROUPS_PER_FILE),
             "layout.optimize_write_bin_mib": str(BIN)}
    kv = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
    spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ({kv})")
    print(f"  {t}: {n:,} rows in {d['numFiles']} file(s), ~{props['layout.rows_per_file']} rows/file, "
          f"{ORDERING}({key or '-'}), {took:,.0f}s")


for t in DIMS:
    write_table(t)
for t in FACTS:
    write_table(t)

# COMMAND ----------

# v2 checkpoints are the one thing Direct Lake genuinely cannot read, and liquid clustering enables
# them by default. Checked before anything else looks at the tables, because a table that reaches
# the mirror with one is a benchmark run spent on a model that will not load.
if ORDERING == "cluster":
    for t in FACTS:
        fq = FQ(t)
        feats = spark.sql(f"DESCRIBE DETAIL {fq}").collect()[0].asDict().get("tableFeatures") or []
        if "v2Checkpoint" in feats:
            print(f"  {t}: v2Checkpoint present, dropping")
            spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ('delta.checkpointPolicy' = 'classic')")
            try:
                spark.sql(f"ALTER TABLE {fq} DROP FEATURE v2Checkpoint")
            except Exception as e:                       # noqa: BLE001
                print(f"  {t}: DROP FEATURE v2Checkpoint failed: {str(e)[:200]}")

# COMMAND ----------

# The paper ran this on every table. Metadata only -- it does not touch the files.
for t in DIMS + FACTS:
    spark.sql(f"ANALYZE TABLE {FQ(t)} COMPUTE STATISTICS FOR ALL COLUMNS")

# COMMAND ----------

# MAGIC %md ## Report
# MAGIC
# MAGIC Row counts against the paper's Table 4.3.1, file geometry and the Delta protocol. Row groups
# MAGIC are NOT inspected here (Unity Catalog blocks path reads of managed-table files) and neither is
# MAGIC the ordering: `layout_stats` over the mirror reports rows per row group, the dictionary state
# MAGIC per column, and the overlap count on the key -- which is the number that says whether the
# MAGIC ordering reached the files at all.

# COMMAND ----------

paper = PAPER_ROWS.get(sf, {})
rows = []
for t in DIMS + FACTS:
    d = spark.sql(f"DESCRIBE DETAIL {FQ(t)}").collect()[0].asDict()
    hist = [r.operation for r in spark.sql(f"DESCRIBE HISTORY {FQ(t)}").collect()]
    n = spark.table(FQ(t)).count()
    p = paper.get(t)
    props = d.get("properties") or {}
    feats = d.get("tableFeatures") or []
    rows.append({
        "table": t, "arm": ARM, "ordering": ORDERING, "rows": n, "paper_rows": p,
        "matches_paper": None if p is None else n == p,
        "num_files": d["numFiles"], "size_mb": round(d["sizeInBytes"] / 1024 / 1024, 1),
        "avg_file_mb": round(d["sizeInBytes"] / max(d["numFiles"], 1) / 1024 / 1024, 1),
        "rows_per_file": props.get("layout.rows_per_file"),
        "rows_per_file_target": props.get("layout.rows_per_file_target"),
        "target_file_bytes": props.get("layout.target_file_bytes"),
        "delta_target_file_size": props.get("delta.targetFileSize"),
        "rows_per_group_target": props.get("layout.rows_per_group_target"),
        "groups_per_file_target": props.get("layout.groups_per_file_target"),
        "order_key": props.get("layout.order_key"),
        "cluster_columns": ",".join(d.get("clusteringColumns") or []),
        "partition_columns": ",".join(d.get("partitionColumns") or []),
        "bin_mib": props.get("layout.optimize_write_bin_mib"),
        "min_reader_version": d.get("minReaderVersion"),
        "min_writer_version": d.get("minWriterVersion"),
        "table_features": ",".join(sorted(feats)),
        "checkpoint_policy": props.get("delta.checkpointPolicy"),
        "v2_checkpoint": "v2Checkpoint" in feats,
        "compression": props.get("delta.parquet.compression.codec"),
        "history_ops": ",".join(hist),
        "optimize_ran": any("OPTIMIZE" in op.upper() for op in hist),
    })

import pandas as pd
report = pd.DataFrame(rows)
display(report)

# v2 checkpoints: Direct Lake cannot read them at all, so this is an assert, not a warning.
broken = [r["table"] for r in rows if r["v2_checkpoint"] or r["checkpoint_policy"] != "classic"]
assert not broken, (f"v2 checkpoints on {broken}: Direct Lake cannot read these. "
                    f"DROP FEATURE v2Checkpoint before mirroring.")
# Reader version is REPORTED, not asserted. DBR 19 defaults deletion vectors on and the recipe now
# leaves them on, so every table lands at minReaderVersion 3 -- deliberately: without DVs a
# DELETE/UPDATE/MERGE rewrites whole files and VertiPaq re-transcodes every column in them.
# `deletionVectors` in tableFeatures means the FEATURE is enabled, not that a deletion vector exists.
print("reader versions:", {r["table"]: r["min_reader_version"] for r in rows})
# No arm runs OPTIMIZE, the `partition` arm included -- an OPTIMIZE commits nothing on a table that
# already holds one file per partition (LEARNING.md), and no arm here orders rows inside a file. So an
# OPTIMIZE in the history means a background job replaced the geometry, which voids the comparison.
bad = [r["table"] for r in rows if r["optimize_ran"]]
assert not bad, f"layout compromised on {bad}: OPTIMIZE ran and replaced the geometry"
if ORDERING == "cluster":
    unclustered = [t for t in FACTS if not report.loc[report.table == t, "cluster_columns"].iloc[0]]
    assert not unclustered, f"clustering keys missing on {unclustered} -- this is not the cluster arm"

# Geometry is a REQUIREMENT, not a report: an arm that lands somewhere else is not comparable to the
# others, which is the whole point of building them from one notebook. This can only check rows per
# FILE -- UC hides the footers -- so it is a proxy; `layout_stats` over the mirror is what actually
# confirms the row groups.
#
# What is checked depends on which knob is the outermost cut on that arm. On an unordered arm it
# is maxRecordsPerFile, so a rows-per-file band around the cap is the right test. On `cluster` it is the
# byte target, which lands rows-per-file wherever the table's width puts it -- ~3.7M at 36 B/row,
# ~2.3M at 59 -- so asserting one row number across both would be asserting a coincidence; that arm
# is checked against the 1M-16M VertiPaq segment window and a file count instead.
#
# ONE arm is exempt, and not as a courtesy: `partition` exists to reproduce the Fabric arm's
# geometry, where one file per date is ~120k rows at SF100 and ~1.2M at SF1000, so every cap sits
# inert far above it and it would fail this check by design. Its geometry is reported instead.
if ORDERING == "partition":
    for t in FACTS:
        r = report.loc[report.table == t].iloc[0]
        print(f"  {t}: {int(r['num_files']):,} files, {int(r['rows_per_file']):,} rows/file "
              f"(partitioned by {r['order_key']}, no ordering inside a file) "
              f"-- the row caps did not bind, by design")

# `cluster` gets the pair that catches clustering's one SILENT failure: a write that collapses to
# a single range partition comes out unplaced while `DESCRIBE DETAIL` still reports
# `clusteringColumns` and the history still says "late-stage clustering triggered" (measured, SF10,
# with a 512 MiB byte target on a 932 MB fact). The keys check above passes in that state -- exactly
# why it is not enough on its own. A file count near 1 does not pass, and neither does a row count
# outside the segment window. Any byte target on the table is reported, not required.
elif ORDERING == "cluster":
    off = []
    for t in FACTS:
        r = report.loc[report.table == t].iloc[0]
        files, per_file = int(r["num_files"]), int(r["rows_per_file"])
        carried = r["delta_target_file_size"]
        print(f"  {t}: {files:,} files, {per_file:,} rows/file, {r['avg_file_mb']} MB avg "
              f"(clustered by {r['order_key']}"
              + (f", delta.targetFileSize {int(carried) / 2**20:,.0f} MB)" if carried
                 else ", delta.targetFileSize NOT on the table)"))
        if files < 4:
            off.append(f"{t}: {files} file(s) -- a clustered write this coarse places nothing")
        if not (1_000_000 <= per_file <= 16_000_000):
            off.append(f"{t}: {per_file:,} rows/file, outside the 1M..16M segment window")
    assert not off, ("the clustered geometry is not usable: " + "; ".join(off)
                     + ". A clustered write this small collapses to one range partition and places "
                       "nothing; the table does not belong at this scale factor.")
else:
    off = []
    for t in FACTS:
        got = int(report.loc[report.table == t, "rows_per_file"].iloc[0])
        lo, hi = int(ROWS_PER_FILE * 0.5), int(ROWS_PER_FILE * 1.5)
        if not (lo <= got <= hi):
            off.append(f"{t}: {got:,} rows/file, wanted {lo:,}..{hi:,}")
    assert not off, ("file geometry missed the target, so this arm is not comparable to the others: "
                     + "; ".join(off) + ". Check that maxRecordsPerFile is the binding lever and "
                     "that no byte knob closed a file first.")

mism = [r["table"] for r in rows if r["matches_paper"] is False]
print("row counts vs paper:", "all match" if not mism else f"DIFFER on {mism}")

(spark.createDataFrame(report.astype({"paper_rows": "float", "rows_per_file": "string",
                                      "rows_per_file_target": "string", "bin_mib": "string",
                                      "rows_per_group_target": "string",
                                      "groups_per_file_target": "string"}))
      .withColumn("scale_factor", F.lit(sf)).withColumn("schema", F.lit(arm_schema))
      .withColumn("built_at", F.current_timestamp())
      .write.format("delta").mode("append").option("mergeSchema", "true")
      .saveAsTable(f"{catalog}.{raw_schema}.layout_build_report"))
print(f"report appended to {catalog}.{raw_schema}.layout_build_report")

dbutils.notebook.exit(json.dumps({"sf": sf, "schema": arm_schema, "arm": ARM, "ordering": ORDERING,
                                  "order_keys": ORDER_KEYS, "rows_per_group": ROWS_PER_GROUP,
                                  "rows_per_file": ROWS_PER_FILE, "groups_per_file": GROUPS_PER_FILE,
                                  "bin_mib": BIN, "report": rows}, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next (Fabric side)
# MAGIC 1. Add this schema to the mirrored Azure Databricks catalog item; wait for the 10 tables.
# MAGIC 2. Run `layout_stats` at this `sf`: rows per row group against the arm's cap, dictionary state
# MAGIC    per column, and the overlap count on the key -- 0 overlaps means the ordering reached the
# MAGIC    files, a high count means it did not and the arm measures nothing.
# MAGIC 3. `python fabric/deploy_paper_model.py --workspace <ws> --arm <arm> --sf <sf>`, then the
# MAGIC    `run_benchmark` pipeline with `arms=<arm>`.
