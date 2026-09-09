# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DS raw data: DuckDB `dsdgen` -> local disk -> Unity Catalog Volume
# MAGIC
# MAGIC The paper generated its data with LakeBench v1.0.1, which is a thin wrapper over DuckDB's
# MAGIC `tpcds` extension (`CALL dsdgen(sf=N)` in memory, then one `COPY ... TO parquet` per table).
# MAGIC This notebook calls DuckDB directly: same generator, same rows, no wrapper.
# MAGIC
# MAGIC Only the 10 tables the paper uses are kept (2 facts + 8 dims); the other 14 are dropped from
# MAGIC memory as soon as `dsdgen` returns. DuckDB cannot write to `abfss://`, so the Parquet lands on
# MAGIC the driver's local disk first and is copied into the Volume with `dbutils.fs.cp`.
# MAGIC
# MAGIC `dsdgen` has no chunking: SF100 materialises ~100 GB of tables in memory, hence the single
# MAGIC 256 GB node in `databricks.yml`. The row-group size written here is irrelevant to the
# MAGIC benchmark -- the layout is applied by `build_layout.py` -- 1M rows just keeps the Spark read cheap.

# COMMAND ----------

import json
import os
import shutil
import time

dbutils.widgets.text("catalog", "databricks_ne")
dbutils.widgets.text("raw_schema", "tpcds_raw")
dbutils.widgets.text("scale_factor", "10")
dbutils.widgets.text("force", "false")           # "true" regenerates even if the SF already landed

catalog    = dbutils.widgets.get("catalog")
raw_schema = dbutils.widgets.get("raw_schema")
sf         = int(dbutils.widgets.get("scale_factor"))
force      = dbutils.widgets.get("force").strip().lower() == "true"

# The paper's subset (section 4.3): 2 facts + 8 dims.
TABLES = [
    "store_sales", "catalog_sales",
    "catalog_page", "customer_address", "customer_demographics", "date_dim",
    "item", "promotion", "ship_mode", "store",
]

# TPC-DS row counts by scale factor, BEFORE customisation. Dims are exact per the spec (and match
# the paper's Table 4.3.1 where it lists them); the facts are what dsdgen produces and are checked
# to within 1 % so a wrong SF is caught but a build-to-build wobble is not fatal.
EXPECTED = {
    1:    {"store_sales": 2_880_404,     "catalog_sales": 1_441_548,     "catalog_page": 11_718, "customer_address": 50_000,    "customer_demographics": 1_920_800, "date_dim": 73_049, "item": 18_000,  "promotion": 300,   "ship_mode": 20, "store": 12},
    10:   {"store_sales": 28_800_991,    "catalog_sales": 14_401_261,    "catalog_page": 12_000, "customer_address": 250_000,   "customer_demographics": 1_920_800, "date_dim": 73_049, "item": 102_000, "promotion": 500,   "ship_mode": 20, "store": 102},
    100:  {"store_sales": 287_997_024,   "catalog_sales": 143_997_065,   "catalog_page": 20_400, "customer_address": 1_000_000, "customer_demographics": 1_920_800, "date_dim": 73_049, "item": 204_000, "promotion": 1_000, "ship_mode": 20, "store": 402},
    1000: {"store_sales": 2_879_987_999, "catalog_sales": 1_439_980_416, "catalog_page": 30_000, "customer_address": 6_000_000, "customer_demographics": 1_920_800, "date_dim": 73_049, "item": 300_000, "promotion": 1_500, "ship_mode": 20, "store": 1_002},
}
FACTS = {"store_sales", "catalog_sales"}

LOCAL  = f"/local_disk0/tpcds_sf{sf}"
TMP    = "/local_disk0/duckdb_tmp"
VOLUME = f"/Volumes/{catalog}/{raw_schema}/landing"
DEST   = f"{VOLUME}/sf{sf}"
MARKER = f"{DEST}/_GENERATED.json"

print(f"catalog={catalog} raw_schema={raw_schema} sf={sf} force={force}")
print(f"local={LOCAL}  volume={DEST}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{raw_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{raw_schema}.landing")

# Resumable at the SF level: the marker is written last, after every table landed and was counted.
if os.path.exists(MARKER) and not force:
    print(f"sf{sf} already generated -- {MARKER}:")
    print(open(MARKER).read())
    dbutils.notebook.exit(json.dumps({"status": "skipped", "sf": sf, "volume": DEST}))

# COMMAND ----------

import duckdb

os.makedirs(TMP, exist_ok=True)
shutil.rmtree(LOCAL, ignore_errors=True)
os.makedirs(LOCAL, exist_ok=True)

con = duckdb.connect()                       # in-memory database, spills to TMP
con.execute(f"SET threads TO {os.cpu_count()}")
con.execute(f"SET temp_directory = '{TMP}'")
con.execute("SET preserve_insertion_order = false")
con.execute("INSTALL tpcds; LOAD tpcds;")
print("duckdb", duckdb.__version__, "threads", os.cpu_count())

t0 = time.time()
con.execute(f"CALL dsdgen(sf={sf})")
print(f"dsdgen(sf={sf}) done in {time.time() - t0:,.0f}s")

generated = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
missing = [t for t in TABLES if t not in generated]
assert not missing, f"dsdgen did not produce {missing}; got {generated}"
for t in generated:                          # free memory before the COPYs
    if t not in TABLES:
        con.execute(f"DROP TABLE {t}")

# COMMAND ----------

counts = {}
for t in TABLES:
    t1 = time.time()
    counts[t] = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
    con.execute(
        f"COPY {t} TO '{LOCAL}/{t}' "
        "(FORMAT parquet, COMPRESSION snappy, ROW_GROUP_SIZE 1000000, PER_THREAD_OUTPUT, OVERWRITE)"
    )
    con.execute(f"DROP TABLE {t}")
    print(f"  {t:<22} {counts[t]:>15,} rows  {time.time() - t1:>6.0f}s")
con.close()

# COMMAND ----------

# Copy into the Volume (dbutils copies in parallel; the FUSE mount is too slow for DuckDB to
# write there directly), then count what LANDED with Spark -- the copy is what the build reads.
if any(f.path.rstrip("/").endswith(f"sf{sf}") for f in dbutils.fs.ls(VOLUME)):
    dbutils.fs.rm(DEST, recurse=True)
for t in TABLES:
    t1 = time.time()
    dbutils.fs.cp(f"file:{LOCAL}/{t}", f"{DEST}/{t}", recurse=True)
    print(f"  copied {t:<22} {time.time() - t1:>6.0f}s")

landed = {t: spark.read.parquet(f"{DEST}/{t}").count() for t in TABLES}
expected = EXPECTED.get(sf, {})
problems = []
for t in TABLES:
    exp = expected.get(t)
    ok = landed[t] == counts[t] and (
        exp is None or (abs(landed[t] - exp) / exp <= 0.01 if t in FACTS else landed[t] == exp)
    )
    print(f"  {t:<22} landed {landed[t]:>15,}  generated {counts[t]:>15,}  spec {exp if exp is None else f'{exp:,}':>15}  {'ok' if ok else 'MISMATCH'}")
    if not ok:
        problems.append(t)
assert not problems, f"row-count mismatch on {problems} -- not writing the marker"

shutil.rmtree(LOCAL, ignore_errors=True)
shutil.rmtree(TMP, ignore_errors=True)

marker = {
    "sf": sf, "tables": landed, "duckdb": duckdb.__version__,
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}
with open(MARKER, "w") as fh:
    json.dump(marker, fh, indent=2)
print(json.dumps(marker, indent=2))
dbutils.notebook.exit(json.dumps({"status": "generated", "sf": sf, "volume": DEST}))
