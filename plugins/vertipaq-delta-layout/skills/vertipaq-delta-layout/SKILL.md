---
name: vertipaq-delta-layout
description: How to configure Databricks so the Delta tables it writes are in the parquet layout Power BI Direct Lake (VertiPaq) transcodes fast and holds small — a cluster spark_conf block for row-group geometry and dictionary encoding, plus CLUSTER BY on one key per fact table with the one Delta table property that makes it readable. Use whenever a Databricks-written Delta table is read by a Fabric semantic model (Direct Lake, Mirrored Azure Databricks Catalog, OneLake shortcut), or when someone asks about V-Order, row groups, segments, dictionary encoding, liquid clustering, OPTIMIZE, targetFileSize, maxRecordsPerFile, or why Direct Lake is slow on Databricks tables. Consult it BEFORE proposing any Spark config, orderBy, repartition, OPTIMIZE or table property for such a table — the obvious moves are measured not to work.
---

# Writing Delta tables VertiPaq prefers, from Databricks

VertiPaq reads parquet from any producer. Direct Lake is not a compatibility gate and V-Order is
not a ticket of entry; the engine has a *preference*, a layout it transcodes fast and holds
small. One parquet row group becomes one VertiPaq segment, and whatever parquet leaves PLAIN the
engine dictionary-encodes itself at transcode, on every cold load. So the layout it prefers is:
uniform row groups of 2–5M rows (usable window 1M–16M), one row group per file, dictionary
encoding on every column the data allows, files disjoint on the column the reports filter by,
and a Delta log it can read (no v2 checkpoints).

Everything here was measured on Databricks Runtime 19, TPC-DS SF100 and SF1000, read through a
Mirrored Azure Databricks Catalog by Direct Lake under Microsoft's own benchmark protocol (20
concurrent readers, the white paper's queries and semantic model). Source, numbers and the
things that did not work: https://github.com/djouallah/parquet_layout_vertipaq_spark.

**The whole recipe in one breath.** Put the config block on the cluster, disable predictive
optimization on the schema, and for every fact table declare `CLUSTER BY` on the one column the
reports filter on, with the checkpoint policy pinned first. User code stays
`df.write.saveAsTable()`. No `orderBy`, no `repartition`, no `OPTIMIZE` afterwards.

## 1. Cluster config: the same lines on every cluster that writes these tables

```
spark.sql.files.maxRecordsPerFile                                      6000000     # default 0 = no limit
spark.hadoop.parquet.block.size                                        2147483648  # default 134217728 (128 MB)
spark.databricks.delta.optimizeWrite.enabled                           true        # default false
spark.databricks.delta.optimizeWrite.binSize                           4096        # default 512 (MiB of shuffle per task)

spark.hadoop.parquet.page.row.count.limit                              16000000    # default 20000
spark.hadoop.parquet.page.size                                         67108864    # default 1048576 (1 MB)
spark.hadoop.parquet.dictionary.page.size                              67108864    # default 1048576 (1 MB)
spark.hadoop.parquet.enable.dictionary                                 true        # default true

spark.databricks.delta.properties.defaults.checkpointPolicy            classic     # default v2 once CLUSTER BY is declared
spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite  true        # default unset
spark.databricks.delta.properties.defaults.autoOptimize.autoCompact    false       # default unset
```

Put it in the job or all-purpose cluster `spark_conf` (or a cluster policy), never in notebook
code: the `spark.hadoop.*` keys land in the Hadoop configuration, and from a notebook they would
have to be set on both `spark.conf` and `spark.sparkContext._jsc.hadoopConfiguration()`. As a
Databricks Asset Bundle job cluster:

```yaml
job_clusters:
  - job_cluster_key: writer
    new_cluster:
      spark_version: <latest DBR, not an LTS>   # newest runtime; never an LTS
      runtime_engine: STANDARD                  # Photon OFF: parquet-mr is the writer that honours parquet.*
      spark_conf:
        spark.databricks.delta.optimizeWrite.enabled: "true"
        spark.databricks.delta.optimizeWrite.binSize: "4096"
        spark.sql.files.maxRecordsPerFile: "6000000"
        spark.hadoop.parquet.block.size: "2147483648"
        spark.hadoop.parquet.page.row.count.limit: "16000000"
        spark.hadoop.parquet.page.size: "67108864"
        spark.hadoop.parquet.dictionary.page.size: "67108864"
        spark.hadoop.parquet.enable.dictionary: "true"
        spark.databricks.delta.properties.defaults.checkpointPolicy: classic
        spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite: "true"
        spark.databricks.delta.properties.defaults.autoOptimize.autoCompact: "false"
        spark.task.cpus: "4"                    # memory, not layout: see below
```

Three things that are not config lines but are part of it:

- **Photon off.** Photon's writer ignores the `parquet.*` keys. `runtime_engine: STANDARD`.
- **Latest runtime**, for the dictionary keys and for Delta behaviour generally. The geometry
  itself no longer needs one: `maxRecordsPerFile` has been in Spark since 2.2. That is deliberate.
  The other way to size a segment, `parquet.block.row.count.limit`, needs parquet-java 1.16 (DBR 19,
  Fabric Runtime 2.0) and below that is ACCEPTED AND SILENTLY IGNORED, so the recipe would look
  disproved rather than unsupported. It also measured no faster. See *One number, not two* below.
- **`spark.task.cpus = 4` on wide facts.** 64 MB page plus 64 MB dictionary, per column: a writer
  task on a 34-column fact buffers 2–4 GB, and 16 of them on one executor die with exit 52 at
  SF1000. Output is byte-identical with the setting; it only spaces the tasks out. For a faster
  build add nodes rather than bigger ones.

## 2. Per schema: turn predictive optimization off

```sql
ALTER SCHEMA <catalog>.<schema> DISABLE PREDICTIVE OPTIMIZATION;
```

Anything that rewrites the files later, from compute that does not carry the block above, undoes
the layout. Measured on a stock-default `OPTIMIZE`: dictionary-encoded column chunks 93 % → 38 %,
one row group per file → 2 to 3, average file 58 MB → 260 MB. Predictive optimization and SQL
warehouses are that compute. The `autoCompact = false` lines are insurance for the same reason;
auto compaction was never observed to fire on DBR 19 either way.

## 3. Per fact table: `CLUSTER BY` one key, checkpoint policy first, then the user's write

```python
fq  = "catalog.schema.store_sales"
key = "ss_sold_date_sk"          # the ONE column the reports filter on; usually the date key

df.limit(0).write.format("delta").saveAsTable(fq)                                    # empty table, schema only
spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ('delta.checkpointPolicy' = 'classic')")
spark.sql(f"ALTER TABLE {fq} CLUSTER BY ({key})")
df.write.format("delta").mode("append").saveAsTable(fq)                              # the write, unchanged
```

The order is the whole trick:

- **`checkpointPolicy = classic` before the key exists.** Liquid clustering turns `v2Checkpoint`
  on by default from DBR 14.3 LTS, and Direct Lake cannot read a v2 checkpoint at all. If the table
  already has one: `ALTER TABLE t DROP FEATURE v2Checkpoint`.
- **No byte target.** The clustering exchange sizes its own files in bytes, and on a typical fact
  it lands every segment inside VertiPaq's window on its own, so the recipe sets none. A much
  wider fact could fall below it; the knob is then `delta.targetFileSize` as a table property on
  that fact, set before the append. Never a session default (it overrides the row caps on every
  write), never coarse relative to the table (section 6, item 1).
- **One key, the filter column.** Only the first clustering column eliminates segments. Two keys
  become a Hilbert curve, which orders no single column. On a star schema that column is the
  fact's date key.
- **Then append with whatever write the pipeline already does.** Clustering on write replaces the
  Optimized Writes exchange with its own range partitioning, so nothing in user code decides the
  files. Leave Optimized Writes on: turning it off left overlap unchanged and made file sizes
  1,582x more ragged in a probe.

**Dimensions get a plain `df.write.saveAsTable()`.** Do not cluster small tables: under
~500 MB a 128 MB target yields too few range partitions to place anything, and below
Databricks' clustering-on-write threshold the write lands unclustered anyway (300 MB on a path
table did nothing). Either way `DESCRIBE DETAIL` still reports `clusteringColumns`. A 300–500
byte-per-row table is a dimension; it gets one file and no segment decision binds on it.

**What it buys.** SF100, 20 concurrent readers, steady state; suite seconds = sum of the
per-query medians over the paper's 15 visual queries:

| arm | rows per row group | dictionary | suite s | p95 s | worst query s |
|---|---|---|---|---|---|
| Databricks, this recipe, clustered by date | 1.1–2.0M (built before the 128 MB target) | 80–100 % | **2.5** | 0.33 | 0.69 |
| Databricks, same config, unclustered | 5.6–5.9M | 100 % | 7.9 | 1.89 | 9.13 |
| Fabric Spark, V-Order + partition by date (the paper's layout) | 78k–144k | 100 % | 2.4 | 0.30 | 0.80 |
| Fabric Spark, V-Order alone, no layout work | 1.5–2.9M | 100 % | 11.7 | 2.76 | 12.05 |

The date-filtered queries fall 10.6x, 3.6x and 2.6x and land within 15 % of V-Order; the rest
are a wash. V-Order without geometry is the slowest arm in the table, which is the control: the
layout buys this, not the encoding. Caveat to repeat when quoting: the clustered arm differs from
the unclustered one in ordering AND segment size, so the win is not all ordering. At SF1000 with
the 128 MB target in place: `store_sales` 540 files of 4.85M rows (160 MB), `catalog_sales` 512
files of 2.78M rows (158 MB), both fully clustered, classic checkpoint.

## 4. What each line does

**Geometry: five knobs, nested, and an outer one wins only by leaving the inner one nothing to
do.** The recipe sets two of them: knob 3, and knob 5 to keep it honest.

| # | knob | unit | decides |
|---|---|---|---|
| 1 | `delta.targetFileSize` | bytes | the range partition, clustered or not |
| 2 | `optimizeWrite.binSize` | bytes | the partition only when 1 is unset; never on a clustered write |
| 3 | `spark.sql.files.maxRecordsPerFile` | rows | cuts FILES inside that partition, and the close ends the row group |
| 4 | `parquet.block.row.count.limit` | rows | cuts ROW GROUPS inside that file. Not in the recipe |
| 5 | `parquet.block.size` | bytes | row group by bytes; 2 GiB so it never wins |

On a clustered fact the exchange's own byte target decides everything (about 65 MB when nothing is
set): partitions of 1–2M rows at SF100 sit under the row cap, so 3 is inert and each file is
exactly one row group anyway. On an unclustered write (dimensions, or a fact nobody clustered) the
row cap takes over: 6M-row files, one 6M row group each, on any table width, no arithmetic
anywhere. Knob 5 is set high so bytes never close a
row group before the row count does; parquet-mr checks it against buffered bytes, and with 64 MB
pages the default would split a wide fact at ~5M rows on its own schedule.

**Dictionary: parquet-mr decides once, on the first page.** For each column chunk it keeps the
dictionary only if, when the first data page closes, `encoded + dictionary < raw`. That page
closes at `parquet.page.size` (1 MB) or `parquet.page.row.count.limit` (20,000 rows) by default,
so a money column is judged on a 20k-row sample and loses every time. The three page lines make
the first page the whole row group: fact bytes dictionary-encoded went from 37–60 % to
99.7–99.9 %. Two things no setting changes: a near-unique column (an order number in a 6M-row
group) still fails the test, and small row groups move high-cardinality columns to the losing
side, not the winning one. At 1.11M rows per group `catalog_sales` lost 19.6 % of its bytes to
PLAIN on five price columns; at 143k rows per group (a partition per date) parquet-mr holds only
~50 % of fact bytes as dictionary. That is why the segment floor in practice is ~2M, not the
window's 1M, and why the recipe never partitions by date.

**Why aim low inside the window.** Warm queries favour small segments by 3–4x (segment
elimination plus more parallel work per query under concurrency); cold loads favour big ones by
2.4x, but cold is one model load and is dominated by the dictionary (2.2x), not by geometry. An
F64 caps a Direct Lake table at 1.5B rows, so at 2–5M rows per segment the biggest table a common
capacity holds is a few hundred segments: segment size is the constraint, never segment count.

## 4a. One number, not two

There are two ways to set a VertiPaq segment, and the recipe deliberately uses the older one.

| | `spark.sql.files.maxRecordsPerFile` | `spark.hadoop.parquet.block.row.count.limit` |
|---|---|---|
| what it cuts | the FILE; the close ends the row group | the ROW GROUP, directly |
| available since | Spark 2.2 | parquet-java 1.16 |
| honoured on | every runtime | DBR 19, Fabric Runtime 2.0 |
| on anything older | n/a | **accepted and silently ignored** |
| row groups per file | always 1 | as many as fit |

The newer key is the one that looks right, and it works where it is supported. It is not in the
recipe for two measured reasons. Its only extra capability is several row groups inside one file,
and an arm built that way (6M groups, two to a 12M-row file) measured indistinguishable from one
group per file at SF100 with 20 concurrent readers. And below parquet 1.16 it fails silently: no
error, no row groups, and the result reads as a disproof of the whole idea rather than a missing
dependency. Fabric Runtime 1.3 ships parquet 1.13.1 and is in that category.

The trade you accept: one file is one segment, so file size is not independent of segment size. At
6M rows that is about 195 MB on a 35-byte-per-row fact and about 310 MB on a 57-byte one. Keep
`parquet.block.size` at 2 GiB regardless -- it is what stops bytes closing the group before the row
count does.

If you are on DBR 19 or Fabric Runtime 2.0 and only your own jobs read these tables, the row-group
key is the better-behaved lever and several groups per file is fine. It just is not what a portable
recipe can publish.

## 5. What NOT to do

Each of these was measured, on DBR 19, to do nothing or to undo the layout.

**In user code**

- **No `df.orderBy(key)` / `ORDER BY` / `SORT BY` before the write.** Databricks plans Optimized
  Writes as a repartition above the query and Spark's `EliminateSorts` deletes a sort under a
  repartition, so no `Sort` node runs. The executed plan and the files (100 % overlap, every date
  in every file) were identical to an unordered write. `EXPLAIN` still shows the sort; only the
  executed `WriteIntoDeltaCommand` plan tells the truth.
- **No `repartition`, `coalesce`, `count` or per-table geometry arithmetic.** A recipe that needs
  them will not be adopted, and Optimized Writes already repartitions regardless of the
  DataFrame.
- **No `OPTIMIZE ... ZORDER BY`.** On a partitioned table with one file per partition it commits
  nothing; on anything else it is a rewrite from whatever session runs it (section 2).

**In config**

- **No per-table file size.** Bytes per row differ per table, so a byte target is a different row
  count on every table. 128 MB is one number that holds from SF10 to the F64 ceiling on any fact
  width; do not derive a "better" one.
- **No `delta.enableDeletionVectors = false`.** DBR defaults them on and that is wanted: without
  a DV a later `DELETE`/`UPDATE`/`MERGE` rewrites whole parquet files, and every rewritten file is
  a segment VertiPaq re-transcodes. Direct Lake reads DVs; reader version 3 is the price, paid on
  purpose.
- **No compression codec.** DBR stamps `delta.parquet.compression.codec = zstd` itself.
- **No column names in the cluster config.** Keys are per-table decisions (section 3).

**When clustering**

1. **If you set `delta.targetFileSize` at all, do not set it coarse relative to the write.** Partitions are bytes /
   target; 512 MiB on a 900 MB write gave ONE partition, every date in every file, while
   `DESCRIBE DETAIL` still said clustered.
2. **Do not set `spark.sql.files.maxRecordsPerFile` where a clustered partition can exceed it.**
   The cap cuts each partition into full files plus a remainder, and the remainders overlap:
   50 % measured. The recipe's 6M row cap is inert on a clustered write only because clustered partitions (about 65 MB unset, 128 MB if set) stay far under it.
3. **Do not write below the size bar and expect placement.** Small appends land unclustered and
   only `OPTIMIZE` fixes them.
4. **Do not `ALTER TABLE ... CLUSTER BY` on existing data and stop there.** It registers the key
   and moves nothing; `OPTIMIZE t FULL` or it never clusters.
5. **Do not run `OPTIMIZE` from a session without the parquet settings.** Under the writing
   session's own config, `OPTIMIZE` and `OPTIMIZE FULL` on a clustered table changed nothing at
   all; from a SQL warehouse or predictive optimization it rewrites with default row groups and
   38 % dictionary. The rule is about whose session runs it, not about the command.
6. **Do not trust `clusteringColumns` or `EXPLAIN`.** Both say clustered when nothing moved.
   Measure per-file min/max of the key (section 7).
7. **Do not use more than one key.** Two keys become a Hilbert curve, which orders no single
   column.
8. **Do not let `v2Checkpoint` in.** Pin `checkpointPolicy = classic` before the key exists.

What did not matter for placement: Optimized Writes on or off, `binSize`, CTAS versus
create-then-alter. Clustering on write replaces the Optimized Writes exchange entirely.

## 6. How an ordering reaches a Delta file

| construct | what reaches the files under this config |
|---|---|
| `df.orderBy(x)`, `ORDER BY x` | nothing: the `Sort` is deleted under the optimized-write exchange (measured plan) |
| `SORT BY x` | nothing, same rule (local sort under a repartition) |
| `CLUSTER BY x` **in a SELECT** | Hive's `DISTRIBUTE BY` + `SORT BY`; the sort half is deleted, no total order |
| `partitionBy(date)` | one file per date, exact elimination, but 143k-row segments and half the dictionary; cold loads 2.2x slower than V-Order's on the same geometry |
| `CREATE/ALTER TABLE ... CLUSTER BY (x)` | files disjoint on x, one row group each, 0 % overlap measured: **the practice** |
| `OPTIMIZE ... ZORDER BY (x)` | nothing on one-file-per-partition tables; otherwise a rewrite by whoever runs it |

If a genuine global sort is ever wanted: keep the `orderBy`, set `.option("optimizeWrite",
"false")` on that write, and size files with the sort's own exchange
(`spark.sql.adaptive.advisoryPartitionSizeInBytes` or `repartitionByRange(n)`). That is user
code plus a per-write option, outside this recipe by definition, and it is unpopular with Spark
teams for a reason: a full shuffle of the table before a byte is written. `CLUSTER BY` buys the
same file-level disjointness incrementally.

## 7. Verify: `DESCRIBE DETAIL` and `EXPLAIN` lie

`clusteringColumns` in `DESCRIBE DETAIL` means the key is registered. It says nothing about
whether a row moved. The only honest Databricks-side signal is `DESCRIBE HISTORY`, whose
`operationMetrics`/`clusteringOnWriteStatus` should read `late-stage clustering triggered` on the
append. Then measure. All of this works on a Unity Catalog managed table through Spark alone,
which matters because UC blocks path reads of managed-table files.

```python
from pyspark.sql import functions as F
t, KEY = spark.table(FQ), "ss_sold_date_sk"
fp = F.col("_metadata.file_path")

# 1. PLACEMENT: distinct key values per file against the table total.
#    Near the total = rows stayed where they fell. Much lower = clustering placed them.
per_file = (t.groupBy(fp.alias("f"))
             .agg(F.count("*").alias("rows"), F.min(KEY).alias("lo"), F.max(KEY).alias("hi"),
                  F.countDistinct(KEY).alias("keys")).collect())
print("keys per file", sum(r.keys for r in per_file) / len(per_file),
      "of", t.select(F.countDistinct(KEY)).collect()[0][0])

# 2. OVERLAP: how many files' [lo, hi] ranges intersect another's. 0 % is perfectly disjoint.
rng, overlapping, run_hi = sorted((r.lo, r.hi) for r in per_file), 0, None
for lo, hi in rng:
    if run_hi is not None and lo <= run_hi:
        overlapping += 1
    run_hi = hi if run_hi is None else max(run_hi, hi)
print(f"{100 * overlapping / len(rng):.0f}% of files overlap another")

# 3. GEOMETRY: rows per file and how ragged. Want every file in 1M..16M, ideally 2M..5M.
n = sorted(r.rows for r in per_file)
print(f"{len(n)} files, {n[0]:,}..{n[-1]:,} rows, spread {n[-1] / n[0]:.1f}x")
```

**You need an unordered control.** If the source already arrives in key order, an unclustered
write produces disjoint files and looks exactly like successful clustering.

**Row groups and dictionary need the footers**, which UC will not serve by path. Read them
downstream, from the Fabric side, over the mirrored table's OneLake path (`duckrun.get_stats()`
reads every footer of a Delta table; or `pyarrow.parquet.ParquetFile(path).metadata`), or write
the same thing to a path table under a Volume. Per row group check `num_rows` in 1M–16M and,
per column chunk, that `encodings` contains `RLE_DICTIONARY` or `PLAIN_DICTIONARY`; a chunk that
fell back reads `PLAIN` with no dictionary page at all. After any `OPTIMIZE`, replay the Delta
log's add/remove actions for the live file list; a directory listing returns the replaced
generation too until `VACUUM`.

## 8. Delta features and what Direct Lake reads

- **v2 checkpoints: no.** This is the one genuine read blocker found. Keep
  `delta.checkpointPolicy = classic` on every table a semantic model reads.
- **Liquid clustering (writer version 7): yes.** Mirroring accepts it and Direct Lake reads it.
- **Deletion vectors (reader version 3): yes.** Keep them on.
- **Row tracking**, which clustering turns on by itself: free, no materialised column in the
  parquet.
- **Column mapping, type widening, and anything else that raises the reader version:** not
  measured here. Check `DESCRIBE DETAIL` `minReaderVersion` and `tableFeatures` before assuming.

## 9. Limitations: the config asks, the writer does not always comply

- **Dictionary encoding is a request.** Near-unique keys, and price columns whenever row groups
  get small, fall back to PLAIN regardless of config. Fabric's V-Order writer dictionary-encodes
  every column; parquet-mr does not, and no setting changes that.
- **A clustered write is sized in bytes, not rows.** Its range exchange sizes files in bytes (its
  own target, or `delta.targetFileSize` if set), so `maxRecordsPerFile`, `binSize` and the 6M
  group cap are ceilings a clustered partition never reaches, and rows per segment follows row
  width. There is no row-denominated floor on a clustered write. A byte target, if set, is a
  target and not a cap (25–55 % overshoot measured).
- **Clustering on write is best-effort and size-gated (documented: 64 MB per key on a UC managed
  table), and says nothing when it skips.** Below the gate, or with a target coarse relative to
  the table, the write lands unplaced while `DESCRIBE DETAIL` still reports `clusteringColumns`.
- **Anything that rewrites the files afterwards undoes the layout.** Disabling predictive
  optimization is part of the recipe, not housekeeping.
- **The dictionary budgets are expensive to write.** `spark.task.cpus = 4` on wide facts.

## References

- Source repo, measurements and the config's justification line by line:
  https://github.com/djouallah/parquet_layout_vertipaq_spark (`README.md`, `LEARNING.md`,
  `summary.csv`).
- Microsoft, *Modern Power BI Architecture Choices for Reporting on Azure Databricks*: the
  reference protocol, queries and semantic model.
- Companion repo on Fabric capacity, four writers, capacity units as the metric:
  https://github.com/djouallah/direct-lake-parquet-layout.
