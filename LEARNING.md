## The result, both scale factors

The recipe against the best each other writer reached: `CLUSTER BY` on our side, delta-rs sorted on
the date key, and the paper's Fabric layout of one partition per date plus Z-order plus V-Order.
Three load tests back to back over one model, 20 readers, log scale. Run 1 is the first touch and
pays the whole transcode; runs 2 and 3 are the same suite again on the same resident model.

![SF100](chart_sf100.png)

![SF1000](chart_sf1000.png)

At SF100 every ordered arm converges near 2.4 s and the config alone is about 3x behind. At SF1000
the config alone stops warming altogether -- 814 -> 875 -> 1,045 s, slower run over run -- while all
three ordered arms settle. That is the case for the one `CLUSTER BY`, in a picture.

Read the run-1 numbers as suite seconds, never as a layout result: the mirrored arms read North
Europe storage from a capacity in another region and the Fabric-written arms read OneLake in region,
so cold is confounded and steady state is not.

Both charts and the per-arm geometry behind them come from [summary.csv](summary.csv) -- one row per
scale factor x arm x fact table, with files, row groups, rows per group, dictionary percentage,
ordering and the three run totals. Regenerate all three with `python benchmark/summary.py`.

## The config

The block itself is in [README.md](README.md); the live copy is the `spark_conf` on the
`tpcds_build_spark` job cluster in `databricks.yml`. This file is everything that justifies it,
and it is ONE number, in ROWS:

| # | number | unit | knob | cuts |
|---|---|---|---|---|
| 1 | 6,000,000 | rows | `spark.sql.files.maxRecordsPerFile` | the FILE, and the close ends the ROW GROUP = one VertiPaq segment |

It was two for a fortnight, the second being `parquet.block.row.count.limit` at 6M inside a 12M-row
file. That arm measured identical to this one and the key needs a runtime most readers do not have,
where it fails silently -- see *Two ways to set a segment, and why the recipe uses the older one*.

Rows, not bytes, so the same segment comes out of a 5-column table and a 200-column one. **There is
no byte target in the recipe.** A clustered write is sized in bytes by the clustering exchange on
its own (it replaces Optimized Writes; both row caps sit below its cut and never fire), and that
unaided geometry -- 2.05M rows per file on `store_sales`, 1.11M on `catalog_sales` at SF100, with a
fifth of `catalog_sales`'s bytes falling to plain at 1.11M -- is the arm that reached V-Order.
`delta.targetFileSize` = 128 MB on the clustered facts lifts `catalog_sales` to 2.23M rows and
96.8 % dictionary bytes and changes nothing else that was measured (`clustersn`, 2026-09-09: steady
state identical). It was in the recipe for two days on the strength of that dictionary argument and
came out on 2026-09-09: less config is better, and the measured headline never used it. It stays an
option for a fact wider than these -- a TABLE property before the append, never the session form
(it overrides the row caps on every write, measured at SF10), never coarse relative to the table
(512 MiB on 900 MB collapsed clustering to one partition; see *What NOT to do*).

Set on the cluster. Your code stays `df.write.format("delta").saveAsTable(...)` — no
`repartition`, no `coalesce`, no `count`, no sort. Every value is the same for every table,
whatever its width; that is the property being defended, not the numbers themselves.

**Some of these lines already match the platform default, and they stay anyway.** They are guards,
not redundancy: this block gets pasted into estates where a cluster policy, a workspace default or
whoever came before has already set something the other way, and a default is only a default until
somebody changes it. Every line is stated so the recipe declares its intent rather than inheriting
it. The annotations below say which lines move the default and which pin it.

### One thing the config above will cost you: the dictionary settings are expensive to WRITE

`parquet.page.size` and `parquet.dictionary.page.size` are 64 MB each, and both budgets are **per
column**. A writer task on the 34-column `catalog_sales` therefore buffers 2–4 GB. Spark runs one
task per core by default, so 16 of them at once on an E16ds_v5 and the executor dies with exit 52.

Measured at SF1000: it failed this way on 4 × E8ds_v5 **and** on 3 × E16ds_v5, always on
`catalog_sales`, never on the 23-column `store_sales`. Bigger nodes do not help — more cores bring
proportionally more buffers. What fixes it is fewer concurrent writers:

```
spark.task.cpus                                                        4
```

Four writer tasks per executor instead of one per core. Byte-identical output, just slower — it
changes nothing about the files, which is why it is not in the config block above. Set it if your
widest table OOMs the writer, and size clusters for it: more nodes beat bigger ones, because with
`task.cpus=4` half the cores on a 16-core executor sit idle anyway.

## The config in prose

The README carried this commentary under the block until 2026-09-09, when the README was cut to
the block itself. The block it describes is the one in [README.md](README.md).

The first four lines are geometry: 6M-row files on any table width, with the byte triggers set high
enough that rows decide. There is no row-group setting, and that is the point -- with no row-group
cap the file close ends the row group, so one file is one row group is one VertiPaq segment, and the
one number you set is the segment size. The next four are the dictionary: they
make the first page of every column chunk the whole row group, which is where parquet-mr takes its
dictionary-or-PLAIN decision. The last three are the Delta log. `checkpointPolicy = classic` is the
one hard read blocker found in this work: Direct Lake cannot read a v2 checkpoint at all, and
Databricks is pushing v2 as the default, liquid clustering first, so the line pins classic on every
table the cluster creates, clustered or not. The two `autoOptimize` lines stamp on each table what
the session asks for, so the layout survives a later write from a session that never carried this
block.

Three things that are not config lines but are part of it:

- **Photon off.** Photon's writer ignores the `parquet.*` keys. `runtime_engine: STANDARD` on the
  cluster that writes these tables.
- **Latest runtime.** `parquet.block.row.count.limit` exists from parquet-java 1.16.0 (DBR 19
  ships 1.17.0). An older runtime accepts the key and silently ignores it, and the result reads as
  "row groups did not help".
- **Predictive optimization off**, per schema: `ALTER SCHEMA <catalog>.<schema> DISABLE PREDICTIVE
  OPTIMIZATION`. Anything that rewrites the files from compute without this config undoes the
  layout: a stock `OPTIMIZE` took dictionary encoding from 93 % to 38 % and files from 58 MB to
  260 MB. Predictive optimization and SQL warehouses are that compute. The `autoCompact` line is
  insurance for the same reason; auto compaction was never observed to fire on DBR 19, on or off.

**6M rows per row group is a general-purpose default, not the optimum for every table.** One
parquet row group becomes one VertiPaq segment, and the usable window is 1M-16M rows. 6M is one
number that holds across table widths: big enough for the dictionary to win on every column that
can pay for one, small enough to stay inside the window. Segment size was measured to pay only once
the rows are ordered, and from both directions: 4M unordered was no better than 6M and 8M was no
better either, while the clustered table, at about 2M, is three times faster. The floor is the
dictionary, about 2M rows in practice: at 1.1M rows per group `catalog_sales` lost 19.6 % of its
bytes to PLAIN. Do not go small without reading the footers.

**Why the file cap and not the row-group cap.** Spark exposes two ways to size a segment, and the
other one, `parquet.block.row.count.limit`, is the one that looks right: it cuts the row group
directly and can put several inside one file. It is not here for two measured reasons. Several
groups per file is the only thing it can do that the file cap cannot, and a table built that way
read the same as one holding a single group per file. And it needs parquet-java 1.16, meaning a
current Databricks runtime or Fabric Runtime 2.0; below that it is accepted and silently ignored, so
the geometry never happens and the result reads as "row groups did not help". `maxRecordsPerFile`
has been in Spark since 2.2. If you are on a new runtime and only your own jobs read these tables,
the row-group cap is the better-behaved lever. The long form, with the measurements, is *Two ways
to set a segment, and why the recipe uses the older one* below.

**What it delivers at SF100.** 20 concurrent readers, the paper's 24-query suite three times back to
back over one model; suite seconds are the sum of the per-query medians over the paper's 15 visual
queries in the steady-state run.

| SF100, no ordering on either side | rows per row group | dictionary, fact bytes | steady-state suite |
|---|---|---|---|
| Databricks, this config, nothing else | 5.6-5.9M, one per file | 99.9-100 % | 7.9 s |
| Fabric Spark, V-Order, nothing else | 1.5-2.9M | 100 % | 11.7 s |

The default parquet-mr write held 37-60 % of fact bytes dictionary-encoded; the config takes it to
99.9-100 %, parity with V-Order, and the table reads faster than V-Order's own unordered layout.
Nothing in user code changed.

**What the config cannot do.**

- **Dictionary encoding is a request, not a guarantee.** parquet-mr decides dictionary vs PLAIN once
  per column chunk, on its first page, and never revisits. The config moves that first page from
  20k rows to the whole row group, and that is the whole of the dictionary gain. But a column whose
  values barely repeat inside a row group still falls back to PLAIN regardless of the config:
  near-unique keys, and money columns whenever the row groups get small. Fabric's V-Order writer
  dictionary-encodes every column; Databricks does not, and no setting changes that.
- **The dictionary budgets are expensive to write.** 64 MB page plus 64 MB dictionary, per column:
  a writer task on a 34-column fact buffers 2-4 GB, and enough of them on one executor die with
  exit 52 (seen at SF1000, never at SF100). `spark.task.cpus = 4` spaces the tasks out; it is a
  memory setting, not a layout one, which is why it is not in the block.

## Why each line is there

Each carries its DEFAULT and what changing it buys, because a setting nobody can justify is a
setting that gets copied into someone's cluster policy on faith.

```
# --- geometry: 6M-row row groups, 2 to a file, on any table shape -------------------------------
#
# Say the two numbers and stop. Both are denominated in ROWS, so 12M and 6M mean the same thing on
# a 5-column table and a 200-column one, which is the property being defended. `block.size` is the
# same idea negatively: keep the BYTE trigger from taking the decision away. On a clustered write
# neither row cap is reachable and the clustering exchange sizes the files in bytes on its own.

spark.databricks.delta.optimizeWrite.enabled                          true
#   default: false (true on some SKUs/tables). The only config-level knob that REPARTITIONS the
#   data regardless of what the user's DataFrame looks like -- without it the file count is
#   whatever their last shuffle happened to leave, and no geometry rule can hold.

spark.databricks.delta.optimizeWrite.binSize                          4096
#   default: 512 (MiB of SHUFFLE bytes per task). Not a geometry knob at all now that the row cap
#   is set: it decides how many FILES a task writes, and therefore only how many ragged tails
#   there are -- one per task, ~46 at SF1000 store_sales. Left at 4096 because LOWERING it is the
#   expensive way to shrink files: at 1024 that table takes ~184 tasks instead of 46, so ~184 ragged
#   row groups instead of 46. Shrink files with maxRecordsPerFile instead -- it splits a task's
#   output without adding tasks, so file size falls and the tail count does not.

spark.sql.files.maxRecordsPerFile                                     6000000
#   default: 0 (off) -- meaning NO row limit, so binSize alone would decide and a 4096 bin projects
#   to ~1.9 GB files at SF1000. Rows per FILE, and with no row-group cap the file close ends the
#   group, so this number IS the segment: one file, one row group, one VertiPaq segment. It bounds
#   in ROWS, which is the point -- a byte target would give ~30M rows on a narrow fact and under 1M
#   on a wide one. Bytes still track column width: a full file is ~195 MB on store_sales (35 B/row)
#   and ~310 MB on the 34-column catalog_sales (57 B/row) at SF100. Measured at SF100 as the
#   `default` arm: 20 of 24 catalog_sales files and 42 of 47 store_sales files at exactly 6,000,000
#   rows, one row group each, 100 % / 99.9 % of fact bytes dictionary-encoded.
#   Databricks documents this setting as NOT recommended except to avoid the parquet row-count
#   error. The recipe uses it anyway, deliberately: it is the only row-denominated FILE boundary
#   Spark exposes, it works on every runtime, and the whole geometry is denominated in rows. A
#   choice made against the doc's advice, not a discovery.

# parquet.block.row.count.limit is NOT here.
#   default: 2147483647 (Integer.MAX_VALUE). Rows per ROW GROUP, directly -- the lever that looks
#   right, and it was in the recipe from 2026-09-07 to 2026-09-09. Added in parquet-java 1.16.0;
#   DBR 19 ships parquet-mr 1.17.0-databricks-0001. Measured honoured there on 2026-09-07
#   (rowgroup_probe C and F): the cap fires repeatedly inside a single file, every full group
#   EXACTLY 6,000,000 rows, 99-100% of column chunks still dictionary-encoded. It works, and it is
#   out anyway, for two reasons. First, the only thing it can express that the file cap cannot is
#   several groups inside ONE file, and the `default2rg` arm measured that worth nothing. Second,
#   below parquet 1.16 it is ACCEPTED AND SILENTLY IGNORED -- Fabric Runtime 1.3 ships 1.13.1 -- so
#   a reader on an older runtime gets no row groups and no error, and concludes row groups do not
#   help. It also cannot help when maxRecordsPerFile is set to the SAME value: measured, variant D
#   is indistinguishable from B, because both are upper bounds and the file close forces the group
#   closed on the same row.

spark.hadoop.parquet.block.size                                       2147483648
#   default: 134217728 (128 MB). Raised to 2 GiB so BYTES never close a row group before the row
#   cap does -- parquet-mr checks this against BUFFERED bytes, and with 64 MB pages the wider fact
#   tripped it at ~4.97M rows and split one file into two row groups. Not a target: it is set high
#   enough to be inert.

# --- dictionary encoding on every column --------------------------------------------------------

spark.hadoop.parquet.page.row.count.limit                             16000000
#   default: 20000. THE dictionary fix, and the largest single effect found in this project.
#   parquet-mr decides dictionary-vs-plain ONCE per column chunk, on the FIRST page, and falls back
#   unless (encoded + dictionary) < raw. A page closes at whichever comes first, page.size bytes or
#   THIS many rows -- so by default the dictionary's whole cost is charged against 20,000 values
#   and a wide column loses every time. Measured on the facts: 37-39% of bytes dictionary-encoded
#   before, 99.7-99.9% after, at SF100 and SF1000 -- parity with Fabric's V-Order writer.

spark.hadoop.parquet.page.size                                        67108864
#   default: 1048576 (1 MB). The other page-closing trigger, raised for the same reason: 1 MB is
#   ~262k INT32s, still far too small a sample for the dictionary to win on a wide column.

spark.hadoop.parquet.dictionary.page.size                             67108864
#   default: 1048576 (1 MB). The dictionary's size budget -- the OTHER fallback exit. Not what was
#   failing before (a 32 MB layout arm beat a 64 MB default one, which proves this exit was never
#   the binding one at 20k-row pages), but now that a page can hold 16M rows the dictionary itself
#   can exceed 1 MB, so it is raised in step. Justified by reasoning, NOT independently measured.

spark.hadoop.parquet.enable.dictionary                                true
#   default: true. A no-op, kept as documentation so the recipe states its intent rather than
#   relying on a default someone may have overridden in a cluster policy.

# --- Delta protocol, stamped on every NEW table -------------------------------------------------

spark.databricks.delta.properties.defaults.checkpointPolicy           classic
#   default: v2 for liquid-clustered tables on DBR 14.3 LTS+, classic otherwise. LOAD-BEARING:
#   VertiPaq cannot read a v2 checkpoint at all -- the table mirrors and then fails to load. Must
#   be set BEFORE `CLUSTER BY` if clustering is ever declared.

spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite true
spark.databricks.delta.properties.defaults.autoOptimize.autoCompact   false
#   default: unset. optimizeWrite is the table-property twin of the session knob above, so a table
#   keeps the behaviour even when written from a session that never carried this block.
#   autoCompact false is the ONE auto-compaction line in the recipe, and the load-bearing one: a
#   compaction rewrites the files with ITS own sizing (128 MB in BYTES, so a 6M-row group comes
#   back as one sub-6M group), and the current Databricks docs make this table property the thing
#   that keeps background auto compaction off a Unity Catalog managed table. The session switch
#   spark.databricks.delta.autoCompact.enabled was dropped from the recipe on 2026-09-07:
#   measured (ac_off / ac_on / ac_prop, SF10, DBR 19) it made no observable difference either way,
#   and the table property is what a table keeps.
#
#   At the recipe's own geometry compaction has nothing to do anyway: ~1 remainder file per task,
#   3 at SF10, 46 at SF1000, under minNumFiles (50). Where it would bite is the `partition` arm:
#   1,823 files of 5.2 MB at SF100, far past the threshold, and compaction would replace exactly
#   the geometry that arm exists to reproduce.
#
#   Do not confuse this with the measured disaster, which is a DIFFERENT mechanism: `OPTIMIZE` run
#   by compute that never saw these settings -- predictive optimization's serverless, or a SQL
#   warehouse -- took dictionary 93% -> 38%, row groups per file 1 -> 2..3, average file 58 MB ->
#   260 MB. That is why `ALTER SCHEMA ... DISABLE PREDICTIVE OPTIMIZATION` is a separate step and
#   load-bearing rather than tidy.

```

**`delta.targetFileSize` -- measured, and NOT in the recipe.** It was adopted on 2026-09-07 at
128 MB as a table property on the clustered facts and dropped on 2026-09-09. What was measured:

- **The five geometry knobs are NESTED, not competing.** An outer number never overrides an inner
  one; it wins by leaving it nothing to do. Make 1 coarse enough and 3 comes straight back: a
  512 MiB target put all 26.2M SF10 rows in one partition and the 12M cap cut it into 12M + 12M +
  2.2M.

  | # | knob | unit | decides |
  |---|---|---|---|
  | 1 | `delta.targetFileSize` | bytes | the PARTITION, on clustered and unclustered writes alike |
  | 2 | `optimizeWrite.binSize` | bytes | the partition only when 1 is unset; never on a clustered write |
  | 3 | `spark.sql.files.maxRecordsPerFile` | rows | cuts FILES inside that partition |
  | 4 | `parquet.block.row.count.limit` | rows | cuts ROW GROUPS inside that file. NOT in the recipe -- see *Two ways to set a segment* |
  | 5 | `parquet.block.size` | bytes | row group by bytes; set to 2 GiB so it never wins |

- **Which knobs are live, per arm.** On `default` the file cap decides, and since the recipe sets no
  row-group cap the file close ends the group: 6M-row files, one 6M group each. On `cluster` the
  clustering exchange's own byte target decides (~65 MB unset; the row cap is inert while partitions
  hold 1-2M rows). On `partition` one Hive partition per date decides (~120k rows, the cap inert).
- **It is not cluster-only** (SF10, `ns_tfs128`, then SF100, `nosortt128`): on an unclustered write
  with `maxRecordsPerFile` 12M in force, 128 MB took `store_sales` to 49 files of 5.35M rows and
  `catalog_sales` to 40 of 3.56M, both row caps dead. As a session default it would reshape every
  table on the cluster, which is why, if used at all, it is a table property.
- **On a clustered wide fact it fixes the dictionary and nothing else** (SF100, `clustersn`):
  `catalog_sales` 1.11M -> 2.23M rows per group, dictionary 80.4 -> 96.8 % of bytes, file count
  halved -- and steady state identical to the arm without it (2.6/2.4 vs 2.6/2.5 s). Cold 42.0 vs
  36.9 s on one experiment: no improvement.
- **The one way it bites** is silent: a target coarse relative to the table collapses the clustered
  write to one range partition, unplaced, while `DESCRIBE DETAIL` still says clustered (SF10,
  512 MiB on a 932 MB fact, every date in every file). `build_spark.py` asserts file count and the
  1M-16M window on every clustered build for that reason, whether or not a target is set.

So: rows are portable across table widths and bytes are not, the arm that reached V-Order had no
byte target, and the one thing the target buys -- the dictionary on a wide fact -- did not move the
clock. Keep it in reserve for a fact wider than ~60 B/row whose footers show plain money columns.

Deliberately NOT set:

- **`enableDeletionVectors`.** Removed 2026-09-07; the recipe used to stamp it `false`. DBR 19
  defaults it ON and that default is now wanted: without DVs a DELETE/UPDATE/MERGE reverts to
  copy-on-write and rewrites whole parquet files, which makes VertiPaq re-transcode every column in
  them. A DV leaves the files alone and the resident segments survive. Cost accepted:
  `minReaderVersion=3`, so a Delta reader that does not implement DVs cannot open these tables.
- **compression.** DBR 19 stamps `delta.parquet.compression.codec=zstd` itself. The `layout` arm
  used to force SNAPPY, which made every cross-arm comparison before 2026-09-06 compare a snappy
  table against zstd ones.
- **`delta.targetFileSize`, in either form.** Not in the recipe since 2026-09-09 (measured: on a
  clustered wide fact it moves the dictionary and nothing else; see *`delta.targetFileSize` --
  measured* above). If ever used, the TABLE property on the fact, never the session form, which
  overrides the row caps on every unclustered write on the cluster (SF10, measured, 3 files at the
  12M cap -> 6 files of 3.1-4.9M). Databricks documents the property as honoured by `OPTIMIZE` on a
  managed table; measured, the managed-table WRITE path reads it too.
- **anything naming a column.** Sort keys, clustering keys and partition columns are all per-table
  decisions, so none of them can live in a config that is meant to be pasted unchanged.

- The cost is the tail, and **it is one ragged ROW GROUP per TASK, not per file** -- a task writes
  floor(N/12M) full files of 2 groups each, then a last file whose final group is the remainder.
  The count of ragged groups therefore equals the count of tasks, and `binSize` is the only thing
  that changes it. PROJECTED at SF1000 (measured shuffle B/row from variant E; not yet built):
  store_sales 46 tasks -> 230 files of ~408 MB, 460 groups, 46 ragged (10%); catalog_sales 37 tasks
  -> 148 files of ~558 MB, 259 groups, 37 ragged (14%). MEASURED under the OLD one-group-per-file
  config: store_sales 418 of 466 files exactly at 6,000,000, 48 remainders, 19 under 1M (min 228k);
  catalog_sales 215 of 255 at the cap, 40 remainders, 15 under 1M. **The tail fraction does not move
  with the switch** -- the tails were always per-task. What moves is that the remainder is now a
  small group inside an otherwise full file rather than a whole short file, and the file count
  halves.
- **Shrinking files via `binSize` is the expensive way and the arithmetic says so.** store_sales at
  binSize 1024: ~184 tasks instead of 46, so ~184 ragged groups instead of 46. Shrink files with
  `maxRecordsPerFile` instead -- it splits a task's output without adding tasks, so file size falls
  and the tail count does not. Measured corollary (variant E, this run): rows per file scale
  linearly with the bin -- 7,200,248 at 512 MiB to 9,600,330 at 768 on store_sales -- so **Unity
  Catalog's file-size autotuning does NOT override binSize on managed tables**, which had been open.

## What the delta_rs arm has that ours did not -- MEASURED from the footers, 2026-09-09

`ducksort` is the best COLD arm in the project at SF100 (17.7 s against the clustered arm's 36.9)
and level with it warm (2.4 / 2.5 s against 2.6 / 2.5, and our warm median per query is actually
lower, 124 ms against 145). delta-rs is open source, so "something about the writer" is not an
answer. Three differences, from `chunks_*.parquet`:

| | `ducksort` (delta_rs) | `cluster` (parquet-mr) |
|---|---|---|
| files | 82 at 246 MB | 256 at 62-70 MB |
| row groups | 162 | 256 |
| dictionary bytes | 100 % | 100 % store_sales, 80.4 % catalog_sales |
| codec | SNAPPY | ZSTD |

**The dictionary difference is a writer RULE, and it is the one this repo already documented.**
Every one of `ducksort`'s 4,647 fact column chunks carries `RLE_DICTIONARY`; not one is plain.
parquet-mr leaves 495 of 7,552 chunks fully plain, 1.53 GB of 16.6. parquet-mr decides
dictionary-or-plain ONCE per column chunk, on the first page, on a break-even test, and a chunk that
fails it is plain end to end. parquet-rs applies no such test -- it dictionary-encodes and falls back
only when the dictionary page outgrows its byte limit. So at 1.11M rows per group our
`catalog_sales` money columns fail a test delta-rs never runs. **The fix is ours to make**: bigger row
groups pass the same test, which is what `delta.targetFileSize` buys -- and `clustersn` then
measured that passing it changes nothing on the clock.

**THE COLD COMPARISON IS CONFOUNDED and must not be quoted as a layout result.** The mirrored arms
(`default`, `cluster`, `clustersn`, `partition`) sit in North Europe and are read through the Mirrored
Azure Databricks Catalog; the lakehouse arms (`vorder`, `vonly`, `duckdb`, `ducksort`) sit in the
capacity's own region. Run 1 is region-sensitive and steady state is not, so 17.7 s against 36.9 s is
a layout comparison and a geography comparison at once. **On the metric that survives the confound,
the clustered Databricks arm already matches delta_rs.** The arm that would settle it does not exist:
a Databricks-layout table written INTO the lakehouse, so every writer is read off the same storage.

**`clustersn` is the clean test of the two levers we control**, both table properties, both inside the
config-only recipe, and both against `cluster` in the SAME item, so no geography enters:
snappy, and a 128 MB byte target to lift the row groups over parquet-mr's break-even test.
The codec is defensible as a recipe default on its own terms -- it has NO effect on VertiPaq's memory
footprint, since the engine re-encodes into its own format, so it only trades bytes on the wire
against transcode CPU, and for a Direct Lake table that trade favours snappy.

## `clustersn` measured: snappy and a fixed dictionary bought nothing -- SF100, 2026-09-09

One experiment, 3 x 20 users, value check passing, against `cluster`'s two.

| | `cluster` | `clustersn` |
|---|---|---|
| run 1 (cold) | 36.9 s | 42.0 s |
| runs 2 / 3 | 2.6 / 2.5 | 2.6 / 2.4 |
| median / p95 / max | 99 ms / 0.328 / 0.721 | 93 ms / 0.335 / 0.624 |
| 15 visual queries, median | 132 ms | 124 ms |

The files changed exactly as intended -- `catalog_sales` doubled its row groups to 2.23M and its
dictionary went from 80.4 % of bytes to 96.8 % (5 plain columns down to 1), the file count halved,
and the codec is snappy throughout -- and steady state did not move. Cold is 5 s WORSE on a single
experiment, which reads as "no improvement", not a regression.

**What this settles.** The two things delta_rs had that our clustered arm did not are now on our
side of the table, and delta_rs's 17.7 s cold is nowhere in sight. So the cold gap is not the codec
and not the dictionary. The one remaining difference is WHERE THE FILES LIVE -- mirrored North
Europe storage against the capacity's own region -- and that is the confound already flagged in
*What the delta_rs arm has that ours did not*. The only test left that could settle it is a
Databricks-layout table written INTO the lakehouse.

**For the recipe:** the codec stays at DBR's zstd default, and `delta.targetFileSize` comes OUT --
it moves the dictionary on the wide fact (80.4 -> 96.8 %) and does not move the clock, and the arm
that reached V-Order never had it. Less config. It stays an option for a wider fact.

## Segment geometry alone is settled: neither knob moves a resident model -- MEASURED at SF100, 2026-09-09

Two arms, one experiment each (3 x 20 users, 24 queries, 0 errors, value check passing), against
`default`'s two. Each isolates ONE thing about the geometry the recipe specifies, with no ordering
anywhere: `default2rg` changes groups-per-file at a fixed 6M segment, `defaultf8` changes the segment
size at a fixed one-group-per-file.

| | `default` (6M, 1/file) | `default2rg` (6M, 2 per 12M file) | `defaultf8` (8M, 1/file) | `cluster` |
|---|---|---|---|---|
| run 1 (cold) | 32.4 s | 32.8 s | 151.8 s | 36.0 s |
| runs 2 / 3 | 9.4 / 7.9 | 10.0 / 8.1 | 9.4 / 10.0 | 2.5 / 2.4 |
| p50 / p95 (last run) | 207 ms / 3.1 s | 196 ms / 3.0 s | 213 ms / 2.8 s | 122 ms / 0.36 s |
| `catalog_sales` files / groups | 24 / 24 | 13 / 25 | 20 / 20 | 128 / 128 |
| `store_sales` files / groups | 47 / 47 | 24 / 47 | 34 / 34 | 128 / 128 |
| dictionary, both facts | 100 / 99.9 % | 99.8 / 99.8 % | 100 / 100 % | 80.4 / 100 % |

Both builds came out exactly as asked. `default2rg` was the FIRST table ever written with two 6M
row groups inside a 12M-row file -- 1.92 and 1.96 groups per file measured, every full group at
exactly 6M -- and it carries the same bytes as `default` in half the files. `defaultf8` holds one 8M
group per file, averaging 7.13M and 7.71M rows.

**Neither knob moved a resident model.** Every steady-state difference in that table is inside
run-to-run noise, and the three unordered arms sit together at 8-10 s where the clustered arm sits
at 2.4. Per query the pattern is the same one every unordered arm has: q6 / q14 / q24, the three
date-filtered shapes, cost 4.3-4.8 s / 1.0-1.6 s / 0.8-1.1 s on all three of them and 0.40 / 0.27 /
0.27 on `cluster`.

**What this settles.**

- **The published config is not a regression.** `default2rg` ties `default`, so every number in this
  file measured on one group per file transfers to the two-group shape and back. That was the open
  question the arm was built for, and answering it is what let the recipe go back to one number.
- **Groups per file is not a lever.** One 6M group per file and two 6M groups per file measure the
  same. VertiPaq gets its segments from row groups, and how they are packed into files does not
  reach it.
- **Segment SIZE is not a lever without an ordering, and the falsifiable half held.** 8M was
  predicted on the record to land within noise of 6M and did. Together with the earlier `nosort4m`
  result (4M, no better) the window 4M-8M is now measured flat on an unordered table, which is the
  claim under *What it prefers* tested from both directions.
- **And it took `parquet.block.row.count.limit` out of the recipe.** Two groups per file is the only
  thing that key can express which the file cap cannot, and it measured worth nothing. What it costs
  is portability -- see the section below. The recipe went back to one number, the file cap, at the
  6M the `default` arm was always built under.
- **So geometry alone is finished as a line of enquiry.** Three knobs -- 4M, 6M with one or two
  groups per file, 8M -- and none of them is worth a rebuild. What is left is the ordering, and
  that is the one thing that has ever moved this benchmark.

**The SF100 `default` timings in this file were measured on the PRE-REBUILD table.** Same config,
same row counts, and the file geometry came back within a file of itself (`catalog_sales` 25 files
against 24, `store_sales` 47 both times), so they are not suspect -- but they were not taken against
the object now sitting in `tpcds_sf100_default`. A re-benchmark would close that gap; nothing else
depends on it.

`defaultf8`'s 151.8 s cold is the one number NOT to read as a result: it is a single run 1 on the
mirrored path, where the region confound lives (see *What the delta_rs arm has that ours did not*),
and `default2rg` on the same path and the same protocol came in at 32.8 s.

## Two ways to set a segment, and why the recipe uses the older one

One parquet row group becomes one VertiPaq segment. Spark gives two ways to decide how big it is,
and they are not equivalent in the way that matters for a config other people are meant to copy.

| | `spark.sql.files.maxRecordsPerFile` | `spark.hadoop.parquet.block.row.count.limit` |
|---|---|---|
| what it cuts | the FILE; the close ends the row group | the ROW GROUP, directly |
| available since | Spark 2.2 | parquet-java 1.16 |
| runtimes that honour it | all of them | DBR 19, Fabric Runtime 2.0 |
| on an older runtime | n/a | **accepted and silently ignored** |
| groups per file | always 1 | as many as fit |
| Databricks' own advice | "not recommended" | the intended lever |
| measured at SF100 | 9.4 / 7.9 s | 10.0 / 8.1 s |

**The recipe uses the file cap.** The row-group key's one extra capability is several groups inside
one file, and `default2rg` measured that capability worth nothing. Against that, it fails SILENTLY
below parquet 1.16: the key is accepted, ignored, and the result reads as "row groups did not help".
Fabric's own Runtime 1.3 ships parquet 1.13.1 and is in that category. A config published for other
people cannot have a line whose failure mode is looking like a disproof of the whole idea.

So the trade is deliberate: one file equals one segment, file size is no longer independent of
segment size, and at 6M rows that is ~195 MB on `store_sales` and ~310 MB on the 34-column
`catalog_sales`. Both are reasonable files, and the number is denominated in ROWS, which is the
property the whole geometry exists to defend. Databricks documents `maxRecordsPerFile` as not
recommended except to avoid the parquet row-count error; the recipe uses it anyway, knowingly, for
the reason above.

`parquet.block.size` stays at 2 GiB and is NOT optional under this arrangement: it is what stops
bytes closing a row group before the row cap does, and with 64 MB pages 1 GiB split the wider fact
at ~4.97M rows.

**If you are on DBR 19 or Fabric Runtime 2.0 and only ever read your own tables**, the row-group key
is the better-behaved lever and you can have several groups per file. It just is not what this
recipe publishes, because the recipe has to work where it lands.

## The framing

- VertiPaq reads parquet from **any** producer — Fabric Spark, OSS Spark, Databricks, Snowflake,
  DuckDB, delta-rs. Who wrote the file is not a factor.
- It has a *preference*, not a requirement: a layout it transcodes fast and holds small.
- V-Order is one producer's way of reaching that layout, not the standard itself.
- So every question here is "what layout?", never "which writer?".

**The reference.** To validate the config, I use Microsoft's excellent white paper *Modern Power BI
Architecture Choices for Reporting on Azure Databricks* as the reference: its protocol, its 24-query
DAX capture and its semantic models.

The paper is worth reading in its own right. Benchmarks of a lakehouse the way the architecture is
meant to work, one vendor writing and another reading with the files on storage as the only
contract, are rare, and the few that exist put a SQL engine on both sides. As far as I know this is
the first mainstream one where the reader is a BI engine and the workload is a report.

The paper tunes the Databricks write the way a careful engineer would, with the mainstream,
well-documented settings. This repo goes down a less-travelled path and tries a few little-known
knobs instead: forcing dictionary encoding, 6M-row row groups, and clustering on a single key to get
a global sort, to see how much further the parquet layout Databricks produces can be pushed.

## Arms

Every arm is the same rows: the paper's 10-table TPC-DS subset from DuckDB's `dsdgen`, with its §4.5
customisations (any-null fact rows dropped, `cache_buster INT = 1`, `d_date_sk_1 = d_date_sk − 8401`
mapping 1998–2003 onto 2021–2026, `date_dim` trimmed to 2,191 rows). Every build asserts its row
counts against the paper's Table 4.3.1; all match at SF10, SF100 and SF1000. Same table names on
every arm — the arm lives in the schema suffix, and builds never overwrite.

| arm | schema | written by | layout | measured |
|---|---|---|---|---|
| `default` | `tpcds_sf{N}_default` | Databricks, parquet-mr | THE RECIPE: plain `saveAsTable`, a 6M-row file cap and no row-group cap, so one 6M row group per file and one segment per file. Called `nosort` until 2026-09-09; renamed because it is what the job writes when asked for nothing. **SF100 was DROPPED AND REBUILT on 2026-09-09 15:16 UTC** under the one-number config rather than carried over by the rename, so the table is the README rather than a table that happens to match it: `store_sales` 47 files at 5,576,221 rows / 180.9 MB, `catalog_sales` 25 files at 5,702,309 rows / 295.3 MB, `rows_per_group_target` 0 and 1 group per file in the ledger, no OPTIMIZE, classic checkpoints, zstd, every row count matching the paper | SF100 rebuilt (numbers below PREDATE it), SF1000 |
| `defaultf8` | `tpcds_sf{N}_defaultf8` | Databricks, parquet-mr | `default` at an 8M file cap, one group per 8M-row file -- the same one-group-per-file shape as the measured `default`, so segment SIZE is the only difference. The falsifiable half of "small segments pay only with an ordering": prediction on the record, within noise of `default`. Built SF100 2026-09-09 (20 min): `store_sales` 34 files, 7.71M rows/file avg, 245.7 MB; `catalog_sales` 20 files, 7.13M rows/file, 362.5 MB; 15.6 GB against `default`'s 15.5 | SF100 MEASURED 2026-09-09: within noise of `default` (9.4 / 10.0 s vs 9.4 / 7.9), p50 213 vs 207 ms -- the prediction held |
| `cluster` | `tpcds_sf{N}_cluster` | Databricks, parquet-mr | the config + `CLUSTER BY (date_sk)`, one key, clustered on write. The clustering exchange replaces Optimized Writes, so `binSize` and the row caps are not in its plan and the file is sized in BYTES. **The two scale factors are two recipes under one name**: the SF100 build (2026-09-07 16:07) has NO `delta.targetFileSize` -- the exchange sized itself, 128 files/fact, 2.05M and 1.11M rows per group -- and it is the arm the 128 MB number was later DERIVED from; the SF1000 build (18:38 the same day) carries the 128 MB target. `clustersn` (the target + snappy) ties the SF100 arm at steady state, so the target's measured value is the dictionary (80.4 -> 96.8 % on `catalog_sales`), not speed | SF100 (no target), SF1000 (128 MB) |
| `clustersn` | `tpcds_sf{N}_clustersn` | Databricks, parquet-mr | the `cluster` arm with the two things the delta_rs arm had and it did not, both TABLE PROPERTIES: snappy, and `delta.targetFileSize` = 128 MB (the SF100 `cluster` build predates it). 64 files/fact, `store_sales` 4.10M rows/group, `catalog_sales` 2.23M, dictionary 96.8 % on `catalog_sales` (was 80.4 %), 0 overlaps | SF100 MEASURED 2026-09-09: steady state identical to `cluster` (2.6/2.4 vs 2.6/2.5 s), cold 42.0 vs 36.9 s on one experiment -- neither lever moved the clock |
| `partition` | `tpcds_sf{N}_partition` | Databricks, parquet-mr | `partitionBy(date_sk)` and nothing else — one file per date, ~143k-row groups, both row caps inert far above them. **A control**, see below | SF100, SF1000 |
| `vorder` | `tpcds_sf{N}` | Fabric Spark, `readHeavyForPBI` | the paper's own arm: V-Order, ZSTD, optimize write 1 GB, partitioned by date, then `OPTIMIZE ... ZORDER BY` the address key | SF100, SF1000 |
| `vonly` | `tpcds_sf{N}_vonly` | Fabric Spark, `readHeavyForPBI` | V-Order and nothing else — no partition, no sort, no `OPTIMIZE`. Its geometry lands on `default`'s, so the pair isolates the encoding | SF100 |
| `duckdb` | `tpcds_sf{N}_duckdb` | delta_rs, via duckrun | `SORTED BY AUTO` — duckrun profiles the data and picks the key itself, to minimise modelled memory rather than to prune, so it is **not** the date key | SF100 |
| `ducksort` | `tpcds_sf{N}_ducksort` | delta_rs, via duckrun | `SORTED BY` the date key — the same key `cluster` orders on, so the pair holds the key fixed and changes the writer | SF100, SF1000 |

The Databricks arms live in the mirrored Unity Catalog (`databricks_ne`) and reach Fabric through
the Mirrored Azure Databricks Catalog item; the Fabric and delta_rs arms are schemas in the
`tpcds_vorder` lakehouse, read by Direct Lake on OneLake.

**Gone, and why** — `layout` (a global sort via `repartitionByRange` + `sortWithinPartitions`, 16M
target) was written by the since-deleted `build_layout.py`; its SF100/SF1000 numbers survive under
*Findings* and cannot be re-checked. `duckpart` (delta_rs at the 143k-row geometry) and `vcluster` (V-Order + liquid
clustering) were removed on 2026-09-08 — neither was ever built or benchmarked. `nosortt128`
(`default` + `delta.targetFileSize` = 128 MB) and `nosort4mt128` (both) were removed on 2026-09-09:
`nosortt128` WAS built at SF100 and its geometry is recorded above — the byte target took the write
and both row caps went dead — but it was never benchmarked and its schema was dropped, because the
question it existed to answer is settled on portability grounds rather than speed. Rows are portable
across table widths and bytes are not, so a byte target could only ever have been a clustering-side
option -- and after `clustersn` (2026-09-09) it is not in the recipe at all. `nosort4mt128` was
never built. `default2rg` (6M row groups TWO to a 12M-row file, via
`spark.hadoop.parquet.block.row.count.limit`) was withdrawn on 2026-09-09 -- and it is the arm that
earned its own withdrawal. It WAS built and benchmarked at SF100, 3 load tests, 1,440 rows still in
`perfresults3`, and it measured INDISTINGUISHABLE from `default` at one group per file
(32.8 / 10.0 / 8.1 s against 32.4 / 9.4 / 7.9, p50 196 against 207 ms). Its geometry was exactly as
asked: `catalog_sales` 13 files / 25 row groups, `store_sales` 24 / 47, 1.92 and 1.96 groups per
file, every full group at 6M. So the only thing the row-group key can express that the file cap
cannot -- several groups inside one file -- buys nothing, and the key left the recipe with the arm.
See *Two ways to set a segment, and why the recipe uses the older one*. `nosort4m` (`default` at a 4M row-group cap) was removed on 2026-09-09: it WAS built,
read and benchmarked -- 3 load tests, 1,440 rows still in `perfresults3` -- and the numbers were not
an improvement on `default` at 6M. What it delivered geometrically: the 4M cap exactly, 2.8 groups per
file against the older `default` build's 1, 69 and 37 row groups against 47 and 24 (about 1.5x the
segments), the dictionary intact at 99.8 % of bytes on both facts, and the ragged tail IMPROVING from
10.6 % of groups to 7.2 % on `store_sales`, because the remainder is one per task however the groups
are cut. So the geometry was delivered and it bought nothing.

**And that is the general shape, which matters more than the arm.** Every arm in this project with
SMALL row groups and NO useful ordering is slow: `nosort4m` at 4M is no better than `default` at 5.6M
(7.9 s), `duckdb` at 2.4M is 14.2 s, `vonly` at 1.5-2.9M is 11.7 s. Every arm with small row groups
AND an ordering is fast: `cluster` at 2.0M is 2.5 s, `ducksort` at 2.1-2.8M is 2.4 s. **Smaller
segments pay only once the rows are ordered** -- more segments give VertiScan more parallel work, but
without an ordering there is nothing to eliminate and the extra segments are pure overhead. Earlier
notes here inferred "warm favours small segments" from the 1,823-segment `partition` and `vorder`
arms, which are partitioned by the date key and therefore ORDERED; that inference was confounded and
`nosort4m` is the same-writer control that shows it.

**The `partition` arm is a CONTROL. Nothing here recommends it.** Hive-partitioning a fact table by
date is bad practice on Databricks, and the measurements agree: it is the worst cold arm in the
project on either engine, 159.2 s at SF100 and 912.2 s at SF1000, because one date per file means
1,823 tiny row groups and 1,823 segments to transcode.

It exists for one job. It reproduces the paper's Fabric geometry under parquet-mr instead of
V-Order, so `partition` against `vorder` holds the geometry, the segment count and the in-file
ordering fixed and leaves the WRITER as the only variable. That pair is the whole of *The 2×2
closes: cold is the dictionary, warm is the partition* below, and without it V-Order's dictionary
advantage would be asserted rather than measured.

**The recommendation is unchanged: one `CLUSTER BY` on a single key.** It buys the same date
ordering that makes row groups eliminable, without the 1,823-segment cold tax — a global sort's
benefit at a fraction of its cost. It is the arm that actually reached V-Order's steady state at
SF100, 2.5 s against 2.4 s, at 37.5 s cold against `partition`'s 159.2.

## What it prefers

- **Two priorities, in this order.** Priority 1 is row-group geometry (uniform, ~6M rows, inside
  the window) plus dictionary encoding on every column — reachable by cluster config and table
  properties alone, with the user's code left as `df.write.saveAsTable()`. Priority 2 is row
  order, which costs a per-table decision: a global sort, liquid clustering, Hive partitioning,
  Z-order and the sort V-Order applies inside a row group are all the same lever, differing in
  who does the work and when, not in what the engine sees. Priority 1 is the deliverable;
  priority 2 measures what ordering adds on top of it.
- One parquet row group = one VertiPaq segment. Usable window 1M–16M rows.
- **Inside that window, aim LOW: 2M–5M rows per segment.** The window is the constraint; this is the
  optimum, and it is measured. Warm favours small segments by 3–4x (the 1,823-segment arms settle at
  ~2.5 s against 8–12 s for the big-row-group arms; `cluster` at ~2M medians 131 ms against `default`
  at ~5.6M with 213 ms; `default` at ~5M beat `layout` at 16M on every scan-bound query) — segment
  elimination, plus more parallel work units for VertiScan under concurrency. Cold pulls the other
  way by 2.4x, but cold is one model load and is dominated by the dictionary (2.2x) rather than by
  geometry. The floor is not 1M in practice but ~2M, because the dictionary breaks first: measured,
  `catalog_sales` at 1.11M rows per group lost 19.6 % of its bytes to plain encoding.
- **Facts are 25–75 B/row, and facts are the only tables whose geometry matters.** Measured here:
  `store_sales` 35–36, `catalog_sales` 57–59 — foreign keys and measures. A 300–500 B/row table is a
  dimension; it is small, it gets one file, and no segment decision binds on it. Sizing a geometry
  default for wide tables optimises for a case that does not arise — that error is what produced the
  rejected 512 MB target.
- **F64 caps a Direct Lake table at 1.5B rows**, which bounds segment count rather than constraining
  it: at 2–5M rows per segment the largest table a common capacity will hold is a few hundred
  segments. Segment count is never the binding consideration; segment SIZE is.
- **6M is not claimed to be the best row-group size.** It is a compromise, chosen as an input, not
  measured as an optimum: between cold (fewer, bigger segments = less per-segment transcode
  overhead) and hot (more, smaller segments = more parallel work units per query), and it has to
  hold across table sizes without a per-table decision. Smaller sizes will win some hot queries
  (measured: default at ~5M beat layout at 16M on every scan-bound query) and bigger ones will win
  cold; 6M is where one number serves both well enough. It is a deliberate constant, not a target
  to re-tune.
- Direct Lake does not re-sort at transcode: parquet row order becomes VertiPaq row order.
- Uniform row groups within a table beat hitting an exact size. Ragged groups are the failure mode.
- Only the first sort key eliminates row groups. A low-cardinality second key is what shrinks
  memory (RLE runs); a high-cardinality one shrinks the file and gains VertiPaq nothing.
- Optimized Writes is the only config-only knob that repartitions regardless of the user's
  DataFrame. Its bin counts shuffle BYTES, so rows per file follows row width.
- A row cap fires only on tasks already over it, leaving one ragged tail per task — ceiling only.
  **That tail is irreducible**: measured identical (4,800,991 rows) whether the cap is on the file or
  on the row group. Nothing in this config removes it; only coarser tasks make it rarer.
- **Three lines state the whole geometry, and all three are about keeping ROWS in charge.**
  `spark.sql.files.maxRecordsPerFile` = rows per file, `parquet.block.row.count.limit` = rows per row
  group, `parquet.block.size` = 2 GiB so the BYTE trigger never fires first. 12M / 6M = 2 groups per
  file, on any table width, with no arithmetic anywhere in the write.
- **parquet-mr closes a row group on BYTES by default** (`parquet.block.size`), which is why the
  recipe used to reach a row-denominated group size the long way round: make bytes inert, cap the
  *file* at 6M rows, one file is one group. `parquet.block.row.count.limit` — "the maximum number of
  rows per row group", default `Integer.MAX_VALUE`, parquet-java **1.16.0**+; DBR 19 ships 1.17.0 —
  removes that indirection. **Measured honoured on DBR 19, 2026-09-07**: variant C wrote 3 groups per
  file with 4 of them exactly 6,000,000; variant F did the same from configuration alone, at 99–100 %
  dictionary coverage. So "there is no reliable way to ask Spark for several row groups in one file"
  is false, and was the answer given to the reviewers before this was measured.
- **Set the two row caps EQUAL and the second one does nothing.** Measured: variant D
  (`maxRecordsPerFile` 6M + group cap 6M) is indistinguishable from variant B (file cap alone) — 6
  files, 6 groups, one per file, same 232.2 MB. Both are upper bounds, and the file close forces the
  group closed on the same row, so the group cap never gets to bind. It also cannot defend against
  the byte split: a row cap only ever closes a group EARLIER, so `parquet.block.size` at 2 GiB is
  still the only thing standing between the recipe and bytes deciding.

## Dictionary encoding: the two writers do not make the same decision

- Fabric Spark's V-Order writer dictionary-encodes **every column, regardless of cardinality**: 100 %
  of bytes on both facts at SF1000 (23/23, 34/34 columns), including `ss_ticket_number` and
  `cs_order_number`, which are near-unique inside a row group and cannot pay for a dictionary on
  any cost test. It is not parquet-mr; it does not run parquet-mr's fallback.
- Databricks (parquet-mr, Photon off) runs `FallbackValuesWriter`: the dictionary is dropped for
  the whole column chunk if it exceeds `parquet.dictionary.page.size`, **or** if on the first page
  `encoded + dictionary` is not smaller than raw. The verdict is taken once and never revisited.
- That first page closes at `parquet.page.size` (1 MB default) or `parquet.page.row.count.limit`
  (**20,000 rows** default), whichever first. So the dictionary's whole cost is charged against a
  20k-row sample and a wide column loses every time. Measured: 37 % (default) to 60 % (layout) of
  fact bytes dictionary-encoded; every decimal measure and every high-cardinality key is PLAIN.
- The budget is not the lever: layout at a 32 MB `dictionary.page.size` beat default at 64 MB.
- **Smaller row groups move a high-cardinality column to the LOSING side, not the winning one** —
  measured at SF100, 2026-09-07, and it is the opposite of what this section used to claim. The
  `partition` arm's 143k-row chunks hold ~50 % of fact bytes as dictionary (6 of 24 columns PLAIN on
  `store_sales`, 11 of 35 on `catalog_sales`); the 6M-row arms hold far more. The casualties are
  every decimal measure — `ss_ext_list_price` 766 MB, `ss_net_profit` 750 MB,
  `ss_ext_wholesale_cost` 747 MB, and the `cs_*` equivalents. The reason is arithmetic: in 143k rows
  a price column has ~130k distinct values, so the dictionary IS the data and the cost test rejects
  it; in 6M rows the same values repeat 10× and it wins easily. Sorting still helps a key column —
  layout kept `ss_item_sk` where default lost it.
- **So the recipe's 6M row groups and the paper's partition-by-date are in direct conflict over the
  dictionary, and V-Order is what resolves it.** The Fabric arm holds 100 % dictionary at the same
  143k-row geometry where parquet-mr holds ~50 %. That is the one thing in this whole comparison
  that a non-Fabric writer cannot reproduce.
- A parquet fallback is invisible to a dictionary-page count: parquet-mr discards the dictionary
  rather than mixing, so a fallen-back chunk reads `PLAIN` with zero dictionary pages. In
  `layout_stats` that is the `plain` row; the `fell_back` state (dictionary + PLAIN together) never
  fires on parquet-mr output and should not be read as "no fallback happened".
- What parquet leaves PLAIN, VertiPaq dictionary-builds at transcode: time and memory on every cold
  read, on the widest columns of the table. **Priced at SF100: 2.2×** — `vorder` (100 % dictionary)
  loads in 70.0 s against `partition`'s 156.9 s, with the geometry, the segment count and the
  within-partition sort all held equal. See the 2×2 under Findings.

## The few things it genuinely cannot read

- **v2 checkpoints.** With them off, liquid-clustered tables read fine.
- That is the list. Liquid clustering blocks nothing: a clustered table is writer 7 and, as
  documented, reader 3 (deletion vectors on), and mirroring and Direct Lake take both.
  `v2Checkpoint` is on by default for LC tables from DBR 14.3 LTS — set
  `delta.checkpointPolicy='classic'` before `CLUSTER BY`, or `DROP FEATURE v2Checkpoint` after;
  the docs list that property as the override, with "no effect on liquid clustering behavior".
  Reader 1 is reachable only the documented way — `enableDeletionVectors=false` as a TABLE property
  on an existing table, then `ALTER TABLE ... CLUSTER BY` — and DBR 19 matched the docs: created
  empty and clustered by ALTER, reader 1; `CREATE TABLE ... CLUSTER BY ... AS SELECT` with only the
  session default `properties.defaults.enableDeletionVectors=false`, reader 3. A session default
  is not the documented override. **That distinction no longer bites** — the recipe stopped
  setting the property on 2026-09-07, so every table is reader 3 on purpose. The
  create-empty-then-`ALTER` sequence stays for its other reason: pinning
  `checkpointPolicy=classic` before the keys exist. LC also turns row tracking on by itself, which
  costs nothing in the files — 21 columns in, 21 out, no materialised row-id.
- **Compression is not set by the recipe.** DBR 19 stamps `delta.parquet.compression.codec=zstd`
  on its own; that is what every arm now gets. The `layout` arm and `rowgroup_probe` used to force
  SNAPPY, so every cross-arm comparison made before 2026-09-06 compared a snappy table against zstd
  ones -- an uncontrolled variable in file size, and therefore in anything read off bytes.
- Deletion vectors are **not** a Fabric blocker -- Fabric reads them -- and as of 2026-09-07 the
  recipe no longer turns them off. Measured on DBR 19 (SF10, 2026-09-06) with the property unset,
  Databricks defaults them ON and the new table lands at `delta.enableDeletionVectors=true`,
  `delta.feature.deletionVectors=supported`, **`minReaderVersion=3`** (writer 7). That default is now
  the intent. **The reviewers' argument, and it is the right way round:** without DVs a later
  DELETE/UPDATE/MERGE reverts to copy-on-write and rewrites whole parquet files; new files are new
  segments, so VertiPaq re-transcodes every column in them. With a DV the parquet is untouched and
  the segments already resident survive. Turning DVs off buys reader v1 -- any Delta reader can open
  the table, not only ones implementing DVs -- and charges the difference to the one engine this
  whole repo is aimed at. Reader v3 is the price, paid on purpose. Two older claims are dead: that
  "Fabric's pinned Delta reader refuses a table whose protocol lists deletion vectors" (wrong about
  Fabric), and that reader v1 is worth defending here (a courtesy to other readers, not a Direct
  Lake requirement).
- `deletionVectors` in `tableFeatures` means the FEATURE is enabled, not that any deletion vector
  exists. Do not assert on it as if it were evidence of a rewrite -- assert on `minReaderVersion`.
- LC and partitioning + Z-order are mutually exclusive on the same table. V-Order and LC compose
  (file-internal layout vs cross-file value ranges).

## Fabric

- `readHeavyForPBI` **is** `vorder=true` + `optimizeWrite.enabled=true` + `optimizeWrite.binSize=1g`.
- The resource profile is applied AFTER session conf, so anything `%%configure` sets that the
  profile also defines gets overwritten. ZSTD is not in the profile; set it at runtime.
- An API-triggered pipeline cannot run a notebook that needs a user token — trigger it in the UI.

- **SF1000 fact builds are long, and `spark.task.cpus` is the multiplier.** A SF1000 fact write on
  3 x E16ds_v5 runs 30-45 min each against ~9 min for the whole of SF100 on 4 x E8ds_v5. The work is
  one shuffle, Optimize Write packing 4 GiB bins, plus ZSTD with 64 MB pages on every row.
  `spark.task.cpus=4` gives 12 concurrent writer tasks instead of 48. For a faster SF1000 build the
  lever is MORE nodes, not bigger ones: with task.cpus=4 half the cores on a 16-core executor sit
  idle, so 6 x E8ds_v5 gives the same memory per task with more task slots inside the same 100-core
  regional quota.

- **Wide facts OOM the writer** -- see *the dictionary settings are expensive to WRITE*, at the top.

## Measurement

- **The protocol is the paper's**: one model, three back-to-back 24-query load tests over it, then
  delete. Run 1 is the first touch; runs 2-3 are steady state. `run_index` is a column in
  `perfresults3`; the old `perfresults` holds the previous protocol (a probe plus one warm pass,
  a fresh model per run) and the two are never unioned.
- **Never quote a pooled cold median.** Run 1's 24 queries are not one population: q1 pays the
  transcode and q24 runs warm behind it. Quote run 1 per query, or as the **suite sum** — the sum
  of the per-query p50s over the 15 visual queries, which is the paper's own metric and the one
  `benchmark/results.py` reproduces their 99.5 / 5.0 / 6.2 and 114.8 / 142.7 / 166.9 with exactly.
- Take the per-query median FIRST, then combine queries (two-step). One pooled median weights a
  query by how many readers finished it.
- Deleting a semantic model clears VertiPaq residency, **not** the OneLake file cache. That cache
  is a feature and it is left ON — the paper's mirrored runs had it off (raised with its author),
  which is part of why that arm never warms in its CSV. The consequence for reading our numbers:
  run 1 measures the VertiPaq transcode, and only a first-ever run at a scale factor is also cold
  from storage. A repeat trigger's run 1 reads parquet the capacity already cached — which
  understates it, and understates it in the mirrored arm's favour, since that is the arm paying the
  North Europe → West Central US hop.
- Steady state is region-insensitive; run 1 is not.
- **The paper's numbers are quoted for shape, never compared bar to bar.** They were taken on its
  own F128, region and hour. Every comparison made here is between arms measured on this capacity
  with the paper's own queries and model.
- SF100 fits Direct Lake on F64; SF1000 needs F128, as in the paper.
- **Never quote the SF1000 `default` median on its own.** 60.6 s vs the sorted arm's 96.6 s reads as
  "the sort is worthless"; the p95 (428.8 vs 283.3) says the opposite on the queries the sort is
  for. Median and tail must be quoted together, or the per-query split instead.
- Run 1 is a full suite and clears all-24-or-out like any other run, so `results.py` reports it
  (it used to show no cold number at all, because a one-query probe could never satisfy that
  filter). The cost: one unrecovered query failure drops that thread's run 1, and enough dropped
  threads drop the whole run 1 load test — a silent loss of exactly the evidence the protocol
  exists for. That is why the dropped-load-test report is printed, not optional.
- `results.py` is strict on purpose: a run counts only if all 24 queries succeeded, a rung that
  asked for 20 users and got 8 is dropped, and arms are matched on exact labels so two Databricks
  arms can never fold into one number.
- **The headline table is [summary.csv](summary.csv) at the repo root**: the results notebook's
  `layout` table, one row per scale factor x arm x fact -- geometry, dictionary, ordering, the
  three run totals and the cold/warm p50/p95/max -- produced by running that notebook's cells
  verbatim over `perfresults3` and the `layout_stats` footer export. Regenerate it, never edit it.
- **`probe_results/` is local-only** (gitignored since 2026-09-08): the probe JSON dumps and the
  `layout_stats` pulls stay on the build machine. Every probe number used here is quoted in the
  text, with the Databricks job run id where there is one.

## Findings

- The paper's own CSV: at SF100 / 20 users its V-Order arm warms (99.5 / 5.0 / 6.2 s) while its
  mirrored arm never does (114.8 / 142.7 / 166.9 s, degrading). A fixed mirroring tax cannot
  produce that shape; layout can. The metric behind those six numbers is now pinned down and
  reproduced by `benchmark/results.py --table dbo/perfresults`: **the sum of the per-query p50s
  over the 15 visual queries, per run**, medians taken per query first.
- SF100, same F128, same hour: `layout` 137 ms warm / 50.0 s cold vs `vorder` 156 ms / 58.4 s.
  Measured under the OLD protocol (fresh model per run, cold = one probe query by one reader), so
  not comparable to a run 1 under the current one. To be re-measured, not reinterpreted.
- SF1000 `layout`: 1,440 queries, 0 errors, warm median 96.6 s, cold transcode 240 s, at a scale
  the paper reports its mirrored arm did not complete at on F128. The cold number carries a
  regional handicap: the mirrored files sit in North Europe, the capacity in West Central US.
- ⚠️ **`layout` is the LEGACY arm and its tables are gone**, so nothing below can be re-checked.
  It was built with `repartitionByRange` + `sortWithinPartitions`, NOT a bare `orderBy` — and a
  bare `orderBy` is measured to produce no ordering at all (see *How an ordering reaches a Delta
  file*). Do not read the two as the same thing.
- **SF1000 `default` vs `layout`, 3 × 20 users, 900 visual runs each, 0 errors, no degradation
  across the three load tests: the config-only arm BEATS the globally sorted one on the median,
  60.6 s vs 96.6 s — and loses badly on the tail** (avg 137.5 vs 103.6, p90 396.7 vs 192.1,
  p95 428.8 vs 283.3). Each of those three load tests recreated the model, so "no degradation"
  there is three independent first touches, not a warming curve; under the current protocol the
  same claim becomes a measured `drift_x`.
- That whole split is **three queries**: 6, 14 and 24, all the same visual (*Store Revenue Time
  Analysis*, the date-filtered one), at 2.17× / 2.80× / 2.64× worse without the sort. The other 12
  are a wash or better on `default` (q23 0.67×, q9 and q22 0.77×, q5 0.82×). The median sits in the
  12, the tail is the 3.
- So **a global sort buys exactly one thing, on one query shape**: row-group elimination on the
  date. It costs on everything else — which is the case FOR the `cluster` arm (that same date
  ordering, no global shuffle), not against ordering as such.
- Probable cause of `default`'s edge on the non-date queries: 5.37M rows/group vs the layout arm's
  16M target at SF1000, so ~3× the segments for VertiScan to spread over 20 users. Follows from the
  two build reports, not from a query-level measurement — treat as likely, not proven.
- **SF1000 `default` as built** (2026-09-05, 29 min on 3 × Standard_E16ds_v5, DBR 19, Photon off):
  store_sales 2,620,785,279 rows in 488 files (5.37M rows, 188 MB each, 91.7 GB),
  catalog_sales 1,425,579,810 rows in 363 files (3.93M, 222 MB, 80.7 GB), 8 dimensions one file
  each; row counts equal to the paper. Protocol as built: reader v1, features
  `appendOnly, invariants`, no deletion vectors, classic checkpoints, no `OPTIMIZE` in the
  history. Reader v1 there is history — the recipe stopped stamping deletion vectors off on
  2026-09-07, so tables built after that are reader 3. No deletion vector exists on an
  append-only build either way, so the parquet is identical and these numbers stand.
- **Geometry as Fabric sees it** (`fabric/verify_layout.py` over the mirror, `layout` arm): 100 %
  of row groups inside 1M–16M at SF10 and SF100, one row group per file, zero overlapping ranges
  on the sort key, dictionary encoding declared in the footer on 18/24 and 25/35 columns. For
  context, the paper measured 96–100 % of row groups in that window for its Z-order-only arm and
  0 % for its partitioned Fabric arm.

## The 2×2 closes: cold is the dictionary, warm is the partition — MEASURED at SF100, 2026-09-07

Four arms, one scale factor, the same 24-query suite over 20 readers, three back-to-back runs over a
model created minutes earlier. `suite_s` is the paper's own metric: the sum of the per-query medians
over the 15 visual queries, i.e. what one reader spends on a full pass.

| arm | writer | row groups | rows/group | dictionary | **cold (run 1)** | run 2 | run 3 |
|---|---|---|---|---|---|---|---|
| `vonly` | Fabric, V-Order | 89 | 2.9M | 100 % | **29.9 s** | 11.0 | 11.7 |
| `default` | Databricks, recipe | ~46 | 6M | high | 31.7 s | 9.6 | 8.0 |
| `vorder` | Fabric, V-Order + partition | 1,823 | 143k | 100 % | **70.0 s** | 2.3 | 2.6 |
| `partition` | Databricks, partition | 1,823 | 143k | **~50 %** | **156.9 s** | 2.7 | 2.5 |

**The two factors are separable, they are roughly equal in size, and they pull in opposite
directions.**

- **Warm is the partition, and V-Order is worth nothing there.** Both 1,823-segment arms settle at
  ~2.5 s; both large-row-group arms sit at 8–12 s. That is 3–4× in favour of one file per date, and
  `partition` (no V-Order, half its dictionary gone) matches `vorder` exactly — 2.5 against 2.6.
  Steady state is decided by segment elimination: the partition arm has **zero overlapping row
  groups on the date key**, the only arm in the project that does.
- **Cold is the dictionary.** `vorder` and `partition` have the same geometry, the same 1,823
  segments and the same within-partition sort. The only difference is that V-Order holds a 100 %
  dictionary at 143k rows per chunk and parquet-mr holds ~50 %. That is 70.0 s against 156.9 s —
  **2.2×**, paid every time the model is loaded. VertiPaq builds its own dictionary at transcode; a
  parquet dictionary page is a head start it reuses, and a PLAIN column means hashing 262M raw
  decimals instead.
- **The geometry itself also costs cold, separately.** `vonly` → `vorder` is the same writer, the
  same V-Order and the same 100 % dictionary, differing only in row-group size: 29.9 → 70.0 s,
  **2.4×**. 1,823 segments cost more to transcode than 89.

The two multiply: 29.9 → 156.9 s is 5.3×, and it factors as ~2.4× geometry × ~2.2× dictionary.

**This accounts for the mirrored-arm gap the paper measured, and neither half of it is about which
engine wrote the files.** Its layout takes both penalties at once — small row groups from partitioning, and no
V-Order to hold the dictionary at that size. Only the second is a genuine Fabric advantage. The
first is a layout choice available to any writer, and it is a *good* one if the model stays
resident: 3–4× on every query after the first load.

### The within-partition sort never reached the files — MEASURED 2026-09-08, and the code is gone

**This section replaces "The within-partition sort was never the confound", which was wrong.** That
section said the `partition` arm had been rebuilt with
`sortWithinPartitions(ss_sold_date_sk, ss_addr_sk)`, that the rebuild measured 156.9 s cold against
the unsorted build's 157.3 and 165.3, and concluded the in-file ordering was worth nothing. The
first half is what the code asked for. The parquet says it never happened, so the two builds were
the same build and the experiment compared a thing to itself.

Read back in physical file order (single-threaded scan, one date partition, `ss_addr_sk`):

| file | non-decreasing | runs / distinct |
|---|---:|---:|
| SF1000 `partition` store_sales | 95.42 % | 1.022 |
| SF100 `partition` store_sales | 95.41 % | 1.012 |
| SF1000 `default` source, one file | 95.42 % | 1.046 |
| SF1000 Fabric `vorder` store_sales | 50.07 % | 11.141 |

A sort is 100.00 % and exactly 1.000. The `partition` arm is neither, and its signature is the
source's: values sit in contiguous blocks because they already did, and about half the block
boundaries go DOWN. The read order is sound — the partition column in the same file measures one
run, one distinct value, 100 %.

**Why it did not land is NOT established, and either way it is our bug, not Spark's.** The write
set the session switch `spark.databricks.delta.optimizeWrite.enabled = false` around a
`repartition(key).sortWithinPartitions(key, skey)` and a partitioned `saveAsTable`; the table
carries `delta.autoOptimize.optimizeWrite = true` from the recipe's property defaults, read back
off the live table. An earlier version of this section said the table property beat the session
switch and so the bin-pack shuffle ran after the sort. That was inferred, not measured: no
executed plan was captured for that build, and Databricks documents the opposite precedence —
`false` at the session level overrides optimized writes "for all tables modified in the
workload". The docs also say UC-registered partitioned tables get optimized writes on CTAS and
INSERT, which is what that write was. So an optimized-write exchange above the sort is still the
likely cause, exactly the shape *How an ordering reaches a Delta file* predicts, but nothing here
verified it, and the code is gone. What is certain is ours: the write was never checked against
its plan or its files before its numbers were quoted. The sort cost a shuffle and a sort on every
build and bought nothing.

**Removed from `build_spark.py` on 2026-09-08.** The `partition` arm is now `partitionBy(date_key)`
and nothing else, and its label is `partition per date`. The existing tables are NOT stale: the sort
never changed a byte, so what is built is what an unsorted partition build produces. Only the
`layout.within_partition_sort_key` property they carry is a lie, and it is no longer written.

**What this costs the 2×2 above.** The `partition` vs `vorder` pair differs by TWO things, not one:
V-Order, and the row order inside each file. Fabric's `OPTIMIZE ... ZORDER BY` genuinely rewrote its
1,823 files and interleaved them (11.141 runs per distinct key is interleaving, not sorting);
the Databricks arm has no in-file ordering at all. So that pair **bounds** the V-Order effect rather
than isolating it. The direction still favours the dictionary explanation — the arm with the
*worse* in-file locality is the faster one — but the 2.2× is no longer attributed to the dictionary
by elimination. Nothing else in the 2×2 moves: the geometry factor (`vonly` → `vorder`, same
writer, same encoding) never involved a sort.

**Neither a Z-order nor a local sort has ever reached parquet written by Databricks in this
project.** `OPTIMIZE ... ZORDER BY` commits nothing at this geometry (below), and the sort that
stood in for it was deleted by optimized writes. The only orderings measured to survive a Spark
write here are `CLUSTER BY` on write and the Hive partition itself.

## The clustered arm at SF100 — MEASURED 2026-09-07, and it reaches V-Order

One experiment, 3 x 20 users against one model, 20,160 rows surviving all-24-or-out, zero failures,
zero short rungs, all 14 model lifetimes carrying their three runs, value check passing on every
arm. `benchmark/results.py` over `perfresults3`.

**Steady state, runs 2-3, the fair comparison** (no storage on the path):

| arm | suite s run2 | run3 | median ms | p95 s | max s |
|---|---:|---:|---:|---:|---:|
| `cluster` | 2.7 | **2.5** | 106 | **0.326** | **0.686** |
| `default` | 9.4 | 7.9 | 145 | 1.885 | 9.134 |
| `partition` | 2.7 | 2.6 | 101 | 0.350 | 0.859 |
| `vonly` (V-Order, no layout) | 11.0 | 11.7 | 128 | 2.763 | 12.049 |
| `vorder` (the paper's Fabric arm) | 2.3 | 2.4 | 88 | 0.304 | 0.797 |

- **The clustered arm warms to V-Order's steady state: 2.5 s against 2.4 s.** `default` never gets
  below 7.9 s. That is the headline this project was built to test, and clustering is what delivered
  it -- the same rows, the same recipe, one `CLUSTER BY` on the date.
- **It fixes the tail, which was `default`'s real weakness.** p95 falls 1.885 s -> 0.326 s (5.8x) and
  the worst query 9.134 s -> 0.686 s (13x). The unclustered arms' tails were the three date-filtered
  queries queueing behind whole-table scans; with zero overlapping row groups there is nothing to
  scan.
- **The date queries collapse, exactly where the layout predicted.** Per query at 20 users,
  `default` -> `cluster`: q6 4,332 -> 409 ms (**10.6x**), q14 1,036 -> 289, q24 775 -> 294. All three
  land within 10-15 % of V-Order's 405 / 252 / 263.
- **And it wins broadly, not only on dates.** q2 254 -> 140, q5 213 -> 130, q13 223 -> 103,
  q23 185 -> 105. Reasoning, not measured: 128 files of ~2M rows against `default`'s 47 of ~5.6M is
  2.7x the segments, so VertiScan has more parallel work at 20 users.
- **Median over the 15 visual queries: `cluster` 131 ms, `default` 213, our V-Order 109.** The clustered mirrored arm is within 20 % of our own V-Order, where `default` was 95 %
  behind it.
- **Cold favours the clustered arm too**, though run 1 is not like-for-like (mirrored storage is
  North Europe, capacity is West Central US): 37.5 s against V-Order's 70.5 s and `partition`'s
  159.2 s. `partition`'s 1,823 tiny files are the most expensive thing here to transcode.
- **`vonly` is the control that makes the point.** V-Order encoding with no layout work is the
  WORST arm in steady state -- 11.7 s, p95 2.763 s. V-Order alone does not buy this; layout does.
- **Caveat, and it is the important one: nobody designed this geometry, and it is not fully
  understood.** The recipe asks for 6M-row groups as a CEILING. The clustered files never reached
  it — 2.05M on `store_sales` and 1.11M on `catalog_sales` — because the clustering exchange sizes
  the file in bytes and the row caps never fire. That the smaller groups ALSO kept 100 % dictionary encoding on
  `store_sales` is not something the recipe arranged -- and on `catalog_sales` at 1.11M it did not
  hold, 19.6 % of bytes fell to plain. So this arm's win rests on a group size the recipe does not
  set and has not been swept. **Superseded 2026-09-09:** `clustersn` set the 128 MB target (and snappy) on the same
  `CLUSTER BY` and tied this arm at steady state. The target lifts `catalog_sales` to 2.23M rows and
  96.8 % dictionary bytes and changes nothing else measured. So this unchosen geometry IS the
  recipe's clustered geometry, and the caveat is closed: the number the recipe does not set is a
  number that does not matter to the clock. The chart labels say what clustering did, which is
  still right.
- Caveat: one experiment for `cluster` and `vonly` against two for `default`. Repeats pool
  into the same medians by design, so these can move.

## `delta.targetFileSize` BEATS the row cap on a non-clustered write too — MEASURED at SF10, 2026-09-07

Written up because LEARNING.md previously asserted this on reasoning alone ("a second lever fighting
the row caps"). It is now measured, and the reasoning was right.

`notebooks/cluster_config_probe.py` variants `ns_recipe` / `ns_tfs128` / `ns_sess128`, run
781717025329052, 4 x E8ds_v5, DBR 19, SF10 `store_sales` (26.2M rows). The same write as the
`default` arm — **no `CLUSTER BY`** — under the recipe's `spark_conf`, changing only how the byte
target is supplied.

| variant | how the target is set | files | rows/file min–max | at the 12M cap | MB avg |
|---|---|---:|---|---:|---:|
| `ns_recipe` | not set | 3 | 2.21M – 12.00M | **2** | 280.6 |
| `ns_tfs128` | table property `delta.targetFileSize` | 6 | 3.13M – 4.92M | 0 | 142.6 |
| `ns_sess128` | session `...properties.defaults.targetFileSize` | 6 | 3.13M – 4.92M | 0 | 142.6 |

- **The byte target wins and the row cap goes inert.** `spark.sql.files.maxRecordsPerFile` was
  12,000,000 and `optimizeWrite.binSize` 4096 MiB in ALL THREE variants — verified from
  `spark.conf` at write time, recorded in `in_force`. The only difference is the target, and it
  moved the write from 2 files at the 12M cap to 6 files that never reach it.
- **The session form and the table-property form are the same thing.** Byte-identical outcomes: 6
  files, 3,131,376 / 4,919,612 rows at the extremes, 142.6 MB average. So the session default is not
  a weaker variant to be dismissed — it works on the write path.
- **This is why a byte target, if ever used, is a TABLE property and never a config line.** It is not
  cluster-only; set session-wide it would silently reshape every table written on the cluster,
  including the arms whose whole point is the row caps.
- **CONFIRMED AT SF100, 2026-09-09, then the question was CLOSED.** A full unclustered arm was
  built with the recipe's row caps in force (`maxRecordsPerFile` 12M, `parquet.block.row.count.limit`
  6M) plus `delta.targetFileSize` = 128 MB on the facts. The byte target took the geometry outright
  and **both row caps went dead**: `store_sales` 49 files at 5,348,620 rows and 175.7 MB,
  `catalog_sales` 40 files at 3,563,943 rows and 199.6 MB. Neither fact reached the 6M group cap, so
  every file is one row group, and neither came near the 12M file cap. The 128 MB ask over-delivered
  to 176-200 MB, the same ~35-55 % overshoot the clustered arm showed.
- **The recipe does NOT adopt it, and the reason is portability, not speed.** Rows are portable
  across table widths and bytes are not: the same 128 MB produced 5.35M rows on the 36 B/row fact
  and 3.56M on the 59 B/row one, and on a narrow table it would sail past 16M or, on a small one,
  drop under the 1M floor — silently, in both directions. The row caps hold the same segment on any
  table. So a byte target was only ever a clustering-side option, and after `clustersn` (2026-09-09)
  it is not in the recipe at all. The arm built to test this was dropped without being benchmarked
  (see *Gone, and why*); the geometry above is the whole of what it was built to find out.
- Append seconds (139.4 / 32.9 / 30.9) are NOT comparable: `ns_recipe` ran first and paid the
  session's warm-up. No timing claim is made here.

## Auto compaction did not fire in three probes, and the switches were not what stopped it -- MEASURED at SF10, 2026-09-07

`notebooks/cluster_config_probe.py` variants `ac_off` / `ac_on` / `ac_prop`, runs 827422124538819
and 272011068317823, single node
E8ds_v5, DBR 19. Same unclustered SF10 `store_sales` write both times with
`maxRecordsPerFile = 500000`, giving 53 files of ~21 MB -- past `autoCompact.minNumFiles` (50) and
all far under the 128 MB `autoCompact.maxFileSize`. `ac_on` additionally set
`spark.databricks.delta.autoCompact.enabled = true`. Written to a VOLUME as path tables so pyarrow
could read the footers directly.

| | files | rows/file | MB/file | row groups | rows/group | dict chunks | dict bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| `ac_off` | 53 | 206,837-500,000 | 9.3-21.5 | 53 (1 per file) | 500,000 | 83.2 % | 64.4 % |
| `ac_on` | 53 | 206,837-500,000 | 9.3-21.5 | 53 (1 per file) | 500,000 | 83.2 % | 64.4 % |

**Byte-identical, and no compaction commit in either history** (`WRITE`, `SET TBLPROPERTIES`,
`WRITE`, and nothing else). The session switch did nothing.

- **What this pair was first read as, and why that reading is withdrawn.** Both tables carry
  `delta.autoOptimize.autoCompact = false` as a TABLE PROPERTY, stamped by the config's
  `spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = false`, and `ac_on` was
  written up as "the table property beats the session switch". `ac_prop` below — both switches
  true, still nothing — shows the pair never tested precedence at all: nothing fired under any
  setting. Databricks documents the precedence the other way round, for optimized writes and auto
  compaction alike: `false` at the session level "can be set ... to override ... for all tables
  modified in the workload". So the `...properties.defaults.` line is a default stamped on new
  tables, not a lock a later session cannot undo. The bare
  `spark.databricks.delta.autoCompact.enabled = false` session line was dropped from the recipe on
  2026-09-07 because it was never observed to change anything — which is still true, and is all
  the pair shows.
- **Why the recipe stamps table-property twins anyway**: a table written under this config keeps
  the property in a session that never carried the block. That is what a table property is for,
  and it is documented; it is not a measured result of this probe.
- **`ac_prop`: BOTH switches on, and it STILL did not fire.** Run 272011068317823, same 53 files of
  9.3-21.5 MB, with `delta.autoOptimize.autoCompact = true` confirmed on the table AND
  `spark.databricks.delta.autoCompact.enabled = true` in the session. History is `WRITE`,
  `SET TBLPROPERTIES`, `SET TBLPROPERTIES`, `WRITE` -- all four versions, no `OPTIMIZE`, and the 53
  files of 500,000 rows are untouched.

  So auto compaction did not run on DBR 19 under conditions that should satisfy it: 53 small files
  against a default `minNumFiles` of 50, every one far under the 128 MB `maxFileSize`, both switches
  explicitly true. **What this does NOT establish is that it never fires** -- a failure to trigger is
  not proof of a mechanism. Untested candidates for why: these are PATH tables in a Volume rather
  than catalog tables; the trigger may count only files added per partition by the committing
  transaction; DBR 19 may have moved this behaviour under predictive optimization entirely. Three
  attempts have now failed for three different reasons and no more compute is going into it without
  a specific hypothesis to test.

  **Practical consequence, stated carefully:** the recipe's two `autoCompact = false` lines have
  never been observed to prevent anything on DBR 19, because nothing has been made to happen. They
  cost nothing and the failure they guard against is real in principle, so they stay -- but they are
  now documented as unproven insurance rather than a measured fix. The measured disaster in this
  repo was `OPTIMIZE` by foreign compute, which is a different mechanism and IS guarded, by
  `ALTER SCHEMA ... DISABLE PREDICTIVE OPTIMIZATION`.
- Incidental, and worth remembering when reading the dictionary numbers elsewhere: at 500k-row row
  groups the dictionary holds only **83.2 % of chunks and 64.4 % of bytes**, against 99.7-99.9 % at
  6M. Consistent with the 1.11M `catalog_sales` result -- small groups cost dictionary coverage.


## `OPTIMIZE ... ZORDER BY` committed nothing on Databricks with one file per partition — MEASURED at SF100, 2026-09-07

The `partition` arm partitions `store_sales` by `ss_sold_date_sk`, which lands **1,823 files, one
per date, ~5 MB each** — the paper's Fabric geometry exactly. The paper then runs
`OPTIMIZE ... ZORDER BY (ss_addr_sk)` on top. Running the same statement on Databricks committed
**nothing**:

| | Delta history after the build |
|---|---|
| Databricks `partition` | `v0 CREATE TABLE AS SELECT` (1,823 files, 262,082,396 rows) · `v1 SET TBLPROPERTIES`. **No OPTIMIZE commit at all.** |
| Fabric `vorder` (the paper's arm) | `v0 CREATE OR REPLACE TABLE AS SELECT` (1,823 files, 262,082,396 rows) · **`v1 OPTIMIZE`, 1,823 files removed, 1,823 added** · `v2/v3 VACUUM` |

Same table, same row count, same file count, same statement — opposite outcome. Fabric Spark
rewrote every file to reorder the rows inside it (in OSS Delta's source a single-file bin is kept
when Z-ordering). Why Databricks committed nothing is **reasoning, not measured and not
documented**: the docs say only that Z-ordering "operates incrementally" and has no effect on a
partition with no new data, and the guess here is that its candidate selection found nothing to
pack in a one-file partition. The statement succeeds on both; only one of them does anything, and
nothing in the output says so — `DESCRIBE HISTORY` is the only place it shows.

**Consequence for anyone building the paper's Fabric layout on Databricks:** in this run, Table
4.6.1's in-file ordering was not reached by the same statement, and the cause is unconfirmed. The
attempt that followed put the ordering into the write instead —
`repartition(part_key).sortWithinPartitions(part_key, zorder_key)` — because a Z-ORDER on ONE
column is a sort, and that is all Fabric's OPTIMIZE did to a single-file partition. Sorting by the
partition column first matters: Spark's partitioned writer inserts a sort of its own on the
partition columns otherwise, and that would throw the address order away.

Optimized Writes has to be off for that write, because its bin-pack shuffle runs *after* a sort
and discards the order — see *How an ordering reaches a Delta file* below. The attempt used the
session switch and never checked the executed plan; the sort did not reach the files, and the
cause was never pinned down (see *The within-partition sort never reached the files*). The
per-write `.option("optimizeWrite", "false")` is the other documented form; it is equally
unverified here.

## How an ordering reaches a Delta file

An ordering only counts if it survives the write path. Under this recipe a plain
`df.orderBy(key)` does not: Databricks plans Optimized Writes as a repartition above the query, and
Spark's `EliminateSorts` deletes a global sort under a repartition, so the sort never runs and the
files come out in the writer's order. Verified on the executed plan at SF1000 and on the resulting
files, which were identical to an unordered write on overlap, in-file order and distinct keys per
file, to three decimals.

That is why an ordering here is expressed as `CLUSTER BY` or a partition, never as an `ORDER BY`
before the write. The table below is the general rule for each construct.

### What each construct does

From Spark and Delta source (master, 2026-09-07), Databricks docs, and a measured plan dump where
marked **measured**. "Across files" is what segment elimination needs; "within file" is what RLE and
in-file row-group pruning need.

| construct | shuffle | across files | within file | under this recipe |
|---|---|---|---|---|
| `ORDER BY x` / `df.orderBy(x)` | range exchange (boundaries sampled, 100 rows per input partition), then a sort per partition | disjoint and ordered, by contract of `RangePartitioning`; AQE coalesces ADJACENT ranges, so it changes the file count, not the order; `maxRecordsPerFile` splits a task in sequence | sorted | the `Sort` is deleted by the optimizer under the optimized-write exchange -- **measured plan** |
| `repartitionByRange(n, x)` + `sortWithinPartitions(x)` (the legacy `layout` arm) | the same plan with an explicit `n` | same | sorted | its files measured disjoint at SF10 and SF100, so that build did not run under optimized write; under this recipe the local sort would be removed by the same rule (not measured) |
| `SORT BY x` | none | overlapping | sorted | deleted: `EliminateSorts` removes a local sort under a repartition too |
| `DISTRIBUTE BY x` | hash | overlapping, every file spans the domain | unsorted | -- |
| `CLUSTER BY x` **in a SELECT** | `DISTRIBUTE BY x SORT BY x`, Hive's meaning; Spark docs: "does not guarantee a total order" | every file spans the domain; all rows of one key in one file | sorted | the sort half is deleted the same way; the optimized-write exchange decides the files |
| `CREATE/ALTER TABLE ... CLUSTER BY (x)` -- liquid clustering, a table property | none at write time unless the transaction clears Databricks' clustering-on-write bar (UC tables: 64 MB for one key, 256 MB / 512 MB / 1 GB for two / three / four; other tables 4x that); OSS Delta never clusters on write, only `OPTIMIZE` does | after `OPTIMIZE` (OSS `MultiDimClustering`): each key bucketed by `range_partition_id(x, 1000)`; one key falls back from Hilbert to Z-order, and a single-column Z-order IS the range id; then `repartitionByRange` on it, so files are disjoint at 1,000-bucket granularity | **unsorted** unless the internal `spark.databricks.io.skipping.mdc.sortWithinFiles` is true (default false); two or more keys become a Hilbert curve, which orders no single key | Databricks' write path is closed; `p2` measured clustering surviving optimized write |
| `partitionBy(date)` | `V1Writes` adds a local sort on the partition column; optimized write hashes ON the partition column | one reducer and one file per date | irrelevant: one date per file | the `partition` arm; elimination is exact by construction |

If a real global sort is ever wanted on Delta: keep the `orderBy`, turn optimized write off for
that write (`.option("optimizeWrite", "false")`, or the session conf), and size the files with the
sort's own exchange -- `spark.sql.adaptive.advisoryPartitionSizeInBytes`, or an explicit
`repartitionByRange(n)` -- with `maxRecordsPerFile` and `parquet.block.row.count.limit` still
capping rows per file and per group. That is user code plus a per-write conf, so it sits outside
the configuration-only recipe by definition.

## CLUSTER BY in prose

The README carried this under its `CLUSTER BY` snippet until 2026-09-09; the snippet itself, create
empty, pin `checkpointPolicy`, declare the key, append, is still in [README.md](README.md).

The checkpoint property comes before the key on purpose: liquid clustering turns v2 checkpoints on
the moment the key exists, and the session default in the block only covers tables created under
it, so the table says so itself. The append is whatever the pipeline already does. Clustering on
write replaces the Optimized Writes exchange with its own range partitioning, so nothing in user
code decides the files: no `orderBy`, no `repartition`, no `OPTIMIZE` afterwards.

**What it buys at SF100**, the paper's protocol, steady state:

| SF100, 20 users | rows per row group | suite | p50 | p95 | worst query |
|---|---|---|---|---|---|
| Databricks, the config, nothing else | 5.6-5.9M | 7.9 s | 207 ms | 2.8 s | 8.3 s |
| Databricks, the config + `CLUSTER BY` date | 1.1-2.0M | **2.5 s** | 124 ms | 0.35 s | 0.71 s |
| Fabric Spark, V-Order + partition by date (the paper's layout) | 78k-144k | 2.4 s | 119 ms | 0.37 s | 0.80 s |

The three date-filtered queries fall 10.6x, 3.6x and 2.6x; the other twelve are a wash. That puts
the Databricks table level with the paper's own Fabric layout on steady state, 2.5 s against 2.4,
and the cold load barely moves (36.9 s against 32.4 s, same mirrored catalog). One caveat to carry
when quoting it: the clustered table differs from the plain one in ordering and in segment size, so
the win is not all ordering.

**What breaks it.** Each was measured on DBR 19; the full list with the numbers is *What NOT to do,
if you want CLUSTER BY to work* below.

- **One key, the filter column.** Two keys become a Hilbert curve, which orders no single column.
- **`checkpointPolicy = classic` before the key exists.** If a table already has a v2 checkpoint,
  `ALTER TABLE t DROP FEATURE v2Checkpoint`.
- **No `df.orderBy(key)` before the write.** Databricks plans Optimized Writes as a repartition
  above the query and Spark deletes the sort under it; the executed plan has no `Sort` node and the
  files are identical to an unordered write. `CLUSTER BY` is how an ordering reaches a Delta file
  on this write path.
- **A clustered write is sized in bytes, not rows.** Its own exchange decides the files, so
  `maxRecordsPerFile` and the 6M cap sit below the cut and never fire, and rows per segment follows
  row width. On these facts it landed 1.1-2.0M rows per group, inside VertiPaq's window, so the
  recipe sets no byte target. A much wider fact could fall below it; the knob is then
  `delta.targetFileSize` as a table property on that fact, never a session default (it overrides
  the row caps on every write) and never coarse relative to the table (512 MiB on a 900 MB fact
  gave one range partition and nothing placed).
- **Clustering on write is best-effort, size-gated and silent when it skips.** Databricks documents
  the gate (64 MB per clustering key on a Unity Catalog managed table). Below it, the write lands
  unplaced while `DESCRIBE DETAIL` still reports `clusteringColumns`. Verify per file, min and max
  of the key, not from the metadata.
- **No `OPTIMIZE` from compute without the config**, as above. `ALTER TABLE ... CLUSTER BY` on
  existing data registers the key and moves nothing without `OPTIMIZE FULL` from a session that
  carries the block.

## Liquid clustering and OPTIMIZE — measured 2026-09-06

Was a standalone `CLUSTERING.md`; merged here 2026-09-07 so there is one place to look. Everything
marked **measured** comes from `notebooks/clustering_probe.py` (bundle job `clustering_probe`), run
2026-09-06 on synthetic data, NOT TPC-DS: 40M rows x 21 columns, key `dt` derived from a hash so the
input is provably unordered, geometry scaled to 1M rows/group, DBR 19, Photon off, 2 x E8ds_v5.
Conditions in full at the end. **The control passed** — the unclustered write left 98 % of files
overlapping, all 1,800 key values in every file, sortedness 0.001 — and everything below is judged
against that. **documented** is Databricks' word, not ours; anything else is reasoning, and labelled.

The short version: `CLUSTER BY` on a single key works, and works well. Almost everything that goes
wrong with it goes wrong because something *else* in the write is fighting it, or because the write
was too small to trigger it at all. And `clusteringColumns` will happily tell you the table is
clustered when not one row has moved.

### What it actually does

Liquid clustering maps each row's clustering keys onto a **Hilbert curve** and uses that coordinate
to decide which file the row belongs in. Files already organised this way are grouped into
**ZCubes**, and a later `OPTIMIZE` skips them, so reclustering is incremental instead of a full
rewrite (documented; the ZCube names come from the OSS Delta design doc).

With **one** key the curve degenerates to a plain range, which is the easy case and the one you want
if a single column drives your filters. With more keys the curve interleaves the dimensions, so no
single column is contiguous any more. Databricks' own guidance: up to four keys, but on tables under
10 TB **more keys make single-column filtering worse** — four keys skip fewer files than two.

Two things it does that are easy to conflate, and both matter:

| | what it means | why you care |
|---|---|---|
| **placement** | each file holds a narrow slice of the key range | file and row-group elimination at query time |
| **in-file order** | rows are sorted by the key *inside* each file | run-length encoding, and it is what Direct Lake / VertiPaq inherits, since parquet row order becomes segment row order |

**Measured: it does both.** Distinct key values per file fell from 1,800 (every value, i.e. nothing
moved) to 56, and in-file sortedness came out at 0.96 on a 0-to-1 scale where 1 is perfectly sorted.
This was against a deliberately shuffled input, so the control genuinely had no order to inherit.

### Two ways it gets applied, and only one of them is automatic

**Clustering on write.** Applies to `INSERT INTO`, `CTAS`/`RTAS`, `COPY INTO` from parquet, and
`spark.write.mode("append")`. It is **best-effort** and **threshold-gated per transaction**.
Databricks documents the threshold as 64 MB for a Unity Catalog managed table with one clustering
key, and 256 MB for any other Delta table (256 MB / 1 GB / 2 GB and 512 MB / 1 GB / 4 GB for two,
three and four keys).

**Measured: the bar sat above the documented one in this probe.** Appending to one
clustered path-based table, judged per commit by the key spans of the files that commit added:

| commit size on disk | rows | clustered? |
|---|---|---|
| 30 MB | 561,737 | no |
| 100 MB | 1,872,457 | no |
| 300 MB | 5,617,371 | **no** |
| 1,024 MB | 19,173,961 | yes |

300 MB is above the documented 256 MB and still did nothing. Small and medium appends silently do
not cluster, no matter what the table declares. Two caveats before reading that as Databricks
missing its own number, both reasoning and untested: the probe measured bytes ON DISK of the files
each commit added, while the documented threshold is on "data in the transaction", decided before
the write and presumably from the plan's size estimate; and the probe's input was generated
in-session from `spark.range` and a hash, with no statistics for that estimate to use. The gap may
be the estimate, not the bar. We also did not separate the managed-table bar from the path-table
bar in that run, so the documented 64 MB for managed tables is untested here.

**`OPTIMIZE`.** This is what Databricks tells you to lean on, precisely because "not all operations
apply liquid clustering". It is incremental: it only rewrites files that are not already in a ZCube.
`OPTIMIZE <t> FULL` reclusters everything and is what you must run after enabling keys on existing
data or changing keys — a plain `OPTIMIZE` will leave the old layout alone.

### How to tell whether it actually worked

**Do not use `clusteringColumns`.** `DESCRIBE DETAIL` reporting a clustering column means the key is
registered on the table. It says nothing about whether a single row moved. This is the single most
common way to fool yourself, and it is what this repo did for weeks.

Four checks, in order of how much they tell you. All of these work on a Unity Catalog managed table
through Spark alone, which matters because UC blocks path reads of managed-table files.

```python
from pyspark.sql import functions as F, Window
t   = spark.table(FQ)
KEY = "your_cluster_key"
fp  = F.col("_metadata.file_path")

# 1. PLACEMENT: distinct key values per file, against the table total.
#    Near the total = the rows stayed where they fell. Much lower = clustering placed them.
per_file = (t.groupBy(fp.alias("f"))
             .agg(F.count("*").alias("rows"),
                  F.min(KEY).alias("lo"), F.max(KEY).alias("hi"),
                  F.countDistinct(KEY).alias("keys"))
             .collect())
print("keys per file", sum(r.keys for r in per_file) / len(per_file),
      "of", t.select(F.countDistinct(KEY)).collect()[0][0])

# 2. OVERLAP: how many files' [lo, hi] ranges intersect another file's. 0% is perfectly disjoint.
rng, overlapping, run_hi = sorted((r.lo, r.hi) for r in per_file), 0, None
for lo, hi in rng:
    if run_hi is not None and lo <= run_hi:
        overlapping += 1
    run_hi = hi if run_hi is None else max(run_hi, hi)
print(f"{100 * overlapping / len(rng):.0f}% of files overlap another")

# 3. IN-FILE ORDER: count how often the key changes from one row to the next, per file.
#    row_index is PHYSICAL order inside the file -- ordering by anything else measures your own
#    sort, not the writer's. Needs DBR 13.3+.
w = Window.partitionBy("_f").orderBy("_ri")
st = (t.select(fp.alias("_f"), F.col("_metadata.row_index").alias("_ri"), F.col(KEY))
       .withColumn("_prev", F.lag(KEY).over(w))
       .groupBy("_f")
       .agg(F.sum(F.when(F.col("_prev").isNull() | (F.col("_prev") != F.col(KEY)), 1)
                   .otherwise(0)).alias("trans"),
            F.countDistinct(KEY).alias("keys"), F.count("*").alias("n"))
       .agg(F.sum("trans").alias("trans"), F.sum("keys").alias("keys"), F.sum("n").alias("n"))
       .collect()[0])
# sorted -> transitions equal the distinct count; random -> one transition per row.
print("sortedness", (st.n - st.trans) / (st.n - st.keys))       # 1.0 sorted, 0.0 random

# 4. GEOMETRY: rows per file, and how ragged.
n = sorted(r.rows for r in per_file)
print(f"{len(n)} files, {n[0]:,}..{n[-1]:,} rows, spread {n[-1] / n[0]:.1f}x")
```

**You must have an unordered control.** If your source data already arrives in key order, an
unclustered write produces perfectly disjoint files and looks exactly like successful clustering.
Generate the key from a hash, or shuffle, before you believe any of the numbers above.

**ZCube tags are not a usable signal on Databricks.** The `ZCUBE_ID` tag on a Delta log `add` action
belongs to the OSS Delta implementation. A Databricks engineer states on the community forum that it
does not appear in a liquid table written by DBR, which runs its own implementation. Measured: zero
tagged files across every clustered arm, including ones that demonstrably clustered. An absent tag
proves nothing.

**Row groups and dictionary encoding need the footers**, and UC will not let you read a managed
table's files by path. Either write the same thing to a path-based Delta table under a Volume and
read it with `pyarrow.parquet`, or read them downstream. When you do read footers after an
`OPTIMIZE`, replay the Delta log's add/remove actions to get the *live* file list — `os.listdir`
returns the replaced generation too, until `VACUUM`.

### The four things that break it

#### `spark.sql.files.maxRecordsPerFile` — the big one

A row cap and liquid clustering fight, and the cap wins. It splits each clustered task's output into
full files plus one remainder, and the remainders are what end up overlapping.

**Measured**, same data, same everything else, only the cap varying:

| | cap on | cap off |
|---|---|---|
| files | 64, **32 of them short** | 32, one short |
| rows per file | 21,849 to 1,000,000 | 909,510 to 1,665,395 |
| spread (max/min) | **45.8x** | 1.83x |
| files overlapping another | **50%** | **0%** |
| row groups in the 1M–16M window | 82% | 100% |
| distinct keys per file | 56 | 56 |
| in-file sortedness | 0.966 | 0.969 |

Note the last two rows. The cap does not stop clustering from placing or sorting the rows. It wrecks
the *file sizes*, and the ragged remainders are what reintroduce the overlap.

This is the same shape as [delta-io/delta#1718](https://github.com/delta-io/delta/issues/1718),
where `maxRecordsPerFile` breaks `ZORDER`: the ordering algorithm sizes its output by bytes and is
never told about the row cap, so the file count it planned for is not the file count it gets. That
issue is closed as not planned. Databricks' own docs also say they do not recommend
`maxRecordsPerFile` unless you need it to avoid a parquet row-count error.

**The row target survives without the fight, one level down.** What `maxRecordsPerFile` does that
hurts is change the *file* count — the unit the clustering algorithm plans in.
`parquet.block.row.count.limit` (parquet-java 1.16.0+, "the maximum number of rows per row group";
DBR 19 ships 1.17.0) caps the **row group** instead. It sits below the file boundary, does not
repartition anything, and so cannot change a file count behind the ordering algorithm's back.
Measured honoured on DBR 19 (2026-09-07, `rowgroup_probe` C and F; see *Why each line is there*).
On a clustered write under the recipe the question is moot anyway: 128 MB partitions hold 2–5M
rows and never reach the 12M file cap (see *Open*, the closed `maxRecordsPerFile` item).

#### Writes below the trigger threshold

See the threshold table under *Two ways it gets applied*. A steady trickle of small appends
produces a table that declares a clustering
key and has never clustered anything. This is what `OPTIMIZE` exists to clean up.

#### Running `OPTIMIZE` from the wrong session

Covered under *What `OPTIMIZE` does to an existing layout*. It is not `OPTIMIZE` that is
dangerous, it is whose Spark config it inherits.

#### Not running `OPTIMIZE` at all

If you rely purely on clustering on write, you are relying on a documented best-effort path with a
size gate. That is fine for one big load and wrong for incremental ingestion.

**Measured non-cause:** Optimized Writes (`spark.databricks.delta.optimizeWrite.enabled`) is *not*
the problem, despite being an extra shuffle sitting between you and the writer. Turning it off left
overlap unchanged at 50% and made the file spread far worse — 1,582x, with one 632-row file. Leave
it on.

### What `OPTIMIZE` does to an existing layout

**Measured.** Each variant ran on its own fresh copy, before and after.

| what ran | on | result |
|---|---|---|
| `OPTIMIZE` under the writing session's own config | clustered table | **no change at all**, 15 s |
| `OPTIMIZE FULL` under the same | clustered table | **no change at all**, 12 s |
| `OPTIMIZE` under stock defaults | clustered table | no change |
| `OPTIMIZE` under the writing session's config | unclustered table | **no change at all**, 1 s |
| `OPTIMIZE` under stock defaults | unclustered table | **rewrote everything** |

The last row is the one to remember:

| after a stock-default `OPTIMIZE` | before | after |
|---|---|---|
| dictionary-encoded column chunks | 93% | **38%** |
| row groups per file | 1 | **2 to 3** |
| row groups in the 1M–16M window | 91% | 72% |
| average file | 58 MB | 260 MB |

Nothing about the *table* changed. What changed is that the rewriting session had
`parquet.block.size` back at its 128 MB default and `parquet.page.row.count.limit` back at 20,000,
so parquet-mr made different decisions about row groups and about dictionary fallback.

**This is exactly what predictive optimization does.** It runs `OPTIMIZE` asynchronously on
Databricks-managed **serverless compute**, which carries none of your cluster's `spark.hadoop.*`
settings. Same for anyone's SQL warehouse. If your parquet layout depends on non-default writer
settings, `ALTER SCHEMA <s> DISABLE PREDICTIVE OPTIMIZATION` is load-bearing, not housekeeping.

Two more results worth knowing:

- **`OPTIMIZE` did not compact the short remainder files** left by the row cap, even with `FULL`.
  Measured outcome; the mechanism was not isolated, and two documented candidates cover it: liquid
  `OPTIMIZE` is incremental and skips files already clustered, and the session that ran it still
  carried `maxRecordsPerFile`, so any rewrite would have cut the same remainders again. Not an
  `OPTIMIZE` shortcoming; do not expect it to clean up after a cap that is still set.
- **`delta.targetFileSize` set AT the floor produced files at the floor.** Set to one row group's
  worth of bytes — 1M rows, the bottom of the window — it drove `OPTIMIZE` to *split* files to
  that size, and files sized at the floor land just under it: row groups inside the 1M–16M window
  went from 82% to **2%**, dictionary coverage from 90% to 67%. That is the arithmetic of the value
  chosen, not a property misbehaving. **CORRECTED 2026-09-07:** this bullet used to end
  "on a UC managed table this property is respected by `OPTIMIZE` only, not by the write path, so it
  cannot shape your writes anyway". That is wrong. The clustered WRITE path on a managed table reads
  it, measured at SF10, and reading it is how a 512 MiB target collapsed the write to one partition
  and produced an unclustered table. It shapes writes, and it shapes them badly when it is coarse
  relative to the table -- which is what the geometry assertion now catches, loudly, on the arm that
  sets it. Adopted 2026-09-07 as a flat 128 MB, never as a session default (see *The config*). The
  `OPTIMIZE` behaviour above is untouched by that: this arm still runs no `OPTIMIZE`.

### Protocol and feature side effects

**Measured on DBR 19, and it matches the docs.** A clustered table is writer version 7 and picks
up `clustering`, `domainMetadata` and `rowTracking` in `tableFeatures`. Databricks documents
clustered tables as reader version 3 (deletion vectors on), and documents one override: set
`delta.enableDeletionVectors = false` as a TABLE property on an existing table, then
`ALTER TABLE ... CLUSTER BY`. With only the session default
`spark.databricks.delta.properties.defaults.enableDeletionVectors=false` in force:

| how the table was created | reader version |
|---|---|
| create empty, `ALTER TABLE ... CLUSTER BY`, then append | **1** |
| `CREATE TABLE ... CLUSTER BY (...) AS SELECT` | **3** |

A session default is not the documented override, and the CTAS form is where that shows. If any
consumer of your table is a Delta reader that does not implement deletion vectors, the
create-then-`ALTER` sequence is the documented one, and it is worth a comment saying why.

**In this repo that table no longer applies**: since 2026-09-07 the recipe does not set
`enableDeletionVectors` at all and takes DBR 19's default (on), so both rows land at reader 3 — on
purpose. Without deletion vectors a `DELETE`/`UPDATE`/`MERGE` rewrites whole parquet files and Direct
Lake re-transcodes them; a deletion vector leaves the files, and the resident segments, alone. The
create-then-`ALTER` sequence is still the one the build uses, now for the checkpoint-policy reason
below rather than the reader-version one.

Other consequences:

- **Row tracking turns itself on**, and measured, it cost nothing in the files: 21 columns in, 21
  columns out, no materialised row-id column in the parquet schema.
- **`v2Checkpoint` is on by default** for clustered tables from DBR 14.3 LTS. Set
  `delta.checkpointPolicy = 'classic'` **before** the keys exist, or `DROP FEATURE v2Checkpoint`
  after. This one is fatal for Direct Lake, which cannot read a v2 checkpoint at all.
- Clustering is **mutually exclusive with partitioning and `ZORDER`** on the same table.
- Clustering keys must be columns that have statistics collected — by default the first 32 columns.

### What to do

1. **Pick one or two keys**, the ones that actually appear in filters. On anything under 10 TB, more
   keys make single-column filtering worse.
2. **Create the table empty and `ALTER TABLE ... CLUSTER BY`**, then append — not CTAS, if you need
   reader version 1. (This repo does not: it leaves deletion vectors on, so reader 3 either way. It
   still uses the sequence, to pin the checkpoint policy before the keys exist.)
3. **Set `delta.checkpointPolicy = 'classic'` before the keys exist**, if anything downstream is
   fussy about checkpoints.
4. **Do not set `spark.sql.files.maxRecordsPerFile` where a clustered partition can exceed it.**
   Where it binds it is the single biggest cause of a clustered table that isn't; the recipe's 12M
   is inert only because 128 MB partitions stay far under it.
5. **Leave Optimized Writes on.**
6. **Run `OPTIMIZE` from the same session config that wrote the data**, and run `OPTIMIZE ... FULL`
   after enabling or changing keys. Turn predictive optimization off on the schema if your parquet
   layout depends on non-default writer settings.
7. **Measure with *How to tell whether it actually worked* before you believe any of it**, against an unordered control.

### Clustering on write against the recipe -- MEASURED at SF10, 2026-09-07

`notebooks/cluster_config_probe.py`, one-off run 877226984119421, 2 x E8ds_v5, DBR 19, Photon off,
the fourteen `spark_conf` keys of `databricks.yml` verbatim. The arm's exact sequence (empty table,
`checkpointPolicy=classic`, `ALTER TABLE ... CLUSTER BY (ss_sold_date_sk)`, one `append`) on the
real SF10 `store_sales` -- 26.2M rows from the raw Volume with the build's customisations -- with one
session knob changed per variant; the probe's JSON output holds the executed plans and per-file
numbers. Control is
`tpcds_sf10_default.store_sales`: 5 files, all 1,823 dates in every file, 80 % overlapping,
sortedness 0.909.

| variant | change | files | rows per file | MB per file | overlap | dates per file | sortedness | append |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `recipe` | none | 16 | 1.33M-1.86M | 46-62 | **0 %** | 114 | 0.994 | 69 s |
| `nocap` | `maxRecordsPerFile=0` | 16 | 1.45M-2.16M | 50-71 | 0 % | 114 | 0.995 | 58 s |
| `noow` | `optimizeWrite.enabled=false` | 16 | 1.37M-1.91M | 47-63 | 0 % | 114 | 0.995 | 58 s |
| `bin1024` | `optimizeWrite.binSize=1024` | 16 | 1.37M-2.10M | 47-69 | 0 % | 114 | 0.995 | 58 s |
| `tfs512` | table property `delta.targetFileSize` = 512 MiB | **3** | 2.2M, 12M, 12M | 75-384 | **67 %** | **1,823** | 0.956 | 147 s |

The executed plan of the append, the same shape in every variant:

```
Execute WriteIntoDeltaCommand
+- WriteFiles
   +- ShuffleQueryStage
      +- Exchange deltalatestageclusteredwritepartitioning(<scalar subquery: range boundaries on ss_sold_date_sk>, ...)
         +- AQEShuffleRead local
            +- ShuffleQueryStage
               +- Exchange deltalatestageclusteredwritepartitioning(ApproxCountDistinctPassthrough(ss_sold_date_sk), ...)
                  |  DELTA_LATE_STAGE_CLUSTERED_WRITE_PREP
                  +- Project -> Filter -> Scan parquet
side query:  Exchange SinglePartition -> Sort [clusteringCol0 ASC]   over ~990 rows = the boundaries
```

- **Clustering on write REPLACES Optimized Writes; it does not sit on top of it.**
  `deltaoptimizedwritepartitioning` appears only in the empty `limit(0)` create. The append has two
  clustering exchanges -- a PREP pass that counts distinct keys, then the range partitioning on
  boundaries a side query computes by sorting ~990 rank values, i.e. the 1,000-bucket range ids of
  the OSS source -- and nothing else. `optimizeWrite.enabled`, `binSize` and, at this size,
  `maxRecordsPerFile` are not in the plan, which is why the three variants that change them are the
  `recipe` row again. `DESCRIBE HISTORY` stamps the append
  `clusteringOnWriteStatus = late-stage clustering triggered`.
- **What sizes a clustered file is the partition count of that exchange, and the recipe had not
  set the knob that decides it.** 16 partitions of 1.64M rows and ~58 MB. 16 is also the node
  pair's core count, which is a coincidence: the 2026-09-06 probe on the same pair got 32 files
  for 3 GB, and SF100 on 4 workers got 128 for 9.4 GB -- the exchange sizes in bytes, at a default
  target when none is set. The knob is `delta.targetFileSize`, documented as honoured by liquid
  clustering; the next bullet is what happens when it is set badly, and *The config* is where it
  was adopted. The 6M row-group cap and the 12M file cap are inert here only because every
  partition sits far below both.
- **`delta.targetFileSize` IS the knob on it, and 512 MiB was the wrong value for a 932 MB
  table.** 512 MiB collapsed the exchange to
  ONE partition holding all 26.2M rows; the 12M cap then cut that into 12M + 12M + 2.2M, every file
  holds all 1,823 dates, 67 % overlap, and the append took 2.5x longer. A one-partition range is no
  clustering, and nothing sorted the rows inside it -- 0.956 is the source order, the control reads
  0.909. Documented as honoured by `OPTIMIZE` only on a managed table; measured, the write path
  reads it too. **Resolved 2026-09-07:** the failure is the RATIO, not the knob -- 512 MiB against a
  932 MB table is one partition -- this row is a table too small for the value, not a bad value.
  Adopted flat at 128 MB, which is fine on a 932 MB table (7 files) and needs no ratio guard at all.
  See *The config*.
- **The row cap did not bite, and that is the size, not the recipe.** With 1.6M-row partitions no
  file came near 12M. Where a clustered partition exceeds the cap, 2026-09-06 measured the fight
  (50 % overlap from remainders) and `tfs512` reproduced it in miniature.
- In-file order 0.994 with 114 dates per file, against 0.909 inherited from dsdgen on the control.
  The difference is the evidence, not the number; the hash-keyed probe's 0.96 is the clean measure.

**Closed by the SF100 run below:** the partition count scales with the data and rows per file stays
at ~2M, so the 12M cap never fires and neither option below is needed. Kept for the shape of the
question: If they do, the
arm has two options and neither is free: drop `maxRecordsPerFile` for this arm (the row-group cap
still holds 6M segments, files just get bigger), or set `delta.targetFileSize` to about 12M rows'
worth of bytes so partitions stay under the cap. **Taken 2026-09-07**, but in the other direction:
a flat 128 MB, aiming at 2-5M-row segments rather than at the cap, because warm favours small
segments. See *The config*.

### SF1000, clustered, at the 128 MB target -- MEASURED 2026-09-07

The first build on `delta.targetFileSize` = 128 MB, and at the largest scale this project builds.
`tpcds_sf1000_cluster`, `tpcds_build_spark` run 1081531566795847, all ten tables, ~1h55m.

| | rows | files | rows/file | avg file | B/row | checkpoint |
|---|---:|---:|---:|---:|---:|---|
| `store_sales` | 2,620,785,279 | 540 | 4,853,306 | 160.2 MB | 34.6 | classic, no v2 |
| `catalog_sales` | 1,425,579,810 | 512 | 2,784,336 | 157.5 MB | 59.3 | classic, no v2 |

- **The number does what it was designed to do.** Both facts land at 2.8M-4.9M rows per file, which
  is the 2-5M band the segment measurements argue for, at a scale factor where the unaided exchange
  previously chose 2.05M and 1.11M and dropped `catalog_sales` out of dictionary encoding. Row counts
  match the paper on all ten tables; the geometry assertion passed; `clusteringColumns` set on both.
- **The target UNDERSHOOTS the delivered file by about 25 %, and that is fine.** 128 MB asked for,
  158-160 MB delivered; predicted 3.9M / 2.3M rows per file from bytes-per-row arithmetic, got
  4.85M / 2.78M. So `delta.targetFileSize` is a target the clustering exchange overshoots, NOT a cap.
  **The width tables in this file and in `build_spark.py` therefore read about 25 % low** -- they are
  the arithmetic, and the arithmetic is the floor. Not corrected, because the overshoot lands the
  facts higher inside the window rather than outside it, and a table that predicts the floor is the
  safe direction to be wrong in. Do not treat those tables as promises of file size.
- Consequence for the guard question that was argued at length earlier in the day: the overshoot
  makes the small-table failure LESS likely, not more, since a table needs to be smaller still
  before the range collapses to one partition.

### SF100, clustered, 2M-row groups -- MEASURED 2026-09-07

`notebooks/cluster_config_probe.py`, run 263426007767716, 4 x E8ds_v5, DBR 19,
Photon off, the recipe's `spark_conf` with `parquet.block.row.count.limit=2000000`. Real SF100
`store_sales`, 262,082,396 rows, the arm's exact sequence. 20 min for three writes.

| | files | rows/file | MB/file | overlap | dates/file | sortedness | append |
|---|---:|---:|---:|---:|---:|---:|---:|
| control `default` | 47 | 0.63M-6M | 181 | **98 %** | 1,823 | 0.909 | -- |
| `recipe` | 128 | 1.59M-2.82M | 71 | **0 %** | **14** | 0.999 | 300 s |
| `nocap` (row cap off) | 128 | 1.44M-2.74M | 71 | 0 % | 14 | 0.999 | 272 s |

- **Clustering on write holds at 262M rows.** Zero overlapping files, 14 of 1,823 dates per file,
  rows 0.999 sorted inside. Against a control at 98 % overlap with every date in every file. This
  is the row-group elimination the date-filtered queries need, and it cost one write.
- **The row cap is irrelevant at this shape, as at SF10.** `recipe` and `nocap` are the same table:
  clustered partitions land at ~2M rows, six times under the 12M cap, so it never fires. Both plans
  are the two `deltalatestageclusteredwritepartitioning` exchanges and no optimized-write exchange.
- **The partition count scales with the data; rows per file does not.** SF10 gave 16 files of 1.6M
  rows on 2 workers, SF100 gives 128 files of 2.0M rows on 4. So SF1000 should land ~1,300 files of
  ~2M rows per fact, still far under the 12M cap -- which answers the scaling question the SF10 run
  left open, and says the arm needs no change to the recipe to build.
- **Rows per file is now about the row-group size, and that is the open geometry risk.** At a 2M cap
  with ~2M-row files, most files are one full group plus a small remainder: the path-table twin
  (32 files of 8.2M rows) showed 147 groups, 78 % at exactly 2,000,000, smallest 582 rows, 90 % in
  the 1M-16M window, **97.9 % of column chunks dictionary-encoded**. Dictionary survives 2M groups.
  Whether the managed 128-file tables carry one ragged group each is unmeasured here and is a
  question for the mirror (`layout_stats`, `fabric/verify_layout.py`), not for another write.
- Method note: the path twin was a mistake -- UC blocks footer reads of managed tables, but Fabric
  reads them over the mirror, which is how this repo has always done it. The twin is deleted and
  the variant is gone from the probe.

### The clustered arm at SF100, as built -- 2026-09-07

`tpcds_sf100_cluster`, job run 1063316500869176. Both facts came out with **exactly 128 files**:

| table | rows | files | rows/file | columns |
|---|---:|---:|---:|---:|
| `store_sales` | 262,082,396 | 128 | 2,047,519 | 24 |
| `catalog_sales` | 142,557,716 | 128 | 1,113,732 | 35 |

Same file count on two tables of different width and different row count -- and, which is the
point, nearly the same BYTES: 262M x 36 B = 9.4 GB against 143M x 59 B = 8.4 GB. This section
first read the equal count as "the exchange sizes from the compute". It does not; it sizes in
bytes, and two ~9 GB tables get the same number of ~70 MB partitions. Per-file bytes sit in one
band across three cluster sizes -- 58 MB at SF10 on 2 workers (16 files), 73 MB here on 4 workers
(128), 94 MB on the synthetic 3 GB table on 2 workers (32) -- where a compute-sized exchange would
have tracked the cores. Rows per file therefore follows row width: **the wider table gets the
smaller row groups**, and no byte target can equalise rows across widths. What a byte target CAN
do is put both above the dictionary floor, and the knob is `delta.targetFileSize`, which the docs
list under the operations that honour it ("including optimize, liquid clustering, auto compaction,
and optimized writes"). The recipe had simply not set it; it was adopted the same day (see *The
config*), and SF1000 at 128 MB landed 4.85M and 2.78M rows per file. Both facts sit far under the
12M row cap, so the cap is inert on this arm at every scale factor this project builds.

Not measured yet on these tables: row groups and dictionary coverage (mirror, then `layout_stats`),
and overlap on the real facts (`sort_check` with `arms=cluster,default`).

### The clustered SF100 arm, measured both ways -- 2026-09-07

a Spark placement check (run 805607356622129), and `layout_stats` over the mirror for the
footers. Same tables, two surfaces.

| arm | table | files | rows/file | overlap | dates/file | in-file order |
|---|---|---:|---:|---:|---:|---:|
| `cluster` | store_sales | 128 | 2,047,519 | **0 %** | **14** | 0.999 |
| `default` | store_sales | 47 | 5,576,221 | 98 % | 1,823 | 0.909 |
| `cluster` | catalog_sales | 128 | 1,113,732 | **0 %** | **14** | 1.0 |
| `default` | catalog_sales | 24 | 5,939,905 | 96 % | 123 | 1.0 |

Footers, `cluster` arm, one row group per file on every table:

| table | row groups | rows/group min-avg-max | in 1M-16M | groups under 1M | dictionary bytes |
|---|---:|---:|---:|---:|---:|
| store_sales | 128 | 1.69M - 2.05M - 2.60M | **100 %** | 0 | **100 %** |
| catalog_sales | 128 | 0.82M - 1.11M - 1.45M | 80.5 % | 25 | 80.4 % |

- **Elimination is won, on both facts.** Zero overlapping row-group ranges on the date key, 14 dates
  per file against 1,823 and 123 unclustered. `layout_stats` calls both `eliminable`. That is the
  thing a bare `orderBy` never delivered and the whole reason this arm exists.
- **`store_sales` is the clean result**: every row group inside the window, every column chunk
  dictionary-encoded, 3,072 of 3,072.
- **`catalog_sales` is where the file-size lever the recipe had not set shows up as damage.** It
  has 46 % fewer rows than `store_sales` in about the same bytes, so it got the SAME 128 partitions
  and its groups are half the size: 25 of 128 fall under the 1M floor, and **19.6 % of its bytes
  lose dictionary encoding** -- five money columns (`cs_ext_list_price`,
  `cs_net_paid_inc_ship_tax`, `cs_net_paid_inc_ship`, `cs_net_paid_inc_tax`, `cs_net_profit`), and
  the fallback is per chunk, not per column: 128 of 128 chunks plain on the first, 25 of 128 on
  the last. That is parquet-mr's first-page dictionary-vs-plain decision landing marginally at
  ~1.1M rows.
- **It does not go away with scale, because it is bytes per row, not a scale-factor accident.** A
  byte-sized partition holds fewer rows of a wider table at every scale factor. (An earlier version
  of this bullet blamed an exchange "sized from the compute"; see the correction under *The
  clustered arm at SF100, as built*.)
- **The fix is `delta.targetFileSize`, and it was MEASURED on 2026-09-09 (`clustersn`):** 128 MB on
  the clustered facts lifts `catalog_sales` to 2.23M rows per group and 96.8 % dictionary bytes --
  and steady state does not move (2.6/2.4 s against 2.6/2.5 without it). So the dictionary loss
  below the cliff is real and costs nothing on this workload, and the knob is an option for a wider
  fact rather than part of the recipe.

### What NOT to do, if you want `CLUSTER BY` to work -- measured on DBR 19

1. **Do not set `delta.targetFileSize` coarse relative to the write.** The clustered write sizes its
   partitions as bytes / target; too few partitions and nothing is placed. 512 MiB on a 900 MB
   write gave ONE partition, every date in every file (SF10, 2026-09-07). The recipe sets no byte
   target at all; if you add one, 128 MB is far below this failure at any realistic fact size.
2. **Do not set `spark.sql.files.maxRecordsPerFile` where a clustered partition can exceed it.**
   Clustering plans whole files; the cap cuts each into full files plus a remainder, and the
   remainders overlap -- 50 % measured. Inert only while partitions stay under it.
3. **Do not write below the size bar and expect placement.** Small appends land unclustered and
   only `OPTIMIZE` fixes them; 300 MB did nothing on a path table.
4. **Do not `ALTER TABLE ... CLUSTER BY` on existing data and stop there.** It registers the key
   and moves nothing; `OPTIMIZE FULL` or it never clusters.
5. **Do not run `OPTIMIZE` from a session without the parquet settings.** Predictive optimization
   and SQL warehouses rewrite with default row groups and 38 % dictionary. Disable predictive
   optimization on the schema.
6. **Do not trust `clusteringColumns` or `EXPLAIN`.** Both say clustered when nothing moved.
   Measure per-file min/max of the key.
7. **Do not use more than one key when one column drives the filters.** Two keys become a Hilbert
   curve, which orders no single column.
8. **Do not let `v2Checkpoint` in.** Pin `checkpointPolicy=classic` before the key exists; Direct
   Lake cannot read it.

What did not matter: Optimized Writes on or off, `binSize`, and CTAS versus create-then-alter for
placement. Clustering on write replaces the Optimized Writes exchange entirely.

### Re-running the probe

```bash
export DATABRICKS_TF_EXEC_PATH=C:/Users/<you>/bin/terraform.exe
databricks bundle deploy -p ontobricks
databricks bundle run clustering_probe -p ontobricks
```

Widgets worth changing: `rows`, `rows_per_group`, `distinct_keys`, `bin_mib`, `keep_output`,
`run_part1`, `run_part2`. The notebook generates its own data, drops its schema on exit, and prints
a verdict block plus a JSON payload it exits with.

**Conditions for every measured number above.** DBR 19 (Spark 4.2.0, parquet-mr
1.17.0-databricks-0001), **Photon off**, 2 x Standard_E8ds_v5, Unity Catalog. 40M rows x 21 columns,
about 3 GB, zstd. Clustering key `dt` with 1,800 distinct values derived from a hash so the input is
provably unordered. Row-group target 1,000,000 rows, `optimizeWrite.binSize` 1,024 MiB. The control
arm confirmed the input was unordered: 98% of files overlapping, all 1,800 keys in every file,
sortedness 0.001.

**Two caveats on the numbers.** The geometry is scaled down from this project's production target of
6M rows per group; the mechanisms are scale-free but the absolute figures are not. And row groups,
dictionary percentages and the threshold table were measured on path-based Delta tables under a
Volume, because UC blocks footer reads of managed-table files — placement, ordering, geometry and
protocol were measured on real managed tables.

### Sources on clustering

- [Use liquid clustering for tables](https://learn.microsoft.com/en-us/azure/databricks/tables/clustering) — thresholds, `OPTIMIZE FULL`, key-count guidance, protocol, feature overrides
- [Control data file size](https://learn.microsoft.com/en-us/azure/databricks/tables/tune-file-size) — `targetFileSize`, autotuning, the `maxRecordsPerFile` warning
- [Predictive optimization](https://learn.microsoft.com/en-us/azure/databricks/optimizations/predictive-optimization) — runs `OPTIMIZE` on serverless compute
- [delta-io/delta#1718](https://github.com/delta-io/delta/issues/1718) — `maxRecordsPerFile` breaks `ZORDER`; closed as not planned
- [How Delta Lake Liquid Clustering conceptually works](https://dennyglee.com/2024/02/06/how-delta-lake-liquid-clustering-conceptually-works/) — Hilbert curve, ZCubes
- [Debunking 8 data layout myths](https://www.databricks.com/blog/debunking-8-data-layout-myths-why-liquid-clustering-outperforms-partitioning) — best-effort clustering on ingest

### Two findings that did not fit above

- **Without the recipe, clustering still works but the parquet layout does not**: 3-4 row groups per
  file and **38 % dictionary**, against the recipe's 1 group and 91-95 %. The page settings and the
  2 GiB block are orthogonal to clustering and still doing their job.
- The same stock-default `OPTIMIZE` compacted much less on the UC **managed** table than on the path
  table (43 -> 40 files vs 43 -> 11). Managed-table footers are unreadable, so the dictionary damage
  is measured on path tables only; on a managed table it is the same kind, proportionally smaller.

**So the rule "never run OPTIMIZE" is half right, and for the wrong reason.** `OPTIMIZE` does not
destroy the layout — a session carrying the recipe's parquet settings does nothing at all. What
destroys it is `OPTIMIZE` run by compute that never saw those settings. The rule to keep is about
*whose* session runs it, not about the command.

## To be confirmed

What the 2026-09-08 pass left stated as "not established", each with the probe that settles it.
None has been run. Until one is, the sections above carry the measurement and no mechanism.

- **Why the `partition` arm's within-partition sort never reached the parquet.** The candidate is
  an optimized-write exchange above the `Sort`, but the docs say the session
  `optimizeWrite.enabled = false` that write set should have removed it, and a UC partitioned CTAS
  has optimized writes on by platform default. Probe: one `cluster_config_probe` variant that
  repeats the deleted write (`repartition(key).sortWithinPartitions(key, skey)`, partitioned
  `saveAsTable`, session switch false) and captures the executed plan the way the clustered
  variants already do; a second variant with `.option("optimizeWrite", "false")` on the writer.
  Read: is there an `Exchange` above the `Sort`, and is the key 100 % non-decreasing in file order.
  This also settles the precedence between session switch, table property and writer option,
  which this file has asserted both ways and measured neither.
- **Why Databricks' `OPTIMIZE ... ZORDER BY` committed nothing with one file per partition.**
  Probe: the SF10 `partition` table; run the statement and read `DESCRIBE HISTORY`; then append a
  second small file into one partition and run it again. If only that partition is rewritten, the
  skip is candidate selection on file count. Also read `spark.databricks.delta.optimize.minFileSize`
  on the runtime, since a 5 MB file is under any such bar.
- **Whether the clustering-on-write gate is on bytes written or on the plan's size estimate.**
  The probe crossed the documented 256 MB on disk and did not cluster, from a source generated
  in-session with no statistics. Probe: the same append sizes from a Delta source with statistics,
  on a managed table and on a path table, so the documented 64 MB / 256 MB split is measured on
  the right surface.
- **Whether `OPTIMIZE FULL` compacts the row cap's remainder files once the cap is unset.** The
  measurement that said it does not ran in a session still carrying `maxRecordsPerFile`. Probe:
  the same table, `OPTIMIZE FULL` from a session with the cap at 0; read the history and the file
  count.
- **What byte target the clustering exchange uses when `delta.targetFileSize` is unset.** Measured
  58-94 MB per file across three runs, against the docs' 256 MB autotune for tables under 2.56 TB.
  Probe: the same SF10 write, target unset, on 2 workers and on 4. If the file count moves with the
  bytes and not the cores, the "sizes in bytes" reading above holds and only the number is left to
  name.

## Open

- **Whether a THIRD writer resolves the dictionary conflict, or whether V-Order really is the only
  one. NOT YET MEASURED — the arm exists, nothing has been run.** The claim under *Dictionary
  encoding* is that the recipe's 6M row groups and the paper's partition-by-date are in direct
  conflict over the dictionary, that V-Order is what resolves it, and that this is the one thing a
  non-Fabric writer cannot reproduce. That has only ever been tested against parquet-mr. **delta_rs
  — what delta-rs writes with, and therefore what duckrun writes with — does not run
  `FallbackValuesWriter`: there is no first-page cost test at all.** A column keeps its dictionary
  until the dictionary itself passes `dictionary_page_size_limit` (32 MB in duckrun's profile), and
  a 4M-row INT32 key needs ~16 MB, so on the arithmetic it fits where parquet-mr's cost test throws
  it away. **The arm that would have measured this at the paper's 143k-row geometry was dropped on
  2026-09-08** — it was never built and never run, and the geometry it needed is itself a measured
  cold penalty (`vonly` 29.9 s → `vorder` 70.0 s, same writer and encoding), so it could only ever
  have explained something rather than recommended it. What is measured instead: delta_rs holds
  **100 % of fact bytes** dictionary-encoded at 2.1M and 2.8M rows per group, where parquet-mr at
  the same scale holds 47-53 % at 143k rows and 99.9 % at 6M. So delta_rs plainly has no cost test
  to fail; what is still untested is only whether it would hold at 143k rows too.
  **Two things make this arm's numbers narrower than they look**, and neither is fixable from a
  notebook: duckrun writes **SNAPPY** where every other arm is ZSTD, so bytes and cold loads are not
  comparable (encodings are, being read from the footer); and its geometry is a 4M-row CEILING
  (duckrun 0.4.68) inside a 256 MB file roll, so **every file ends on a truncated group and the tail
  size is bytes / row width**. On the arithmetic that is a ~3.1M tail on `store_sales` (~36 B/row,
  ~7.1M rows to a 256 MB file) and a ~0.5M tail on `catalog_sales` (~57 B/row, ~4.5M) — the second
  one is **under the 1M segment floor**. Per-table, byte-derived, ragged: exactly what the recipe's
  three row-denominated numbers exist to prevent, arrived at from the other direction. Not a defect
  in the arm, but it is why `avg_row_group` has to be read per table before any of its query numbers
  are read at all.
- **Whether a sort key chosen for SIZE lands anywhere near one chosen for PRUNING. NOT YET
  MEASURED.** Every ordered arm here sorts on the date surrogate, picked by hand because 23 of the
  paper's 24 captured queries filter on it. `build_duckdb.ipynb` `variant="default"` is
  `SORTED BY AUTO`: duckrun profiles the data and picks the `ORDER BY` itself, to minimise modelled
  in-memory columnar bytes, taking the coarsest temporal column first, then ascending cardinality up
  to 4 dimension columns, with measures allowed only in tail slots below the whole key. **That is a
  different objective from segment elimination**, and `ss_sold_date_sk` is an INT surrogate rather
  than a temporal type, so it competes on cardinality (~1,823 distinct) against genuinely coarser
  columns — `ss_quantity` (100), `ss_store_sk` (402). Since only the FIRST key eliminates row
  groups, the prediction is a split: the `default` variant smaller and cheaper cold, `cluster` faster on the
  date-filtered queries. The arm is only readable with the key it chose, which the notebook captures
  per table from duckrun's advisory and prints in its report — record it with the numbers. Note the
  sort is GLOBAL (delta-rs has no Optimized Writes exchange to delete it, unlike Spark), so the
  ordering does reach the files; what is open is whether it is the right ordering.
- **Whether auto compaction is actually harmful when it DOES fire. THREE failed attempts; parked.**
  (1) `rowgroup_probe` G never made enough files -- `optimizeWrite.binSize` shrinks tasks not output,
  so it produced 12 files of 88 MB. (2) `ac_off`/`ac_on`, 2026-09-07: a small `maxRecordsPerFile` DID
  make 53 small files, but the recipe's own table property blocked it. (3) `ac_prop`, same day: both
  switches explicitly true, 53 files of 9-21 MB, and it STILL did not fire. See *Auto compaction
  did not fire in three probes*. Not resuming without a specific hypothesis -- candidates are path tables
  in a Volume vs catalog tables, a per-partition trigger count, or DBR 19 folding this into
  predictive optimization. The case that would matter is concrete: the `partition` arm's 1,823 files
  of 5.2 MB.
- **Whether auto compaction under this recipe is actually harmful.** (Duplicate of the item above;
  kept because it states the mechanism.) It inherits the session's
  parquet settings (documented: it runs synchronously on the writing cluster), so it cannot make the
  parquet-mr decisions the stock-default `OPTIMIZE` made. What it brings is its own 128 MB byte
  target aimed at the ragged remainder files. Never measured here -- `clustering_probe` measured
  `OPTIMIZE`, not `autoCompact`. Turning it on for one arm and reading the footers would settle
  whether the recipe is being over-cautious.

- ~~Whether 12M / 6M together give 2 clean groups per file.~~ **CLOSED 2026-09-09 by the `default2rg`
  build, read off the mirror's footers.** The pairing had gone into the recipe as the composition of
  two measured results (C: the group cap fires repeatedly inside one file; D: the two caps coexist)
  and the multiple itself had never been run. It does what it says: `catalog_sales` 13 files / 25
  row groups, `store_sales` 24 / 47, 1.92 and 1.96 groups per file, every full group at exactly 6M.
  The fractional part is the tail file, not a miscut. What it does NOT buy is speed -- see *Segment
  geometry alone is settled*.
- **Whether Fabric mirroring accepts a `minReaderVersion=3` table.** Now that the recipe leaves
  deletion vectors on, every table is reader 3. Fabric is documented to read DVs and LEARNING has
  said so since 2026-09-06, but no reader-3 table has been mirrored from this repo. Check at SF10
  before rebuilding anything larger.
- ~~Whether Fabric mirroring accepts a writer-7 (clustered) table at all.~~ **Verified 2026-09-07
  (user): it does. The only thing mirroring and Direct Lake cannot take is `v2Checkpoint`.**
- Phase 2 — ordering across files with geometry held constant. The `default` vs global-sort half is
  MEASURED at SF1000 (see Findings); what is open is **LC**: whether one clustering key on the date
  recovers queries 6/14/24 without giving back the ~12 the sort costs. Geometry is not held constant
  in that comparison yet — layout ran at 16M rows/group, default at ~5M. The *mechanism* half of that
  question is now closed: clustering places and sorts the rows, and the row cap is what wrecks the
  geometry. What is still open is the query result.
- **Whether the `cluster` arm can drop `spark.sql.files.maxRecordsPerFile` without losing the
  geometry it exists to defend.** Measured on synthetic data, dropping the cap gave 0 % overlap,
  1.83x spread and 100 % of row groups in the window — but the clustering write sized those groups
  by BYTES, and bytes give a different row count on every table, which is the whole reason the cap
  is in the recipe. **SF10 measured 2026-09-07 (see *Clustering on write against the recipe*):
  the cap is not in the clustered write's plan and never fired -- partitions were 1.6M rows, far
  under it -- so at SF10 the question does not arise. It arises only where a clustered partition
  exceeds 12M rows, and how the partition count scales with table size is what SF100 measures next.**
  **CLOSED 2026-09-07 at SF10 and SF100.** There is no fight: on a clustered write the row cap is
  never reached, because clustering picks ~2M-row files and the cap sits at 12M. `recipe` and
  `nocap` produced the same table at both scale factors. Nothing needs to bound clustered file
  size, `binSize` is not in the clustered plan at all, and `delta.targetFileSize` is the one lever
  that does reach the clustered write -- adopted 2026-09-07 at a flat 128 MB, not to bound the files
  but to stop them coming out smaller than anyone chose (see *The config*).
- Whether the managed-table clustering-on-write threshold is the documented 64 MB. The probe measured
  the path-table bar (>300 MB, docs say 256 MB) and did not separate the two surfaces.
- Whether the same reviewers accept liquid clustering or Z-order: ordering bought incrementally
  by the engine, not in one global shuffle.
- Whether `parquet.page.size` = 64 MB and `parquet.page.row.count.limit` = 16M (in `databricks.yml`,
  first carried by the SF100 cluster rebuild) get parquet-mr to V-Order's 100 %. The arithmetic says
  they rescue the mid-cardinality measures and not the near-unique keys: a `ticket_number` chunk
  loses the cost test over a whole row group too. Only a writer that skips the test gets to 100 %.
