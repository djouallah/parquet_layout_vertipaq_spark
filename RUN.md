# Run

What the arms are and what has been measured: [LEARNING.md](LEARNING.md).

The live copy of the config is the `spark_conf` on the `tpcds_build_spark` job cluster in
[databricks.yml](databricks.yml). It and the block in [README.md](README.md) are kept identical;
[build_spark.py](notebooks/build_spark.py) *checks* the config is in force and refuses to build
under anything else, so no arm can be written under settings that are not the ones being shared.

The paper's companion repo is not vendored. The Fabric notebooks embed the DAX capture and the TMDL
they need, so nothing from it is required to run the benchmark. Rebuilding the notebooks
(`build_notebooks.py`) or deploying the paper's model (`deploy_paper_model.py`) does need it: fetch
[lipinght/DB-DQ-Whitepaper](https://github.com/lipinght/DB-DQ-Whitepaper) at commit `99f9904d`,
`load_test_json/PowerBIPerformanceData.json` into `paper/` and `test_model_report/` into
`paper/model/`. `paper/` is gitignored.

## Databricks

Profile `ontobricks`; `bundle deploy` needs `DATABRICKS_TF_EXEC_PATH` pointed at a local terraform.
The scale factor and node sizes are deploy-time variables:

```bash
export DATABRICKS_TF_EXEC_PATH=C:/Users/<you>/bin/terraform.exe
V='--var=scale_factor=1000 --var=gen_node_type=Standard_E64ds_v5 --var=build_node_type=Standard_E16ds_v5 --var=build_workers=3'
databricks bundle deploy -p ontobricks $V
databricks bundle run tpcds_generate       -p ontobricks $V   # once per SF; resumable
databricks bundle run tpcds_rowgroup_probe -p ontobricks $V   # SF10, minutes: which knob states a row-group size
```

The three Databricks arms come from one job and one `spark_conf`; `ordering` is the only variable,
and it must be on the **deploy**, not just the run — the notebook reads it from `base_parameters`,
which resolve at deploy time:

```bash
for ARM in none cluster partition; do
  databricks bundle deploy -p ontobricks $V --var=ordering=$ARM
  databricks bundle run tpcds_build_spark -p ontobricks $V --var=ordering=$ARM
done
```

`ordering=none` IS the recipe and needs no flag of its own — it writes `tpcds_sf{N}_default`. Two
variables make further arms from the same write, and the arm name carries the number so none of them
can be a silent variant of the recipe (the build refuses them with any other `ordering`):

```bash
# clustered, snappy, 128 MB files      -> tpcds_sf100_clustersn
V="--var=scale_factor=100 --var=ordering=cluster --var=compression=snappy"
databricks bundle deploy -p ontobricks $V && databricks bundle run tpcds_build_spark -p ontobricks $V

# an unordered arm at another segment  -> tpcds_sf100_defaultf8
V="--var=scale_factor=100 --var=ordering=none --var=rows_per_file=8000000"
databricks bundle deploy -p ontobricks $V && databricks bundle run tpcds_build_spark -p ontobricks $V
```

`rows_per_file` is `spark.sql.files.maxRecordsPerFile`, whole millions, inside Direct Lake's 1M–16M
window — and since the recipe sets no row-group cap, the file close ends the group, so this number
IS the segment. The deployed job's name carries it. Use `&&`, never `;`: `bundle run` after a failed
deploy launches whatever arm was deployed last.

Builds never overwrite: table names are the paper's, the arm lives in the schema suffix, and an
existing table stops the run. Each build appends its report to
`databricks_ne.tpcds_raw.layout_build_report` and fails if `OPTIMIZE` ran or a v2 checkpoint made it
into the protocol. Reader version is reported, not asserted — deletion vectors are expected now.

## Fabric

Workspace `51650f82-…`, mirrored item `01b539f3-…`, lakehouse `tpcds_bench`.

1. Databricks: external data access on the metastore, `EXTERNAL USE SCHEMA` on the schema for the
   identity Fabric's connection uses.
2. Fabric UI: add the schema to the mirrored Azure Databricks catalog item; wait for the 10 tables.
3. `python fabric/deploy_paper_model.py --workspace <ws> --arm default --sf 1000` (optional: the
   benchmark recreates the model itself before every run).
4. `python notebooks/build_notebooks.py` then `python deploy_bench.py --deploy-only`, which
   creates the lakehouse and deploys the notebooks and the pipeline into the `dbx` workspace
   folder. (Raw equivalent: `ws.deploy("notebooks", folder="dbx", overwrite=True)`,
   `ws.deploy("pipelines", notebook="run_benchmark", folder="dbx", overwrite=True)`.)
5. Trigger `run_benchmark_pipeline` in the Fabric UI (an API-triggered pipeline cannot run a
   notebook that needs a user token). Parameters: `sf`, `arms` (`default`, `defaultf8`, `cluster`, `clustersn`,
   `partition`, `vorder`, `vonly`, `duckdb`, `ducksort`, comma list), `models`. Then open `results`.

### The Fabric-written arms

Two notebooks rewrite the mirrored `tpcds_sf{sf}_default` rows into the `tpcds_vorder` lakehouse.
Both are **hand-written**: `build_notebooks.py` does not emit them and a rebuild will not touch
them. Neither regenerates data — the mirror is the source, so there is no second `dsdgen` cycle.
Both refuse to overwrite: a table that exists is skipped, and a real rebuild means dropping the
schema by hand.

| notebook | compute | writer | `variant` → schema |
|---|---|---|---|
| `build_vorder` | Fabric Spark, `readHeavyForPBI` | V-Order | `paper` → `tpcds_sf{sf}`, `vonly` → `_vonly` |
| `build_duckdb` | Fabric **Python**, 64 vCores | delta_rs (delta-rs, via duckrun) | `default` → `_duckdb`, `sorted` → `_ducksort` |

`build_duckdb` takes duckrun's write profile as it ships — the geometry knobs are dbt model configs
and the notebook API passes none of them — so the setup cell prints the profile in force and that
print is the record of what the arm was written under. **It writes SNAPPY where every other arm is
ZSTD**, and duckrun cannot be told otherwise from a notebook, so its bytes are not comparable to
another arm's. Its encodings are: dictionary coverage is read from the parquet footer.

Its `default` variant is `SORTED BY AUTO` — duckrun profiles the data and picks the `ORDER BY`
itself, optimising modelled in-memory bytes rather than pruning, so it is **not** the date key every
other ordered arm uses. The key it chose is captured per table and printed in the report; without it
the arm cannot be read. That variant is also the expensive one: a global sort of the whole table,
spilled to the node's local disk, and above 30M rows each fact is read from the mirror twice (a
~30M-row substrate for the profile, then the real read). **Validate at `sf=10` first.**

Its `sorted` variant orders the facts on the **date key** — the same key the Databricks `cluster`
arm uses, and the one 23 of the paper's 24 queries filter on. That is the point of it: `cluster` vs
`ducksort` holds the ordering key fixed and changes the writer, which `default` could never do.

Two arms were dropped on 2026-09-08, both never built and never run: `duckpart` (delta_rs at the
paper's 143k-row partition geometry) and `vcluster` (V-Order plus liquid clustering). Both needed a
layout already measured to cost more than it returns, and neither was wired end to end.
