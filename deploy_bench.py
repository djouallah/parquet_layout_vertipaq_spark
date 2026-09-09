"""Deploy and run the concurrency ladder.

  python deploy_bench.py                      # deploy everything, then run the ladder
  python deploy_bench.py --deploy-only        # deploy, run nothing
  python deploy_bench.py --smoke              # deploy, then ONE 1-thread rung as a smoke test

Everything deployed here is filed in the workspace folder `dbx`, the same folder the semantic
models go to, so the whole experiment is one folder in the workspace.

Steps, in dependency order:

  1. lakehouse `tpcds_bench` -- holds `Tables/dbo/perfresults3` and the uploaded query file. Kept
     apart from `tpcds_vorder` so benchmark output never mixes into an arm's own data. Its NAME is
     still `tpcds_bench`; only the folder it lives in is `dbx`.
  2. (the DAX suite needs no upload -- build_notebooks.py embeds it in RunPerfScenario.)
  3. check both arms' semantic models exist (fabric/deploy_paper_model.py deploys them).
  4. deploy `RunPerfScenario` and `run_benchmark`, then the pipeline pointed at `run_benchmark`.
  5. run it.

The ladder itself (rungs, models, query count) lives in the pipeline's parameter DEFAULTS, because
`duckrun.run()` takes no parameters -- see pipelines/run_benchmark_pipeline.json.

Its default IS the paper's protocol: 20 concurrent virtual users, 24 queries, and THREE separate
load tests -- but all three now happen INSIDE one notebook invocation, against ONE semantic model,
because that is what the paper did. Their Run 1 is the first touch of a model they had just
created; creating a fresh model per run (which is what the old three-rung `ladder` did) makes every
run a Run 1, and there is no warming curve left to measure. `runs = 3` carries that now, and
`iterations` stays 1 -- that repeats the suite inside a single thread, not as a separate load test.

`ladder` is therefore a CONCURRENCY ladder again, one rung by default. The old 1/4/16 rungs were a
scaling curve of our own, not their test; pass them in `ladder` at trigger time to get that chart
back -- each rung then gets its own model lifetime, which is correct.
"""
from __future__ import annotations

import argparse
import os
import sys

WORKSPACE = "51650f82-6bb5-4023-b0ab-db197d32e0be"   # mimbenchmarking, F128, West Central US
MIRROR_ITEM = "01b539f3-4a9d-45ef-b1ef-0ba59552eb21"  # Mirrored Azure Databricks catalog
VORDER_LAKEHOUSE = "tpcds_vorder"
BENCH_LAKEHOUSE = "tpcds_bench"
FOLDER = "dbx"                  # workspace folder deployed items land in -- the same one the
                                # semantic models use (fabric/deploy_paper_model.py, and
                                # build_notebooks.py MODEL_FOLDER for the ones a run recreates), so
                                # everything this repo deploys sits in one folder. Note this is NOT
                                # BENCH_LAKEHOUSE above: that is the lakehouse's name, this is where
                                # it is filed. duckrun only PLACES items it creates -- an existing
                                # item is updated where it already lives, so anything left in the
                                # old `tpcds_bench` folder has to be moved once in the Fabric UI.

# The two arms' semantic models are deployed by fabric/deploy_paper_model.py, which ships the
# paper's own TMDL. This script only names them so it can check they exist before running.
# These names must match build_notebooks.py's SCHEMA map or the check below reports every model as
# missing.
ARM_MODELS = ["tpcds_sf100_cluster", "tpcds_sf100_vorder",
              "tpcds_sf1000_cluster", "tpcds_sf1000_default"]

HERE = os.path.dirname(os.path.abspath(__file__))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workspace", default=WORKSPACE)
    ap.add_argument("--deploy-only", action="store_true")
    ap.add_argument("--smoke", action="store_true",
                    help="run run_benchmark once at its defaults (1 thread) instead of the ladder")
    ap.add_argument("--skip-models", action="store_true", help="skip the semantic-model presence check")
    args = ap.parse_args()

    os.chdir(HERE)
    import duckrun

    ws = duckrun.workspace(args.workspace)
    print(f"workspace {args.workspace} -> {ws.id}")

    # 1 + 2: the results lakehouse and the query file
    # schema-enabled: results land at Tables/dbo/perfresults3, i.e. schema `dbo`.
    bench_id = ws.create_lakehouse(BENCH_LAKEHOUSE, schemas=True, folder=FOLDER)
    print(f"lakehouse {BENCH_LAKEHOUSE} -> {bench_id}")
    # No query-file upload: the suite is EMBEDDED in RunPerfScenario by
    # notebooks/build_notebooks.py, so there is nothing to keep in sync in the lakehouse and the
    # notebook records exactly the DAX it ran.

    # 3: the arms' semantic models are NOT deployed here, and they no longer need to EXIST here
    # either -- `run_benchmark` deletes and recreates its arm's model from the paper's TMDL before
    # measuring it, which is what makes its first pass genuinely cold. (This used to check they
    # were present, on the reasoning that rebuilding one would drop the segments a previous rung
    # left resident. That is now the point, not a hazard.) The check stays as a warning only,
    # because a model already sitting there is the normal case and its absence is not an error.
    if not args.skip_models:
        have = {it["displayName"] for it in ws.list_items("semanticModels")}
        missing = [m for m in ARM_MODELS if m not in have]
        print(f"semantic models present: {sorted(set(ARM_MODELS) & have) or 'none'}"
              + (f"; {missing} will be created by the run itself" if missing else ""))

    # 4: notebooks first -- the pipeline is bound to run_benchmark by name.
    ws.deploy("notebooks", folder=FOLDER, overwrite=True)
    ws.deploy("pipelines", notebook="run_benchmark", folder=FOLDER, overwrite=True)

    if args.deploy_only:
        print("\ndeployed; nothing run (--deploy-only)")
        return

    if args.smoke:
        print("\nsmoke test: run_benchmark at its own defaults (1 thread, both arms)")
        print(ws.run("run_benchmark"))
        return

    print("\nrunning the paper's protocol (per arm: 1 model lifetime x 3 load tests x 20 users)")
    print(ws.run("run_benchmark_pipeline"))


if __name__ == "__main__":
    main()
