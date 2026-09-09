"""Deploy the white paper's own semantic model, as TMDL, pointed at one of our arms.

The paper's repo ships two complete TMDL model folders (fetched into `paper/model/`, gitignored), so the
model is not something to rebuild from a generator -- it is something to deploy verbatim. That is
the only way to get the objects its query capture depends on:

  * `Measures 1`  a calculated import table hosting all 11 measures. Every query qualifies its
                  measures against it (`'Measures 1'[Store Revenue]`), so a model that hosts them
                  anywhere else fails all 24 queries.
  * `Time Unit`   a field parameter over `date_dim[d_year]` / `[d_quarter_name]`, read by queries
                  3, 11 and 21.
  * `calendar tpcds_calendar` on `date_dim`  what `TOTALYTD(..., 'tpcds_calendar')` and
                  `SAMEPERIODLASTYEAR('tpcds_calendar')` resolve against. It needs compatibility
                  level 1702, which is why this is TMDL and not a TMSL model.bim.

  python fabric/deploy_paper_model.py --workspace <ws> --arm layout
  python fabric/deploy_paper_model.py --workspace <ws> --arm cluster --sf 1000
  python fabric/deploy_paper_model.py --workspace <ws> --arm all --recreate
  python fabric/deploy_paper_model.py --workspace <ws> --dry-run     # print the diff, deploy nothing

Substitutions are applied IN MEMORY and printed as a diff, so `paper/` stays a clean upstream copy
of github.com/lipinght/DB-DQ-Whitepaper (MIT), commit 99f9904d, folder test_model_report/. Three
are mechanical (the OneLake URL, `schemaName`, the database name and id) and one is deliberate:
`directLakeBehavior: directLakeOnly`, so a query Direct Lake cannot serve fails loudly instead of
being served by the SQL endpoint and recorded as a slow Direct Lake. Applied to both arms.
"""
from __future__ import annotations

import argparse
import difflib
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PAPER = os.path.join(HERE, os.pardir, "paper", "model")

# arm -> (vendored TMDL folder, item holding the tables, schema in that item, semantic model name).
# The only place these four facts appear together.
ARMS = {
    # Direct Lake over the MIRRORED Databricks catalog. Upstream's own mirrored model, whose
    # partitions already read a mirrored schema; only the schema name differs. This is the
    # configuration-only arm: the recipe's spark_conf and nothing else.
    "default": ("fabric_mirrored", "01b539f3-4a9d-45ef-b1ef-0ba59552eb21", "tpcds_sf100_default",
               "tpcds_sf100_default"),
    # THE FALSIFIABLE HALF of "small segments pay only with an ordering": the same unordered write
    # at 8M rows per group, one group per file, against `default`'s 6M. If size is not the lever
    # without an ordering, this lands within noise of it. If it is clearly worse, the claim is wrong
    # and smaller IS better unordered.
    "defaultf8": ("fabric_mirrored", "01b539f3-4a9d-45ef-b1ef-0ba59552eb21",
                   "tpcds_sf100_defaultf8", "tpcds_sf100_defaultf8"),
    # WITHDRAWN. 6M-row groups TWO to a 12M-row file -- the geometry only
    # parquet.block.row.count.limit can express. It tied `default`'s one group per file, so the key
    # bought nothing and left the recipe; the arm left with it. Kept addressable so its footers stay
    # readable, filtered by name everywhere its rows or chunks could reach a chart.
    "default2rg": ("fabric_mirrored", "01b539f3-4a9d-45ef-b1ef-0ba59552eb21", "tpcds_sf100_default2rg",
                   "tpcds_sf100_default2rg"),
    # The same mirrored item again, the liquid-clustering schema: same TMDL, only the schema.
    "cluster": ("fabric_mirrored", "01b539f3-4a9d-45ef-b1ef-0ba59552eb21", "tpcds_sf100_cluster",
                "tpcds_sf100_cluster"),
    # The same clustered arm in SNAPPY, and with `delta.targetFileSize` = 128 MB, which the first
    # clustered build predates. Together those are what the delta_rs arm has that ours did not:
    # a codec that decompresses fast and row groups in the 2-5M band with the dictionary intact.
    # delta_rs is the best COLD arm in the project (17.7 s against this arm's 36.9), and cold is
    # where a codec and a dictionary decide the answer.
    "clustersn": ("fabric_mirrored", "01b539f3-4a9d-45ef-b1ef-0ba59552eb21", "tpcds_sf100_clustersn",
                  "tpcds_sf100_clustersn"),
    # The same mirrored item again: partitioned by the date key, NOT V-Ordered and with no ordering
    # inside a file -- the paper's Fabric arm's geometry, minus its encoding. Paired against
    # `vorder` it BOUNDS V-Order (the Fabric arm is also Z-order interleaved in-file); paired
    # against `default` it isolates the geometry.
    "partition": ("fabric_mirrored", "01b539f3-4a9d-45ef-b1ef-0ba59552eb21", "tpcds_sf100_partition",
                  "tpcds_sf100_partition"),
    # Direct Lake on OneLake over the Fabric Spark V-Order copy.
    "vorder": ("fabric_direct_lake", "f19d8f93-2956-4cf7-a1bb-c526d0a953cd", "tpcds_sf100",
               "tpcds_sf100_vorder"),
    # The same lakehouse, V-Order AND NOTHING ELSE: no partitionBy, no sort, no OPTIMIZE
    # (notebooks/build_vorder.ipynb, variant="vonly"). The paper's arm partitions the facts by the
    # date key, which lands one file per partition -- 1,823 files, ~143k-row row groups, so ~1,800
    # Direct Lake segments against a 6M-row Databricks arm's ~46. vorder vs vonly says how much of
    # that arm's advantage is the ENCODING and how much is the partition.
    "vonly": ("fabric_direct_lake", "f19d8f93-2956-4cf7-a1bb-c526d0a953cd", "tpcds_sf100_vonly",
              "tpcds_sf100_vonly"),
    # The delta_rs arms (notebooks/build_duckdb.ipynb): delta-rs via duckrun, reading the mirrored
    # `default` rows back and rewriting them. Same lakehouse as the V-Order arms and the same
    # `fabric_direct_lake` TMDL -- Direct Lake on OneLake, not over the mirror -- but NOT V-Ordered,
    # which the item name does not say. `duckdb` is `SORTED BY AUTO` -- duckrun profiles the data and
    # picks the ORDER BY itself, to minimise modelled in-memory bytes rather than to prune, so it is
    # NOT the date key every other ordered arm here uses.
    "duckdb": ("fabric_direct_lake", "f19d8f93-2956-4cf7-a1bb-c526d0a953cd", "tpcds_sf100_duckdb",
               "tpcds_sf100_duckdb"),
    # `ducksort` is the one that pairs cleanly with the Databricks `cluster` arm: the SAME date key,
    # ordered by a delta-rs sort instead of a clustering exchange.
    "ducksort": ("fabric_direct_lake", "f19d8f93-2956-4cf7-a1bb-c526d0a953cd",
                 "tpcds_sf100_ducksort", "tpcds_sf100_ducksort"),
}

ONELAKE = "https://onelake.dfs.fabric.microsoft.com"


def set_scale_factor(sf: int) -> None:
    """Repoint both arms at another scale factor's schemas.

    ARMS is written for SF100, and the only thing that varies between scale factors is the schema
    name (and therefore the model name) -- the mirrored item, the TMDL and the measures are all the
    same. Rewriting in place keeps one definition of each arm rather than a second ARMS table that
    can drift out of step with the first.
    """
    if sf == 100:
        return
    for arm, (folder, item_id, schema, model) in ARMS.items():
        ARMS[arm] = (folder, item_id,
                     schema.replace("sf100", f"sf{sf}"), model.replace("sf100", f"sf{sf}"))


def substitute(name: str, text: str, ws_id: str, item_id: str, schema: str, model: str) -> str:
    """The three mechanical substitutions, plus the one deliberate deviation."""
    out = text
    if name == "expressions.tmdl":
        # Upstream redacted the GUIDs as XXXX; point the shared expression at our item.
        out = re.sub(r'AzureStorage\.DataLake\("[^"]*"',
                     f'AzureStorage.DataLake("{ONELAKE}/{ws_id}/{item_id}"', out)
    elif name == "database.tmdl":
        out = re.sub(r"^database .*$", f"database {model}", out, count=1, flags=re.M)
        # Drop the hard-coded id: both arms ship the same file and would collide on it.
        out = re.sub(r"^\tid: .*\n", "", out, flags=re.M)
    elif name == "model.tmdl":
        # THE deliberate deviation. Upstream sets nothing, which defaults to `automatic`: a query
        # Direct Lake cannot serve is then served by the SQL analytics endpoint instead, and that
        # shows up as a slow duration rather than an error -- recorded as "Direct Lake was slow"
        # when Direct Lake never ran. This benchmark compares two Direct Lake layouts, so such a
        # number is worse than none. Applied identically to both arms, so it favours neither.
        out = re.sub(r"^(model .*)$", r"\1\n\tdirectLakeBehavior: directLakeOnly", out,
                     count=1, flags=re.M)
    elif name.startswith("tables/"):
        out = re.sub(r"^(\s*)schemaName: .*$", rf"\g<1>schemaName: {schema}", out, flags=re.M)
    return out


def parts_for(arm: str, ws_id: str, show_diff: bool = True):
    """Every TMDL file as a definition part, substituted, with a diff of what changed."""
    from duckrun.fabric_remote import _PBISM, _b64_part

    folder, item_id, schema, model = ARMS[arm]
    root = os.path.join(PAPER, folder)
    if not os.path.isdir(root):
        sys.exit(f"paper model not found: {root} -- fetch test_model_report/ from "
                 "github.com/lipinght/DB-DQ-Whitepaper (commit 99f9904d) into paper/model/")

    files = [f for f in sorted(os.listdir(root)) if f.endswith(".tmdl")]
    files += [f"tables/{f}" for f in sorted(os.listdir(os.path.join(root, "tables")))
              if f.endswith(".tmdl")]

    parts, changed = [], 0
    for rel in files:
        with open(os.path.join(root, rel), encoding="utf-8") as fh:
            original = fh.read()
        new = substitute(rel, original, ws_id, item_id, schema, model)
        if new != original:
            changed += 1
            if show_diff:
                for line in difflib.unified_diff(original.splitlines(), new.splitlines(),
                                                 f"upstream/{rel}", f"deployed/{rel}",
                                                 lineterm="", n=0):
                    print("   " + line)
        parts.append(_b64_part(f"definition/{rel}", new))
    parts.append(_b64_part("definition.pbism", _PBISM))
    print(f"  {len(files)} tmdl files, {changed} modified, {len(parts)} definition parts")
    return parts, model


def deploy(ws, arm: str, recreate: bool, refresh: bool, folder: str = "dbx") -> str:
    from duckrun.auth import get_powerbi_token
    from duckrun.fabric_remote import (_FABRIC_API, _create_item, _ensure_folder, _http_request,
                                       _refresh_semantic_model)

    folder, item_id_src, schema, model = ARMS[arm]
    print(f"\n=== arm {arm}: {folder} -> schema {schema} in item {item_id_src}")
    parts, model = parts_for(arm, ws.id)

    existing = next((it for it in ws.list_items("semanticModels")
                     if it.get("displayName") == model), None)
    if existing and recreate:
        # The model this replaces hosted its measures on the fact tables and had neither
        # `Measures 1` nor `Time Unit`. updateDefinition would replace the definition anyway, but a
        # clean create leaves no doubt about what is deployed.
        r = _http_request("DELETE",
                          f"{_FABRIC_API}/workspaces/{ws.id}/semanticModels/{existing['id']}",
                          token=ws._token)
        print(f"  deleted existing {model!r} ({existing['id']}) -> HTTP {r.status_code}")
        existing = None

    item_id = existing["id"] if existing else None
    # The same workspace folder `run_benchmark` creates its per-run models in, so a model deployed
    # from the laptop and one created by a run sit side by side instead of one of them landing in
    # the workspace root. Cosmetic, and `_ensure_folder(required=False)` treats it that way.
    folder_id = _ensure_folder(ws._token, ws.id, folder) if folder else None
    try:
        new_id = _create_item(ws._token, ws.id, "semanticModels", model, parts, item_id=item_id,
                              folder_id=folder_id)
    except Exception as e:                                          # noqa: BLE001
        # TMSL goes in with no `format`. If the API wants one for TMDL, it says so here.
        print(f"  plain definition rejected ({str(e).splitlines()[0][:160]}); retrying format=TMDL")
        new_id = _create_item(ws._token, ws.id, "semanticModels", model, parts, fmt="TMDL",
                              item_id=item_id, folder_id=folder_id)
    print(f"  {'updated' if item_id else 'created'} semantic model {model!r} ({new_id})")

    if refresh:
        print("  reframing...")
        _refresh_semantic_model(get_powerbi_token(), ws.id, new_id)
        print("  reframe complete")
    print(f"  https://msit.powerbi.com/groups/{ws.id}/datasets/{new_id}")
    return new_id


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workspace", required=True)
    ap.add_argument("--arm", choices=list(ARMS) + ["all", "both"], default="all",
                    help="one arm, or all of them (`both` is the old name for all)")
    ap.add_argument("--sf", type=int, default=100,
                    help="scale factor: picks the tpcds_sf{N}_* schemas (default 100)")
    ap.add_argument("--recreate", action="store_true",
                    help="delete the model first instead of updating its definition in place")
    ap.add_argument("--no-refresh", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="print the diff, deploy nothing")
    args = ap.parse_args()

    import duckrun
    set_scale_factor(args.sf)
    ws = duckrun.workspace(args.workspace)
    arms = list(ARMS) if args.arm in ("all", "both") else [args.arm]

    if args.dry_run:
        for arm in arms:
            print(f"\n=== arm {arm}")
            parts_for(arm, ws.id)
        return

    for arm in arms:
        deploy(ws, arm, args.recreate, not args.no_refresh)


if __name__ == "__main__":
    main()
