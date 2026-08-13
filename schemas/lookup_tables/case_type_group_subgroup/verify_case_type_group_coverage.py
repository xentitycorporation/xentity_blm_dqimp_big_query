#!/usr/bin/env python
"""
Verify the Case Type Group / Subgroup SQL queries against the authoritative taxonomy.

WHY THIS EXISTS
---------------
Correctness of these queries cannot be established by reading them. Three separate
false conclusions were reached on 2026-08-10/11 by grepping the files, because the
change-log comments inside them quote the very codes they removed. This script
answers the question the only way that is reliable: it RUNS every subgroup query
against blm_product in BigQuery and reconciles the result against the taxonomy CSV.

It also catches things a human review will not. The 262400 double-classification had
survived multiple reviews; this reconciliation found it immediately.

WHAT IT CHECKS
--------------
  1. Every taxonomy code is claimed by at least one subgroup query
       (unclassified codes absent from blm_product are reported separately -- the
        taxonomy is a comprehensive HISTORICAL list, so this is expected, not a fault)
  2. No code is claimed by more than one subgroup   <- the 262400 class of bug
  3. No query claims a code that is not in the taxonomy
  4. Every code lands in the subgroup the taxonomy assigns it to
  5. Every product code in blm_product has a taxonomy row  <- catches new codes like 380800

USAGE
-----
    python verify_case_type_group_coverage.py                 # latest snapshot found
    python verify_case_type_group_coverage.py --snapshot 20260802
    python verify_case_type_group_coverage.py --queries <dir> --csv <file>

Exit code 0 = all checks pass. Non-zero = at least one check failed.

REQUIREMENTS
------------
bq CLI authenticated (gcloud auth login). No Python packages beyond the stdlib.
"""

import argparse
import csv
import os
import re
import subprocess
import sys
import tempfile
from collections import defaultdict

DEFAULT_BASE = os.path.dirname(os.path.abspath(__file__))
# Repo layout (paths are relative to THIS file, so the script works from a clone
# anywhere on disk):
#   schemas/lookup_tables/case_type_group_subgroup/   <- this file + the CSV
#   schemas/case_type_group_queries/                  <- the subgroup .sql files
DEFAULT_QUERIES = os.path.normpath(
    os.path.join(DEFAULT_BASE, "..", "..", "case_type_group_queries")
)
DEFAULT_CSV = os.path.join(DEFAULT_BASE, "Product Code with Case Type Subgroups.csv")
PROJECT = "xentity-sandbox-huy"
DATASET = "blm_seta_dqimp"

# (Case Type Group, Case Type Subgroup) -> query file implementing it.
# Keep in sync with the taxonomy: a subgroup with no entry here is reported as unmapped.
SUBGROUP_QUERIES = {
    ("Fluid Minerals", "Agreements"): "caseTypeGroup_FM_agreements.sql",
    ("Fluid Minerals", "Fluid Minerals"): "caseTypeGroup_FM_fluidminerals.sql",
    ("Fluid Minerals", "Geothermal Leases"): "caseTypeGroup_FM_geotherm.sql",
    ("Fluid Minerals", "Oil/Gas Leases"): "caseTypeGroup_FM_oilandgas.sql",
    ("Fluid Minerals", "Participating Areas"): "caseTypeGroup_FM_participating.sql",
    ("Land Tenure", "Acquisitions"): "caseTypeGroup_LTe_acquisitions.sql",
    ("Land Tenure", "Classifications"): "caseTypeGroup_LTe_classifications.sql",
    ("Land Tenure", "Exchange"): "caseTypeGroup_LTe_exchanges.sql",
    ("Land Tenure", "Grants"): "caseTypeGroup_LTe_grants.sql",
    ("Land Tenure", "Occupancy Use"): "caseTypeGroup_LTe_occupancyuse.sql",
    ("Land Tenure", "Other Case Type Groups"): "caseTypeGroup_LTe_othercase.sql",
    ("Land Tenure", "Public Admin Procedures"): "caseTypeGroup_LTe_publicadmin.sql",
    ("Land Tenure", "RPP Patents Reconveyances"): "caseTypeGroup_LTe_rpppatents.sql",
    ("Land Tenure", "Sales"): "caseTypeGroup_LTe_sales.sql",
    ("Land Tenure", "Withdrawals"): "caseTypeGroup_LTe_withdrawls.sql",
    ("Land Transfer", "Land Transfer"): "caseTypeGroup_Ltr_landtransfer.sql",
    ("Land Use Authorizations", "Leases Permits Easements"): "caseTypeGroup_LUA_leasepermitease.sql",
    ("Land Use Authorizations", "Rights of Way"): "caseTypeGroup_LUA_rightsofway.sql",
    ("Mining Claims", "Lode Claim"): "caseTypeGroup_MC_lodeclaim.sql",
    ("Mining Claims", "Mill Site"): "caseTypeGroup_MC_millsite.sql",
    ("Mining Claims", "Placer Claim"): "caseTypeGroup_MC_placerclaim.sql",
    ("Mining Claims", "Tunnel Site"): "caseTypeGroup_MC_tunnelsite.sql",
    ("Solid Minerals", "AML"): "caseTypeGroup_SM_aml.sql",
    ("Solid Minerals", "Coal"): "caseTypeGroup_SM_coal.sql",
    ("Solid Minerals", "Free Use Permit"): "caseTypeGroup_SM_freeusepermit.sql",
    # New subgroup, SME decision 2026-08-11. Holds only 380800 "ABANDONED MINE LAND INV",
    # deliberately kept isolated -- see caseTypeGroup_SM_generalmininglaws.sql header.
    ("Solid Minerals", "General Mining Laws"): "caseTypeGroup_SM_generalmininglaws.sql",
    ("Solid Minerals", "Locatables"): "caseTypeGroup_SM_locatables.sql",
    ("Solid Minerals", "Non-Energy Leaseables"): "caseTypeGroup_SM_nonenergyleaseables.sql",
    ("Solid Minerals", "Oil/Shale"): "caseTypeGroup_SM_oilshale.sql",
    ("Solid Minerals", "Saleables"): "caseTypeGroup_SM_saleables.sql",
    ("Solid Minerals", "Solid Minerals"): "caseTypeGroup_SM_solidminerals.sql",
    ("Survey", "Survey"): "caseTypeGroup_Survey_survey.sql",
}


def _bq_env():
    """
    Environment for the bq CLI.

    On Windows, `bq` is a .cmd shim that shells out to `python`. Launched from inside a
    Python subprocess, that bare `python` can resolve to the Microsoft Store alias stub
    instead of a real interpreter, and bq dies with:

        "Python was not found; run without arguments to install from the Microsoft Store"

    bq/gcloud honour CLOUDSDK_PYTHON, so point it at the interpreter already running this
    script. Respect the value if the caller has set one.
    """
    env = os.environ.copy()
    env.setdefault("CLOUDSDK_PYTHON", sys.executable)
    return env


def run_bq(sql, fmt="csv", max_rows=20000):
    """
    Run SQL via the bq CLI.

    SQL is piped in on STDIN, never passed as an argument: several queries in this project
    open with `--` comment lines, which bq would otherwise parse as flags (KB 4.17.4).

    `input=` is used rather than a temp file + `stdin=`. On Windows `bq` is a .cmd shim, so
    it needs shell=True to execute -- and under shell=True a redirected file handle does not
    reach the child, which surfaces as the misleading "No query string provided".
    """
    proc = subprocess.run(
        ["bq", "query", "--use_legacy_sql=false", "--format=" + fmt,
         "--max_rows=%d" % max_rows],
        input=sql, capture_output=True, text=True, shell=True, env=_bq_env(),
    )
    if proc.returncode != 0:
        sys.stderr.write("bq query failed:\n%s\n" % (proc.stderr or proc.stdout))
        sys.exit(2)
    return proc.stdout


def latest_snapshot():
    out = run_bq(
        "SELECT MAX(table_id) AS t FROM `%s.%s.__TABLES_SUMMARY__` "
        "WHERE STARTS_WITH(table_id, 'blm_product_')" % (PROJECT, DATASET))
    rows = [r for r in csv.DictReader(out.splitlines())]
    if not rows or not rows[0].get("t"):
        sys.stderr.write("could not determine the latest blm_product snapshot\n")
        sys.exit(2)
    return rows[0]["t"].replace("blm_product_", "")


def extract_where(path):
    """Pull the WHERE clause out of a caseTypeGroup_*.sql file.

    The clause sits inside an EXECUTE IMMEDIATE FORMAT(\"\"\"...\"\"\") block, where %
    is doubled for FORMAT(). Comments are never included, which is the entire point:
    a change-log comment quoting a removed code cannot influence the result.
    """
    txt = open(path, encoding="utf-8", errors="replace").read()
    body = "\n".join(l for l in txt.splitlines() if not l.lstrip().startswith("--"))
    i = body.upper().find("WHERE")
    j = body.upper().find("ORDER BY", i)
    if i < 0 or j < 0:
        raise ValueError("no WHERE ... ORDER BY found in %s" % os.path.basename(path))
    return body[i + 5:j].strip().replace("%%", "%")


def load_taxonomy(path):
    sheet = {}
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            code = (row.get("BLM Product Code") or "").strip()
            if code:
                sheet[code] = ((row.get("Case Type Group") or "").strip(),
                               (row.get("Case Type Subgroup") or "").strip(),
                               (row.get("BLM Product") or "").strip())
    return sheet


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--queries", default=DEFAULT_QUERIES)
    ap.add_argument("--csv", default=DEFAULT_CSV)
    ap.add_argument("--snapshot", default=None, help="YYYYMMDD (default: latest found)")
    args = ap.parse_args()

    snap = args.snapshot or latest_snapshot()
    sheet = load_taxonomy(args.csv)
    print("taxonomy CSV   : %s" % args.csv)
    print("query folder   : %s" % args.queries)
    print("snapshot       : blm_product_%s" % snap)
    print("taxonomy codes : %d" % len(sheet))
    print()

    taxonomy_subgroups = {(g, s) for g, s, _ in sheet.values()}
    unmapped = sorted(taxonomy_subgroups - set(SUBGROUP_QUERIES))
    if unmapped:
        print("!! subgroups in the taxonomy with no query mapped in this script:")
        for g, s in unmapped:
            print("     %s / %s" % (g, s))
        print()

    parts = []
    for (g, s), fn in sorted(SUBGROUP_QUERIES.items()):
        clause = extract_where(os.path.join(args.queries, fn))
        parts.append(
            "SELECT %s AS grp, %s AS sub, CASE_TYPE_CODE\nFROM `%s.%s.blm_product_%s`\nWHERE %s"
            % ("'" + g.replace("'", "\\'") + "'", "'" + s.replace("'", "\\'") + "'",
               PROJECT, DATASET, snap, clause))

    sql = ("WITH classified AS (\n" + "\nUNION ALL\n".join(parts) + "\n)\n"
           "SELECT grp, sub, CASE_TYPE_CODE FROM classified\n"
           "GROUP BY grp, sub, CASE_TYPE_CODE ORDER BY grp, sub, CASE_TYPE_CODE\n")

    claimed = defaultdict(list)
    for row in csv.DictReader(run_bq(sql).splitlines()):
        claimed[row["CASE_TYPE_CODE"].strip()].append((row["grp"], row["sub"]))

    prod = {r["CASE_TYPE_CODE"].strip() for r in csv.DictReader(run_bq(
        "SELECT DISTINCT CASE_TYPE_CODE FROM `%s.%s.blm_product_%s` "
        "WHERE CASE_TYPE_CODE IS NOT NULL" % (PROJECT, DATASET, snap)).splitlines())}

    failures = []

    dupes = {c: v for c, v in claimed.items() if len(v) > 1}
    print("[1] codes claimed by more than one subgroup : %d" % len(dupes))
    for c, v in sorted(dupes.items()):
        print("      %-8s %s | taxonomy says %s" % (c, v, sheet.get(c, "NOT IN TAXONOMY")))
    if dupes:
        failures.append("%d double-classified" % len(dupes))

    phantom = sorted(set(claimed) - set(sheet))
    print("[2] codes claimed but absent from taxonomy   : %d" % len(phantom))
    for c in phantom:
        print("      %-8s claimed by %s" % (c, claimed[c]))
    if phantom:
        failures.append("%d phantom" % len(phantom))

    wrong = [(c, sheet[c][:2], claimed[c][0]) for c in sheet
             if c in claimed and len(claimed[c]) == 1 and claimed[c][0] != sheet[c][:2]]
    print("[3] codes in the wrong subgroup              : %d" % len(wrong))
    for c, want, got in wrong:
        print("      %-8s taxonomy=%s / %s  query=%s / %s" % (c, want[0], want[1], got[0], got[1]))
    if wrong:
        failures.append("%d misclassified" % len(wrong))

    unclassified = sorted(set(sheet) - set(claimed))
    live = [c for c in unclassified if c in prod]
    dormant = [c for c in unclassified if c not in prod]
    print("[4] taxonomy codes no query claims           : %d" % len(unclassified))
    print("      of which LIVE in blm_product (FAIL)    : %d" % len(live))
    for c in live:
        print("        %-8s %s / %s" % (c, sheet[c][0], sheet[c][1]))
    print("      of which absent from blm_product (ok)  : %d" % len(dormant))
    if dormant:
        print("        %s" % ", ".join(dormant))
    if live:
        failures.append("%d live codes unclassified" % len(live))

    missing = sorted(prod - set(sheet))
    print("[5] blm_product codes with no taxonomy row   : %d" % len(missing))
    for c in missing:
        print("      %s  <- needs SME classification" % c)
    if missing:
        failures.append("%d product codes untaxonomised" % len(missing))

    print()
    if failures:
        print("FAIL: " + "; ".join(failures))
        return 1
    print("PASS: all checks clean against blm_product_%s" % snap)
    return 0


if __name__ == "__main__":
    sys.exit(main())
