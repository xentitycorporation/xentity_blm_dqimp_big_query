# Web Experience table builder

Builds the tables behind the **BLM Land Records Data Quality Viewer** (ArcGIS Web Experience,
BLM HQ Portal) and publishes them, so the app shows current data without anything being assembled
by hand.

```
BigQuery snapshots  ->  SQL in this folder  ->  CSVs in Query_Results\  ->  hosted tables in the Portal
                                                                            (overwritten in place,
                                                                             item IDs unchanged, so
                                                                             dashboards update on their own)
```

Nothing is written back to BigQuery, and no CSV is ever edited by hand: re-run the script instead.

## Folders

| Path | What it is |
|---|---|
| `dqimp_webapp.py` | The `build` and `publish` commands |
| `webapp_config.json` | Snapshot list, output folder, Portal item IDs, table names and column display names |
| `mlrs_case_completeness/` | SQL for the MLRS Case Completeness page |
| `MLRS_Case_Completeness_Validate.sql` | One-snapshot check of the four-truths logic against known counts |

## Setup (once per machine)

The script needs ArcGIS **and** BigQuery in one environment, so it runs in a **clone of ArcGIS
Pro's Python environment**, not the repo's `.venv` (which has no `arcgis`).

1. In ArcGIS Pro: **Package Manager → clone** `arcgispro-py3`. Call the clone `arcpro_clone`.
2. Add the BigQuery packages to that clone. **The list depends on the ArcGIS Pro version**: each
   Pro release ships different core packages (protobuf, pyarrow), and with `--no-deps` pip checks
   nothing. Use the list for your Pro version below. For any other version, resolve a new one
   first, with the environment's existing versions locked:
   `pip install --dry-run --ignore-installed --only-binary=:all: --platform win_amd64 --python-version <X.Y> --target tmp <packages> <existing pins>`.

   **ArcGIS Pro 3.5.x** (Python 3.11, protobuf 4.21) — resolved 2026-10-09:

   ```
   conda run -n arcpro_clone python -m pip install --no-deps google-cloud-bigquery==3.46.1 google-cloud-core==2.8.0 google-api-core==2.29.0 google-auth==2.61.0 google-resumable-media==2.11.0 googleapis-common-protos==1.73.0 grpcio==1.84.0 grpcio-status==1.62.3 proto-plus==1.27.1 pyasn1==0.6.4 pyasn1-modules==0.4.2 db-dtypes==1.7.2 google-crc32c==1.9.0
   ```

   **ArcGIS Pro 3.7.x** (Python 3.13, protobuf 5.29) — the list below. A plain
   `pip install google-cloud-bigquery` replaces ArcGIS Pro's own protobuf, pyarrow, requests and
   cffi, because pip cannot see conda's packages — which can break `arcpy` and `arcgis`.
   `--no-deps` adds only what is missing:

   ```
   conda run -n arcpro_clone python -m pip install --no-deps google-cloud-bigquery==3.45.2 google-cloud-core==2.7.0 google-api-core==2.30.3 google-auth==2.58.0 google-resumable-media==2.10.2 googleapis-common-protos==1.75.0 grpcio==1.84.0 grpcio-status==1.71.2 proto-plus==1.28.2 pyasn1==0.6.4 pyasn1-modules==0.4.2 db-dtypes==1.7.1
   ```

3. Check both import:

   ```
   conda run -n arcpro_clone python -c "from google.cloud import bigquery; import arcgis; print(bigquery.__version__, arcgis.__version__)"
   ```

4. Sign in to Google Cloud for `build`: `gcloud auth application-default login`.
5. Sign in to the **BLM HQ Portal inside ArcGIS Pro**, and leave it as Pro's
   active portal. `publish` reuses that sign-in; it never holds a password.

In VS Code, pick `arcpro_clone` as the interpreter, and open **this repo folder** as the workspace
(not its parent — that breaks Git detection and the output path).

## Running it

The script finds its SQL and its output folder from its own location, so it runs from any working
directory. From the repo root:

```
python web-app/tests/dqimp_webapp.py build
python web-app/tests/dqimp_webapp.py publish
python web-app/tests/dqimp_webapp.py display
```

**Where the CSVs go.** Two layouts work with the same `webapp_config.json`:

| Layout | Script at | CSVs written to |
|---|---|---|
| Repo (any Git clone, including the one the dashboard agent runs from) | `<repo>/web-app/tests/` | `output_dir` in the config, relative to the repo root: `../Query_Results/web-app/<page>/`, outside the repo |
| Flat (an older deployed copy outside Git) | `<folder>/tests/` | `<folder>/Query_Results/<page>/`, used whenever a `Query_Results` folder sits next to `tests` |

The page's SQL is always in a folder next to the script (`sql_dir`, e.g. `mlrs_case_completeness/`).
Both commands print the folder they use.

**`build`** runs the page's SQL in one BigQuery session and writes one CSV per table, plus
`build_manifest.json`, to that folder. The output never goes in the repo: the repo is public and
the case table holds serial numbers. `build` stops without writing anything if a snapshot is missing
or if the flows do not balance.

**`publish`** overwrites each hosted table from its CSV. On a table's first time it needs
`--create`, which makes the Portal item in the configured Portal folder (which must already exist)
and writes the new item ID into `webapp_config.json`. After that, plain `publish` overwrites in
place and item IDs never change.

**`display`** sets each hosted table's item title, layer name (`Cases`, `Flows`, `Summary`) and
column display names from `webapp_config.json`, without uploading anything. `publish` runs the same
step itself after every create or overwrite. Use `display` on its own to repair names without
touching data. (Publishing from a CSV item copies the CSV item's title onto the hosted table; this
step renames it.)

Useful flags: `--page` (default `mlrs_case_completeness`), `--dry-run` (publish: show the plan,
send nothing), `--output-dir` (use another output folder; overrides both layouts). `--only TABLE …` (publish/display: just those tables, e.g. `publish --create --only changes` to add a new table without re-uploading the others).

### A new monthly snapshot

The normal way is the dashboard update agent: open `web-app\` in VS Code and
type `/dashboard-update`. It does the steps below with checks around them. See
[../README_dashboard_agent.md](../README_dashboard_agent.md), "Each month".

By hand:
1. Load the snapshot into BigQuery (`build/`, unchanged by this work).
2. Add its date to `snapshots` in `webapp_config.json`.
3. `build`, then `publish`.

## The MLRS Case Completeness page

Catalogued as `WEB01`. Four checks, two populations, one set of tables. The checks are the four
things every real case must have: a **serial number**, a **case land**, a **customer** and a
**case action**. For a single snapshot, the same cases are listed by `tests/attribute/`
(`AT06.1`, `AT13.1`, `AT14.1`, `AT20` + `AT20.1`); this folder runs that logic across many
snapshots for the app. If a check changes, change both.

**No test IDs in the published tables.** The client reads the checks by name ("Missing case
land", ...) in the `Subject_Name` column. Test IDs stay in the SQL headers and the catalog.

**Populations (`Scope`):** real cases (Bond and Status record types excluded) with disposition
ACTIVE, AUTHORIZED, INTERIM or CLOSED (`Core`), and the same plus PENDING (`Core + Pending`).
The dashboard's Population switch filters `Scope` on all four tables. A row carries one value,
so **core rows appear twice**, once per scope. Month-to-month changes are worked out within each
scope: a PENDING → ACTIVE case has *entered* the Core list but is *still failing* in Core + Pending.

| SQL | Output (layer name) | Grain |
|---|---|---|
| `01_build_state.sql` | (session temp tables) | every case, every snapshot, four pass/fail flags |
| `02_cases.sql` | Cases | scope × snapshot × check × failing case, plus each case in the month it left the list |
| `03_flows.sql` | Flows | scope × snapshot × check: last and current totals and the six change counts; feeds the headline indicators |
| `04_summary.sql` | Summary | scope × snapshot × check × disposition × origin × mining claim × group × subgroup × product, with the population as the denominator |
| `05_changes.sql` | Changes | the month-to-month changes counted by the same groups as Summary (scope × snapshot × check × change type × disposition × origin × mining claim × group × subgroup × product): `Cases` and `Signed_Cases` (+ joined, − left). Feeds the monthly-changes chart so every dashboard filter applies to it |

`01` creates the session (including `cc_rows`, the change classification `02` and `05` share); `02`–`05` read its temp tables and **cannot run on their own**. To run a
piece by hand, use `MLRS_Case_Completeness_Validate.sql`, which is self-contained.

Columns on every table: `Snapshot_Date`, `Snapshot`, `Months_Back` (0 = latest snapshot, 1 = the
one before, recalculated on every build; the dashboard filters the breakdowns and case list to
`Months_Back` = 0), `Scope`, `Subject_Name`, `Subject_Order`. Cases, Summary and Changes add the
group columns `Disposition`, `Disposition_Rank` (1 = ACTIVE … 5 = PENDING, for severity sorting),
`Origin` (Legacy = has a legacy serial number, else New), `Mining_Claim` (convention S7),
`Case_Type_Group`, `Case_Type_Subgroup`, `BLM_Product` and `BLM_Product_Code`. These are the
columns every dashboard filter uses. Flows also has signed `Chart_*` columns (joined +, left −);
the dashboard's monthly-changes chart uses Changes' `Signed_Cases` instead, so the group filters
reach it.

`publish` gives each table a short layer name (`Cases`, `Flows`, `Summary`, `Changes`) and each column a
readable alias, both from `webapp_config.json` (`layer_name`, `field_aliases`).

### Reading the numbers

- **Only `Fixed` is a fix.** `Left scope, still failing` means the case still fails and merely
  changed disposition or record type. Counting those as fixes overstated land remediation by about
  30% in earlier work.
- **`Lost record (regression)`** is a case that had the record last snapshot and lost it.
- **The flows balance by construction:** last snapshot's total, plus the three ways onto the list,
  minus the three ways off it, equals this snapshot's total (`Balance_Check` = 0). `build` refuses
  to write the CSVs otherwise.
- **Never add `Failing_Cases` across checks or scopes** and call it a number of cases: a case
  failing two checks is counted once per check, and a core case once per scope.
- **Zero is the expected answer for the serial check** in the core dispositions. A serial is issued
  before a case goes live, so only PENDING rows are worth a look.
- **Created and last modified dates are shown as-is.** Both are dominated by bulk loads:
  3.5M cases share four days in March 1999, and 41% of last-modified dates fall on 06/23/2023.
  Fine as filters, meaningless as trends.

### Changing a table's columns

`publish` overwrites in place only while the columns stay the same. After a column change,
recreate the tables once: delete the old items in the Portal, clear `portal_item_id` /
`source_item_id` in `webapp_config.json`, and run `publish --create`. Widgets built on the old
items must then be re-pointed, so settle the columns before building dashboards.
