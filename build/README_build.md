# BLM DQIMP — Monthly Data Snapshot Workflow

## Project Context

Xentity (BLM GIS Team contractor) performs monthly Data Quality (DQIMP) testing by comparing
monthly MLRS snapshots against legacy NLSDB/LR2000 snapshots in BigQuery, hunting for
migration-induced errors and synchronization issues after the ASRC → Salesforce (MLRS) migration.

This document captures the full workflow for loading each month's snapshot data into Google Cloud
so BigQuery DQ queries can run against it. It was established in June 2026 and should be updated
as the process evolves.

---

## Google Cloud Infrastructure

| Resource | Value |
|---|---|
| GCP Project | `xentity-sandbox-huy` |
| GCS Bucket | `sandbox-blm-seta-dqimp-qaqc` |
| BigQuery Dataset | `blm_seta_dqimp` |
| GCS Snapshot Prefix | `gs://sandbox-blm-seta-dqimp-qaqc/snapshots/<YYYYMMDD>/` |

> **Note on old dataset name:** Early scripts (e.g., `load_commands.txt` from May 2025) used
> dataset `blm_dqimp_qaqc`. All work from September 2025 onward uses `blm_seta_dqimp`.

---

## Data Sources (What BLM Sends Each Month)

### 1. MLRS Full Extract

Arrives as a folder of state-partitioned `.tar` files, one per BLM state office:

```
2025-12-07_MLRS_Full/
  CR_FULL_AK_20251207.tar
  CR_FULL_AZ_20251207.tar
  CR_FULL_CA_20251207.tar
  CR_FULL_CO_20251207.tar
  CR_FULL_ES_20251207.tar   ← Eastern States
  CR_FULL_ID_20251207.tar
  CR_FULL_MT_20251207.tar
  CR_FULL_NM_20251207.tar
  CR_FULL_NV_20251207.tar
  CR_FULL_OR_20251207.tar
  CR_FULL_UT_20251207.tar
  CR_FULL_WY_20251207.tar
```

Folder naming convention from BLM: `YYYY-MM-DD_MLRS_Full`  
File naming convention: `CR_FULL_<STATE>_YYYYMMDD.tar`

### 2. MLRS Mining Claims Extract

Separate folder, same state structure, contains only mining claims records:

```
2025-12-07_MLRS_MC/
  MC_AK_20251207.tar
  MC_AZ_20251207.tar
  ... (same 12 states)
```

Folder naming convention: `YYYY-MM-DD_MLRS_MC`  
File naming convention: `MC_<STATE>_YYYYMMDD.tar`

### 3. NLSDB File Geodatabase

A single `.gdb` folder for the month's NLSDB/LR2000 snapshot:

```
nlsdb_12092025.gdb    ← naming: nlsdb_MMDDYYYY.gdb (month-day-year, not ISO)
```

> **Layer naming quirk:** The GDB layers embed both a date AND a time-of-day in their name,
> e.g. `Case_09082025_1300` and `CaseLands_09082025_1330`. The timestamp varies each month
> (it reflects when the export was generated). Scripts must auto-detect the layer names rather
> than hardcoding them.

### 3b. NLSDB Public Extract (alternate source)

When BLM does not provide a standard NLSDB GDB snapshot, a monthly extract can be produced
from the Esri public REST API. This data has a different structure and requires separate scripts.

> **The full end-to-end public-extract process is documented separately** in
> `Public_Extract_Specific\README_Public_Extract_build.md` — preflight, endpoint extraction,
> merge, GDB conversion, validation gates, and load. **That document is authoritative for the
> upstream half**; this section covers only how a public-extract month differs once the GDB
> exists. The upstream scripts (`extract_feature_service.py`, `merge_public_extract.ipynb`)
> now live alongside it rather than outside the repo. See also
> `DQIMP\MLRS_Database_Quality_Checks.md` §6.3 for the underlying audit — endpoint inventory,
> deficit math (Land Transfer bug, §6.3.5, ~32K cases lost), and the hardening plan.

> ⚠️ **Two landmines to know before running a public-extract month** (both verified 2026-08-10
> against the scripts, neither previously documented):
>
> 1. **`extract_feature_service.py` ships with 5 of its 6 endpoint URLs commented out** — it is
>    frozen in the state of the February 2026 Land-Tenure-only retry. Run it unread and you pull
>    Land Tenure and nothing else, with no error. Uncomment all six first.
> 2. **The Case layer in the GDB must be named `case`, not `nlsdb_case`.**
>    `prepare_snapshot_public_extract.py` matches `Case_*`, exactly `case`, or
>    `nlsdb_public_extract_*`; a GDB whose Case layer is `nlsdb_case` makes it `sys.exit(1)`.
>    The *gpkg* layer stays `nlsdb_case` (SQLite reserves `case`) — only the GDB layer is
>    renamed, via `-nln case` at conversion time.

**Typical shape of a public extract (Oct/Nov 2025 baseline — see per-month table below for
variations):**

| Attribute | Standard BLM GDB | Public Extract (typical) |
|---|---|---|
| Zip name | `YYYY-MM-DD_NLSDB.gdb.zip` | `YYYY-MM-DD_NLSDB_Public_Extract.zip` |
| GDB name | `nlsdb_MMDDYYYY.gdb` | `nlsdb_public_extract_MMDDYYYY.gdb` |
| Case layer name | `Case_MMDDYYYY_HHMM` | `case` (or `nlsdb_public_extract_MMDDYYYY`) |
| CaseLands layer name | `CaseLands_MMDDYYYY_HHMM` | `case_lands` |
| Case field count | 52 (excl. geometry) | 38 (union of 5 API endpoints) + `fid` |
| CaseLands field count | 33 (excl. geometry) | 28 (matches BLM's aggregate `/Case_Lands/MapServer/0` schema) |
| Date field types | Timestamp | Real (epoch milliseconds) |
| OBJECTID field | `OBJECTID` | `OBJECTID` + auto-added `fid` (GeoPandas GPKG writer) |
| CRS | Varies (reprojected to EPSG:4326) | EPSG:4269 (NAD83, reprojected to 4326 in ogr2ogr) |

**Fields present in standard GDB but NOT exposed by the public REST API (Case layer):**
`PAT_ISS_DT`, `PUB_DT`, `PUB_TYPE`, `TITLE_ACC_DT`, `FUND_BY`, `PLSSIDS`, `GIS_ACRS`,
`PLSS_UPDATE_DT`, `ID`, `STAGE_ID`, `Created`, `Modified`.
**These cannot be recovered from any public source** — DQ tests referencing them will
silently return null/false-mismatch on public-extract months. Enumerate before running.

**Fields on the public API but not on the standard GDB:**
`fid` (added by GeoPandas when writing the individual per-endpoint GPKGs, survives all
conversions), plus `SRC` (Data Source — Mining-Claims-native, populated only for Mining
Claims rows in the merged Case table, NULL for other case types).

**Land Transfer coverage gap:** The upstream `extract_feature_service.py` URL list omits
`Land_Transfer_Case_Land`. That endpoint has 929,809 rows (case-land-level, many-to-one)
and its unique fields — `DOC_TYPE`, `DOC_NR`, `DOC_DT`, `TITLE_ACC_DT`, `LND_SELECTED_BY`,
`PRIORITY`, `CSE_LND_STATUS`, `CSE_LND_ACRS`, `Modified` — are absent from every public
extract's Case table. Quantified: ~32,321 MLRS Land Transfer cases are absent from public
snapshots vs a paired standard GDB. Fix documented in DQIMP §6.3.10 §A (requires editing
3 files, not just one, and deduping on `SF_ID` because the endpoint is many-to-one).

**Date conversion:** The public extract stores dates as Real (epoch milliseconds since
1970-01-01). The public extract PS1 converts these to TIMESTAMP in the BQ CTAS step using
`TIMESTAMP_MILLIS(CAST(field AS INT64))` so downstream DQ queries don't need special
handling. Each layer has its own date-field list (passed per-load via `-DateFields`):
Case/StatusRecords convert `CSE_DISP_DT`, `EFF_DT`, `EXP_DT`, `SALE_DT`; CaseLands
converts `CSE_LND_STATUS_DT`, `DOC_DT`, `Created`, `Modified` (July 2026 — previously
only the Case fields were converted).

**Scripts (this repo):** Use `prepare_snapshot_public_extract.py` and
`BLM_DQIMP_OneStop_param_v4_public_extract.ps1` instead of the standard versions.
The MLRS workflow is identical — only the NLSDB handling differs.

---

## Internal File Structure (MLRS Tars)

Each `.tar` contains `.gz`-compressed files; each `.gz` decompresses to a pipe-delimited
`.load` file. State suffix is the last 3 characters of the filename (e.g., `_AK`, `_AZ`).

```
CR_FULL_AK_20251207.tar
  └── CR_FULL_ACCOUNT_RPT_AK.gz  →  CR_FULL_ACCOUNT_RPT_AK.load  (pipe-delimited)
  └── CR_FULL_BLM_CASE_AK.gz     →  CR_FULL_BLM_CASE_AK.load
  └── ... (one .gz per table)
```

BigQuery loads these files directly via wildcard URI (e.g. `ACCOUNT_RPT/CR_FULL_ACCOUNT_RPT_*.load`),
merging all state files for a table at BigQuery scale — no local consolidation step is needed.
Each table's files live in a dedicated GCS subdirectory (`<TABLE>/`) to prevent wildcard prefix
collisions (e.g. `ACTION` vs `ACTION_CASE` and `ACTION_LAND`).

---

## MLRS Tables Loaded to BigQuery (24 tables)

Each is loaded as `blm_seta_dqimp.<table_lower>_<YYYYMMDD>`.

| Table Name | BQ Table Pattern | Schema File |
|---|---|---|
| ACCOUNT_RPT | `account_rpt_YYYYMMDD` | `account_rpt_schema.json` |
| ACTION | `action_YYYYMMDD` | `action_schema.json` |
| ADMIN_UNIT | `admin_unit_YYYYMMDD` | `admin_unit_schema.json` |
| ADMIN_UNIT_AGG | `admin_unit_agg_YYYYMMDD` | `admin_unit_agg_schema.json` |
| BLM_CASE | `blm_case_YYYYMMDD` | `blm_case_schema.json` |
| BLM_PRODUCT | `blm_product_YYYYMMDD` | `blm_product_schema.json` |
| CASE_ACTION | `case_action_YYYYMMDD` | `case_action_schema.json` |
| CASE_CUSTOMER | `case_customer_YYYYMMDD` | `case_customer_schema.json` |
| CASE_LAND | `case_land_YYYYMMDD` | `case_land_schema.json` |
| CASE_TRANSACTION | `case_transaction_YYYYMMDD` | `case_transaction_schema.json` |
| INTREL_D | `intrel_d_YYYYMMDD` | `intrel_d_schema.json` |
| PRODUCT_FEE | `product_fee_YYYYMMDD` | `product_fee_schema.json` |
| RPT_CALENDAR | `rpt_calendar_YYYYMMDD` | `rpt_calendar_schema.json` |
| CASE_ACTION_STATUS_D | `case_action_status_d_YYYYMMDD` | `case_action_status_d_schema.json` |
| COMMODITY_D | `commodity_d_YYYYMMDD` | `commodity_d_schema.json` |
| CASE_GROUP_D | `case_group_d_YYYYMMDD` | `case_group_d_schema.json` |
| MERIDIAN_D | `meridian_d_YYYYMMDD` | `meridian_d_schema.json` |
| SURVEY_TYPE_D | `survey_type_d_YYYYMMDD` | `survey_type_d_schema.json` |
| ACTION_CASE | `action_case_YYYYMMDD` | `action_case_schema.json` |
| ACTION_LAND | `action_land_YYYYMMDD` | `action_land_schema.json` |
| CASE_ADMIN_UNIT | `case_admin_unit_YYYYMMDD` | `case_admin_unit_schema.json` |
| RECORD_TYPE | `record_type_YYYYMMDD` | `record_type_schema.json` |
| SUPPLEMENTAL_USE | `supplemental_use_YYYYMMDD` | `supplemental_use_schema.json` |
| US_RIGHT_CASE_LAND | `us_right_case_land_YYYYMMDD` | `us_right_case_land_schema.json` |

Schema `.json` files live in this repo at `schemas/mlrs_export_schemas/` and are located
automatically at runtime — see "Schema resolution" below.

## NLSDB Tables Loaded to BigQuery (2 tables; 3 on public-extract months)

| Source Layer | BQ Table Pattern | Format |
|---|---|---|
| `Case_<MMDDYYYY>_<HHMM>` | `nlsdb_case_YYYYMMDD` | Parquet (ext table + `ST_GEOGFROMWKB`) |
| `CaseLands_<MMDDYYYY>_<HHMM>` | `nlsdb_case_land_YYYYMMDD` | Parquet (ext table + `ST_GEOGFROMWKB`) |
| Case layer, Status Records only (**public-extract months only**, July 2026+) | `nlsdb_status_records_YYYYMMDD` | Parquet (ext table + `ST_GEOGFROMWKB`) |

> **Parquet naming convention (new as of June 2026):** `prepare_snapshot.py` renames the
> output parquet files to `Case_<YYYYMMDD>.parquet` and `CaseLands_<YYYYMMDD>.parquet`,
> dropping the time component. This allows the OneStop PS1 to construct the GCS path from
> `$Date` alone without needing the per-month timestamp.

### NLSDB Public Extract (alternate data source)

When BLM does not deliver a standard NLSDB GDB snapshot for a given month, an extract from
the public Esri API is used instead. Use the `_public_extract` versions of both scripts.

> **Public extract schemas differ between months due to different tooling paths, not because
> the API is inconsistent.** The API endpoint schemas are stable (verified June 30 2026 — see
> `DQIMP\MLRS_Database_Quality_Checks.md` §6.3.2). What varies is which merge/GDB-conversion
> tooling the operator used. When BQ loads fail, use `pyarrow.parquet.read_schema()` to
> inspect the actual Parquet schema and generate correct external table DDL manually.

**Common differences from standard BLM GDB (all public extract months):**

| | Standard BLM GDB | Public Extract |
|---|---|---|
| GDB naming | `nlsdb_MMDDYYYY.gdb` | `nlsdb_public_extract_MMDDYYYY.gdb` |
| Zip naming | `*_NLSDB.gdb.zip` | `*_NLSDB_Public_Extract.zip` |
| Date field types | `Timestamp` | `Real` (epoch milliseconds) |
| CRS | Varies | EPSG:4269 (NAD83) |
| 14 fields absent | present | not exposed by API (see §3b) |
| Land Transfer cases | present | ~32K missing (see §3b and DQIMP §6.3.5) |
| Status Records | Not included | **Included** — split by `prepare_snapshot_public_extract.py` into `StatusRecords_<YYYYMMDD>.parquet` → `nlsdb_status_records_<YYYYMMDD>` (July 2026 change; split requires the layer to have a `CSE_DISP` field). Main `nlsdb_case_*` table stays Status-Record-free |

**Snapshot inventory in BigQuery (public extracts only):**

| BQ table | Rows | Fields | Tooling / provenance | DQ policy |
|---|---:|---:|---|---|
| `nlsdb_case_20251001` | 10,528,722 | 39 (`OBJECTID`+`fid`) | Oct 2025 Path 2 (gpkg-direct) via ogr2ogr | **Only usable with `WHERE CSE_DISP != 'Status Record'`** — the ogr2ogr Status filter was skipped for this load |
| `nlsdb_case_20251005` | 5,315,332 | 39 (`OBJECTID`+`fid`) | Same Oct 2025 GDB, Status-filtered on load | Usable directly |
| `nlsdb_case_20251101` | 10,528,722 | 39 (`OBJECTID`+`fid`) | Nov 2025 pull returned byte-identical data — **shutdown-freeze on API**, not a copy (see below) | Same as 20251001 |
| `nlsdb_case_20251103` | 5,315,332 | 39 (`OBJECTID`+`fid`) | Same Nov 2025 GDB, Status-filtered on load | Usable directly |
| `nlsdb_case_20260202` | 5,359,028 | 41 (`OBJECTID_1`+`nlsdb_Shape`+`Shape_Length`+`Shape_Area`) | **ArcPro Export Features** — see Feb 2026 root cause | Usable case-level with caveats; **case_land NOT usable — see below** |
| `nlsdb_case_land_20251001/005/101/103` | 1,808,361 each | 28 (correct Case_Land schema) | ogr2ogr — case_lands from `/Case_Lands/MapServer/0` aggregate | Usable directly |
| **`nlsdb_case_land_20260202`** | **5,359,028** | **41 (Case schema — WRONG)** | **Case data was loaded into the case_land table** — the Case_Lands endpoint was never pulled for Feb 2026 | **EXCLUDE from all case_land DQ** — see below |

**Oct/Nov 2025 — shutdown-freeze, not file copies.** Nov 2025 pulls returned byte-identical
data to Oct 2025 pulls because the BLM public API was frozen during the federal shutdown.
Evidence: (1) manager's Oct 8 workflow docx explicitly mentions the shutdown; (2) MLRS
blm_case growth Oct→Nov was ~100 cases/day vs ~330/day surrounding periods (BQ-verified);
(3) API refreshes normally now (June 2026 probe). Treat 20251005 and 20251103 as representing
the same underlying NLSDB state — do not compare them in trend analyses.

**Feb 2026 — root cause and DQ policy.** The Feb 23 gpkg on disk
(`E:\Xentity\BLM\NLSDB_Public_Extract\Extracts\nlsdb_public_extract_02232026.gpkg`, 10 GB)
contains **only one layer, `case`** — no `case_lands`. The Case_Lands endpoint was never
pulled (the extract had persistent Land Tenure batch failures on Feb 23 the user couldn't
resolve; per-endpoint gpkgs were not preserved). The gpkg was then converted to GDB via
**ArcPro Export Features** (rather than ogr2ogr — the user's ogr2ogr install was fragile
per the README 02/27/2026 update), which introduced `OBJECTID_1`, `nlsdb_Shape`,
`Shape_Length`, `Shape_Area` and converted dates to Timestamp. When loaded to BQ, the same
Case data ended up in both `nlsdb_case_20260202` and `nlsdb_case_land_20260202` — hence
the identical row counts (5,359,028).

> **DQ policy for Feb 2026 (§6.3.11 of DQIMP knowledge base):** Any report or query using
> `nlsdb_case_land_*` must add `snapshot NOT IN ('20260202')`. The Feb 2026 case_land data
> **does not exist and is not recoverable** (per-endpoint gpkgs are gone from disk; API
> returns current state, not Feb 23). Case-level `nlsdb_case_20260202` is still usable with
> two caveats: (a) Land Tenure records may be under-counted due to Feb 23 batch failures,
> (b) queries referencing `fid` need `COALESCE(fid, OBJECTID_1)`.

**Excluded-snapshot registry:**

| Snapshot | Table | Reason | Include for other DQ? |
|---|---|---|---|
| `20260202` | `nlsdb_case_land_20260202` | Loaded with Case schema — no case_land data | Case-level `nlsdb_case_20260202` usable with caveats |
| `20251001` | `nlsdb_case_20251001` | Loaded without Status Record filter — 10.5M rows include ~5.2M Status Records | Only with `WHERE CSE_DISP != 'Status Record'` |

**BQ loading approach for public extracts:**

As of July 2026, the public extract PS1's hardcoded column DDL matches the **ogr2ogr-path
parquet schema** (the standardized go-forward path, Oct/Nov 2025 pattern), verified by
reading the actual parquet footers of `Case_20251005.parquet` / `CaseLands_20251005.parquet`
with pyarrow and validated end-to-end in BQ against those files. Key facts about that schema:
geometry is in a WKB column named `geom` (not `Shape`); a junk FLOAT64 column named `Shape`
exists in both layers and is dropped in the CTAS; a `geom_bbox` STRUCT (GeoParquet covering)
is deliberately omitted from the DDL (BQ matches parquet columns by name); Case has
`OBJECTID` + `fid`, CaseLands has its own 28-field schema (no `fid`).

When the Parquet schema differs from this (as with Feb 2026's ArcPro output — `OBJECTID_1`,
`nlsdb_Shape`, `Shape_Length/Area`, TIMESTAMP dates), the NLSDB BQ loads fail. In those cases:

1. Inspect the actual Parquet schema: `pyarrow.parquet.read_schema('Case_YYYYMMDD.parquet')`
2. Generate external table DDL with the correct columns (geometry column as BYTES, skip
   `*_bbox` struct columns)
3. Run a CTAS with `SELECT * EXCEPT(geom, Shape), ST_GEOGFROMWKB(geom, make_valid => TRUE) AS Shape`
   (adjust column names to match the actual geometry column)
4. Drop the external table

**Status Records:** The per-case-type API endpoints return cases where `CSE_DISP = 'Status
Record'` (~5.2M rows, roughly 49% of the raw Case data). These are not present in the
standard BLM GDB snapshots. **As of July 2026, Status Records are split out rather than
discarded.** `prepare_snapshot_public_extract.py` converts the Case layer twice:

- `Case_<YYYYMMDD>.parquet` — `-where "CSE_DISP <> 'Status Record'"` → loads to
  `nlsdb_case_<YYYYMMDD>` (consistent with standard-GDB months, safe for all DQ queries)
- `StatusRecords_<YYYYMMDD>.parquet` — `-where "CSE_DISP = 'Status Record'"` → loads to
  a separate `nlsdb_status_records_<YYYYMMDD>` table (full data preserved, never mixed
  into case-level DQ)

The split is conditional on the layer schema (checked via fiona): it requires the layer to
have a `CSE_DISP` field, since ogr2ogr errors on a WHERE clause referencing a nonexistent
field. CaseLands typically lacks `CSE_DISP` and has no Status Records, so it converts
unfiltered with an informational note (filtered defensively if the field is present). If the
**Case** layer lacks `CSE_DISP`, no split is possible: the script prints a prominent WARNING
(all rows including any Status Records go into `Case_<YYYYMMDD>.parquet` — verify before
running DQ), produces no StatusRecords parquet, and the PS1's `nlsdb_status_records` load
fails with a benign WARNING. **Never let Status Records into `nlsdb_case_*` — the Oct
20251001 incident shows what happens when the filter is skipped.**

---

## File Inventory

All build scripts live in this repo, in `build/`. The repo is the single source of truth for
them — do not keep or edit master copies anywhere else.

**Upstream extract pipeline (public-extract months only)** now lives with the loading scripts in
`Public_Extract_Specific\`, alongside its own end-to-end SOP,
`README_Public_Extract_build.md`. It runs BEFORE the scripts here and produces the GDB they
consume. That document is authoritative for the upstream half; `MLRS_Database_Quality_Checks.md`
§6.3 holds the supporting audit and deficit math. The original author's README at
`E:\Xentity\BLM\NLSDB_Public_Extract\README.md` is superseded and retained for provenance only.

### Active Scripts (use these going forward)

| File | Language | Purpose |
|---|---|---|
| `prepare_snapshot.py` | Python | Steps 1–4: unzip inputs, extract tars, upload raw per-state `.load` files to GCS, convert GDB to Parquet and upload |
| `BLM_DQIMP_OneStop_param_v4.ps1` | PowerShell | Step 5: wildcard BQ load (CR_FULL create, then MC append) for all 24 MLRS tables + 2 NLSDB Parquet tables. GCS cleanup is **manual** (see Step 6) |
| `prepare_snapshot_public_extract.py` | Python | Same as `prepare_snapshot.py` but handles NLSDB public extract GDB (different zip/GDB naming, lowercase layer names) and splits the Case layer into `Case_<date>.parquet` (Status Records excluded) + `StatusRecords_<date>.parquet` (Status Records only). **Does NOT pull from the API** — reads a GDB produced by the upstream pipeline. |
| `BLM_DQIMP_OneStop_param_v4_public_extract.ps1` | PowerShell | Same as standard PS1 but with public extract column schemas, epoch-ms→TIMESTAMP date conversion in the CTAS step, and a third NLSDB load: `StatusRecords_<date>.parquet` → `nlsdb_status_records_<date>` |
| `Public_Extract_Specific\README_Public_Extract_build.md` | Markdown | End-to-end SOP for the public-extract fallback: preflight, extraction, merge, GDB conversion, validation gates, load. Authoritative for everything upstream of the GDB |
| `Public_Extract_Specific\extract_feature_service.py` | Python | Pulls the BLM public REST endpoints to per-endpoint gpkgs. **Public-extract months only.** Check the URL list before running |
| `Public_Extract_Specific\merge_public_extract.ipynb` | Notebook | Merges per-endpoint gpkgs/parquets into one gpkg with `nlsdb_case` + `case_lands`; third cell validates gpkg against the converted GDB |

### Schema resolution

**No path to any particular workstation appears in any script** (changed 2026-08-10). The repo's
own `schemas/mlrs_export_schemas/` is the single source of truth for the 24 BQ schema JSONs. Both
`prepare_snapshot*.py` and both PS1s locate it at runtime, in this order:

1. **Any ancestor directory containing `schemas/mlrs_export_schemas`** — i.e. the repo, found by
   walking up from the script. Works at any folder depth.
2. A `schemas/` folder sitting beside the script.
3. **`$BLM_DQIMP_SCHEMA_DIR`** — an environment variable, for running from a month folder outside
   the repo (see Step 1).

First candidate that actually contains `*_schema.json` files wins. `--schema-dir` / `-SchemaDir`
overrides the search entirely. If nothing is found, the script prints every location it searched
and exits rather than guessing.

### Upstream extract pipeline

Now kept with the loading scripts in `Public_Extract_Specific\` and fully documented in
`README_Public_Extract_build.md`. Runs BEFORE the scripts in this folder. Details below verified
against the scripts 2026-08-10.

| File | Purpose | Known issues |
|---|---|---|
| `extract_feature_service.py` | Async paginated pulls from per-case-type Esri REST endpoints → per-endpoint gpkgs. Settings: `max_records=1800`, `max_concurrent=7`, 120s timeout | **5 of 6 URLs commented out as committed** (§3b); Land Transfer URL absent entirely (DQIMP §6.3.5); warns only at <90% retrieval; failed batches silently dropped after 3 attempts (2s then 4s backoff — *not* 2s/4s/6s as DQIMP §6.3.10 §B states); no offset-level retry or checkpointing; **requires `pyogrio`** (passes `engine="pyogrio"` to `to_file()`); writes every per-endpoint layer as `case_lands` regardless of endpoint — the layer name is meaningless, do not use it to identify content |
| `merge_public_extract.ipynb` (was `dev.ipynb`) | 3 cells: Path A parquet merge, Path B direct-gpkg merge, and a gpkg↔GDB QA comparison | Folder path and output filename hardcoded per month (Path A's cell still writes a `12082025` filename while reading a `02232026` folder); Land Transfer file missing from both file lists. **Merge path decided 2026-08-10: use the Parquet merge (cell 1, ~2 min).** The direct gpkg merge (cell 2, ~1 hr and ~32 GB RAM) is retained as an emergency fallback for when ogr2ogr is unavailable — it is pure GeoPandas and needs no GDAL command line, which is exactly the Feb 2026 failure. Using it makes the month a documented deviation |
| `README.md` (at `E:\Xentity\BLM\NLSDB_Public_Extract\`) | Original author's 8-step instructions — **superseded** by `README_Public_Extract_build.md` | Retained for provenance only. Documents both paths; states 1900 records/batch (actual: 1800); PATH-workaround for the fragile ogr2ogr install (02/27/2026) — needing that workaround is what forced the Feb 2026 ArcPro fallback |
| ArcPro Export Features (manual GUI) | gpkg → File Geodatabase conversion — **fallback only** | Introduces `OBJECTID_1`, `nlsdb_Shape`, `Shape_Length`, `Shape_Area`. Manual `Shape → nlsdb_Shape` rename required. Prefer `ogr2ogr -f OpenFileGDB`, naming the source layer explicitly and using `-nln case` (DQIMP §6.3.10 §C3) |

### Reference / Historical Scripts (do not use for new snapshots)

| File | Notes |
|---|---|
| `extract.py` | Original extraction script — hardcodes `sub_folder = '2025-09-07'`, must be hand-edited each month. Superseded by `prepare_snapshot.py`. |
| `extract_parquet.txt` | Manual reference commands for GDB → Parquet conversion (ogr2ogr) and GCS upload. Hardcodes GDB layer names including the time-of-day. Superseded by `prepare_snapshot.py`. |
| `load_commands.txt` | Manual reference for May 2025 snapshot (uses old dataset `blm_dqimp_qaqc`). Historical record only. |
| `load_commands_Elii.txt` | Colleague Elii's original manual load commands for September 2025 snapshot. Historical record only. |
| `blm_compose_load_20251002.ps1` | Date-specific PS1 for October 2025. NLSDB Parquet loads commented out. Superseded by v4. |
| `BLM_DQIMP_OneStop_param_v3.ps1` | Parameterized PS1, but NLSDB Parquet filenames still hardcoded to `Case_09082025_1300.parquet`. Superseded by v4. |

---

## Monthly Workflow (Standard Operating Procedure)

> **All commands below are PowerShell.** Run them in a PowerShell terminal (not Command Prompt).
> If you see an error about script execution policy, run this first:
> ```powershell
> Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
> ```

---

### Date format reference

Two different date formats appear in this workflow. Know which is which before you start:

| Format | Example | Used in |
|---|---|---|
| `YYYY-MM-DD` | `2025-12-07` | BLM folder names as received |
| `YYYYMMDD` | `20251207` | `--date` flag, BQ table names, GCS snapshot path |
| `MMDDYYYY` | `12092025` | NLSDB GDB filename (month-day-year, note the order) |

---

### Prerequisites — verify before running

> **Required: activate the `dev` conda environment first.**
> `prepare_snapshot.py` depends on fiona and ogr2ogr from the `dev` env. The `dev` env's
> ogr2ogr (GDAL 3.10 from conda-forge, with GEOS 3.13 + `libgdal-arrow-parquet`) is the
> only install on this machine that has both Parquet support and `-makevalid` (GEOS).
> **Always run `conda activate dev` before Steps 3 and 4.**

Run these in PowerShell to confirm all tools are available and authenticated:

```powershell
# 0. Activate the correct conda environment (required every new terminal session)
conda activate dev

# 1. Verify gsutil is installed and authenticated
gsutil version
gsutil ls gs://sandbox-blm-seta-dqimp-qaqc/

# 2. Verify bq CLI is installed and authenticated
bq version
bq ls xentity-sandbox-huy:blm_seta_dqimp

# 3. Verify ogr2ogr has both Parquet and GEOS (must be the dev env's ogr2ogr)
ogr2ogr --version
ogr2ogr --formats | Select-String "Parquet"

# 4. Verify Python environment has required packages
python -c "import fiona; print('fiona OK')"
```

Expected: each command returns a version string or listing without error.
`ogr2ogr --formats | Select-String "Parquet"` must return at least one line —
if it returns nothing, the dev env is not active or `libgdal-arrow-parquet` is not installed.

**If `libgdal-arrow-parquet` is missing from the dev env** (one-time fix):
```powershell
conda install -n dev -c conda-forge libgdal-arrow-parquet
```
This installs the GDAL Arrow/Parquet driver alongside the existing GEOS 3.13 already in
the dev env. See Known Issues for background on why this matters.

---

### Step 1 — Create the month folder and copy the scripts into it

Each month gets its own folder holding that month's data. **Month folders live outside this
repo**, under `E:\Xentity\BLM\DQIMP\Data\<Month_Year>\`, because they hold multi-GB zips,
GDBs, Parquet files and run logs. **Never create one inside the repo working tree** — this repo
is public, and the data is live BLM case data. The same reasoning applies to `Query_Results\`.

Create the folder, then copy the two scripts into it from the repo:

```powershell
# Create the month folder (change September_2026 to match the current month)
mkdir "E:\Xentity\BLM\DQIMP\Data\September_2026"

# Copy the two scripts in from the repo
copy "E:\Xentity\BLM\BLM_DQIMP_GitRepo\xentity_blm_dqimp_big_query\build\prepare_snapshot.py" `
     "E:\Xentity\BLM\DQIMP\Data\September_2026\"

copy "E:\Xentity\BLM\BLM_DQIMP_GitRepo\xentity_blm_dqimp_big_query\build\BLM_DQIMP_OneStop_param_v4.ps1" `
     "E:\Xentity\BLM\DQIMP\Data\September_2026\"
```

> **The master copies of both scripts live in this repo, in `build/`.** Copy them into each new
> month folder — never edit the copies in a month folder, and never treat a month-folder copy as
> the source. Fixes go into the repo and flow outward.

> **Set `BLM_DQIMP_SCHEMA_DIR` once per machine.** Scripts running from a month folder have no
> repo above them, so they cannot find `schemas/mlrs_export_schemas` by walking up. Point the
> variable at the repo clone so month runs and repo runs read the identical schema files:
>
> ```powershell
> [Environment]::SetEnvironmentVariable('BLM_DQIMP_SCHEMA_DIR', 'E:\Xentity\BLM\BLM_DQIMP_GitRepo\xentity_blm_dqimp_big_query\schemas\mlrs_export_schemas', 'User')
> ```
>
> Open a new terminal afterwards. Alternatively pass `--schema-dir` / `-SchemaDir` on every run.
> If neither is set, the scripts stop and list what they searched — they never silently guess.

---

### Step 2 — Place incoming BLM zip files into the month folder

BLM delivers three zip files each month. Copy them into the month folder:

```
E:\Xentity\BLM\DQIMP\Data\September_2026\
  2026-09-06_MLRS_Full.zip      ← MLRS Full extract from BLM
  2026-09-06_MLRS_MC.zip        ← MLRS Mining Claims extract from BLM
  2026-09-08_NLSDB.gdb.zip      ← NLSDB File Geodatabase from BLM
  prepare_snapshot.py            ← copied from the repo in Step 1
  BLM_DQIMP_OneStop_param_v4.ps1 ← copied from the repo in Step 1
```

Zip file naming conventions from BLM:

| File | Pattern | June 2026 example |
|---|---|---|
| MLRS Full | `YYYY-MM-DD_MLRS_Full.zip` | `2026-06-07_MLRS_Full.zip` |
| MLRS MC | `YYYY-MM-DD_MLRS_MC.zip` | `2026-06-07_MLRS_MC.zip` |
| NLSDB GDB | `YYYY-MM-DD_NLSDB.gdb.zip` | `2026-06-08_NLSDB.gdb.zip` |

> **Note:** The MLRS and NLSDB dates can differ — MLRS and NLSDB are exported on different days.
> The `--date` used for BigQuery table names is always derived from the **MLRS** date.

Verify the three zips are present before continuing:

```powershell
cd "E:\Xentity\BLM\DQIMP\Data\September_2026"
ls *.zip
```

You should see exactly three `.zip` files.

---

### Step 3 — Run prepare_snapshot.py

> **Ensure `conda activate dev` has been run in this terminal session before continuing.**
> The dev env's ogr2ogr is required for the NLSDB GDB → Parquet conversion step.

Navigate to the month folder and run the script with no arguments:

```powershell
conda activate dev
cd "E:\Xentity\BLM\DQIMP\Data\September_2026"
python prepare_snapshot.py
```

The script auto-detects the three zip files, unzips them (with correct renaming), and
handles all tar extraction and GCS upload automatically.

When it finishes successfully you will see:

```
============================================================
Preparation complete for snapshot 20260607.
All files are in: gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20260607/

Next step — run the BigQuery load:
  .\BLM_DQIMP_OneStop_param_v4.ps1 -Date 20260607
============================================================
```

**What it does in order:**
1. Finds `*_MLRS_Full.zip` → unzips → renames extracted folder to `2026-06-07_MLRS_Full`
2. Finds `*_MLRS_MC.zip` → unzips → renames extracted folder to `2026-06-07_MLRS_MC`
3. Finds `*_NLSDB.gdb.zip` → unzips (GDB folder inside is already named `nlsdb_MMDDYYYY.gdb`)
4. Derives snapshot date `20260607` from the MLRS zip filename
5. Extracts all `.tar` → `.gz` → `.load` for both MLRS folders
6. **Validates** every `.load` file's column count against the BQ schema files — any rows with wrong column counts (usually caused by an embedded pipe `|` in a text field) are reported with file name and line number. See "Fixing embedded pipe errors" below.
7. Uploads all non-empty per-state `.load` files to `gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20260607/<TABLE>/` — one parallel `gsutil -m cp` call per table with `-o "GSUtil:check_hashes=always"` to verify upload integrity, grouping all 12 state files into a per-table GCS subdirectory (empty files — states with no records — are skipped automatically)
8. Auto-detects GDB layer names via `fiona.listlayers()`, converts to `Case_20260607.parquet` and `CaseLands_20260607.parquet`
9. Uploads both Parquet files to GCS

**If the zip files have non-standard names** or you are re-running with already-unzipped data,
you can pass paths explicitly:

```powershell
python prepare_snapshot.py `
  --full-dir "2026-06-07_MLRS_Full" `
  --mc-dir   "2026-06-07_MLRS_MC" `
  --gdb      "nlsdb_06082026.gdb" `
  --date     20260607
```

> **If the bucket ever changes,** add `--bucket <new-bucket-name>` to either command above.

#### Fixing embedded pipe errors

If the validation step reports rows with wrong column counts, the source `.load` file contains
a literal `|` inside a text field (e.g., a company name or remark). The script continues and
uploads all files including the problematic one. To fix before running Step 4:

1. Note the file name and line number from the validation output, e.g.:
   ```
   WARNING: CR_FULL_ACCOUNT_RPT_WY.load: 1 row(s) with wrong column count (expected 53):
     line 4528: found 54 columns
   ```
2. Open the `.load` file in a text editor and jump to the reported line number
3. Find the extra `|` character in a text field and replace it with a space (or remove it)
4. Save the file
5. Re-upload just that file to GCS, overwriting the bad copy:
   ```powershell
   gsutil cp "CR_FULL_ACCOUNT_RPT_WY.load" `
     gs://sandbox-blm-seta-dqimp-qaqc/snapshots/<YYYYMMDD>/ACCOUNT_RPT/
   ```
6. Proceed to Step 4 — all BQ loads will succeed with zero skipped rows

> **This recurs in one specific file.** Observed in May 2026, June 2026 and August 2026 — every
> time in `CR_FULL_ACCOUNT_RPT_WY.load` (August: line 4521, 54 columns where 53 were expected).
> Three occurrences in the same table and state is a quirk of BLM's Wyoming account-report
> export, not random chance. Expect it, and consider raising it with BLM. The validation catches
> it before it reaches BigQuery.

---

### Step 4 — Run BLM_DQIMP_OneStop_param_v4.ps1

This handles the BigQuery side: all the `bq load` operations (GCS cleanup stays manual —
see Step 6). The exact command to run is
printed at the end of Step 3 — copy it from the terminal output. The general form is:

**Template:**

```powershell
.\BLM_DQIMP_OneStop_param_v4.ps1 -Date <YYYYMMDD>
```

**June 2026 example (ready to copy-paste):**

```powershell
.\BLM_DQIMP_OneStop_param_v4.ps1 -Date 20260607
```

> **`-Date` is required (July 2026 change).** The script used to default to a hardcoded
> date (`20250903`), which meant running it bare would silently target the September 2025
> snapshot — and because Parquet files are kept in GCS long-term, the NLSDB CTAS would
> have rebuilt that old snapshot's `nlsdb_case`/`nlsdb_case_land` tables. The parameter
> is now mandatory: if you run the script without `-Date`, PowerShell prompts for it
> instead of proceeding. The same applies to the `_public_extract` variant.

The script prints one line per operation. Successful output for each table looks like:

```
  [BLM_CASE]
  Loading blm_seta_dqimp.blm_case_20260607 ...
  OK:      bq load CR_FULL BLM_CASE
  Loading blm_seta_dqimp.blm_case_20260607 ...
  OK:      bq load MC BLM_CASE (append)
```

When it finishes:

```
=== Done for 20251207. Review any WARNING/ERROR lines above. ===
Press Enter to close
```

Scan the output for any `WARNING` or `ERROR` lines before pressing Enter. A warning on a single
table does not stop the script — all other tables will still have loaded.

**What it does internally:**
1. `bq load --replace` — for each of the 24 tables, truncates+loads `gs://.../<TABLE>/CR_FULL_<TABLE>_*.load` (all 12 state files via GCS wildcard) into `blm_seta_dqimp.<table>_<DATE>`; `--replace` ensures re-runs start clean with no duplicate rows; `--quote=""` disables CSV quote recognition (BLM `.load` files use no quoting — literal `"` in field values must not trigger CSV quoting logic); `--max_bad_records=0` fails on any bad row instead of silently dropping data
2. `bq load` (default WRITE_APPEND) — appends `gs://.../<TABLE>/MC_<TABLE>_*.load` into the same table (mining claims merged with the full extract at BigQuery scale; same `--quote=""` and `--max_bad_records=0` flags)
3. External table + CTAS (3 BQ jobs) — creates a temp external table over `Case_<DATE>.parquet` with `Shape BYTES` (bypasses BQ's GeoParquet geography validator), then `CREATE TABLE AS SELECT ... ST_GEOGFROMWKB(Shape, make_valid => TRUE)` to repair spherically-invalid polygons and write `blm_seta_dqimp.nlsdb_case_<DATE>`, then drops the temp external table
4. Same 3-job pattern for `CaseLands_<DATE>.parquet` → `blm_seta_dqimp.nlsdb_case_land_<DATE>`

> **GCS cleanup is intentionally NOT part of this script.** The `gsutil -m rm` step is
> commented out so that a failed PS1 run can be retried immediately without re-running
> `prepare_snapshot.py` (which takes 2+ hours). Clean up manually in Step 6 only after
> the BigQuery verification in Step 5 passes.

> **Schema folder is auto-detected** (changed 2026-08-10): the repo's
> `schemas\mlrs_export_schemas\` found by walking up from the script, then a `schemas\` folder
> beside the script, then `$BLM_DQIMP_SCHEMA_DIR`. To force a specific folder, add
> `-SchemaDir "C:\full\path\to\schemas"`. If none of the candidates contains `*_schema.json`
> files the script lists what it searched and exits. See "Schema resolution" under File Inventory.

---

### Step 5 — Verify in BigQuery

Run these in the BigQuery console (or `bq query`) to confirm the load completed:

```sql
-- Quick row count check across key tables — replace 20260607 with the actual snapshot date
SELECT 'blm_case'        AS tbl, COUNT(*) AS rows FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_20260607`
UNION ALL
SELECT 'blm_product'     AS tbl, COUNT(*) AS rows FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_product_20260607`
UNION ALL
SELECT 'nlsdb_case'      AS tbl, COUNT(*) AS rows FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_20260607`
UNION ALL
SELECT 'nlsdb_case_land' AS tbl, COUNT(*) AS rows FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_land_20260607`;
```

Compare row counts against the prior month's snapshot to sanity-check for unexpected drops or spikes.

---

### Step 6 — Manual GCS cleanup (only after Step 5 verification passes)

Cleanup is deliberately manual (July 2026 change): if the PS1 fails and cleanup were
automatic, the `.load` files would be gone and recovering would mean re-running
`prepare_snapshot.py` from scratch (2+ hours). Once the row counts in Step 5 look good,
remove the `.load` files yourself (Parquet files are kept long-term):

```powershell
gsutil -m rm "gs://sandbox-blm-seta-dqimp-qaqc/snapshots/<YYYYMMDD>/**/*.load"
```

Then confirm only the two Parquet files remain:

```powershell
gsutil ls "gs://sandbox-blm-seta-dqimp-qaqc/snapshots/<YYYYMMDD>/"
```

Expected output: `Case_<YYYYMMDD>.parquet` and `CaseLands_<YYYYMMDD>.parquet` only
(public-extract months also keep `StatusRecords_<YYYYMMDD>.parquet`).

---

## Design Decisions (June 2026)

### Why two scripts instead of one?

Python is needed for the zip/tar/gz extraction logic and the `fiona`/`ogr2ogr` GDB-to-Parquet
conversion; PowerShell is better for the `bq` CLI interaction (the `--field_delimiter="|"` pipe
character requires the `bq --%` stop-parsing workaround that is specific to PowerShell/cmd.exe).
Keeping the boundary between them clean (Python handles everything before GCS; PS1 handles
GCS→BQ) makes each half independently testable and re-runnable if a step fails midway.

### Why rename the Parquet files to `Case_YYYYMMDD.parquet`?

The GDB internal layer names embed a time-of-day (e.g., `Case_09082025_1300`) that varies
each month based on when BLM generated the export. Hardcoding that timestamp in the PS1 was
the bug in v3. `prepare_snapshot.py` auto-detects the layer name via `fiona.listlayers()`,
converts to Parquet, but writes the output as `Case_<YYYYMMDD>.parquet` (date only). This
means the PS1 only needs to know `$Date` to construct the correct GCS path.

### Why keep `$SchemaDir` as a parameter rather than copying the JSON files?

*(Mechanism superseded 2026-08-10 — the default is now a search, not one absolute path, so the
scripts run unchanged from the repo or from a month folder. The reasoning below still explains
why the JSONs are not copied next to the scripts.)*

The 24 schema JSON files are maintained in **one** place — the repo's
`schemas/mlrs_export_schemas/`. Copying them next to the scripts would create a second copy that
could drift out of sync, and a schema that disagrees with the `.load` files fails the BQ load or,
worse, loads the wrong columns. The `-SchemaDir` / `--schema-dir` override lets the path be
changed at runtime without editing the script; `$BLM_DQIMP_SCHEMA_DIR` covers month folders that
sit outside the repo. Both point back at the same repo folder in normal use.

### Why wildcard BQ loads instead of local consolidation + gsutil compose

The original workflow consolidated all 12 per-state `.load` files into a single CSV per table
using pandas, then `gsutil compose` merged the MC and CR_FULL CSVs server-side before `bq load`.
This was bottlenecked by local pandas processing (e.g., ACTION_LAND at 3.4M rows) on a Windows
laptop.

The current approach uploads raw per-state `.load` files directly to GCS into per-table
subdirectories and uses BigQuery's native wildcard URI loading
(`gs://.../<TABLE>/CR_FULL_<TABLE>_*.load`) to merge all state files at BigQuery scale.
MC records are appended via a second `bq load` call (default WRITE_APPEND — no extra flag needed).
This eliminates the pandas dependency from the data loading path and moves the merge work into
BigQuery's infrastructure, which scales without local resource constraints.

The per-table subdirectory layout (`<TABLE>/`) was introduced after the June 2026 first run to
prevent wildcard prefix collisions: `CR_FULL_ACTION_*.load` was matching `ACTION_CASE` and
`ACTION_LAND` files, causing schema mismatch failures for those three tables.

The tradeoff: there are no longer intermediate consolidated CSVs as local artifacts. The raw
per-state `.load` files in GCS serve as the staging layer instead (they are removed manually
in Step 6, after the BQ loads have been verified — see the manual-cleanup design note below).

> **Note on existing pre-June 2026 snapshot folders in GCS:** The old workflow actually left
> *two layers* of CSVs in each snapshot folder — the `CR_FULL_<TABLE>.csv` source files **and**
> the composed `<TABLE>.csv` files (e.g. both `CR_FULL_ACCOUNT_RPT.csv` and `ACCOUNT_RPT.csv`).
> The old cleanup step only deleted the `MC_*` and `CR_FULL_*` parts, leaving the composed files
> as long-term GCS artifacts. Those old snapshot folders are untouched by the new code — they
> retain their CSVs. Only new snapshot runs going forward use the `.load` → Parquet-only pattern.
> If the data from any old snapshot is needed, export it directly from the corresponding BigQuery
> tables rather than downloading from GCS.

### Why GCS cleanup is manual (July 2026)

The v4 PS1 originally ended with `gsutil -m rm "$base/**/*.load"` to delete the staged
`.load` files after loading. This is now commented out permanently and cleanup moved to a
manual Step 6. Rationale: the `.load` files in GCS are the recovery point for the PS1. If
any `bq load` fails (bad row, transient error, wrong flag) the fix is simply to re-run the
PS1 — but only if the `.load` files are still there. Automatic cleanup after a partial
failure would force a full re-run of `prepare_snapshot.py` (2+ hours of extraction and
upload) just to re-stage the same files. Deleting them is cheap and safe to do by hand
once the Step 5 row-count verification confirms the loads are complete.

### NLSDB Parquet load approach

`extract_parquet.txt` (older) loaded NLSDB Parquet without `--autodetect` into non-date-suffixed
tables (`nlsdb_case_geo`). The v3/v4 PS1 originally used `--autodetect` and date-suffixed table
names (`nlsdb_case_YYYYMMDD`), which is the naming convention BQ queries expect.

**June 2026 geometry fix:** `bq load --autodetect` detects GeoParquet file metadata and
validates the `Shape` column as GEOGRAPHY during load, applying BQ's strict S2 spherical
geometry validator. The NLSDB data contains polygons with self-intersecting rings that GEOS
`-makevalid` repairs in planar space but BQ's S2 engine still rejects (`Edge N crosses edge M`).
The v4 PS1 now uses a three-step pattern: create a temporary external table over the Parquet with
`Shape BYTES` (schema override prevents GeoParquet metadata from triggering geography validation),
run a CTAS with `ST_GEOGFROMWKB(Shape, make_valid => TRUE)` to apply BQ's own spherical repair,
then drop the external table. The final table still has `Shape GEOGRAPHY` — the repair happens
inside BQ rather than being enforced at load time.

---

## Known Issues / Watch-outs

| Issue | Detail |
|---|---|
| `ogr2ogr` Parquet + GEOS requirement | `prepare_snapshot.py` requires `ogr2ogr` built with both the Arrow/Parquet driver and GEOS (for `-makevalid`). On this machine the only qualifying install is the `dev` conda env (GDAL 3.10, `conda-forge`, with `libgdal-arrow-parquet` and GEOS 3.13 installed). The PostgreSQL-bundled ogr2ogr (GDAL 3.7.1) has Parquet but **no GEOS** — using it silently produces Parquet files with unrepaired geometry that BigQuery will reject. Always run `conda activate dev` before `python prepare_snapshot.py`. |
| `fiona` on Windows | `fiona.listlayers()` on a File GDB uses the OpenFileGDB driver. This is read-only but sufficient for layer detection. No ESRI license needed. |
| `bq` pipe delimiter and quoting on Windows | The PS1 uses `bq --% load --field_delimiter="\|" --quote=""` with the PowerShell stop-parsing token `--%` and cmd-style env var expansion `%BQ_SCHEMA%`. `--quote=""` disables CSV quote recognition because BLM `.load` files use no quoting — literal `"` in field values (e.g., case names like `"BIG BALDS" - BALD COUGAR`) must not trigger CSV quoting logic. Without this flag, BQ silently skips rows containing double-quote characters. Do not convert these flags to native PowerShell variable syntax — the pipe character will break. |
| Embedded pipe `\|` in text fields | BLM's MLRS export occasionally produces `.load` files with a literal pipe character inside a text field (e.g., a company name or address). This creates an extra column that BQ rejects. `prepare_snapshot.py` validates column counts against the schema files before upload and reports the exact file and line number. **Fix:** edit the `.load` file to remove/replace the embedded pipe, re-upload that one file with `gsutil cp`, then run the PS1. See "Fixing embedded pipe errors" in Step 3. **Recurring, not rare:** seen in May 2026, June 2026 and August 2026, every time in `CR_FULL_ACCOUNT_RPT_WY.load`. |
| GCS cleanup is manual (July 2026 change) | The `gsutil -m rm **/*.load` step in the PS1 is commented out on purpose: if the PS1 fails partway and cleanup ran anyway, the staged `.load` files would be gone and recovery would require re-running `prepare_snapshot.py` (2+ hours). Instead, run the cleanup command manually (Step 6) after the Step 5 BQ verification passes. Only the Parquet files (`Case_*.parquet`, `CaseLands_*.parquet`) are kept long-term. |
| NLSDB invalid polygon geometry | The source GDB contains polygons with geometry errors (duplicate vertices, self-intersecting rings) that fail BigQuery's strict S2 spherical geography validator. Two-layer fix: (1) The `dev` env's ogr2ogr (`-makevalid`, GEOS 3.13) repairs planar invalidity at conversion time — always `conda activate dev` first. (2) `bq load --autodetect` detects GeoParquet metadata and validates `Shape` as GEOGRAPHY on ingest; polygons with crossing edges survive GEOS makevalid but still fail BQ's S2 validator (`Edge N crosses edge M`). The PS1 now bypasses this automatically: it creates a temp external table with `Shape BYTES` (no geography validation), then runs a CTAS with `ST_GEOGFROMWKB(Shape, make_valid => TRUE)` to apply BQ's own spherical repair. No manual intervention needed. |
| MC wildcard no-match WARNING | If every state's MC `.load` file for a given table is empty, `prepare_snapshot.py` uploads nothing for that table's MC prefix. The PS1's `bq load` (append) then gets `Not found: Uris .../MC_<TABLE>_*.load` and exits with WARNING code 2. This is **expected and benign** — the CR_FULL data still loaded. In June 2026, `ACTION_CASE` and `SUPPLEMENTAL_USE` both triggered this (no MC records exist for those tables). |
| NLSDB public extract Status Records | The public Esri API extract includes cases where `CSE_DISP = 'Status Record'` (~5.2M rows, ~49% of raw Case data). Standard BLM GDB snapshots do not include these. As of July 2026, `prepare_snapshot_public_extract.py` **splits** the Case layer: non-Status rows → `Case_<date>.parquet` → `nlsdb_case_<date>`; Status Records → `StatusRecords_<date>.parquet` → `nlsdb_status_records_<date>` (data preserved, kept out of case-level DQ). The split requires the Case layer to have a `CSE_DISP` field — if absent, the script WARNs prominently and loads everything unsplit into `nlsdb_case_<date>` (verify before DQ). CaseLands typically lacks the field and converts unfiltered with a note. **Letting Status Records into `nlsdb_case_*` caused the `nlsdb_case_20251001` incident** — 10.5M rows loaded to BQ. If that happens, remove them in BQ: `DELETE FROM nlsdb_case_YYYYMMDD WHERE CSE_DISP = 'Status Record'` or add `WHERE CSE_DISP != 'Status Record'` to every downstream query. |
| NLSDB public extract — URL list ships mostly commented out | `extract_feature_service.py` has 5 of its 6 endpoint URLs commented out (frozen in the Feb 2026 Land-Tenure-only retry state). Running it unread pulls Land Tenure only and reports success. **Check the URL list before every run.** Verified 2026-08-10. |
| NLSDB public extract — GDB Case layer must be named `case` | `prepare_snapshot_public_extract.py` matches the Case layer against `Case_*`, exactly `case`, or `nlsdb_public_extract_*`. A GDB whose Case layer is named `nlsdb_case` — the name the merge notebook gives the *gpkg* layer — causes `sys.exit(1)` with `ERROR: no Case_*/case/nlsdb_public_extract_* layer in GDB`. Use `-nln case` when converting gpkg → GDB, and name the source layer explicitly or ogr2ogr collapses both layers under one name. Longer-term fix: add `or l == 'nlsdb_case'` to the loader's match list. Verified 2026-08-10. |
| NLSDB public extract — validate before loading | Four gates catch the failure modes that have actually occurred: (1) both `case`-ish and `case_lands` layers present in the GDB, (2) `case_lands` carries land fields (`CSE_LND_STATUS`, `US_RIGHTS`, `DOC_TYPE`, `CSE_LND_ACRS`) and not Case fields (`MC_PATENTED`, `PAT_NR`, `SUPP_USE`, `SEG_MIN`), (3) `case_lands` row count is roughly ⅕–⅓ of `case` — equal counts mean Case was loaded twice, (4) post-load, `SELECT COUNT(*) FROM nlsdb_case_<date> WHERE CSE_DISP = 'Status Record'` returns 0. Gates 1–3 would each have caught Feb 2026; gate 4 catches the Oct `20251001` incident. Full code in `README_Public_Extract_build.md` §8 and §9.5. |
| NLSDB public extract — Land Transfer bug | `extract_feature_service.py` does not pull the `Land_Transfer_Case_Land` endpoint. Costs ~32,321 MLRS Land Transfer cases per snapshot (BQ-verified vs standard GDB). Also removes unique Land Transfer fields (DOC_TYPE, DOC_NR, DOC_DT, TITLE_ACC_DT, LND_SELECTED_BY, PRIORITY, CSE_LND_STATUS, CSE_LND_ACRS, Modified) from the merged Case table. Full fix documented in DQIMP §6.3.10 §A (touches 3 files including the notebook, requires `SF_ID` dedup because endpoint is case-land-level). |
| NLSDB public extract — Feb 2026 `nlsdb_case_land_20260202` is broken | Loaded with the Case schema instead of Case_Land schema — has MC_PATENTED/PAT_NR/SEG_MIN, missing CSE_LND_STATUS/US_RIGHTS/DOC_TYPE/CSE_LND_ACRS. Root cause: Feb 23 gpkg on disk contains only the `case` layer; Case_Lands endpoint was never pulled or was lost. Same Case data was loaded into both `nlsdb_case_20260202` and `nlsdb_case_land_20260202` (identical 5,359,028 row counts confirm). **DQ policy: exclude `nlsdb_case_land_20260202` from all case_land queries** — see excluded-snapshot registry in §NLSDB Public Extract. Not recoverable. |
| NLSDB public extract — Oct/Nov 2025 shutdown-freeze | `nlsdb_case_20251101/20251103` are byte-identical to their Oct counterparts. This is NOT a file copy — the BLM public API was frozen during the federal shutdown. MLRS blm_case growth Oct→Nov was ~100 cases/day vs ~330/day surrounding, docx explicitly notes the shutdown, and API refreshes normally now (June 2026 probe). Treat 20251005 and 20251103 as representing the same underlying NLSDB state. |
| NLSDB public extract — schema differences by tooling path | The differences observed between Feb 2026 and Oct/Nov 2025 are due to different tooling paths, not API inconsistency. Oct/Nov used ogr2ogr end-to-end (`OBJECTID` + `fid`, correct Case_Land schema). Feb 2026 used ArcPro Export Features (`OBJECTID_1`, `nlsdb_Shape`, `Shape_Length`, `Shape_Area`, TIMESTAMP dates) because the user's ogr2ogr install was broken. Standardize on the `dev` conda env's ogr2ogr; see DQIMP §6.3.10 §C and §E. When BQ loads fail with type mismatch (e.g., `Shape has type DOUBLE`), inspect the Parquet with `pyarrow.parquet.read_schema()` and generate the external table DDL manually. |
| Silent upload corruption (gsutil) | `gsutil cp` can silently corrupt large files (~400 MiB+) during upload — the GCS copy differs from the local file but gsutil reports success. This causes BQ load failures with "Too many values in line" errors that the column-count validation cannot catch (the local file is clean). **Fix:** `prepare_snapshot.py` now uses `-o "GSUtil:check_hashes=always"` on all uploads, which forces post-upload hash verification and fails loudly on mismatch. If a hash mismatch occurs, re-upload the affected file. Discovered on September 2025 BLM_CASE MT (429 MiB). Long-term fix: migrate from `gsutil` to `gcloud storage cp`. |
| Re-running extract | `prepare_snapshot.py` skips tar extraction if the output subdirectory already exists. Empty `.load` files (states with no records) are silently filtered before upload — safe to re-run. |
| State code `ES` | Eastern States. Included in both Full and MC folders alongside the 11 western state codes (AK, AZ, CA, CO, ID, MT, NM, NV, OR, UT, WY). |

---

## Folder Structure Reference

Two separate trees: **scripts live in the repo, data lives outside it.** They never mix.

**1. This repo — the scripts and their schemas (version controlled, public)**

```
E:\Xentity\BLM\BLM_DQIMP_GitRepo\
├── xentity_blm_dqimp_big_query\              ← the clone; open THIS as the VS Code workspace
│   ├── build\                                ← MASTER copies — copy out, never edit in place
│   │   ├── README_build.md                   ← this document
│   │   ├── prepare_snapshot.py
│   │   ├── BLM_DQIMP_OneStop_param_v4.ps1
│   │   └── Public_Extract_Specific\          ← NLSDB public-extract fallback
│   │       ├── README_Public_Extract_build.md
│   │       ├── extract_feature_service.py
│   │       ├── merge_public_extract.ipynb
│   │       ├── prepare_snapshot_public_extract.py
│   │       └── BLM_DQIMP_OneStop_param_v4_public_extract.ps1
│   └── schemas\
│       └── mlrs_export_schemas\              ← 24 JSON schema files for bq load
│           ├── blm_case_schema.json          ← located automatically at runtime
│           └── ...
└── Query_Results\                            ← query output; SIBLING of the clone, never inside
```

**2. Local disk — the monthly data (NOT version controlled, never inside the repo)**

```
E:\Xentity\BLM\DQIMP\Data\
├── September_2026\                           ← one folder per monthly snapshot
│   ├── prepare_snapshot.py                   ← copied from the repo
│   ├── BLM_DQIMP_OneStop_param_v4.ps1        ← copied from the repo
│   ├── 2026-09-06_MLRS_Full.zip              ← from BLM (before unzip)
│   ├── 2026-09-06_MLRS_MC.zip                ← from BLM (before unzip)
│   ├── 2026-09-08_NLSDB.gdb.zip              ← from BLM (before unzip)
│   ├── 2026-09-06_MLRS_Full\                 ← auto-created by prepare_snapshot.py
│   ├── 2026-09-06_MLRS_MC\                   ← auto-created by prepare_snapshot.py
│   ├── nlsdb_09082026.gdb\                   ← auto-created by prepare_snapshot.py
│   ├── Case_20260906.parquet                 ← auto-created, uploaded to GCS
│   └── Full_Load_Log_09082026.txt            ← run log
└── August_2026\                              ← prior month (same structure)
```

⚠️ **Month folders must never be created inside the repo working tree.** They hold multi-GB
zips, GDBs and Parquet files containing live BLM case data — serial numbers, case names,
dispositions, legal land descriptions — and this repo is public. `.gitignore` blocks the file
types as a second layer of defence, but the folder location is the first.

---

## Future Iteration Ideas (Not Yet Implemented)

These are candidates for the next version of the workflow:

- **Row-count validation:** After each `bq load`, query `SELECT COUNT(*) FROM <table>` and
  compare against expected row counts to confirm the load was complete. This could also surface
  cases where `--max_bad_records` silently dropped more rows than expected.
- **Single entry point:** A thin wrapper script (PS1 or batch) that calls
  `prepare_snapshot.py` and then the OneStop PS1 in sequence, so the whole pipeline is one
  double-click.
- **Idempotency check:** Before loading, check whether `blm_seta_dqimp.blm_case_<DATE>`
  already exists in BQ and prompt before overwriting.
- **MC wildcard no-match handling:** If a table has no MC records for any state (all MC `.load`
  files are empty), the `bq load --append_table` call for that table will fail to find files
  matching the wildcard. Currently this is caught as a WARNING and skipped. Could add an explicit
  GCS check before the append call to produce a cleaner log message.
