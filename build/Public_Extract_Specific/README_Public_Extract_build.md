# NLSDB Public Extract — End-to-End Process

**Fallback workflow only.** Use this when BLM does **not** deliver a standard NLSDB File
Geodatabase for a given month. When BLM delivers the GDB normally, use the standard process in
`README_build.md` and ignore everything here.

Last used: **February 2026** (`20260202`) — and that run failed partway; see §9.
Last standard-GDB month: **August 2026** (`20260802`), delivered normally.

---

## 1. What this process produces, and what it costs you

The public extract reconstructs an NLSDB snapshot by pulling BLM's public ArcGIS REST services at
`https://gis.blm.gov/nlsdb/rest/services/` and assembling them into a File Geodatabase that looks
close enough to BLM's own delivery that the standard loading scripts can consume it.

It is **not** equivalent to the real thing. Known, quantified deficits:

| Deficit | Impact | Recoverable? |
|---|---|---|
| Land Transfer endpoint not pulled (§4.2) | **~32,321 MLRS cases missing per snapshot** | Yes — fix documented, not yet applied |
| 14 fields not exposed by the API (§2.3) | DQ tests on those fields silently return null | **No** — not available from any public source |
| Status Records included (§9.1) | ~5.2M rows (~49% of raw Case data) absent from standard GDBs | Yes — split out at load time |
| Geometry mutation (§4.4) | ~143K Land Tenure geometries altered per pull | No — inherent to the extract script |
| **Total case-level loss vs a paired standard GDB** | **~58,247 cases** | — |

Before running DQ on a public-extract month, enumerate which tests touch the 14 missing fields.
They will produce false "matches" or false nulls rather than errors.

---

## 2. Prerequisites

### 2.1 Python environment

```
aiohttp>=3.9.0
geopandas>=0.14.0
shapely>=2.0.0
pyproj>=3.6.0
fiona>=1.9.0
pyogrio            # required — the extract script writes with engine="pyogrio"
pandas>=2.2.0
numpy>=1.26.0
```

`pyogrio` is not optional. `extract_feature_service.py` passes `engine="pyogrio"` to
`gdf.to_file()` (added 12/6/2025 after fiona failed on the write).

### 2.2 ogr2ogr with Parquet support

Same requirement as the standard process: an ogr2ogr build with **both** the Arrow/Parquet driver
and GEOS. On this workstation that is the `dev` conda env (GDAL 3.10 conda-forge, GEOS 3.13,
`libgdal-arrow-parquet`).

```bash
conda activate dev
```

Verify before starting — a wrong ogr2ogr silently produces unusable output:

```bash
ogr2ogr --formats | Select-String "Parquet"
```

> **Historical note:** a broken ogr2ogr install (three conflicting copies on PATH) is what forced
> the ArcPro fallback in February 2026, which in turn polluted the schema. If this check fails,
> fix the environment — do not work around it. See §10.1.

### 2.3 The 14 fields the API does not expose

`PAT_ISS_DT`, `PUB_DT`, `PUB_TYPE`, `TITLE_ACC_DT`, `FUND_BY`, `PLSSIDS`, `GIS_ACRS`,
`PLSS_UPDATE_DT`, `ID`, `STAGE_ID`, `Created`, `Modified`.

These exist on the standard BLM GDB and on no public endpoint. Nothing downstream can recover them.

### 2.4 Disk and time budget

- ~10 GB for the merged gpkg alone (Feb 2026 was 10,033,438,720 bytes)
- Land Tenure endpoint alone ran **71 minutes** (Dec 8 v3 log)
- Full extract is a multi-hour, multi-session job — plan for it

---

## 3. Process overview

```
  [1] Preflight              verify env, endpoints, URL list
       |
  [2] Extract                extract_feature_service.py  -> 6 per-endpoint .gpkg
       |
  [3] Merge                  ogr2ogr + merge notebook    -> nlsdb_public_extract_<MMDDYYYY>.gpkg
       |                                                    (layer: nlsdb_case)
  [4] Add Case_Lands         ogr2ogr -update             -> + layer: case_lands
       |
  [5] Convert to GDB         ogr2ogr -f OpenFileGDB      -> nlsdb_public_extract_<MMDDYYYY>.gdb
       |                     (ArcPro = fallback only)
  [6] Validate               layer + schema + count gates  <-- DO NOT SKIP
       |
  ==== from here the standard scripts take over ====
       |
  [7] Prepare                prepare_snapshot_public_extract.py -> parquet -> GCS
       |
  [8] Load                   BLM_DQIMP_OneStop_param_v4_public_extract.ps1 -> BigQuery
       |
  [9] Verify                 row counts, schema, Status Record split
```

Steps 1–6 are this document's subject. Steps 7–9 are the standard loading process, documented in
`README_build.md`; only the differences are noted here.

---

## 4. Step 2 — Extract the endpoints

**Script:** `extract_feature_service.py` (this folder)

### 4.1 ⚠️ Check the URL list before every run

The endpoint list is hardcoded at the bottom of the script in `if __name__ == "__main__":`. **The
committed copy has five of the six URLs commented out** — it is left in the state of the February
2026 Land-Tenure-only retry:

```python
urls = [
    #"https://gis.blm.gov/nlsdb/rest/services/Fluid_Minerals/FluidMinerals_Case/MapServer/0/query",
    "https://gis.blm.gov/nlsdb/rest/services/Land_Tenure/Land_Tenure_Case/MapServer/0/query",
    #"https://gis.blm.gov/nlsdb/rest/services/Land_Use_Authorizations/Land_Use_Auth_Case/MapServer/0/query",
    #"https://gis.blm.gov/nlsdb/rest/services/Mining_Claims/MiningClaims/MapServer/0/query",
    #"https://gis.blm.gov/nlsdb/rest/services/Solid_Minerals/Solid_Minerals_Case/MapServer/0/query",
    #"https://gis.blm.gov/nlsdb/rest/services/Case_Lands/MapServer/0/query"
]
```

**Running it as-is pulls Land Tenure and nothing else.** Uncomment all six before a full run. This
is deliberate on the author's part — commenting URLs out one at a time is the documented way to
retry a single failed endpoint — but it makes the script dangerous to run unread.

### 4.2 The endpoints

| Endpoint | Rows (Jun 30 2026) | In URL list? |
|---|---:|:---:|
| `Fluid_Minerals/FluidMinerals_Case/MapServer/0` | 510,090 | Yes |
| `Solid_Minerals/Solid_Minerals_Case/MapServer/0` | 92,006 | Yes |
| `Land_Use_Authorizations/Land_Use_Auth_Case/MapServer/0` | 236,115 | Yes |
| `Land_Tenure/Land_Tenure_Case/MapServer/0` | 5,384,922 | Yes |
| `Mining_Claims/MiningClaims/MapServer/0` | 4,376,536 | Yes |
| `Case_Lands/MapServer/0` (aggregate) | 1,865,563 | Yes — becomes `case_lands` |
| **`Land_Transfer/Land_Transfer_Case_Land/MapServer/0`** | **929,809** | **NO — this is the bug** |

**Land Transfer is missing and it is a real defect.** BLM publishes **no** case-level Land Transfer
endpoint (`Land_Transfer_Case` returns 404). The only source of Land Transfer case attributes is
`Land_Transfer_Case_Land`, which is **case-land-level and many-to-one on `CSE_NR`** — so it must be
deduplicated on `SF_ID` before merging, unlike every other endpoint.

Fixing it requires edits in **three** places (extract URL list, the parquet conversion command list
in §5, and the merge notebook's file list). Not yet applied.

### 4.3 Run it

```bash
conda activate dev
cd E:\Xentity\BLM\NLSDB_Public_Extract\Extracts\Extract_<MMDDYYYY>
python extract_feature_service.py
```

Output: one GeoPackage per endpoint, named from URL segment 6 —
`Land_Tenure_case.gpkg`, `Fluid_Minerals_case.gpkg`, `Solid_Minerals_case.gpkg`,
`Land_Use_Authorizations_case.gpkg`, `Mining_Claims_case.gpkg`, `Case_Lands_case.gpkg`.

> **Gotcha:** every output layer is named `case_lands` regardless of endpoint — `layer_name` is
> hardcoded in the call. It is harmless (downstream code reads the only layer in each file) but the
> name is a lie for the five Case endpoints. Do not use the layer name to identify content.

### 4.4 Tuning and what the script does to your data

Current settings: `max_records=1800`, `max_concurrent=7`. Elii's original note says it worked at 15
and failed at 20; the 12/6/2025 update settled on 7. Lower `max_concurrent` if the service starts
timing out — these run on modest BLM servers.

The script mutates geometry on the way through, by design:

- null geometries → empty `MultiPolygon` (~134K rows on Land Tenure)
- everything forced to `MultiPolygon` via `force_multipolygon()`
- invalid geometries repaired with `buffer(0)` (~8.8K rows)

Feature **counts** are preserved; spatial fidelity is not byte-identical to source.

### 4.5 ⚠️ Failure handling is weak — watch it yourself

Each batch is attempted 3 times (sleep **2s** after the first failure, **4s** after the second,
then give up — 120s timeout per attempt) and is then **silently dropped**. The only safety net is a
warning that fires when retrieval falls below **90%** of the pre-flight count.

> KB §6.3.10 §B describes this as "2s/4s/6s backoff". That is wrong — the third attempt returns
> `None` without sleeping. Verified against `fetch_batch_with_retry()`.

**10% of Land Tenure is ~540,000 cases.** A run can lose a quarter-million records and still print
a success message. Read the per-endpoint output:

```
Final: N successful, M failed batches
Records retained: X / Y
```

**If `M > 0` or `X < Y`, the pull is incomplete.** Re-run that endpoint (comment out the others)
until it comes back clean. Do not proceed to merge with a partial endpoint — this is precisely how
February 2026 went wrong.

---

## 5. Step 3 — Merge into one GeoPackage

**Notebook:** `merge_public_extract.ipynb` (this folder)

**Use the Parquet merge below.** Decided 2026-08-10: it is the standard path for every
public-extract month. A direct-gpkg fallback is retained in §5.4 for one specific emergency only —
do not treat the two as a choice.

### 5.1 The process — merge via Parquet

Convert each per-endpoint gpkg to Parquet:

```bash
ogr2ogr -f "Parquet" Land_Use_Authorizations_case.parquet Land_Use_Authorizations_case.gpkg -lco GEOMETRY_ENCODING=WKB
ogr2ogr -f "Parquet" Fluid_Minerals_case.parquet Fluid_Minerals_case.gpkg -lco GEOMETRY_ENCODING=WKB
ogr2ogr -f "Parquet" Land_Tenure_case.parquet Land_Tenure_case.gpkg -lco GEOMETRY_ENCODING=WKB
ogr2ogr -f "Parquet" Mining_Claims_case.parquet Mining_Claims_case.gpkg -lco GEOMETRY_ENCODING=WKB
ogr2ogr -f "Parquet" Solid_Minerals_case.parquet Solid_Minerals_case.gpkg -lco GEOMETRY_ENCODING=WKB
```

Run the notebook's **first** cell (parquet merge) — takes ~2 minutes — then convert back to gpkg:

```bash
ogr2ogr -f "GPKG" "nlsdb_public_extract_<MMDDYYYY>.gpkg" "nlsdb_public_extract_<MMDDYYYY>.parquet" -nln nlsdb_case -lco FID=ogc_fid
```

> `-lco FID=ogc_fid` is a required workaround. The parquets already carry an `fid` column inherited
> from their gpkg origin; without the rename the conversion fails on the duplicate. The extra
> `ogc_fid` column can be dropped later.

Layer name must be `nlsdb_case`, not `case` — `case` is a SQL reserved word and a GeoPackage is a
SQLite database. (The *GDB* layer is renamed to `case` later, at §7.1 — different file, different
constraint.)

### 5.2 Why Parquet and not the direct merge

| | **Parquet merge (standard)** | Direct gpkg merge (fallback) |
|---|---|---|
| Merge time | **~2 min** | ~1 hour |
| Peak RAM | modest | ~32 GB (near-exhausts a 32 GB machine) |
| ogr2ogr needed | yes, twice | **no** |
| Manual steps | more | fewer |
| Used in | Dec 2025 | Feb 2026 |

Thirty times faster, and it does not put the machine into swap. KB §6.3.10 §C2 originally
recommended the opposite — standardize on the direct merge to cut manual steps and drop the FID
workaround — but that was written before the runtime and RAM costs were measured. The extra
ogr2ogr commands are cheap and scriptable; an hour of saturated RAM is not.

### 5.3 Known bugs in the merge cell

1. **Land Transfer is absent from the file list** (see §4.2). Add it, with
   `.drop_duplicates(subset=["SF_ID"], keep="first")` before the concat.
2. **The folder path and output filename are hardcoded per month.** The cell still writes
   `nlsdb_public_extract_12082025.parquet` while reading from `Extract_02232026`. Update both every
   month.

### 5.4 Emergency fallback — direct gpkg merge

**Do not use this by preference.** It exists for exactly one situation: **ogr2ogr is unavailable or
broken and cannot be fixed in time.** The direct merge is pure GeoPandas and touches no GDAL
command line, so it is the only way to complete a merge when the environment is broken — which is
precisely the failure that derailed February 2026 (§10.1).

Run the notebook's **second** cell. No ogr2ogr, no FID workaround. Budget an hour and expect RAM
saturation. Everything downstream is unchanged, except that §6 uses `FID=fid` instead of
`FID=ogc_fid`.

If you use it, **record the month as a deviation** — same as an ArcPro conversion (§7.2).

---

## 6. Step 4 — Add the Case_Lands layer

The merge above covers only the five Case endpoints. Case Lands is added separately:

```bash
ogr2ogr -f "Parquet" Case_Lands_case.parquet Case_Lands_case.gpkg -lco GEOMETRY_ENCODING=WKB
ogr2ogr -f "GPKG" -update "nlsdb_public_extract_<MMDDYYYY>.gpkg" "Case_Lands_case.parquet" -nln case_lands -lco FID=ogc_fid
```

> `FID=ogc_fid` on the standard Parquet merge (§5.1); `FID=fid` only if you fell back to the
> direct gpkg merge (§5.4).

**This step is the single most-skipped part of the process, and skipping it is what broke February
2026.** The gpkg must end up with **two** layers: `nlsdb_case` and `case_lands`.

---

## 7. Step 5 — Convert to File Geodatabase

### 7.1 Preferred: ogr2ogr

```bash
ogr2ogr -f "OpenFileGDB" nlsdb_public_extract_<MMDDYYYY>.gdb nlsdb_public_extract_<MMDDYYYY>.gpkg nlsdb_case -nln case
ogr2ogr -f "OpenFileGDB" -update nlsdb_public_extract_<MMDDYYYY>.gdb nlsdb_public_extract_<MMDDYYYY>.gpkg case_lands -nln case_lands
```

Requires GDAL with OpenFileGDB **write** support — present in GDAL 3.10+ conda-forge, i.e. the
`dev` env.

> ⚠️ **Name the source layer explicitly, and output the Case layer as `case` — not `nlsdb_case`.**
> Two separate reasons:
>
> 1. The gpkg holds two layers. Passing the datasource without naming one makes ogr2ogr translate
>    both, collapsing them under a single `-nln` name.
> 2. `prepare_snapshot_public_extract.py` detects the Case layer by matching `Case_*`, exactly
>    `case`, or `nlsdb_public_extract_*`. **`nlsdb_case` is not in that list** — a GDB whose Case
>    layer is named `nlsdb_case` makes the script exit with
>    `ERROR: no Case_*/case/nlsdb_public_extract_* layer in GDB`. The gpkg layer stays `nlsdb_case`
>    (SQLite reserves `case`); only the GDB layer is renamed. See defect #10 in §11.

### 7.2 Fallback: ArcPro Export Features — avoid unless forced

If ogr2ogr write support is unavailable, ArcPro's Export Features tool works, with one manual fix:
expand the **Fields** dropdown, click **edit**, and rename the `Shape` field to `nlsdb_Shape`
before running. `Shape` is reserved in a GDB and ArcPro will reject it.

**Document the month as a deviation if you do this.** ArcPro adds `OBJECTID_1`, `nlsdb_Shape`,
`Shape_Length`, `Shape_Area` and converts dates to Esri Date type. That schema does not match what
the loading PS1 expects, and the BQ load will fail until the DDL is hand-generated (§8.2).

### 7.3 Naming

| | Standard BLM GDB | Public extract |
|---|---|---|
| Zip | `YYYY-MM-DD_NLSDB.gdb.zip` | `YYYY-MM-DD_NLSDB_Public_Extract.zip` |
| GDB | `nlsdb_MMDDYYYY.gdb` | `nlsdb_public_extract_MMDDYYYY.gdb` |
| Case layer | `Case_MMDDYYYY_HHMM` | `case` or `nlsdb_public_extract_MMDDYYYY` |
| CaseLands layer | `CaseLands_MMDDYYYY_HHMM` | `case_lands` |

`prepare_snapshot_public_extract.py` accepts all of these variants.

---

## 8. Step 6 — Validation gates (do not skip)

Every one of these would have caught a real, expensive failure.

### 8.1 Layer presence

Mirror exactly what `prepare_snapshot_public_extract.py` accepts — a gate that passes while the
loader rejects the GDB is worse than no gate:

```python
import fiona
layers = fiona.listlayers(r"nlsdb_public_extract_<MMDDYYYY>.gdb")

# same matching rules as prepare_snapshot_public_extract.py — keep in sync
case = [l for l in layers if l.startswith("Case_") or l == "case"
        or l.startswith("nlsdb_public_extract_")]
lands = [l for l in layers if l.startswith("CaseLands_") or l == "case_lands"]

assert case,  f"no Case layer the loader will recognise: {layers}"   # catches an nlsdb_case-named GDB
assert lands, f"case_lands MISSING: {layers}"                        # would have caught Feb 2026
```

`prepare_snapshot_public_extract.py` performs this same check itself and calls `sys.exit(1)` if
either layer is missing (verified) — but run it here, before you spend an hour on conversions
rather than after.

### 8.2 case_lands schema fingerprint

Confirms `case_lands` actually holds land-level data rather than a second copy of the Case layer:

```python
with fiona.open(gdb_path, layer="case_lands") as src:
    fields = set(src.schema["properties"])
must_have     = {"CSE_LND_STATUS", "US_RIGHTS", "DOC_TYPE", "CSE_LND_ACRS"}
must_not_have = {"MC_PATENTED", "PAT_NR", "SUPP_USE", "SEG_MIN"}
assert not (must_have - fields),   f"case_lands missing land fields: {must_have - fields}"
assert not (must_not_have & fields), f"case_lands has CASE fields: {must_not_have & fields}"
```

This exact check would have caught the Feb 2026 mis-load immediately.

### 8.3 Row-count sanity

Healthy `case_lands` is roughly ⅕ to ⅓ of `case` (June 2026: 1.87M vs 10.6M).

- `case_lands` rows **==** `case` rows → **red flag**, you loaded Case twice
- `case_lands` < 1M or > 3M → **red flag**

### 8.4 gpkg vs GDB comparison

The merge notebook's third cell compares feature counts and per-field attribute values between the
gpkg and the converted GDB (requires `arcpy`). Run it — it confirms the conversion did not silently
drop or alter records. Update the four paths at the top of the cell first.

---

## 9. Steps 7–9 — Load into BigQuery

From here the standard scripts take over. Use the `_public_extract` variants of both:

```bash
conda activate dev
cd <month folder>
python prepare_snapshot_public_extract.py
.\BLM_DQIMP_OneStop_param_v4_public_extract.ps1 -Date <YYYYMMDD>
```

The MLRS half of the month is **identical** to the standard process. Only NLSDB handling differs.

> **Scope boundary.** §1–§8 of this document are self-contained: everything needed to turn BLM's
> public API into a finished GDB is in this folder. §9 covers only what is *different* about
> loading a public-extract month. The generic loading process — environment preflight, GCS/BQ
> authentication, the 24 MLRS tables, embedded-pipe handling, GCS cleanup — lives in
> `README_build.md` one level up, and is not duplicated here.

### 9.0 Month folder inputs

Zip the finished GDB as `YYYY-MM-DD_NLSDB_Public_Extract.zip` and place it in the month folder
alongside the two MLRS zips, which are unchanged from a normal month:

```
<Month_Year>\
  YYYY-MM-DD_MLRS_Full.zip                 <- from BLM, unchanged
  YYYY-MM-DD_MLRS_MC.zip                   <- from BLM, unchanged
  YYYY-MM-DD_NLSDB_Public_Extract.zip      <- what this document produced
  prepare_snapshot_public_extract.py
  BLM_DQIMP_OneStop_param_v4_public_extract.ps1
```

The snapshot date for BQ table names is still derived from the **MLRS** zip, not the extract date.

> Month folders live outside the repo — they hold multi-GB zips, parquets and load logs. Never
> create one inside the repo working tree.

### 9.1 Status Record split (July 2026 change)

The per-case-type endpoints return `CSE_DISP = 'Status Record'` rows (~5.2M, ~49% of raw Case
data) that standard BLM GDBs do not contain. `prepare_snapshot_public_extract.py` converts the Case
layer **twice**:

| Output | Filter | BQ table |
|---|---|---|
| `Case_<YYYYMMDD>.parquet` | `CSE_DISP <> 'Status Record'` | `nlsdb_case_<YYYYMMDD>` |
| `StatusRecords_<YYYYMMDD>.parquet` | `CSE_DISP = 'Status Record'` | `nlsdb_status_records_<YYYYMMDD>` |

The split requires the Case layer to have a `CSE_DISP` field. If it does not, the script prints a
prominent WARNING and loads everything unsplit — **verify before running any DQ**.

**Never let Status Records into `nlsdb_case_*`.** That is the `nlsdb_case_20251001` incident:
10,528,722 rows loaded instead of 5,315,332. Every query against that table needs
`WHERE CSE_DISP != 'Status Record'` bolted on forever.

### 9.2 Date conversion

Public-extract dates arrive as **Real / epoch milliseconds**, not Timestamps. The PS1 casts them in
the CTAS via `TIMESTAMP_MILLIS(CAST(field AS INT64))`, per layer:

- Case / StatusRecords: `CSE_DISP_DT`, `EFF_DT`, `EXP_DT`, `SALE_DT`
- CaseLands: `CSE_LND_STATUS_DT`, `DOC_DT`, `Created`, `Modified`

### 9.3 When the BQ load fails on schema

The PS1's hardcoded DDL matches the **ogr2ogr-path** parquet schema (geometry in a WKB column named
`geom`; a junk FLOAT64 `Shape` column dropped in the CTAS; `geom_bbox` deliberately omitted). If
the GDB came from ArcPro, the load will fail. Recovery:

1. `pyarrow.parquet.read_schema('Case_<YYYYMMDD>.parquet')` to see the real schema
2. Hand-write external table DDL (geometry column as `BYTES`, skip `*_bbox` structs)
3. `CREATE TABLE AS SELECT * EXCEPT(geom, Shape), ST_GEOGFROMWKB(geom, make_valid => TRUE) AS Shape`
4. Drop the external table

### 9.4 CRS and conversion flags

The API returns EPSG:4269 (NAD83). `prepare_snapshot_public_extract.py` converts every layer with:

```
-makevalid -lco GEOMETRY=AS_WKB -t_srs EPSG:4326
```

`-makevalid` needs GEOS — another reason the `dev` env is mandatory (§2.2). A Parquet-capable
ogr2ogr **without** GEOS produces files that BigQuery rejects at load time, with no warning at
conversion time.

### 9.5 Verify, then clean up

Public-extract months need one check that standard months do not — that the Status Record split
actually happened:

```sql
SELECT 'nlsdb_case'            AS tbl, COUNT(*) AS n FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_<YYYYMMDD>`
UNION ALL
SELECT 'nlsdb_status_records', COUNT(*) FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_status_records_<YYYYMMDD>`
UNION ALL
SELECT 'nlsdb_case_land',      COUNT(*) FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_land_<YYYYMMDD>`
UNION ALL
SELECT 'LEAKED status recs',   COUNT(*) FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_<YYYYMMDD>`
  WHERE CSE_DISP = 'Status Record';
```

Expected: `nlsdb_case` ≈ 5.3–5.4M, `nlsdb_status_records` ≈ 5.2M, `nlsdb_case_land` ≈ 1.8M, and
**`LEAKED status recs` = 0**. A non-zero leak count is the `20251001` incident repeating — fix it
before running any DQ (§10.3).

Then clean up the staged `.load` files manually, exactly as on a standard month (see
`README_build.md` Step 6). Parquet files stay in GCS long-term; public-extract months keep three
rather than two, the extra being `StatusRecords_<YYYYMMDD>.parquet`.

---

## 10. Failure history — read before running

### 10.1 February 2026 — the run that broke

Land Tenure failed repeatedly on Feb 23 and never produced a clean pull (a zero-byte marker file
`land tenure case had a failure_02232026.txt` survives as the only record). Case_Lands was never
pulled at all. The merge ran anyway against whatever was on disk, producing a gpkg with **only the
`case` layer**. Conversion then went through ArcPro because ogr2ogr was broken, adding
`OBJECTID_1` / `nlsdb_Shape` / `Shape_Length` / `Shape_Area`. At load time the same Case data was
written into **both** `nlsdb_case_20260202` and `nlsdb_case_land_20260202` — the identical
5,359,028 row counts confirm it.

Per-endpoint gpkgs were not preserved, so nothing is salvageable. The API returns current state,
not February 23.

**Standing policy:** any query touching `nlsdb_case_land_*` must exclude `20260202`. Case-level
`nlsdb_case_20260202` remains usable with two caveats — Land Tenure may be undercounted, and
queries referencing `fid` need `COALESCE(fid, OBJECTID_1)`.

Four independent gates in §8 would each have caught this before it reached BigQuery.

### 10.2 October / November 2025 — shutdown freeze

The November pull returned data byte-identical to October's. This was **not** a copy-paste error:
the BLM public API was frozen during the federal shutdown. Corroborated by MLRS `blm_case` growth
dropping to ~100 cases/day (vs ~330 surrounding), the manager's Oct 8 workflow doc explicitly
citing the shutdown, and confirmation that all endpoints refresh normally now.

Treat `20251005` and `20251103` as the same underlying NLSDB state. Do not use both in a trend.

### 10.3 Excluded-snapshot registry

| Snapshot | Table | Rule |
|---|---|---|
| `20260202` | `nlsdb_case_land_20260202` | **Exclude from all case_land DQ** — holds Case schema, no land data |
| `20251001` | `nlsdb_case_20251001` | Usable **only** with `WHERE CSE_DISP != 'Status Record'` |

---

## 11. Known defects and hardening backlog

Nothing below is fixed. Priority order if this process is ever needed again:

| # | Defect | Fix |
|---|---|---|
| 1 | Land Transfer endpoint not pulled — ~32,321 cases lost | Add URL + ogr2ogr command + notebook entry with `SF_ID` dedup (3 files) |
| 2 | 5 of 6 URLs commented out in the committed script | Restore full list; move the list to a config file read by both script and notebook |
| 3 | Failed batches silently dropped below a 10% threshold | Track failed offsets, add a recovery pass at `max_concurrent=1` / 300s timeout, hard-fail if any offset never recovers |
| 4 | No checkpointing on multi-hour runs | Append completed offsets to `<endpoint>.offsets.done`; skip on restart |
| 5 | Per-endpoint gpkgs not preserved | Write to `Extracts\<YYYYMMDD>\` so future broken months can be salvaged |
| 6 | Hardcoded month paths in the notebook | Parameterize on a single `<MMDDYYYY>` variable |
| 7 | ~~Two competing merge paths~~ | **RESOLVED 2026-08-10** — Parquet merge is the standard path (§5.1); the direct gpkg merge is retained only as an ogr2ogr-unavailable fallback (§5.4). Notebook cell 2 is labelled accordingly |
| 8 | 8 manual steps, each a failure point | Single `run_public_extract.py --date YYYYMMDD` wrapper |
| 9 | ogr2ogr install fragility | One documented conda env, tested in preflight — this is what caused §10.1 |
| 10 | Loader does not accept a Case layer named `nlsdb_case`, which is the name the merge notebook uses for the gpkg layer | Add `or l == 'nlsdb_case'` to the `case_layers` match in `prepare_snapshot_public_extract.py`. Until then, §7.1's `-nln case` is mandatory, not stylistic |
| 11 | KB §6.3.10 §B misstates the retry backoff as 2s/4s/6s | Correct to 2s/4s (§4.5) |

Full detail: KB §6.3.10. Items 10 and 11 were found during the 2026-08-10 correctness pass and are
not yet in the KB.

---

## 12. Not fixable downstream

Inherent to BLM's public API — do not spend time on these:

- The 14 fields in §2.3 are not exposed anywhere.
- Status Records inherently exist in the per-case-type endpoints; filtering downstream is correct.
- There is no case-level Land Transfer endpoint. `_Case_Land` is the only source and it is
  many-to-one.
- Dates arrive as epoch milliseconds.
- Geometry mutation in §4.4 happens before you ever see the data.

---

## 13. Files and references

### 13.1 This folder — everything §1–§8 needs

| File | Used at | Purpose |
|---|---|---|
| `README_Public_Extract_build.md` | — | This document |
| `extract_feature_service.py` | Step 2 (§4) | Pulls the REST endpoints to per-endpoint gpkgs. **Check the URL list first (§4.1)** |
| `merge_public_extract.ipynb` | Steps 3, 6 (§5, §8.4) | Cell 1 = **Parquet merge — use this** · Cell 2 = direct gpkg merge, emergency fallback only (§5.4) · Cell 3 = gpkg↔GDB validation |
| `prepare_snapshot_public_extract.py` | Step 7 (§9) | GDB → Parquet (with Status Record split) → GCS |
| `BLM_DQIMP_OneStop_param_v4_public_extract.ps1` | Step 8 (§9) | GCS → BigQuery |

No file outside this folder is required to run steps 1–8. Steps 3–5 also need `ogr2ogr` from the
`dev` conda env (§2.2), and the loader resolves the 24 BQ schema JSONs automatically from the
repo's `schemas/mlrs_export_schemas/` (§9 and `README_build.md`).

### 13.2 External references

| What | Where |
|---|---|
| Standard monthly process | `README_build.md` (repo `build/`) |
| Full audit, endpoint inventory, deficit math | KB `MLRS_Database_Quality_Checks.md` §6.3 |
| Original author's README (superseded by this file) | `E:\Xentity\BLM\NLSDB_Public_Extract\README.md` |
| Manager's field-by-field comparison | `NLSDB Schema Differences\NLSDB Extract comparison.xlsx` |
| Endpoint layer count report | `NLSDB Schema Differences\NLSDB Layer Counts using Public APIs.docx` |
