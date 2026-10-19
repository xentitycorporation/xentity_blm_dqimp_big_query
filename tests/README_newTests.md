# BLM SETA Data Quality Tests

The SQL in this folder tests the monthly MLRS and NLSDB extract snapshots loaded into BigQuery
(`xentity-sandbox-huy.blm_seta_dqimp`, tables suffixed with the snapshot date, e.g. `blm_case_20261004`).
Each subfolder holds one type of test and has its own README listing the tests in it.

**Once a tested query is committed here, this repo is the master copy.** Working copies kept elsewhere
are snapshots in time; change the test here, through a pull request.

The extracts are public data (BLM provides no PII or sensitive data), so queries and comments may name
cases, serial numbers and values freely. **Query outputs are never committed** (see [Outputs](#outputs)).

---

## Test types and folders

| Type | ID prefix | Folder | What it checks | Status |
|---|---|---|---|---|
| Synchronization | `SYT` | [`sync/`](sync/README.md) | A case in MLRS is also in NLSDB, and vice versa, and the same case carries the same value in both systems. | Active |
| Attribute | `AT` | [`attribute/`](attribute/README.md) | Missing or incorrect information in one system, on a single snapshot (e.g. cases missing a customer or case lands, NLSDB cases with no BLM Product). | Active |
| Serial Trace | `STT` | [`serial_trace/`](serial_trace/README.md) | Attributes tracked over time: the same cases followed across snapshots (e.g. cases that lost all their customers). | Active |
| UID Trace | `UTT` | [`uid_trace/`](uid_trace/README.md) | On hold. Its earlier purpose, confirming every unique case in MLRS is present in NLSDB, overlaps the Serial Trace and Sync presence tests. Files are kept as they are. | On hold |
| System Validation | `SVT` | [`system/`](system/README.md) | Not yet redefined under the current types; no tests in the folder. | Under review |
| Geometry | to be assigned | not yet created | Planned: the `CSE_META` table, legal land description (LLD) values, and missing NLSDB geometry. | Planned |

Two other folders hold queries that are not numbered tests:

- [`core_elements/`](core_elements/): simple counts and lists (cases, serial numbers, cases by status) used as
  building blocks and quick checks.
- [`report/`](report/): `SYT_Basic.sql` and `SYT_Basic_CL.sql`, the sync test row counts behind the monthly report.

---

## Test IDs and file names

**The prefix is the test type and the repo folder; the number is the subject; `.N` is a sub-test of that
subject.** For example, `SYT04` compares case disposition between the systems and `SYT04.1` is its client list.

**Attribute and Serial Trace tests share subject numbers**, so the single-snapshot test and the over-time
test on the same subject line up:

| Subject | Number | Attribute (one snapshot) | Serial Trace (over time) |
|---|---|---|---|
| Case land | 06 | `AT06.1` cases missing case land | |
| Customer | 13 | `AT13.1` cases missing a customer | `STT13.6` cases that lost all their customers |
| Case action | 14 | `AT14.1` cases missing a case action | |
| Serial number | 20 | `AT20`, `AT20.1` null serial numbers | |

Common sub-test numbers: `.1` client list, `.2` detail, `.3` by disposition, `.4` PENDING by age,
`.5` cohort, `.6` specific to that subject.

**File name:** the ID with the dot written as an underscore, then a short snake_case description, e.g.
`SYT04_1_case_disposition_mismatch.sql`.

**IDs are never reused.** The full test list, including retired IDs and which tests replace which, is the
Test Catalog v2 sheet of `DQIMP_GitRepo_Test_Index_Update.xlsx` (kept outside the repo). Each test's header
gives its catalog row.

---

## Standard header

Every test added to the repo starts with this header:

```sql
-- =====================================================================================
-- Test ID:      AT14.1
-- Test name:    Cases missing a case action (client list)
-- Test type:    Attribute (tests/attribute/) - missing or incorrect information in one system, on a single snapshot
-- Catalog:      Test Catalog v2 row #115 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     none (new test)
-- Produces:     MLRS_Cases_Missing_CaseAction_<snapshot_date>.xlsx, sheet "Cases Missing Action"
-- Parameters:   snapshot_date (YYYYMMDD), lookup_table, dispositions
-- Conventions:  shared conventions S1-S7 in tests/README_newTests.md
-- Validated:    <snapshot(s) run, the date, and what the result was checked against>
-- Added:        YYYY-MM-DD
-- =====================================================================================
```

Below the header, comments explain what the query does and why. Files without this header predate it and may
not follow the conventions below.

---

## Shared conventions (S1-S7)

Every test with the standard header follows these.

| # | Convention |
|---|---|
| S1 | **Real cases only on the MLRS side.** Every `blm_case` query excludes Bond and Status records: `RECORDTYPEID NOT IN ('0123d0000004QFQAA2' /*Bond*/, '0123d0000005ISFAA2' /*Status*/)`. |
| S2 | **Dispositions are filtered on `UPPER(TRIM(CASE_STATUS))` and displayed as stored.** |
| S3 | **`blm_product` is deduplicated** with `ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CASE_TYPE_CODE, NAME) = 1`, because it carries several identical rows per ID and `ANY_VALUE` is not deterministic. |
| S4 | **The Case Type Group/Subgroup taxonomy is joined on `SAFE_CAST(... AS INT64)` on both sides**, from `lookup_table`. Unclassified rows read `(Unclassified)` as the group. The subgroup reads `BLM Product Code <code> (not in taxonomy)` when the case has a code with no taxonomy row, otherwise `(no case type code / not in blm_product)`. |
| S5 | **Missing values are returned as NULL** (a blank cell), never a placeholder such as `(no serial)`. |
| S6 | **Parameters are declared at the top:** `snapshot_date` (YYYYMMDD), or `snapshots` (an ascending array) for over-time tests, plus `lookup_table`. Test-specific parameters (e.g. `flagged_dispositions`) sit alongside them. Table names are fully qualified. |
| S7 | **Every case list carries `Origin` and `Mining_Claim`.** Any output with one row per MLRS case adds two columns: `Origin` is `Legacy` when the case has a legacy serial number (`NULLIF(TRIM(LEGACY_SERIAL_NUMBER), '') IS NOT NULL`, i.e. it predates Salesforce and came from LR2000), otherwise `New`; `Mining_Claim` is `Yes` when `RECORDTYPEID = '012t00000008bvNAAQ'` (Mining Claim record type), otherwise `No`. Count outputs may be split by the same two values. Together they give the four-way Legacy/New x Mining Claim/Other view; System Validation tests use it to compare problems in New cases against Legacy ones. Adopted 2026-10-19: tests added before then do not carry these columns yet. |

The taxonomy lookup table is `xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup`;
its source CSV is in [`schemas/`](../schemas/).

---

## Running a test

Set the parameters in the `DECLARE` lines at the top, then use any of:

- **BigQuery console:** paste the file and run. A script with several `SELECT` statements returns one result
  per statement.
- **VS Code:** open the file and run the task *BigQuery: Run current SQL file*, *... -> CSV*, or *Dry run*
  (`.vscode/tasks.json`; open this repo folder as the workspace, not its parent). The terminal task shows every
  result table, labelled when there are several. The CSV task writes one CSV per result table
  (`<file>_<timestamp>__part1.csv`, `__part2.csv`, ...) to the sibling `Query_Results` folder; it needs the
  repo's `.venv` (`requirements.txt`) and `gcloud auth application-default login`. If a query fails, every
  task stops with the error and writes nothing.
- **`bq` command line:** pipe the file on stdin, because leading `--` comment lines break `bq` when the SQL is
  passed as an argument:

  ```bash
  bq query --use_legacy_sql=false --format=csv --max_rows=100000 < tests/sync/SYT29_record_type_mismatch.sql
  ```

---

## Outputs

Results are never committed. Write them to the sibling folder `D:\Xentity\BLM\BLM_DQIMP_GitRepo\Query_Results\`
(outside this repo); `.gitignore` also blocks spreadsheets and result CSVs as a second layer. Client
spreadsheets are new files named with the snapshot date (`<name>_<YYYYMMDD>.xlsx`), never overwritten.

---

## Adding a test

1. Pick the type, which sets the folder and ID prefix. Take the next free ID from Test Catalog v2.
2. Name the file `<ID with underscore>_<description>.sql` and add the standard header.
3. Follow S1-S7.
4. Validate by running it on a snapshot and checking the result against a known answer; record that in
   `Validated`.
5. If it replaces a query already in the repo (an upgraded, corrected or renamed version), delete the old file
   in the same pull request and name it in the header's `Replaces` line. Git history keeps the old version.
6. Add or update the test's row in Test Catalog v2 (with its repo path) and in the folder README's table.
7. Commit on a branch and open a pull request.
