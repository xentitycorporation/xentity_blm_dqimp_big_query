# Synchronization tests (`SYT`)

**A case in MLRS is also in NLSDB, and vice versa, and the same case carries the same value in both systems.**
MLRS cases are matched to NLSDB on `blm_case.ID = nlsdb_case.SF_ID`.

ID rule, standard header and shared conventions S1-S6: see [`../README_newTests.md`](../README_newTests.md).

## Tests

| ID | File | What it lists | Output |
|---|---|---|---|
| `SYT04.1` | [`SYT04_1_case_disposition_mismatch.sql`](SYT04_1_case_disposition_mismatch.sql) | Cases whose MLRS disposition (`CASE_STATUS`) differs from NLSDB (`CSE_DISP`); client list of `SYT04` | `CaseDisposition_MLRS_NLSDB_Mismatch_<snapshot_date>.xlsx` |
| `SYT05.1` | [`SYT05_1_product_code_mismatch.sql`](SYT05_1_product_code_mismatch.sql) | Cases whose MLRS product code (`blm_product.CASE_TYPE_CODE`) differs from NLSDB `CSE_TYPE_NR`, compared as text so zero-padding differences count; client list of `SYT05` | `ProductCode_MLRS_NLSDB_Mismatch_<snapshot_date>.xlsx` |
| `SYT06.1` | [`SYT06_1_serial_mismatch_same_case.sql`](SYT06_1_serial_mismatch_same_case.sql) | The same case carrying a different serial number in each system (`SERIAL_NUMBER__C` vs `CSE_NR`); client list of `SYT06` | `Serial_Number_MLRS_NLSDB_Mismatch_<snapshot_date>.xlsx` |
| `SYT29` | [`SYT29_record_type_mismatch.sql`](SYT29_record_type_mismatch.sql) | Cases whose MLRS record type name differs from the record type prefix of NLSDB `REC_TYPE_CSE_GRP` | `RecordType_MLRS_NLSDB_Mismatch_<snapshot_date>.xlsx` |
| `SYT30` | [`SYT30_serial_counts_by_subgroup.sql`](SYT30_serial_counts_by_subgroup.sql) | Serial number counts in each system, by Case Type Group and Subgroup | `Serial_Number_MLRS_NLSDB_Count_Differences_<snapshot_date>.xlsx`, sheet "Counts by Subgroup" |
| `SYT30.1` | [`SYT30_1_serials_in_one_system_only.sql`](SYT30_1_serials_in_one_system_only.sql) | Serial numbers present in one system but not the other; the rows behind `SYT30` | the same workbook, sheet "Serial Numbers" |

The parent tests `SYT01`-`SYT26` (one per compared field) and the case land tests `SYTCL01`-`SYTCL18` are
counted in [`../report/SYT_Basic.sql`](../report/SYT_Basic.sql) and
[`../report/SYT_Basic_CL.sql`](../report/SYT_Basic_CL.sql).

## Other files in this folder

These predate the standard header and the shared conventions.

| File | What it does |
|---|---|
| `Case_Disposition_by_Case_Type_wLegNew.sql` | Disposition mismatch counts by Case Type Group, split Legacy / Non-Legacy |
| `BLM_Product_by_Case_Type_wLegNew.sql` | The same for BLM product name (`blm_product.NAME` vs `BLM_PROD`) |
| `CSE_TYP_NR_by_Case_Type_wLegNew.sql` | The same for case type number (`CASE_TYPE_CODE` vs `CSE_TYPE_NR`, compared as numbers) |
| `Commodity_by_Case_Type_wLegNew.sql` | The same for commodity (`COMMODITY` vs `CMMDTY`) |
| `query_workspace/new_sync_for_web_app_groups.sql`, `query_workspace/old_sync_for_web_app.sql` | Working queries for the web app's sync views, new and old versions |
