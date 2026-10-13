# Attribute tests (`AT`)

**Missing or incorrect information in one system, on a single snapshot**: for example cases missing a customer,
missing case lands, or NLSDB cases with no BLM Product. The same subject followed across snapshots is a
Serial Trace test in [`../serial_trace/`](../serial_trace/README.md); the two share subject numbers
(06 case land, 13 customer, 14 case action, 20 serial number).

ID rule, standard header and shared conventions S1-S6: see [`../README_newTests.md`](../README_newTests.md).

## Tests

| ID | File | What it lists | Output |
|---|---|---|---|
| `AT06.1` | [`AT06_1_cases_missing_case_land.sql`](AT06_1_cases_missing_case_land.sql) | MLRS cases with no case land record, split ACTIVE or AUTHORIZED / all other dispositions | `MLRS_Cases_Missing_Case_Lands_<snapshot_date>.xlsx`, two sheets |
| `AT13.1` | [`AT13_1_cases_missing_customer.sql`](AT13_1_cases_missing_customer.sql) | MLRS cases with no customer record, with the same split; same columns as `AT06.1`, so the lists can be compared | `MLRS_Cases_Missing_Customer_<snapshot_date>.xlsx`, two sheets |
| `AT14.1` | [`AT14_1_cases_missing_case_action.sql`](AT14_1_cases_missing_case_action.sql) | MLRS cases with no case action, at CLOSED, INTERIM, AUTHORIZED or ACTIVE | `MLRS_Cases_Missing_CaseAction_<snapshot_date>.xlsx` |
| `AT20` | [`AT20_null_serial_pending.sql`](AT20_null_serial_pending.sql) | MLRS cases with no serial number at PENDING or `-` | `MLRS_Null_Serial_Pending_Dispositions_<snapshot_date>.xlsx` |
| `AT20.1` | [`AT20_1_null_serial_active_authorized_closed_interim.sql`](AT20_1_null_serial_active_authorized_closed_interim.sql) | MLRS cases with no serial number at ACTIVE, AUTHORIZED, CLOSED or INTERIM; same columns as `AT20` plus Admin State | `MLRS_Null_Serial_Active_Authorized_Closed_Interim_<snapshot_date>.xlsx` |
| `AT21` | [`AT21_nlsdb_cases_no_blm_product.sql`](AT21_nlsdb_cases_no_blm_product.sql) | NLSDB cases whose `CSE_TYPE_NR` has no matching `CASE_TYPE_CODE` in `blm_product` (NLSDB-only, so S1 does not apply) | `NLSDB_Cases_No_BLM_Product_Code_<snapshot_date>.xlsx` |

The dispositions `AT20` and `AT20.1` flag are a parameter (`flagged_dispositions`), because the client's rule for
which dispositions should already have a serial number has changed before.
