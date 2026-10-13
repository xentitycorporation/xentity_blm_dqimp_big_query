# UID Trace tests (`UTT`): on hold

UID Trace tests were defined as **confirming that every unique case in MLRS is present in NLSDB**. That purpose
now overlaps two other types: the Sync presence tests in [`../sync/`](../sync/sync.md) (`SYT30`, `SYT30.1`:
serial numbers in both systems or only one) and the Serial Trace tests in
[`../serial_trace/`](../serial_trace/serial_trace.md) (cases followed over time).

**The type is on hold.** No new tests are added here, and the files below are kept as they are until the
type is redefined or retired. They predate the standard header and the shared conventions in
[`../README_newTests.md`](../README_newTests.md); for example, they do not all exclude Bond and Status records (S1).

## Files

| File | What it does |
|---|---|
| `MLRS_count_cases_by_casegroup.sql` | MLRS distinct serial number counts by Case Type Group |
| `MLRS_count_id_by_casegroup.sql` | MLRS distinct case ID counts by Case Type Group |
| `NLSDB_count_cases_by_casegroup.sql` | NLSDB distinct serial number (`CSE_NR`) counts by Case Type Group |
| `MLRS_count_cases_by_status.sql` | MLRS case counts by `CASE_STATUS` |
| `NLSDB_count_cases_by_status.sql` | NLSDB case counts by `CSE_DISP` |
| `Serial_NR_List_Missing_with_Status.sql` | Serial numbers in one system but not the other, with disposition. Replaced by `SYT30.1` in `../sync/`. |
| `Serial_NR_List_Missing_status_csetyp.sql` | The same list with Case Type Group/Subgroup; an earlier version of what is now `SYT30.1`. |
