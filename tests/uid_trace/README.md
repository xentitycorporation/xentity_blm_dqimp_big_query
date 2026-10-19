# UID Trace tests (`UTT`): on hold

**Definition.** A form of Synchronization that goes deeper than one ID. MLRS links its tables through many
unique IDs (the Salesforce case ID, the serial number, case-land IDs and others, roughly 20 in all). A UID
Trace test asks whether a case in MLRS lines up with the same case in NLSDB across all of them, and where it
does not, why: some cases are reported to be in MLRS but not in NLSDB because they did not sync.

**The type is on hold:** this work has not started, and no tests are added here until it does.

## Files

The five files here predate this definition. They count cases by status or by Case Type Group in one system,
which Test Catalog v2 now classifies as **Core Elements**. They will move to
[`../core_elements/`](../core_elements/README.md), with the standard header, in a later change; until then they
are kept as they are. They predate the shared conventions in [`../README_newTests.md`](../README_newTests.md);
for example, they do not all exclude Bond and Status records (S1).

| File | What it does | Catalog ID |
|---|---|---|
| `MLRS_count_cases_by_status.sql` | MLRS case counts by `CASE_STATUS` | `CE01.1` |
| `NLSDB_count_cases_by_status.sql` | NLSDB case counts by `CSE_DISP` | `CE01.2` |
| `MLRS_count_cases_by_casegroup.sql` | MLRS distinct serial number counts by Case Type Group | `CE01.3` |
| `MLRS_count_id_by_casegroup.sql` | MLRS distinct case ID counts by Case Type Group | `CE01.4` |
| `NLSDB_count_cases_by_casegroup.sql` | NLSDB distinct serial number (`CSE_NR`) counts by Case Type Group | `CE01.5` |

Two serial number lists that were here (`Serial_NR_List_Missing_with_Status.sql` and `Serial_NR_List_Missing_status_csetyp.sql`)
were removed when `SYT30.1` in [`../sync/`](../sync/README.md) replaced them; they remain in the Git history.
