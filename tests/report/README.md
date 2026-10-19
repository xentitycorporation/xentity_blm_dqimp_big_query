# Report generators (`SYTR`)

Xentity used to send BLM a monthly data-quality report: data quality over time, month over month, as a PDF.
The queries here generate the data behind those reports. The monthly reports are expected to return.

**A report is assembled from the individual tests; it never re-implements them.** The tests in the type folders
(`sync/`, `attribute/`, and so on) are the master. When a test changes, the report picks up the change because
it runs that test.

## Files

| File | What it does | Catalog ID |
|---|---|---|
| `SYT_Basic.sql` | All 23 case-level Synchronization comparisons as one table: tests x snapshot dates | `SYTR01` |
| `SYT_Basic_CL.sql` | All 17 case-land (`SYTCL`) comparisons as one table | `SYTR02` |

Both predate that principle: each contains its own copy of the SYT or SYTCL logic, so it can drift from the
individual tests. They are to be rebuilt to run the individual tests instead. Test Catalog v2 lists the other
report queries (`SYTR03`-`SYTR07`), which are still in BigQuery.

The report template is in the top-level [`report/`](../../report/README_report.md) folder. Whether the two
folders merge, and where finished PDF reports are kept, are still to be decided.
