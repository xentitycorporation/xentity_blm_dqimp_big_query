# System Validation tests (`SVT`): under review

The earlier definition of this type was to **track record changes over time to provide feedback on system
functionality and identify O&M stories needed to fix or improve the system.** Tracking attributes over time is
now the Serial Trace type ([`../serial_trace/`](../serial_trace/serial_trace.md)), so this type has not yet
been redefined under the current test types (see [`../README_newTests.md`](../README_newTests.md)).

There are no tests in this folder. Several System Validation queries were replaced by tests in other folders
when they were added to the repo; for example `SVT_CaseDisposition_MLRS_NLSDB_Mismatch` is now `SYT04.1`
in [`../sync/`](../sync/sync.md). Test Catalog v2 records each replacement.
