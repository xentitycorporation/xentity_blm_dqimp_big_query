# System Validation tests (`SVT`)

**Is the system itself working properly?** These tests look for problems that point at MLRS or NLSDB as a
system, rather than at individual records. Two kinds:

1. **New vs Legacy.** A data-quality problem that appears in New cases (created in MLRS) but never appeared in
   Legacy cases (migrated from LR2000) points to a system issue rather than a data-entry or migration one. Every
   case-list test carries `Origin` and `Mining_Claim` (shared convention S7 in
   [`../README_newTests.md`](../README_newTests.md)), so any test's results can be compared this way. Keep the
   mining-claim split in view: on the `20261004` snapshot, 95% of New cases are mining claims.
2. **System functions.** A system process produced what it should. Example: the geospatial overlay assigns admin
   units to every case land, and each case land should carry the four core types: **County, District Office,
   Field Office and Surface Management Agency (SMA)**. A case land with fewer than four means the function did
   not work for it. (Some case lands also carry a Native Region.)

These tests matter most when the system or its platform changes: compare the last snapshot before the change
with the first one after it.

## Tests

No tests are in this folder yet. Test Catalog v2 lists the admin-unit queries as `SVT15` to `SVT15.4`, to be
added here with the standard header. Numbering starts at `SVT15` because `SVT01` and `SVT11`-`SVT14` are
retired.

Several queries first filed as "System Validation" were reclassified under the current types and replaced by
tests in other folders; for example `SVT_CaseDisposition_MLRS_NLSDB_Mismatch` is now `SYT04.1` in
[`../sync/`](../sync/README.md). Test Catalog v2 records each one.
