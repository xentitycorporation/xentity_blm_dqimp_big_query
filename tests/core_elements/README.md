# Core Elements tests (`CE`)

**The most basic database checks.** Are all cases present in MLRS? In NLSDB? Do the case counts match between
the two? Are the fields that should be filled actually filled? Are a field's values within its allowed list?
Are table keys unique?

Core Elements tests are generic: the same check applies to any table or field, across the whole population,
and usually produces counts. They answer "is the database structurally sound?". Tests that judge records
against a business rule and list the cases to fix are Attribute tests ([`../attribute/`](../attribute/README.md)).

Test Catalog v2 groups the Core Elements tests by root:

| Root | Checks |
|---|---|
| `CE01` | Counts (cases, by status, by Case Type Group) |
| `CE02`-`CE04` | Simple lists used as building blocks |
| `CE05` | Case presence: is every case in both systems |
| `CE06` | Completeness: are the fields that should be filled, filled |
| `CE07` | Valid values: are values within the allowed list |
| `CE08` | Unique keys |

## Files

These four predate the standard header and the shared conventions in
[`../README_newTests.md`](../README_newTests.md); their file names do not yet follow the ID naming. They will be
renamed and brought up to standard when next changed.

| File | What it does | Catalog ID |
|---|---|---|
| `count of cases.sql` | Number of distinct serial numbers in MLRS (all record types; cases with no serial are not counted) | `CE01` |
| `list of cases.sql` | MLRS serial numbers, with the total | `CE02` |
| `cases grouped by status.sql` | MLRS case counts by disposition (may overlap `CE01.1`) | `CE03` |
| `list serial numbers in a product type code.sql` | Serial numbers of the cases carrying one product code | `CE04` |

**`CE04` returns no rows as written.** It compares the product code to `'8518'`, but codes are stored as
6-digit zero-padded text (`'008518'`). Compare as a number (`SAFE_CAST(... AS INT64) = 8518`) or use the padded
code.

The other Core Elements tests in the catalog (`CE01.1` to `CE08.2`) are to be added here, including the count
queries now in [`../uid_trace/`](../uid_trace/README.md).
