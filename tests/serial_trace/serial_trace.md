# Serial Trace tests (`STT`)

**Attributes tracked over time**: the same cases followed across monthly snapshots, to find changes that a
single snapshot cannot show (for example a case that had a customer and no longer does). Missing or
incorrect information on one snapshot is an Attribute test in [`../attribute/`](../attribute/attribute.md);
the two share subject numbers (06 case land, 13 customer, 14 case action, 20 serial number).

Serial Trace tests take a `snapshots` parameter, an ascending array of snapshot dates whose last entry is
the snapshot being reported on.

ID rule, standard header and shared conventions S1-S6: see [`../README_newTests.md`](../README_newTests.md).

## Tests

| ID | File | What it lists | Output |
|---|---|---|---|
| `STT13.6` | [`STT13_6_cases_lost_all_customers.sql`](STT13_6_cases_lost_all_customers.sql) | Real MLRS cases with no customer record in the latest snapshot that had at least one in an earlier snapshot in the window. Cases that lost their customers and later regained one are not listed. | `MLRS_Cases_Lost_Customer_<latest snapshot>.xlsx` |
