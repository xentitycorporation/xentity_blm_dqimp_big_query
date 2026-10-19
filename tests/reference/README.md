# Reference queries (`REF`)

**Support queries that other tests rely on:** lookup lists of product codes, product names and record types.
They are not pass/fail tests.

No files are here yet. Test Catalog v2 lists them, to be added from BigQuery:

| Catalog ID | What it produces |
|---|---|
| `REF01` | BLM product codes and names for one snapshot |
| `REF01.1` | Every product code found in any snapshot, in MLRS and NLSDB, with the evidence for its classification |
| `REF01.2` | The same list built from the codes carried by cases, for comparison with `REF01.1` |
| `REF02` | Product codes by Case Type Group and Subgroup for a snapshot; it builds the lookup table whose source CSV is in [`schemas/`](../../schemas/) |
| `REF03` | Record type names |
