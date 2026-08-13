# Case Type Group / Subgroup Lookup Table

Everything needed to load and validate
`xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup` — the table that maps
every BLM Product Code to a Case Type Group and Subgroup.

Case Type Group / Subgroup is **the client's primary reporting dimension**: essentially every DQ
result is broken out by it. When this table is wrong, every downstream report is quietly wrong
with it.

## Files

| File | Purpose |
|---|---|
| `Product Code with Case Type Subgroups.csv` | **The authoritative taxonomy.** 966 codes. What BigQuery loads from |
| `Product_Code_Case_Type_Group_Subgroup_schema.json` | Explicit BigQuery schema — **all four columns STRING** |
| `reload_product_code_lookup.ps1` | Backs up the live table, reloads from the CSV, verifies |
| `verify_case_type_group_coverage.py` | Reconciles the taxonomy against `blm_product` **and** the subgroup queries in `schemas/case_type_group_queries/` |

## This CSV is authoritative — not the .xlsx

An `.xlsx` version exists in Mark's local working files. **It is the SME-facing editing surface, not
the source of truth.** The committed CSV here is what BigQuery loads and what any consumer should
treat as correct.

**Update workflow** (Mark, 2026-08-11):

1. A monthly snapshot turns up a new BLM Product Code (the validator reports it — check 5).
2. Add the code to the appropriate Group / Subgroup **in the local `.xlsx`**.
   Group / Subgroup assignment is a **BLM SME decision**, never inferred from data agreement alone.
3. Export a fresh `.csv` from that `.xlsx`.
4. Replace the CSV **in this folder**, commit, and open a PR — so taxonomy changes arrive as a
   reviewable line diff. This is precisely what the `.xlsx` can never give you.
5. Run `reload_product_code_lookup.ps1` to push it to BigQuery.
6. Run `verify_case_type_group_coverage.py` to confirm.

## ⚠ The trap: always load with the explicit schema

**`BLM Product Code` must be STRING.** 22 of the 966 codes carry leading zeros — `000000`,
`000445`, `007500`, `008500`, … CSV auto-detect types the column as `INTEGER` and strips them:
`'007500'` becomes `7500`.

The join partner, `blm_product.CASE_TYPE_CODE`, is a zero-padded STRING. With an INTEGER lookup the
join either **errors** (`uncomparable types STRING and INT64` — loud and safe) or, if someone
bridges it with `CAST(... AS STRING)`, **silently misses all 22 codes**. **21,432 MLRS cases** sit
on them and drop to Unclassified.

This has regressed **three times** (KB §6.4.1 bug 3; KB §9.1) — twice on 2026-08-11 alone. The
reason it keeps happening is mechanical: these column names contain **spaces**, so an inline
`--schema` string cannot express them, and auto-detect becomes the path of least resistance. Use
the JSON schema file.

**One-line check that the load was correct:**

```sql
SELECT COUNTIF(STARTS_WITH(`BLM Product Code`, '0')) AS leading_zero_codes
FROM `xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup`
```

`22` is right. **`0` means the schema did not apply and the table is wrong.**

## Expected state (2026-08-11)

| Property | Value |
|---|---|
| Rows / distinct codes | 966 / 966 (no duplicates) |
| Case Type Groups | 7 — Fluid Minerals, Land Tenure, Land Transfer, Land Use Authorizations, Mining Claims, Solid Minerals, Survey |
| Case Type Subgroups | 32 |
| Codes with leading zeros | 22 |
| Codes in `blm_product_20260802` missing from the taxonomy | 0 |
| Codes in the taxonomy absent from `blm_product_20260802` | 4 — `311131`, `312081`, `328300`, `360050` |

Those 4 are **correct, not errors.** The taxonomy is deliberately a comprehensive **historical**
list: if a code appeared in any monthly snapshot, it stays, even after retirement. Dropping a code
because it is missing from the current snapshot would break historical reporting.

## Expected validator result

Run from this folder, `verify_case_type_group_coverage.py --snapshot 20260802` should end in:

```
[1] codes claimed by more than one subgroup : 0
[2] codes claimed but absent from taxonomy   : 0
[3] codes in the wrong subgroup              : 0
[4] taxonomy codes no query claims           : 4
      of which LIVE in blm_product (FAIL)    : 0
      of which absent from blm_product (ok)  : 4
        311131, 312081, 328300, 360050
[5] blm_product codes with no taxonomy row   : 0

PASS: all checks clean against blm_product_20260802
```

The 4 under `[4]` are the retired historical codes above — expected, not a fault. **Anything under
`of which LIVE in blm_product (FAIL)` is a real problem**: the taxonomy classifies a code that no
subgroup query claims, so lookup-driven and query-driven reporting will disagree.

### What that check caught on its first run — worth knowing

`380800` was assigned to a brand-new subgroup, `Solid Minerals / General Mining Laws`, and the
validator immediately flagged that nothing claimed it. Closing it took **three** changes, not one —
and the second would not have been found by reading the files:

1. `caseTypeGroup_SM_generalmininglaws.sql` — the new subgroup query.
2. `caseTypeGroup_SM.sql` — the **group-level** query had `380800` sitting in its `NOT IN`
   exclusion list, so even with the subgroup file present, group-level Solid Minerals counts would
   have been short by those 2 cases. Removed and logged in that file's header (KB §5.7.0).
3. This validator's subgroup → query-file map, which did not know the new subgroup.

**A grep is not enough to check this.** `grep 380800 *.sql` returned three files and looked like
double-classification; running them showed `locatables` and `SM.sql` both *excluded* it. Exclusion
lists and change-log comments quote the very codes they remove. **Run the queries — do not read
them.** That is what this script is for.

## Known data quirk

`315100`'s product name contains a Unicode replacement character
(`O&G GEOPHYSICAL EXPLORATION � EXCEPT AK`). It is **pre-existing in the source data**, not
introduced by the load, and it round-trips unchanged. Documented in KB §5.4. Do not "fix" it here —
fix it upstream in the `.xlsx` if it ever matters.

## Related

- `schemas/case_type_group_queries/` — the 43 group/subgroup SQL definitions this table mirrors.
  **The validator checks the two agree**; they can and have drifted apart.
- KB `MLRS_Database_Quality_Checks.md` §5 — the full taxonomy history and every classification
  decision behind it.
