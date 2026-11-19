---
name: monthly-build
description: Run the BLM DQIMP monthly snapshot build end to end — fetch the newest MLRS/NLSDB delivery from the shared folders, run the layout checks, upload to GCS, load BigQuery, verify against the previous snapshot, and write a build report. Use when asked to run, do or continue the monthly build or load a new delivery, or on /monthly-build.
argument-hint: "[YYYYMMDD of the MLRS Full zip, optional]"
---

# Monthly build agent

You run the monthly build that `build/README_build.md` documents, from start to finish, without
asking the person who started you anything **unless a rule below says to stop**. They expect to
type `/monthly-build` and come back to a finished load and a report.

The deterministic work stays in the existing scripts. You run them through one wrapper, read
what they print, decide what happens next, and write the report. You do not change the scripts,
the schema files or BLM's data to get past a problem: you stop and explain.

## How to run each step

Every step is one call to the wrapper, from the repo root, with the **PowerShell** tool:

```
pwsh -NoProfile -ExecutionPolicy Bypass -File build/run_build.ps1 -Step <step> [-Date YYYYMMDD] [-CheckOnly]
```

- It activates the `dev` conda env, writes a log (its path is the first line printed) and exits
  with the underlying script's exit code. Read the log when the printed output is not enough.
- **fetch, prepare and load are long** (copying ~7.5 GB from Google Drive; tar extraction and
  layout checks 1–3 hours; BigQuery loads 30–60 minutes). Run them with `run_in_background`
  and wait for the completion notice. Do not poll and do not sleep.
- Machine paths come from `build/monthly_build.local.json` (git-ignored). If it is missing,
  stop and say: copy `build/monthly_build.example.json` to that name and fill it in.

Exit codes: 0 OK · 1 error · 2 layout change · 3 nothing new · 4 disk space · 5 needs a person ·
6 verify flags.

## The steps

Track the steps with your task list. Note each step's start time, end time, exit code and log
path for the report.

### 1. Preflight — `-Step preflight`
- Exit 0: continue. Note the `INFO` line (branch, uncommitted changes).
- If the branch is not `main`, or there are uncommitted changes under `build/` or `schemas/`,
  **stop and ask** whether to build with those scripts. The repo's `main` is the tested version.
- `gcloud is signed in` / `bq` / `gsutil` FAIL: **stop**. Ask the person to run
  `gcloud auth login` in a terminal (it needs their browser), then re-run preflight.
- Any other FAIL: stop and report which check failed.

### 2. Fetch — `-Step fetch` (add `-Date` only if the person named a delivery)
- Exit 3: nothing new to build. Report the delivery list it printed and finish. No report file.
- Exit 4: not enough disk space. Stop and report the numbers.
- Exit 5: **stop**. Show the `REVIEW` lines verbatim and ask what to do. (Examples: a state's tar
  missing, an NLSDB zip dated before the MLRS one, a snapshot already in BigQuery.)
- Exit 0: read `delivery_<date>.json` in the month folder it printed. `snapshot_date` is the date
  for every later step. Any `notes` go in the report.

### 3. Layout checks without upload — `-Step prepare -Date <d> -CheckOnly` (background)
- Exit 2 — **BLM changed a layout. Stop.** Quote the `STOPPED` block. Then read the "Layout
  Change Log" section of `build/README_build.md` and say whether this change is one it already
  expects (with the steps it lists) or a new one. Do not edit any schema file: changing the
  expected layout is the person's decision, made only once the delivery confirms the change.
  If they then ask you to make the documented change, follow the change log's steps exactly,
  show the diff, and re-run this step.
- Exit 1: find the `ERROR` line in the log and report it with what the README says about it.
- Exit 0: search the log for `wrong column count`. Those rows are almost always a literal `|`
  inside a text field (README: "Fixing embedded pipe errors"; it recurs in
  `CR_FULL_ACCOUNT_RPT_WY.load`). The load uses `--max_bad_records=0`, so that table would fail.
  **Stop and ask** before touching the file: show the file, the line number, the field count, and
  the line itself with the extra `|` marked. Never edit a delivered file without a yes.

### 4. Upload — `-Step prepare -Date <d>` (background)
- Re-runs the same checks (finished steps are skipped, so this is quick before the upload) and
  uploads to `gs://sandbox-blm-seta-dqimp-qaqc/snapshots/<d>/`.
- Must exit 0. Exit 1 after an upload failure: re-run this step once (uploads are repeated).
  If it fails again, stop and report.

### 5. Load BigQuery — first `-Step load -Date <d> -CheckOnly`, then `-Step load -Date <d>` (background)
- The check-only pass runs the PS1's pre-flight (GCS folders, Parquet layout as BigQuery reads
  it) and loads nothing. Exit 2: layout change, handle as in step 3. Exit 1: report.
- The full load replaces every `<table>_<d>` table. `SKIP: bq load MC <table>` lines are normal
  (tables with no mining-claims rows). Exit 1 means at least one `WARNING`/`ERROR` step: read
  the `BigQuery error` lines above each one. If the error is transient (backend error, rate
  limit, timeout), re-run the full load once; it is safe to repeat. Otherwise stop and report.

### 6. Verify — `-Step verify -Date <d>`
- Writes `verify_<d>.md` in the month folder. Exit 0: no flags. Exit 6: flags.
- For each flag, find out what changed with small read-only queries (`bq query`, standard SQL),
  e.g. which codes are new in a lookup table, or which states a row-count drop is in. Select
  only the columns you need, and never `SELECT *` on the large tables (`case_action`,
  `admin_unit`, `case_land`, `blm_case`, the NLSDB tables). Report what the data shows, with the
  query. A flag is a prompt to look, not a finding: do not call anything an error.

### 7. Report
Write `Build_Report_<d>.md` in the month folder (see the template below), then tell the person in
a few lines: loaded or not, where the report is, and anything that needs them.

## Always stop and ask before
- changing any file under `schemas/` or `build/`, or any delivered BLM file;
- building a snapshot that is already in BigQuery (fetch reports it; `--allow-rebuild` only on a yes);
- deleting anything in GCS or BigQuery. **GCS cleanup (README Step 6) is never part of this
  run**: the `.load` files are kept until the person decides to remove them;
- `git commit`, `git push`, or switching branches;
- anything not in this runbook.

## Report template — `Build_Report_<d>.md`

```
# Monthly build report — snapshot <d>

**Result:** <Loaded / Stopped at step N: reason>. Built <date/time> from branch <branch>.

## Delivery
| Zip | Shared-folder file | Size | Tar dates / GDB |
(Full, MC, NLSDB rows from delivery_<d>.json) — plus any notes (label vs tar date, pairing).

## Steps
| Step | Exit | Started | Took | Log |
(preflight, fetch, prepare check-only, prepare upload, load check-only, load, verify)

## Layout checks
What passed; any layout change or wrong-column-count rows, quoted.

## Load
Tables loaded; SKIP lines; any WARNING/ERROR with the BigQuery message; retries.

## Comparison with <prior>
(paste verify_<d>.md tables)

### What the flags show
For each flag: what changed, the evidence (counts, values), and the query used.

## Needs your decision
Numbered list, or "Nothing."  Always include: GCS cleanup of snapshots/<d>/ .load files has not
been run (README Step 6).
```

Writing rules for the report: these are the Public Extracts that BLM delivers — call them that.
Report what the data shows; do not propose fixes to BLM's data and do not label records as errors
or duplicates. Never quote values from the NLSDB `CreatedBy` / `ModifiedBy` columns (they hold
people's user names).
