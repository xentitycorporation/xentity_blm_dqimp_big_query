---
name: monthly-build
description: Runs the BLM DQIMP monthly snapshot build end to end — fetches the newest MLRS/NLSDB delivery from the shared folders, runs the layout checks, uploads to GCS, loads BigQuery, verifies against the previous snapshot, and writes a build report. Run by the person typing /monthly-build.
argument-hint: "[YYYYMMDD of the MLRS Full zip, optional]"
# Uploads and BigQuery loads change shared data, so only the person starts this skill (by typing /monthly-build).
disable-model-invocation: true
---

# Monthly build agent

You run the monthly build that `README_build.md` (in this folder) documents, from start to finish, without
asking the person who started you anything **unless a rule below says to stop**. They expect to
type `/monthly-build` and come back to a finished load and a report.

The deterministic work stays in the existing scripts. You run them through one wrapper, read
what they print, decide what happens next, and write the report. You do not change the scripts,
the schema files or BLM's data to get past a problem: you stop and explain.

## How to run each step

This agent's home is the repo's `build/` folder: the session must be opened on that folder, not
on the repo root. Every step is one call to the wrapper, from this folder, with the **PowerShell**
tool:

```
pwsh -NoProfile -ExecutionPolicy Bypass -File ./run_build.ps1 -Step <step> [-Date YYYYMMDD] [-CheckOnly]
```

- It activates the `dev` conda env, writes a log (its path is the first line printed) and exits
  with the underlying script's exit code. Read the log when the printed output is not enough.
- **fetch, prepare and load are long** (copying ~7.5 GB from Google Drive; tar extraction and
  layout checks 1–3 hours; BigQuery loads 30–60 minutes). Run them with `run_in_background`
  and wait for the completion notice. Do not poll and do not sleep.
- Machine paths come from `monthly_build.local.json` in this folder (git-ignored). If it is
  missing, stop and say: copy `monthly_build.example.json` to that name and fill it in.
- The schema files are in `../schemas/`, outside this folder.

Exit codes: 0 OK · 1 error · 2 layout change · 3 nothing new · 4 disk space · 5 needs a person ·
6 verify flags.

## The steps

Track the steps with your task list. Note each step's start time, end time, exit code and log
path for the report.

### 0. Check the home and the guard (before anything else)
1. Use **Glob** with the pattern `run_build.ps1` and no path. If it is not found in the folder
   you are working in, this session is not open on the `build/` folder. **Stop** and tell the
   person to open `...\xentity_blm_dqimp_big_query\build` in VS Code (File → Open Folder) and
   start again. Do not run any step from another folder.
2. Run exactly this with the **PowerShell** tool:
   `Write-Output 'build guard self-test: gsutil rm gs://guard-self-test/none'`
   It only prints text, but the build guard refuses any command that names `gsutil rm`.
   **It must be refused with a message starting `Build guard`.** If it runs, or is stopped any
   other way (for example a permission prompt), the guard is not running in this session:
   **stop**, tell the person, and run no step.

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
  Change Log" section of `README_build.md` and say whether this change is one it already
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
- For each flag, find out what changed with small read-only queries, e.g. which codes are new in
  a lookup table, or which states a row-count drop is in. Always run them as
  `bq query --use_legacy_sql=false --maximum_bytes_billed=10000000000 "<SQL>"`: the build guard
  refuses a query without the cost cap, and any statement that changes data or tables. Select
  only the columns you need, and never `SELECT *` on the large tables (`case_action`,
  `admin_unit`, `case_land`, `blm_case`, the NLSDB tables). Report what the data shows, with the
  query. A flag is a prompt to look, not a finding: do not call anything an error.

### 7. Report
Write `Build_Report_<d>.md` in the month folder (see the template below), then tell the person in
a few lines: loaded or not, where the report is, and anything that needs them.

## Always stop and ask before
- changing any file under `../schemas/` or in this `build/` folder, or any delivered BLM file;
- building a snapshot that is already in BigQuery (fetch reports it; `--allow-rebuild` only on a yes);
- deleting anything in GCS or BigQuery. **GCS cleanup (README Step 6) is never part of this
  run**: the `.load` files are kept until the person decides to remove them;
- `git commit`, `git push`, or switching branches;
- anything not in this runbook.

## Guardrails you will run into
A hook (`.claude/hooks/build_guard.ps1` in this folder) checks every command and file edit before
it runs, and `.claude/settings.json` holds the allow / ask / deny rules. When a call is refused, the message
starts with `Build guard` and says why. Do not look for another way to do the same thing: if the
step needs it, stop and ask the person. In particular:
- write files in the data folder only inside the current delivery's month folder, and never
  delete, move or rename anything there or on the shared drive;
- run only the wrapper (`./run_build.ps1`) for build work; Python, nested shells and
  installs ask the person first;
- never change `.claude/settings*.json` or `.claude/hooks/`: those are the person's.

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
