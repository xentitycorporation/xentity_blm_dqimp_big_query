# DQIMP Web Experience tables

> **Scope.** These instructions are for this folder opened on its own in VS Code, where the dashboard update agent runs. That is the `web-app\` folder of a Git clone of the repo, or an older deployed copy of it. In a session opened at the repo root, this file is background only: the guard below is not running there, and the repo's own `.claude\` settings apply.

This folder builds the four tables behind the **MLRS Case Completeness** dashboard. The dashboard is embedded in the "BLM Land Records Data Quality Viewer" Web Experience in the BLM HQ Portal, and the tables are published there.
The monthly update is the `/dashboard-update` skill (`.claude\skills\dashboard-update\SKILL.md`).

## Layout
- `tests\dqimp_webapp.py`: `build` (BigQuery → CSVs), `publish` (overwrite hosted tables in place), `display`.
- `tests\dashboard_agent.py` and `tests\run_dashboard.bat`: the agent's steps and the wrapper that runs them.
- `tests\guard_pins.json`: fingerprints of every script, SQL file and guard file. The person writes it by hand with `tests\run_dashboard.bat pin`.
- `tests\webapp_config.json`: the snapshot list and the Portal item IDs. The agent adds each month here (`next --add`), and the person commits that change.
- `tests\mlrs_case_completeness\`: the five SQL files.
- The output folder, printed as `OUT <path>` by every step: CSVs, `build_manifest.json`, agent reports, logs, history.
  - In a Git clone: `Query_Results\web-app\mlrs_case_completeness\` in the folder that holds the repo. It is outside the repo, so the CSVs never reach Git.
  - In a deployed copy: `Query_Results\mlrs_case_completeness\` next to `tests\`.
  - Outputs never go in `tests\`.

## Environment
- ArcGIS Pro 3.5.5. **No admin rights.**
- The Python environment is `arcgispro_clone`. In a terminal, activate it with `activate arcgispro_clone` from the ArcGIS Python Command Prompt; `conda activate` needs admin. The wrapper does this itself.
- Portal: `https://hqgis.blm.gov/portal/`, folder *BLM DQIMP Prototype*.
  - Sign-in is `GIS("pro")`: ArcGIS Pro's own sign-in session. If it has expired, the person signs in again in Pro. Never enter or ask for a password or PIN.
- BigQuery project `xentity-sandbox-huy`, dataset `blm_seta_dqimp`. Sign-in is gcloud application-default credentials.
- The network re-signs HTTPS. The scripts already trust the Windows certificate store. Never turn certificate checking off (`verify_cert=False`, `--no-verify`, etc.).
- The HQ Portal stores column names in lower case (`subject_name`). Compare field names case-insensitively.

## What Claude may do in this folder
This folder exists for one job, the dashboard update. A guard hook (`.claude\hooks\dashboard_guard.py`) refuses every tool call except:
- the wrapper `tests\run_dashboard.bat` with one workflow step;
- reading the agent's own reports and logs in the output folder;
- writing `Dashboard_Update_Report_<YYYYMMDD>.md` there.

Everything else is refused: other commands, the CSVs, the scripts, other files or folders, the web, connectors and subagents. The script refuses to run if any pinned file has changed. A refusal is final: don't look for another way; tell the person.

## Rules
- Never create or delete Portal items. `publish --create` makes new item IDs, which disconnects the dashboard.
- Test IDs (AT…, STT…, SYT…, WEB…) are internal. They never go into a published table, a dashboard or a report. Use the plain check names.
- Call the monthly snapshots the **Public Extracts**. Report what the data shows. Don't propose fixes to BLM's data, and don't label records as errors.
- Changes to the scripts, SQL or settings are made in the repo. The person brings them to this computer with `git pull` and re-pins them. Don't run `git`: pulling, committing and pinning are the person's.
