# DQIMP Web Experience tables (BLM computer)

> **Scope.** These instructions are for this folder deployed and opened on its own on the BLM computer, where the dashboard update agent runs. The source is the Git repo's `web-app\` folder. In a session opened at the repo root, this file is background only: the guard below is not running there, and the repo's own `.claude\` settings apply.

This folder builds the four tables behind the **MLRS Case Completeness** dashboard. The dashboard is embedded in the "BLM Land Records Data Quality Viewer" Web Experience in the BLM HQ Portal, and the tables are published there.
The monthly update is the `/dashboard-update` skill (`.claude\skills\dashboard-update\SKILL.md`).

## Layout
- `tests\dqimp_webapp.py`: `build` (BigQuery → CSVs), `publish` (overwrite hosted tables in place), `display`.
- `tests\dashboard_agent.py` and `tests\run_dashboard.bat`: the agent's steps and the wrapper that runs them.
- `tests\guard_pins.json`: fingerprints of every script, SQL file and guard file. The person writes it by hand with `tests\run_dashboard.bat pin`.
- `tests\webapp_config.json`: the snapshot list and the Portal item IDs. **This copy is the live snapshot list**: the agent adds each month here, and the person copies it back to the repo.
- `tests\mlrs_case_completeness\`: the five SQL files.
- `Query_Results\mlrs_case_completeness\`: CSVs, `build_manifest.json`, agent reports, logs, history. Outputs never go in `tests\`.

## This machine
- ArcGIS Pro 3.5.5. **No admin rights.**
- The Python environment is `arcgispro_clone`. In a terminal, activate it with `activate arcgispro_clone` from the ArcGIS Python Command Prompt; `conda activate` needs admin. The wrapper does this itself.
- Portal: `https://hqgis.blm.gov/portal/`, folder *BLM DQIMP Prototype*.
  - Sign-in is `GIS("pro")`: ArcGIS Pro's own PIV-card session. If it has expired, the person signs in again in Pro. Never enter or ask for a PIN or password.
- BigQuery project `xentity-sandbox-huy`, dataset `blm_seta_dqimp`. Sign-in is gcloud application-default credentials.
- The DOI network re-signs HTTPS. The scripts already trust the Windows certificate store. Never turn certificate checking off (`verify_cert=False`, `--no-verify`, etc.).
- The HQ Portal stores column names in lower case (`subject_name`). Compare field names case-insensitively.

## What Claude may do in this folder
This folder exists for one job, the dashboard update. A guard hook (`.claude\hooks\dashboard_guard.py`) refuses every tool call except:
- the wrapper `tests\run_dashboard.bat` with one workflow step;
- reading the agent's own reports and logs in `Query_Results\mlrs_case_completeness\`;
- writing `Dashboard_Update_Report_<YYYYMMDD>.md` there.

Everything else is refused: other commands, the CSVs, the scripts, other files or folders, the web, connectors and subagents. The script refuses to run if any pinned file has changed. A refusal is final: don't look for another way; tell the person.

## Rules
- Never create or delete Portal items. `publish --create` makes new item IDs, which disconnects the dashboard.
- Test IDs (AT…, STT…, SYT…, WEB…) are internal. They never go into a published table, a dashboard or a report. Use the plain check names.
- Call the monthly snapshots the **Public Extracts**. Report what the data shows. Don't propose fixes to BLM's data, and don't label records as errors.
- This folder is a deployed copy, not a Git checkout. Changes are made in the repo, copied here and re-pinned by the person. Don't run `git`.
