# Dashboard update agent: setup on the BLM computer

The agent updates the MLRS Case Completeness dashboard with each new monthly snapshot. You type `/dashboard-update` in Claude Code (VS Code) and it:
1. finds the snapshot;
2. builds the four tables;
3. compares them with last month;
4. overwrites the hosted tables in place;
5. checks them in the Portal;
6. writes a report.

The runbook is `.claude\skills\dashboard-update\SKILL.md`.

**Where it lives:**
- The source is this repo's `web-app\` folder.
- The agent **runs only in the flat folder on the BLM computer**, where `Query_Results\` sits next to `tests\`. The guard hook allows reads and writes only in `Query_Results\mlrs_case_completeness\` beside `tests\`.
- In a repo checkout the outputs go to `..\Query_Results\web-app\`, so the guard would refuse the agent's own logs and the agent would stop. That is safe, but it doesn't work.
- Copy the files from the repo to the BLM folder as in step 1.

## How it is fenced in

It runs on a BLM computer, so it is limited to exactly what the update needs. There are three layers, and each holds without the others.

| Layer | What it does | Holds even if… |
|---|---|---|
| **Guard hook** (`.claude\hooks\dashboard_guard.py`, runs before every tool call) | Allowlist. Allows only:<br>• the wrapper with one of 10 fixed step forms;<br>• reading the agent's own reports and logs;<br>• writing its own report file.<br>Refuses everything else: other commands, the CSVs, the scripts, files outside `Query_Results\mlrs_case_completeness\`, the web, connectors, subagents, stopping a running step. Set to block if the hook itself fails. Logs every decision to `agent_logs\guard_<date>.log`. | …a permission prompt is clicked "always allow" or a permission mode is changed: a hook refusal comes before the permission rules. |
| **Permission rules** (`.claude\settings.json`) | Deny rules for the dangerous ones (`--create`, `pin`, `display`, direct `dqimp_webapp`, `git`, edits to `tests\`/`.claude\`/`CLAUDE.md`, the CSVs, your `.claude`, `AppData` and gcloud credentials, web, subagents, connectors). Bypass mode and auto mode are disabled. Claude's file tools can't read outside this folder (`blockReadsOutsideWorkingDirectories`). | …the hook doesn't run. A deny in this file can't be overridden by an allow anywhere else. |
| **The script itself** (`tests\dashboard_agent.py`) | Every step first checks:<br>• the files are exactly as you pinned them;<br>• the config still points at `hqgis.blm.gov`, the four dashboard items and `xentity-sandbox-huy` (written into the code);<br>• no settings file switches hooks off.<br>Before publishing it signs in and checks that each item is the expected hosted table, owned by you. It never uses `--create`. | …Claude Code's settings are changed: a tampered or redirected setup refuses to run. |

Plus a self-test: before step 1 the agent must try to read `guard_canary.txt`, and the guard must refuse it. If the read works, the guard is off and the agent stops.

The skill only runs when you type `/dashboard-update`: it sets `disable-model-invocation: true`, so asking Claude in your own words to "update the dashboard" doesn't start it.

## 0. Before you start
- **Check that using Claude Code with your personal Claude Pro subscription is allowed on this government computer** (DOI policy and the contract).
  - The agent only ever sees aggregate counts and its own logs. It is barred from the case-level CSVs.
  - The computer and the Portal session are still BLM's.
- **Claude Code must be version 2.1.295 or later.** The setting that makes a failed guard block calls needs it.
  - Check the VS Code extension's version, or run `claude --version`.
  - On an older version, a guard that fails to start lets calls through to the permission rules (still strict, but one layer down).
- The snapshot must already be in BigQuery from the monthly build.

## 1. Copy the files
Into the deployed folder on the BLM computer, `%USERPROFILE%\OneDrive - DOI\Documents\BLM DQIMP Reporting\DQIMP_BLM_WebApp\`:

| From the repo's `web-app\` folder | To |
|---|---|
| `CLAUDE.md`, `guard_canary.txt` | the folder itself |
| `.claude\settings.json`, `.claude\hooks\dashboard_guard.py`, `.claude\skills\dashboard-update\SKILL.md` | `.claude\…` |
| `tests\dashboard_agent.py`, `tests\dashboard_agent.json`, `tests\run_dashboard.bat` | `tests\` |

**Do not copy over** `tests\webapp_config.json` or `tests\dqimp_webapp.py`; the BLM copies stay. Check that `dqimp_webapp.py` is v7:
```
certutil -hashfile tests\dqimp_webapp.py SHA256
```
It must print `6c8abf8780f9691de1c4a29460d87cf072d3384da5a797d34a6ed1a18e23ea65`.

The guard hook runs with `%LOCALAPPDATA%\ESRI\conda\envs\arcgispro_clone\python.exe` through Git Bash. If either is missing, the hook blocks every call: safe, but nothing runs. Fix the path in `.claude\settings.json`, then run `pin` again.

## 2. Pin the files (you, by hand)
In a plain Command Prompt in the folder:
```
tests\run_dashboard.bat pin
```
- It records a fingerprint of:
  - every script;
  - the SQL;
  - the config (all but the snapshot list);
  - the skill, `CLAUDE.md`, the settings and the hook.
- From then on, **every step refuses to run if any of them changes**. The snapshot list is the exception: the agent may add to it through `next --add`.
- If you change one of those files on purpose, run `pin` again yourself. That includes setting `ask_before_publish` to false. The agent can't run `pin`.

## 3. Test by hand
```
tests\run_dashboard.bat preflight
tests\run_dashboard.bat next
```
- `preflight` must print `PASS guard: …`, then PASS for:
  - the environment;
  - the snapshot list;
  - the item IDs;
  - the SQL;
  - the output folder;
  - BigQuery;
  - the Portal sign-in;
  - the Portal items.
- `next` must print `PICK 20261101` (or the next month).
- If the wrapper can't find ArcGIS Pro: `set DQIMP_PRO_DIR=<ArcGIS Pro folder>\`.

## 4. Test Claude Code and the guard
1. Open the folder in VS Code (**File → Open Folder**) and open the Claude Code panel.
   - If it fails with a certificate error (the DOI network re-signs HTTPS), set a user environment variable `NODE_EXTRA_CA_CERTS=<the DOI root .pem>` (Start → "Edit environment variables for your account") and restart VS Code.
2. Check that the guard refuses things. Ask Claude, one at a time:
   - "run `dir`";
   - "read `tests\webapp_config.json`";
   - "read `guard_canary.txt`";
   - "fetch https://example.com".

   Each must be refused with a message starting **Dashboard guard** (or, for the fetch, a permission denial).
3. Open `Query_Results\mlrs_case_completeness\agent_logs\guard_<today>.log`. Each attempt should be listed as `DENY`.
4. If any of them works, the guard isn't running. Don't run the agent until that's found and fixed.

## 5. First run
1. Make sure ArcGIS Pro is signed in to the HQ Portal (PIV card). Type `/dashboard-update`.
2. With `ask_before_publish: true`, it stops after the dry run with a summary and asks you before publishing.
3. Afterwards, open the dashboard and check the latest snapshot.

After two good runs, set `"ask_before_publish": false` in `tests\dashboard_agent.json`, then run `tests\run_dashboard.bat pin` again.

## While using it
- **Don't click "always allow"** on a prompt for anything other than the wrapper. The guard still refuses, but there's no reason to widen the rules.
- **Don't switch the conversation to bypass or auto mode.** Both are disabled here anyway.
- **Optional, strongest (Claude Code 2.1.319 or later only):** in your user settings (`%USERPROFILE%\.claude\settings.json`), add `"permissions": {"denyInsteadOfPrompting": true}`.
  - Any call that would have shown you a prompt is then refused outright.
  - It applies to every Claude Code conversation on this computer.
  - A repository can't set it for you. On an older version the setting does nothing, so check the version first.
- **Audit:** `agent_logs\guard_<date>.log` lists every tool call the agent tried, and whether it was allowed.

## Keeping the repo's snapshot list current
**The repo is the master for the code.** The live snapshot list is the exception: the agent adds a month to `tests\webapp_config.json` on the BLM computer every run.

After each run, copy that file into the repo's `web-app\tests\` and commit it. `pin` ignores the snapshot list, so this doesn't break the pins.
