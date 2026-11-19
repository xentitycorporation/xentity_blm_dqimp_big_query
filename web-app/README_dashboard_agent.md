# Dashboard update agent

The agent updates the MLRS Case Completeness dashboard with each new monthly snapshot. You type `/dashboard-update` in Claude Code (VS Code) and it:
1. finds the snapshot;
2. builds the four tables;
3. compares them with last month;
4. overwrites the hosted tables in place;
5. checks them in the Portal;
6. writes a report.

The runbook is `.claude\skills\dashboard-update\SKILL.md`.

## Where it runs

It runs on the BLM computer, from the `web-app\` folder of a Git clone of this repo, the same way the build agent runs from `build\` on your laptop.

| What | Where (example: the clone is in `%USERPROFILE%\BLM_DQIMP_GitRepo\`) |
|---|---|
| Folder you open in VS Code | `%USERPROFILE%\BLM_DQIMP_GitRepo\xentity_blm_dqimp_big_query\web-app` |
| Outputs: CSVs, `compare_<d>.md`, `verify_<d>.md`, the report, logs | `%USERPROFILE%\BLM_DQIMP_GitRepo\Query_Results\web-app\mlrs_case_completeness\` |

The outputs go **beside the repo, not in it**, as they do on your laptop, so the case-level CSVs can never be committed. Every step prints the folder as `OUT <path>`.

The guard lets the agent read only that one folder. The script checks before every step that it writes to the same folder the guard allows.

The older way, a deployed copy with `Query_Results\` next to `tests\` (`DQIMP_BLM_WebApp`), still works, but it needs files copied over by hand after every change. Moving from it is in "Setup", step 2.

## Each month

1. **Wait for the monthly build** to load the new snapshot into BigQuery (`/monthly-build` on your laptop).
2. **Bring the clone up to date.** In a terminal in the clone: `git switch main`, then `git pull`.
   - If the pull changed any of the agent's files, run `tests\run_dashboard.bat pin` from `web-app\`, in a plain Command Prompt. Until you do, every step refuses to run (see "When the code changes").
3. **Sign ArcGIS Pro in** to the HQ Portal with your PIV card, and leave it as Pro's active portal.
4. **Open the folder** in VS Code: File → Open Folder → the clone's `web-app` folder.
   - If VS Code offers to open the Git repository in the parent folder, say yes.
5. **Start the agent.** Open the Claude Code panel, check the mode reads **Manual**, and type `/dashboard-update`.
   - Add a date (`/dashboard-update 20261206`) only to pick a specific snapshot.
   - Asking in your own words doesn't start it: the skill runs only when the command is typed.
6. **What it does:** a guard check, then preflight, the snapshot pick, build (about 3 minutes), compare and a dry run.
   - While `ask_before_publish` is true, it then stops with a summary and asks you before publishing.
   - After your yes it publishes (10–30 minutes), verifies and writes `Dashboard_Update_Report_<d>.md` in the output folder.
7. **It stops for you only when it needs you:** a Portal or BigQuery sign-in, two new months at once, earlier months that changed, a table without its Portal item, or a failed check. Each stop says what to do.
8. **Check the dashboard** shows `<d>` as the latest snapshot.
9. **Commit the snapshot list.** The agent added the month to `web-app\tests\webapp_config.json`, so Git shows that file as changed. Commit it on a branch and merge it with a pull request, as with any other change (see "The snapshot list").

## When the code changes

The repo is the master for the agent's code, SQL and settings. After a change is merged:
1. `git pull` in the clone on the BLM computer.
2. `tests\run_dashboard.bat pin` from `web-app\`, in a plain Command Prompt.
3. If the change touched `.claude\` (settings, hook or skill), repeat the guard checks in "Setup", step 6.

`pin` records a fingerprint of every script, the SQL, the config (all but the snapshot list), the skill, `CLAUDE.md`, the settings and the hook. From then on **every step refuses to run if any of them changes**, so pulled code never runs until you've pinned it. The agent can't run `pin`. That includes setting `ask_before_publish` to false.

## How it is fenced in

It runs on a BLM computer, so it is limited to exactly what the update needs. There are three layers, and each holds without the others.

| Layer | What it does | Holds even if… |
|---|---|---|
| **Guard hook** (`.claude\hooks\dashboard_guard.py`, runs before every tool call) | Allowlist. Allows only:<br>• the wrapper with one of 10 fixed step forms;<br>• reading the agent's own reports and logs in the output folder;<br>• writing its own report file there.<br>Refuses everything else: other commands, the CSVs, the scripts, every other file and folder, the web, connectors, subagents, stopping a running step. Set to block if the hook itself fails. Logs every decision to `agent_logs\guard_<date>.log` in the output folder. | …a permission prompt is clicked "always allow" or a permission mode is changed: a hook refusal comes before the permission rules. |
| **Permission rules** (`.claude\settings.json`, plus `.claude\settings.local.json` on the BLM computer) | Deny rules for the dangerous ones (`--create`, `pin`, `display`, direct `dqimp_webapp`, `git`, edits to `tests\`/`.claude\`/`CLAUDE.md`, any CSV, your `.claude`, `AppData` and gcloud credentials, web, subagents, connectors). Bypass mode and auto mode are disabled. Claude's file tools can't read outside the open folder and the output folder (`blockReadsOutsideWorkingDirectories`). | …the hook doesn't run. A deny in this file can't be overridden by an allow anywhere else. |
| **The script itself** (`tests\dashboard_agent.py`) | Every step first checks:<br>• the files are exactly as you pinned them;<br>• the config still points at `hqgis.blm.gov`, the four dashboard items and `xentity-sandbox-huy` (written into the code);<br>• it writes to the same folder the guard allows;<br>• no settings file switches hooks off.<br>Before publishing it signs in and checks that each item is the expected hosted table, owned by you. It never uses `--create`. | …Claude Code's settings are changed: a tampered or redirected setup refuses to run. |

Plus a self-test: before step 1 the agent must try to read `guard_canary.txt`, and the guard must refuse it. If the read works, the guard is off and the agent stops.

## Setup on the BLM computer (once)

### 0. Before you start
- **Check that using Claude Code with your personal Claude Pro subscription is allowed on this government computer** (DOI policy and the contract).
  - The agent only ever sees aggregate counts and its own logs. It is barred from the case-level CSVs.
  - The computer and the Portal session are still BLM's.
- **Claude Code must be version 2.1.295 or later.** The setting that makes a failed guard block calls needs it.
  - Check the VS Code extension's version, or run `claude --version`.
  - On an older version, a guard that fails to start lets calls through to the permission rules (still strict, but one layer down).
- **Committing from this computer** means signing in to GitHub on it. Check that this is allowed too. If it isn't, see "The snapshot list".

### 1. Clone the repo
In a plain Command Prompt:
```
mkdir %USERPROFILE%\BLM_DQIMP_GitRepo
cd %USERPROFILE%\BLM_DQIMP_GitRepo
git clone https://github.com/xentitycorporation/xentity_blm_dqimp_big_query.git
mkdir Query_Results\web-app\mlrs_case_completeness
```
- **Not in OneDrive** (Documents and Desktop are synced): OneDrive syncing a `.git` folder causes conflicts.
- Git for Windows is already installed: the guard runs through its Git Bash.
- If the clone fails with a certificate error (the DOI network re-signs HTTPS), run `git config --global http.sslBackend schannel` and try again.

### 2. Bring over last month's outputs
Copy everything in the deployed folder's `Query_Results\mlrs_case_completeness\` into `%USERPROFILE%\BLM_DQIMP_GitRepo\Query_Results\web-app\mlrs_case_completeness\`.

`compare` needs last month's `build_manifest.json` to check that earlier months haven't changed. Without it, the first run has nothing to compare against.

Leave the deployed folder as it is until a run from the clone has worked. Don't open it in VS Code after that.

### 3. Let Claude Code read the output folder
The output folder is outside the folder VS Code has open, so Claude Code must be told it may read it.

Create `web-app\.claude\settings.local.json` (git-ignored, this computer only) with your own path in both places:
```json
{ "permissions": {
    "additionalDirectories": [
      "C:\\Users\\<you>\\BLM_DQIMP_GitRepo\\Query_Results\\web-app\\mlrs_case_completeness" ],
    "allow": [
      "Edit(//c/Users/<you>/BLM_DQIMP_GitRepo/Query_Results/web-app/mlrs_case_completeness/Dashboard_Update_Report_*.md)" ] } }
```
- The first entry lets Claude read that one folder. The guard still limits it to the reports and logs in it.
- The second lets the agent write its report there without asking you.
- Preflight checks the first entry. If it is missing, preflight stops and prints the exact text to use.

### 4. Pin the files (you, by hand)
In a plain Command Prompt in the clone's `web-app\` folder:
```
tests\run_dashboard.bat pin
```
It prints a `PINNED` line per file, then `WROTE …guard_pins.json` (git-ignored).

### 5. Test by hand
```
tests\run_dashboard.bat preflight
tests\run_dashboard.bat next
```
- `preflight` must print `OUT` with the output folder, `PASS guard: …`, then PASS for:
  - the environment;
  - the snapshot list;
  - the item IDs;
  - the SQL;
  - the output folder, and Claude Code may read it;
  - BigQuery;
  - the Portal sign-in;
  - the Portal items.
- `next` must print `PICK` with the next month, or exit 3 if that month isn't in BigQuery yet.
- If the wrapper can't find ArcGIS Pro: `set DQIMP_PRO_DIR=<ArcGIS Pro folder>\`.

### 6. Test Claude Code and the guard
1. Open the clone's `web-app` folder in VS Code (**File → Open Folder**) and open the Claude Code panel.
   - The first time, Claude Code asks you to trust the folder. Accept.
   - If it fails with a certificate error, set a user environment variable `NODE_EXTRA_CA_CERTS=<the DOI root .pem>` (Start → "Edit environment variables for your account") and restart VS Code.
2. Check that the guard refuses things. Ask Claude, one at a time:
   - "run `dir`";
   - "read `tests\webapp_config.json`";
   - "read `guard_canary.txt`";
   - "fetch https://example.com".

   Each must be refused with a message starting **Dashboard guard** (or, for the fetch, a permission denial).
3. Check that it can read what it needs. Ask Claude: "read the newest preflight log in `<output folder>\agent_logs`". It must show the log. If it is refused, step 3 isn't in place, and the agent would stop at its first log.
4. Open `agent_logs\guard_<today>.log` in the output folder. Each refused attempt should be listed as `DENY`.
5. If any of the refusals didn't happen, the guard isn't running. Don't run the agent until that's found and fixed.

### 7. First runs
1. Run "Each month" from step 3.
2. With `ask_before_publish: true`, it stops after the dry run with a summary and asks you before publishing.
3. After two good runs, set `"ask_before_publish": false` in `tests\dashboard_agent.json`, then run `tests\run_dashboard.bat pin` again. That is a change to a tracked file, so commit it too.

## The snapshot list
`next --add` adds each month to `web-app\tests\webapp_config.json` in the clone. Commit it after the run, one of two ways:
- **From the BLM computer** (needs GitHub sign-in there): create a branch, commit the file, push, open a pull request and merge it. Then `git switch main` and `git pull`.
- **From your laptop:** copy the file to the laptop repo's `web-app\tests\`, and commit and merge it there. Then, on the BLM computer, discard the local change (`git restore web-app/tests/webapp_config.json`) and `git pull`, which brings the same file back from `main`.

`pin` ignores the snapshot list, so neither way breaks the pins. Commit it before the next run, so the clone is clean when you pull.

## While using it
- **Don't click "always allow"** on a prompt for anything other than the wrapper. The guard still refuses, but there's no reason to widen the rules.
- **Don't switch the conversation to bypass or auto mode.** Both are disabled here anyway.
- **Don't open the repo root** to run the agent. Its settings apply only when `web-app\` is the open folder; at the root, the agent's first step stops it.
- **Optional, strongest (Claude Code 2.1.319 or later only):** in your user settings (`%USERPROFILE%\.claude\settings.json`), add `"permissions": {"denyInsteadOfPrompting": true}`.
  - Any call that would have shown you a prompt is then refused outright.
  - It applies to every Claude Code conversation on this computer.
  - A repository can't set it for you. On an older version the setting does nothing, so check the version first.
- **Audit:** `agent_logs\guard_<date>.log` in the output folder lists every tool call the agent tried, and whether it was allowed.
