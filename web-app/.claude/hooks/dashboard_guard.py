"""dashboard_guard.py - PreToolUse hook for the dashboard-update agent (see .claude/settings.json).

Claude Code runs this before EVERY tool call Claude makes in this folder. It is an ALLOWLIST:
a call goes ahead only if it is one of the few things the dashboard update needs; everything
else is refused, and Claude is told why. It never answers "allow", so it can only make Claude
Code stricter, never looser: the permission rules in settings.json still apply on top.

What may run:
  * shell (Bash or PowerShell): only the wrapper, one step, in its exact form:
        ./tests/run_dashboard.bat <step>         (Bash)
        & .\\tests\\run_dashboard.bat <step>       (PowerShell)
    <step> = preflight | next | next --add | next --date YYYYMMDD | next --add --date YYYYMMDD
             | build | compare | publish-dry | publish | verify
    No other command, option, chaining, redirect, variable or second line.
  * Read / Grep / Glob: only the agent's own outputs in the output folder OUT (set below): the .md
    reports, build_manifest.json, agent_logs\\*.log, agent_history\\*.json - never the CSVs
    (case-level data), the scripts, the config, or anything else.
  * Write / Edit: only OUT\\Dashboard_Update_Report_<YYYYMMDD>.md.
  * Skill: only dashboard-update. Plus the harmless session tools (task list, asking the person,
    reading a background step's output, loading tool definitions).
Everything else - web access, MCP/connector tools, subagents, notebooks, stopping a running
step, any other file or command - is refused.

If this script hits an error it refuses the call (fails closed). Every decision is appended to
OUT\\agent_logs\\guard_<yyyymmdd>.log.

Test by piping a payload in:
  echo {"tool_name":"Bash","tool_input":{"command":"dir"}} | python -I .claude\\hooks\\dashboard_guard.py
"""

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]   # the folder holding .claude\ and tests\
# The one folder the agent may read and write: the same output folder dqimp_webapp.py uses.
#   deployed folder (a Query_Results folder next to tests\):  <ROOT>\Query_Results\mlrs_case_completeness
#   Git clone (<repo>\web-app\):  <folder holding the repo>\Query_Results\web-app\mlrs_case_completeness,
#     outside the repo, as output_dir in webapp_config.json says. Written here, not read from the config.
if (ROOT / "Query_Results").is_dir():
    OUT = ROOT / "Query_Results" / "mlrs_case_completeness"
else:
    OUT = ROOT.parent.parent / "Query_Results" / "web-app" / "mlrs_case_completeness"

STEP = r"(?:preflight|next(?: --add)?(?: --date \d{8})?|build|compare|publish-dry|publish|verify)"
SHELL_OK = {
    "Bash": re.compile(r"\./tests/run_dashboard\.bat " + STEP),
    "PowerShell": re.compile(r"(?:& )?\.\\tests\\run_dashboard\.bat " + STEP),
}
SESSION_TOOLS = {   # no file, network or command access
    "TodoWrite", "TaskCreate", "TaskUpdate", "TaskList", "TaskGet",
    "AskUserQuestion", "ToolSearch", "BashOutput", "TaskOutput",
}
REPORT = re.compile(r"Dashboard_Update_Report_\d{8}\.md", re.IGNORECASE)   # paths are case-normalised on Windows


def real(p, cwd):
    """Absolute, resolved, case-normalised path. Accepts C:\\x, c:/x, /c/x (Git Bash) and relative."""
    p = str(p).strip().strip('"')
    m = re.match(r"^/([a-zA-Z])(/.*)?$", p)
    if m:
        p = f"{m.group(1)}:{m.group(2) or '/'}"
    if not os.path.isabs(p):
        p = os.path.join(cwd, p)
    return os.path.normcase(os.path.realpath(p))


def under(path, folder):
    folder = os.path.normcase(os.path.realpath(str(folder)))
    try:
        return os.path.commonpath([path, folder]) == folder
    except ValueError:          # different drives
        return False


def readable(path):
    """The agent's own outputs only."""
    if not under(path, OUT):
        return False
    rel = Path(os.path.relpath(path, os.path.normcase(os.path.realpath(str(OUT))))).parts
    name = rel[-1].lower()
    if len(rel) == 1:
        return name.endswith(".md") or name == "build_manifest.json"
    if len(rel) == 2 and rel[0] == "agent_logs":
        return name.endswith(".log")
    if len(rel) == 2 and rel[0] == "agent_history":
        return name.endswith(".json")
    return False


def decide(payload):
    """Return None (no objection; normal permission rules apply) or a reason to refuse."""
    tool = payload.get("tool_name", "")
    ti = payload.get("tool_input") or {}
    cwd = payload.get("cwd") or str(ROOT)

    if tool in SESSION_TOOLS:
        return None
    if tool == "Skill":
        return None if str(ti.get("skill", "")).strip() == "dashboard-update" else \
            "only the dashboard-update skill may be used here"
    if tool in SHELL_OK:
        cmd = str(ti.get("command", ""))
        if "\n" in cmd or "\r" in cmd or not SHELL_OK[tool].fullmatch(cmd.strip()):
            return ("the only command allowed here is the wrapper with one workflow step, e.g. "
                    + ("`./tests/run_dashboard.bat preflight`" if tool == "Bash" else "`& .\\tests\\run_dashboard.bat preflight`"))
        return None
    if tool == "Read":
        p = real(ti.get("file_path", ""), cwd)
        return None if readable(p) else \
            f"reading is limited to the agent's own reports and logs in {OUT}"
    if tool in ("Grep", "Glob"):
        if not ti.get("path"):
            return f"Grep/Glob need a path inside {OUT}"
        p = real(ti["path"], cwd)
        pattern = str(ti.get("pattern" if tool == "Glob" else "glob", "") or "")
        if not under(p, OUT) or ".." in pattern or re.match(r"^([a-zA-Z]:|/|\\)", pattern) \
                or (tool == "Grep" and str(ti.get("glob", "") or "").lower().endswith(".csv")) \
                or (tool == "Glob" and pattern.lower().endswith(".csv")):
            return f"searching is limited to the agent's reports and logs in {OUT} (no CSVs)"
        if tool == "Grep" and os.path.isfile(p) and not readable(p):
            return "that file is not one the agent may read"
        if tool == "Grep" and os.path.isdir(p) and not ti.get("glob"):
            return "Grep in a folder needs glob='*.log' or '*.md' (the folder also holds the CSVs)"
        return None
    if tool in ("Write", "Edit", "MultiEdit"):
        p = real(ti.get("file_path", ""), cwd)
        ok = os.path.dirname(p) == os.path.normcase(os.path.realpath(str(OUT))) \
            and REPORT.fullmatch(os.path.basename(p))
        return None if ok else \
            f"the only file the agent may write is {OUT}\\Dashboard_Update_Report_<YYYYMMDD>.md"
    return f"the tool '{tool}' is not part of the dashboard update workflow"


def audit(payload, reason):
    try:
        logs = OUT / "agent_logs"
        logs.mkdir(parents=True, exist_ok=True)
        ti = payload.get("tool_input") or {}
        subject = ti.get("command") or ti.get("file_path") or ti.get("path") or ti.get("skill") or ""
        subject = re.sub(r"\s+", " ", str(subject))[:500]
        line = (f"{datetime.now():%Y-%m-%dT%H:%M:%S} | s:{str(payload.get('session_id', ''))[:8]} | "
                f"{payload.get('tool_name', '?')} | {'DENY ' + reason if reason else 'ok'} | {subject}\n")
        with open(logs / f"guard_{datetime.now():%Y%m%d}.log", "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def main():
    payload = {}
    try:
        payload = json.loads(sys.stdin.buffer.read().decode("utf-8-sig") or "{}")
        reason = decide(payload)
    except Exception as e:                       # fail closed
        reason = f"the dashboard guard could not check this call ({type(e).__name__}: {e})"
    audit(payload, reason)
    if reason:
        print(json.dumps({"hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": "Dashboard guard (.claude/hooks/dashboard_guard.py): " + reason
                                        + ". This is outside the dashboard update workflow; tell the person instead of trying another way.",
        }}))
    sys.exit(0)


if __name__ == "__main__":
    main()
