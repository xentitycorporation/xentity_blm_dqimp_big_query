"""Steps for the dashboard-update agent (Claude Code skill /dashboard-update).

    tests\\run_dashboard.bat <step> [options]        (the .bat activates the ArcGIS Pro clone env)

    preflight          Config, SQL files, output folder, BigQuery sign-in, Portal sign-in, the four items
    next [--add] [--date YYYYMMDD ...]
                       Find the snapshot to add: one per calendar month, the earliest one whose five
                       source tables are all in BigQuery. --add writes it into webapp_config.json
                       (a dated backup of the config goes to agent_history first).
    build              dqimp_webapp.py build, with a log
    compare            Row counts per snapshot, the latest indicators, and older months unchanged
                       since the last run -> compare_<snapshot>.md
    publish-dry        dqimp_webapp.py publish --dry-run
    publish            dqimp_webapp.py publish (overwrite in place). Never --create.
    verify             Read the hosted tables back from the Portal -> verify_<snapshot>.md
    display            dqimp_webapp.py display (titles and column display names only)

Exit codes: 0 OK - 1 error - 3 nothing new - 5 needs a person - 6 flags to look at.

dqimp_webapp.py does the real work and is not changed by this file. Logs and agent files go
under the page's output folder (Query_Results\\<page>\\agent_logs and agent_history), never in tests\\.
"""

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import warnings
# Not "from datetime import datetime": importing arcgis.gis rebinds the name `datetime` in the
# running script (__main__) to the datetime module, which broke verify on 2026-11-18.
import datetime as dt
from pathlib import Path
from urllib.parse import urlparse

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import dqimp_webapp as W  # noqa: E402  (same folder; reused, never modified)

AGENT_CONFIG = SCRIPT_DIR / "dashboard_agent.json"
ROOT = SCRIPT_DIR.parent
DATASET = "blm_seta_dqimp"
SOURCE_TABLES = ("blm_case", "case_land", "case_customer", "case_action", "blm_product")
OK, ERROR, NOTHING_NEW, NEEDS_PERSON, FLAGS = 0, 1, 3, 5, 6
csv.field_size_limit(10 * 1024 * 1024)

# ---- hard limits: the only Portal, items and project this agent may ever touch ----
# Written here, not read from the config, so an edited config cannot point the agent elsewhere.
EXPECTED_PORTAL_HOST = "hqgis.blm.gov"
EXPECTED_BIGQUERY_PROJECT = "xentity-sandbox-huy"
EXPECTED_ITEMS = {   # the four hosted tables the dashboard reads (published 2026-10-20 / 10-26)
    "cases": "2bf88346f77240ce9d63b6dc0f05896f",
    "flows": "d793576eaab24d2aa43845561efe0cfb",
    "summary": "dee1b7d0daf14f0eaabeb3081e4c1d88",
    "changes": "af3dc30adfc64504b1358853f3e3a79c",
}
# ---- fingerprints: every file the agent runs or is governed by, frozen by the person ----
PINS = SCRIPT_DIR / "guard_pins.json"
PINNED_FILES = [
    "tests/dqimp_webapp.py", "tests/dashboard_agent.py", "tests/run_dashboard.bat", "tests/dashboard_agent.json",
    ".claude/settings.json", ".claude/hooks/dashboard_guard.py", ".claude/skills/dashboard-update/SKILL.md",
    "CLAUDE.md",
]   # plus the page's SQL files and webapp_config.json without its snapshot list (see fingerprints())


# ---------------------------------------------------------------- plumbing

def load_agent_config():
    defaults = {"page": "mlrs_case_completeness", "ask_before_publish": True, "flag_percent": 10, "flag_min_rows": 50}
    if AGENT_CONFIG.exists():
        defaults.update(json.loads(AGENT_CONFIG.read_text(encoding="utf-8")))
    return defaults


def folders(cfg, page):
    out_dir = W.output_dir(cfg, page)
    logs, history = out_dir / "agent_logs", out_dir / "agent_history"
    for d in (logs, history):
        d.mkdir(parents=True, exist_ok=True)
    return out_dir, logs, history


def bq_client(cfg):
    if cfg.get("trust_windows_cert_store", True):
        W.trust_windows_certificates()
    warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials")
    from google.cloud import bigquery
    return bigquery.Client(project=cfg["bigquery_project"])


class Tee:
    """Every step's output goes to the screen and to agent_logs\\<step>_<time>.log, so the agent
    reads a step's result from its log (the only place the guard lets it read)."""
    def __init__(self, stream, log):
        self.stream, self.log = stream, open(log, "a", encoding="utf-8")

    def write(self, s):
        self.stream.write(s)
        self.log.write(s)
        self.log.flush()

    def flush(self):
        self.stream.flush()
        self.log.flush()


def run_webapp(cfg, page, args):
    """Run dqimp_webapp.py in this same Python with fixed arguments, passing every line through."""
    cmd = [sys.executable, "-u", str(SCRIPT_DIR / "dqimp_webapp.py"), *args, "--page", page]
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    print(f"RUN dqimp_webapp.py {' '.join(args)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, encoding="utf-8", errors="replace", env=env)
    for line in proc.stdout:
        sys.stdout.write(line)
    code = proc.wait()
    print(f"EXIT {code}")
    return OK if code == 0 else ERROR


def read_manifest(out_dir):
    path = out_dir / "build_manifest.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def rows_per_snapshot(csv_path):
    """{Snapshot: data rows} read with a real CSV reader (quoted line breaks count once)."""
    counts = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        i = [h.lower() for h in header].index("snapshot")
        for row in reader:
            counts[row[i]] = counts.get(row[i], 0) + 1
    return dict(sorted(counts.items()))


def pct(new, old):
    return None if not old else 100.0 * (new - old) / old


def fmt_pct(p):
    return "n/a" if p is None else f"{p:+.1f}%"


# ---------------------------------------------------------------- guards

def sha256(path):
    import hashlib
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def fingerprints(cfg, page):
    """{name: sha256} for every pinned file, the page's SQL, and the config minus its snapshot list
    (the one part the agent is allowed to change, through `next --add`)."""
    page_cfg = cfg["pages"][page]
    files = list(PINNED_FILES) + [f"tests/{page_cfg['sql_dir']}/{s}" for s in
                                  [page_cfg["build_sql"]] + [o["sql"] for o in page_cfg["outputs"].values()]]
    fp = {f: (sha256(ROOT / f) if (ROOT / f).exists() else "MISSING") for f in files}
    frozen = json.loads(json.dumps(cfg))
    frozen["pages"][page].pop("snapshots", None)
    import hashlib
    fp["tests/webapp_config.json (all but the snapshot list)"] = hashlib.sha256(
        json.dumps(frozen, sort_keys=True).encode("utf-8")).hexdigest()
    return fp


def pin(cfg, page):
    """Run by the PERSON, never by the agent (the guard hook refuses it): freezes the current files."""
    problems = hard_limit_problems(cfg, page)
    if problems:
        print("STOPPED not pinned: " + "; ".join(problems))
        return ERROR
    fp = fingerprints(cfg, page)
    missing = [f for f, h in fp.items() if h == "MISSING"]
    if missing:
        print(f"STOPPED not pinned, files missing: {missing}")
        return ERROR
    PINS.write_text(json.dumps({"pinned_at": dt.datetime.now().isoformat(timespec="seconds"),
                                "files": fp}, indent=2) + "\n", encoding="utf-8")
    for f, h in fp.items():
        print(f"PINNED {h[:12]}  {f}")
    print(f"WROTE {PINS}")
    return OK


def hard_limit_problems(cfg, page):
    out = []
    if cfg.get("bigquery_project") != EXPECTED_BIGQUERY_PROJECT:
        out.append(f"bigquery_project is '{cfg.get('bigquery_project')}', not {EXPECTED_BIGQUERY_PROJECT}")
    host = urlparse(cfg.get("portal", {}).get("url", "")).hostname
    if host != EXPECTED_PORTAL_HOST:
        out.append(f"portal url host is '{host}', not {EXPECTED_PORTAL_HOST}")
    ids = {n: o.get("portal_item_id") for n, o in cfg["pages"][page]["outputs"].items()}
    if ids != EXPECTED_ITEMS:
        out.append(f"Portal item IDs in the config {ids} are not the four the dashboard reads {EXPECTED_ITEMS}")
    return out


def settings_problems():
    """Settings that would switch the guard off or skip the permission rules."""
    out = []
    for p in (ROOT / ".claude" / "settings.local.json", Path.home() / ".claude" / "settings.json"):
        if not p.exists():
            continue
        try:
            s = json.loads(p.read_text(encoding="utf-8-sig"))
        except Exception as e:
            out.append(f"{p} could not be read ({e})")
            continue
        if s.get("disableAllHooks"):
            out.append(f"{p} sets disableAllHooks")
        if (s.get("permissions") or {}).get("defaultMode") in ("bypassPermissions", "acceptEdits", "auto", "dontAsk"):
            out.append(f"{p} sets permissions.defaultMode = {s['permissions']['defaultMode']}")
    return out


def integrity(cfg, page):
    """Every step except `pin` starts here. Returns a list of reasons to stop (empty = all good)."""
    problems = hard_limit_problems(cfg, page) + settings_problems()
    if not PINS.exists():
        return problems + [f"{PINS.name} is missing: the person must run `tests\\run_dashboard.bat pin` once, by hand"]
    pinned = json.loads(PINS.read_text(encoding="utf-8")).get("files", {})
    now = fingerprints(cfg, page)
    changed = [f for f in sorted(set(pinned) | set(now)) if pinned.get(f) != now.get(f)]
    if changed:
        problems.append("changed since the person pinned them: " + ", ".join(changed)
                        + " -- if that was on purpose, the person re-runs `tests\\run_dashboard.bat pin` by hand")
    return problems


def check_portal_items(gis, cfg, page):
    """Before anything is sent: right Portal, and each item is the expected hosted table, owned by
    the signed-in user. Returns a list of problems."""
    problems = []
    if urlparse(gis.url).hostname != EXPECTED_PORTAL_HOST:
        return [f"signed in to {gis.url}, not {EXPECTED_PORTAL_HOST}"]
    me = gis.users.me.username
    for name, out in cfg["pages"][page]["outputs"].items():
        item = gis.content.get(EXPECTED_ITEMS[name])
        if item is None:
            problems.append(f"{name}: item {EXPECTED_ITEMS[name]} not found")
        elif item.owner != me:
            problems.append(f"{name}: item is owned by {item.owner}, not {me}")
        elif item.type != "Feature Service" or item.title != out.get("title"):
            problems.append(f"{name}: item is '{item.title}' ({item.type}), expected '{out.get('title')}' (Feature Service)")
    return problems


# ---------------------------------------------------------------- preflight

def preflight(cfg, acfg, page, skip_portal=False):
    results = []   # (status, check, detail)

    def check(name, ok, detail="", status_if_bad="FAIL"):
        results.append(("PASS" if ok else status_if_bad, name, detail))
        return ok

    env_expected = os.environ.get("DQIMP_ENV_NAME")
    check("Python environment", not env_expected or Path(sys.prefix).name.lower() == env_expected.lower(),
          f"{sys.prefix} (expected env '{env_expected or 'any'}')")

    page_cfg = cfg["pages"][page]
    snaps = page_cfg["snapshots"]
    check("Snapshot list", snaps == sorted(snaps) and len(set(snaps)) == len(snaps),
          f"{len(snaps)} snapshots, {snaps[0]} -> {snaps[-1]}")
    no_id = [n for n, o in page_cfg["outputs"].items() if not o.get("portal_item_id")]
    check("Every table has a Portal item ID", not no_id,
          f"missing: {no_id} -- this agent never creates items" if no_id else f"{len(page_cfg['outputs'])} tables")

    sql_dir = SCRIPT_DIR / page_cfg["sql_dir"]
    sql_files = [page_cfg["build_sql"]] + [o["sql"] for o in page_cfg["outputs"].values()]
    missing_sql = [s for s in sql_files if not (sql_dir / s).exists()]
    check("SQL files", not missing_sql, f"missing {missing_sql}" if missing_sql else f"{len(sql_files)} in {sql_dir}")

    flat = (SCRIPT_DIR.parent / "Query_Results").is_dir()
    out_dir = W.output_dir(cfg, page)
    check("Output folder", out_dir.parent.is_dir(),
          f"{out_dir} ({'flat layout' if flat else 'repo layout'})")

    bq_status = "FAIL"
    try:
        client = bq_client(cfg)
        list(client.query("SELECT 1").result())
        lookup = f"{cfg['bigquery_project']}.{DATASET}.Product_Code_Case_Type_Group_Subgroup"
        client.get_table(lookup)
        check("BigQuery sign-in and lookup table", True, lookup)
        bq_status = "PASS"
    except Exception as e:
        auth = type(e).__module__.startswith("google.auth") or "credentials" in str(e).lower() \
            or "reauth" in str(e).lower()
        check("BigQuery sign-in and lookup table", False,
              f"{type(e).__name__}: {str(e)[:300]}"
              + (" -- run 'gcloud auth application-default login' in a terminal (it opens your browser)" if auth else ""),
              status_if_bad="NEEDS_PERSON" if auth else "FAIL")

    if skip_portal:
        results.append(("SKIP", "Portal", "--skip-portal"))
    else:
        try:
            gis = W.connect_portal(cfg)
            want = urlparse(cfg.get("portal", {}).get("url", "")).hostname
            got = urlparse(gis.url).hostname
            if check("Portal sign-in (ArcGIS Pro)", want == got == EXPECTED_PORTAL_HOST,
                     f"{gis.url} as {gis.users.me.username}; expected {EXPECTED_PORTAL_HOST}",
                     status_if_bad="NEEDS_PERSON"):
                item_problems = check_portal_items(gis, cfg, page)
                check("Portal items (the dashboard's four tables, owned by you)", not item_problems,
                      "; ".join(item_problems) or ", ".join(EXPECTED_ITEMS.values()))
        except Exception as e:
            check("Portal sign-in (ArcGIS Pro)", False,
                  f"{type(e).__name__}: {str(e)[:300]} -- open ArcGIS Pro and sign in to the HQ Portal with your PIV card",
                  status_if_bad="NEEDS_PERSON")

    for status, name, detail in results:
        print(f"{status:<12} {name}: {detail}")
    print(f"INFO         ask_before_publish={acfg['ask_before_publish']} flag_percent={acfg['flag_percent']}")
    statuses = {r[0] for r in results}
    if "FAIL" in statuses:
        return ERROR
    return NEEDS_PERSON if "NEEDS_PERSON" in statuses else OK


# ---------------------------------------------------------------- next snapshot

def next_snapshot(cfg, acfg, page, add=False, dates=None):
    page_cfg = cfg["pages"][page]
    configured = page_cfg["snapshots"]
    last = configured[-1]
    client = bq_client(cfg)
    pattern = re.compile(rf"^({'|'.join(SOURCE_TABLES)})_(\d{{8}})$")
    found = {}
    for t in client.list_tables(f"{cfg['bigquery_project']}.{DATASET}"):
        m = pattern.match(t.table_id)
        if m:
            found.setdefault(m.group(2), set()).add(m.group(1))
    newer = sorted(d for d in found if d > last)
    print(f"INFO configured: {len(configured)} snapshots, latest {last}")
    if not newer:
        print(f"NOTHING_NEW no snapshot after {last} in {DATASET}")
        return NOTHING_NEW

    covered = {s[:6] for s in configured}
    first_in_month, notes = {}, {}
    for d in newer:
        have = found[d]
        if not set(SOURCE_TABLES) <= have:
            notes[d] = f"incomplete: missing {sorted(set(SOURCE_TABLES) - have)}"
        elif d[:6] in covered:
            notes[d] = f"skipped: month {d[:4]}-{d[4:6]} already has a snapshot in the list"
        elif d[:6] in first_in_month:
            notes[d] = f"skipped: {first_in_month[d[:6]]} is earlier in the same month"
        else:
            first_in_month[d[:6]] = d
            notes[d] = "CANDIDATE"
    for d in newer:
        print(f"SNAPSHOT {d}: {notes[d]}")

    if dates:
        bad = [d for d in dates if d not in found or not set(SOURCE_TABLES) <= found[d] or d <= last]
        if bad:
            print(f"STOPPED --date {bad}: not a complete snapshot newer than {last}")
            return NEEDS_PERSON
        picks = sorted(dates)
    else:
        picks = sorted(first_in_month.values())
        if not picks:
            print("NOTHING_NEW no complete snapshot in a month that is not already in the list")
            return NOTHING_NEW
        if len(picks) > 1:
            print(f"STOPPED more than one new month: {picks}. Ask which to add, then re-run with --date.")
            return NEEDS_PERSON

    # Each source table must hold rows; show the case count next to the latest configured one.
    def rows(table, d):
        return client.get_table(f"{cfg['bigquery_project']}.{DATASET}.{table}_{d}").num_rows
    for d in picks:
        empty = [t for t in SOURCE_TABLES if not rows(t, d)]
        if empty:
            print(f"STOPPED {d}: empty table(s) {empty} -- is the load still running?")
            return NEEDS_PERSON
        print(f"ROWS {d}: " + ", ".join(f"{t} {rows(t, d):,} ({fmt_pct(pct(rows(t, d), rows(t, last)))} vs {last})"
                                        for t in SOURCE_TABLES))

    if not add:
        print(f"PICK {' '.join(picks)} (not added; re-run with --add)")
        return OK
    _, _, history = folders(cfg, page)
    backup = history / f"webapp_config_before_{picks[-1]}_{dt.datetime.now():%Y%m%d_%H%M%S}.json"
    shutil.copy2(W.CONFIG_PATH, backup)
    page_cfg["snapshots"] = sorted(set(configured) | set(picks))
    W.save_config(cfg)
    print(f"ADDED {' '.join(picks)} to {W.CONFIG_PATH} (backup {backup})")
    return OK


# ---------------------------------------------------------------- compare

def compare(cfg, acfg, page):
    page_cfg = cfg["pages"][page]
    snaps = page_cfg["snapshots"]
    latest, prev = snaps[-1], (snaps[-2] if len(snaps) > 1 else None)
    out_dir, _, history = folders(cfg, page)
    manifest = read_manifest(out_dir)
    if not manifest or manifest.get("snapshots") != snaps:
        print("STOPPED the last build does not match the snapshot list in the config -- run build first")
        return ERROR
    limit, min_rows = float(acfg["flag_percent"]), int(acfg["flag_min_rows"])
    flags, lines = [], []

    def big(new, old):
        """A change worth a look: over flag_percent AND at least flag_min_rows (4 -> 0 is not)."""
        p = pct(new, old)
        return p is not None and abs(p) > limit and abs(new - old) >= min_rows

    counts = {name: rows_per_snapshot(out_dir / out["csv"]) for name, out in page_cfg["outputs"].items()}
    baseline_files = sorted(p for p in history.glob("counts_*.json") if p.stem[7:] < latest)
    baseline = json.loads(baseline_files[-1].read_text(encoding="utf-8")) if baseline_files else None

    lines += [f"# Build comparison - snapshot {latest}", "",
              f"Built {manifest['built_at']} ({manifest.get('seconds', '?')} s) from {len(snaps)} snapshots, "
              f"{snaps[0]} -> {latest}. A change is flagged when it is over {limit:g}% and at least "
              f"{min_rows:,} rows or cases.", "",
              "## Rows per table", "",
              f"| Table | Total rows | Rows {prev} | Rows {latest} | Change |", "|---|---:|---:|---:|---:|"]
    for name, c in counts.items():
        if latest not in c:
            flags.append(f"{name}: no rows for {latest}")
        p = pct(c.get(latest, 0), c.get(prev, 0))
        if big(c.get(latest, 0), c.get(prev, 0)):
            flags.append(f"{name}: {latest} has {c.get(latest, 0):,} rows vs {c.get(prev, 0):,} for {prev} ({fmt_pct(p)})")
        lines.append(f"| {name} | {sum(c.values()):,} | {c.get(prev, 0):,} | {c.get(latest, 0):,} | {fmt_pct(p)} |")

    lines += ["", "## Older months unchanged since the last run", ""]
    if baseline is None:
        lines.append("No earlier run on record (first run of the agent): nothing to compare.")
    else:
        lines.append(f"Compared with `{baseline_files[-1].name}`.")
        changed = []
        for name, c in counts.items():
            for s, n in baseline.get("counts", {}).get(name, {}).items():
                if c.get(s) != n:
                    changed.append(f"{name} {s}: {n:,} -> {c.get(s, 0):,}")
        if changed:
            flags.append(f"rows of earlier months changed since the last run ({len(changed)}): " + "; ".join(changed[:10]))
            lines += [f"- {x}" for x in changed]
        else:
            lines.append("Every earlier month has exactly the same rows per table as last time.")

    # The four indicators, as the dashboard shows them.
    flows = out_dir / page_cfg["outputs"]["flows"]["csv"]
    cols = ["Prior_Total", "New_Case", "Lost_Record_Regression", "Entered_Scope_Already_Failing", "Fixed",
            "Left_Scope_Still_Failing", "Removed_From_MLRS", "Current_Total", "Net_Change"]
    lines += ["", f"## Checks in {latest} (Flows table)", "",
              "| Population | Check | " + " | ".join(cols) + " |", "|---|---|" + "---:|" * len(cols)]
    with open(flows, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["Snapshot"] != latest:
                continue
            lines.append(f"| {r['Scope']} | {r['Subject_Name']} | "
                         + " | ".join(f"{int(float(r[c])):,}" if r.get(c) else "" for c in cols) + " |")
            cur, prior = int(float(r["Current_Total"] or 0)), int(float(r["Prior_Total"] or 0))
            if big(cur, prior):
                flags.append(f"{r['Scope']} / {r['Subject_Name']}: {prior:,} -> {cur:,} failing "
                             f"({fmt_pct(pct(cur, prior))})")

    lines += ["", "## Flags", ""] + ([f"- FLAG {x}" for x in flags] or ["None."])
    report = out_dir / f"compare_{latest}.md"
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    (history / f"counts_{latest}.json").write_text(
        json.dumps({"snapshots": snaps, "built_at": manifest["built_at"], "counts": counts}, indent=2), encoding="utf-8")
    print("\n".join(lines))
    print(f"\nREPORT {report}")
    return FLAGS if flags else OK


# ---------------------------------------------------------------- publish gate

def publish(cfg, acfg, page, dry_run):
    page_cfg = cfg["pages"][page]
    snaps = page_cfg["snapshots"]
    out_dir, _, _ = folders(cfg, page)
    manifest = read_manifest(out_dir)
    if not manifest or manifest.get("snapshots") != snaps:
        print("STOPPED the CSVs were not built from the current snapshot list -- run build first")
        return ERROR
    report = out_dir / f"compare_{snaps[-1]}.md"
    built = dt.datetime.fromisoformat(manifest["built_at"]).timestamp()
    if not report.exists() or report.stat().st_mtime < built:
        print("STOPPED run compare on this build before publishing")
        return ERROR
    if any(not o.get("portal_item_id") for o in page_cfg["outputs"].values()):
        print("STOPPED a table has no Portal item ID; creating items is not part of this agent")
        return NEEDS_PERSON
    if dry_run:
        return run_webapp(cfg, page, ["publish", "--dry-run"])
    # Last check before anything is sent: the signed-in Portal and the four items are the expected ones.
    problems = check_portal_items(W.connect_portal(cfg), cfg, page)
    if problems:
        print("STOPPED nothing was sent: " + "; ".join(problems))
        return NEEDS_PERSON
    print("PASS the four Portal items are the dashboard's tables, owned by the signed-in user")
    return run_webapp(cfg, page, ["publish"])


# ---------------------------------------------------------------- verify

def verify(cfg, acfg, page):
    page_cfg = cfg["pages"][page]
    snaps = page_cfg["snapshots"]
    latest = snaps[-1]
    out_dir, _, _ = folders(cfg, page)
    manifest = read_manifest(out_dir)
    built = dt.datetime.fromisoformat(manifest["built_at"]).timestamp() if manifest else 0
    gis = W.connect_portal(cfg)
    problems = check_portal_items(gis, cfg, page)
    if problems:
        print("STOPPED " + "; ".join(problems))
        return NEEDS_PERSON
    lines = [f"# Portal check - snapshot {latest}", "", f"Checked {dt.datetime.now():%Y-%m-%d %H:%M} in {gis.url}.", "",
             "| Table | Item | Updated | Portal rows | CSV rows | Latest (Months back 0) | Snapshots | Code columns text |",
             "|---|---|---|---:|---:|---|---:|---|"]

    def values(table, field, where="1=1"):
        fs = table.query(where=where, out_fields=field, return_distinct_values=True, return_geometry=False)
        return sorted({str(next(iter(f.attributes.values()))) for f in fs.features})

    for name, out in page_cfg["outputs"].items():
        item = gis.content.get(out["portal_item_id"])
        if item is None:
            problems.append(f"{name}: item {out['portal_item_id']} not found")
            continue
        tables = (item.tables or []) + (item.layers or [])
        t = tables[0]
        fields = {f["name"].lower(): f for f in t.properties.fields}
        snap_field = fields["snapshot"]["name"]
        mb_field = fields["months_back"]["name"]
        portal_rows = sum(x.query(return_count_only=True) for x in tables)
        csv_rows = W.csv_row_count(out_dir / out["csv"])
        latest_vals = values(t, snap_field, f"{mb_field} = 0")
        all_snaps = values(t, snap_field)
        csv_snaps = sorted(rows_per_snapshot(out_dir / out["csv"]))
        bad_types = [f["name"] for n, f in fields.items()
                     if n in W.TEXT_FIELDS_LOWER and f.get("type") != "esriFieldTypeString"]
        modified = item.modified / 1000
        if portal_rows != csv_rows:
            problems.append(f"{name}: Portal {portal_rows:,} rows, CSV {csv_rows:,}")
        if latest_vals != [latest]:
            problems.append(f"{name}: Months back 0 is {latest_vals}, expected [{latest}]")
        if all_snaps != csv_snaps:
            problems.append(f"{name}: Portal snapshots {all_snaps} differ from the CSV's {csv_snaps}")
        if bad_types:
            problems.append(f"{name}: {bad_types} not text")
        if modified < built:
            problems.append(f"{name}: item last updated before this build")
        lines.append(f"| {name} | `{item.id}` | {dt.datetime.fromtimestamp(modified):%Y-%m-%d %H:%M} | {portal_rows:,} | "
                     f"{csv_rows:,} | {', '.join(latest_vals)} | {len(all_snaps)} | {'no: ' + ', '.join(bad_types) if bad_types else 'yes'} |")

    lines += ["", "## Problems", ""] + ([f"- {p}" for p in problems] or ["None. The dashboard reads these tables, so it now shows "
                                                                          f"{latest} as the latest snapshot."])
    report = out_dir / f"verify_{latest}.md"
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"\nREPORT {report}")
    return FLAGS if problems else OK


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("step", choices=["preflight", "next", "build", "compare", "publish-dry", "publish", "verify",
                                     "display", "pin"],
                    help="display and pin are for the person only (the guard hook refuses them to the agent)")
    ap.add_argument("--add", action="store_true", help="next: write the picked snapshot into webapp_config.json")
    ap.add_argument("--date", nargs=1, metavar="YYYYMMDD", help="next: add this snapshot instead of the automatic pick")
    ap.add_argument("--skip-portal", action="store_true", help="preflight: skip the Portal checks (testing off the BLM network)")
    a = ap.parse_args()
    acfg = load_agent_config()
    cfg = W.load_config()
    page = acfg["page"]
    if page not in cfg["pages"]:
        sys.exit(f"Unknown page '{page}' in {AGENT_CONFIG.name}")
    _, logs, _ = folders(cfg, page)
    log = logs / f"{a.step}_{dt.datetime.now():%Y%m%d_%H%M%S}.log"
    sys.stdout = Tee(sys.stdout, log)
    sys.stderr = sys.stdout
    print(f"STEP {a.step} | page {page} | {dt.datetime.now():%Y-%m-%d %H:%M:%S} | config {W.CONFIG_PATH}")
    print(f"LOG {log}")
    if a.step == "pin":
        code = pin(cfg, page)
    else:
        problems = integrity(cfg, page)
        if problems:
            for p in problems:
                print(f"STOPPED guard: {p}")
            print("EXIT 5 (needs a person: nothing was run)")
            sys.exit(NEEDS_PERSON)
        print("PASS guard: pinned files unchanged, Portal items / Portal / project are the expected ones, hooks not disabled")
        if a.step == "preflight":
            code = preflight(cfg, acfg, page, a.skip_portal)
        elif a.step == "next":
            code = next_snapshot(cfg, acfg, page, a.add, a.date)
        elif a.step == "build":
            code = run_webapp(cfg, page, ["build"])
        elif a.step == "compare":
            code = compare(cfg, acfg, page)
        elif a.step in ("publish-dry", "publish"):
            code = publish(cfg, acfg, page, dry_run=(a.step == "publish-dry"))
        elif a.step == "verify":
            code = verify(cfg, acfg, page)
        else:
            code = run_webapp(cfg, page, ["display"])
    print(f"DONE {a.step} exit {code}")
    sys.exit(code)


if __name__ == "__main__":
    main()
