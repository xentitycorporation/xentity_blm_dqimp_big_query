"""
verify_snapshot.py — Step 5 of the monthly build: compare a newly loaded snapshot with the
previous one and write the result as Markdown.

For the 24 MLRS tables and the two NLSDB tables it reports, against the prior snapshot:
  - row counts and the change (from BigQuery's table metadata, which scans no data);
  - tables that are missing or empty;
  - real cases in blm_case (Status and Bond record types excluded) and the latest
    created / modified dates, parsed from MLRS's MM/DD/YYYY text (a raw MAX() on that text
    sorts alphabetically and gives the wrong answer);
  - the latest Created / Modified in the two NLSDB tables.

FLAG marks a number a person should look at: a missing or empty table, a row count that moved
by FLAG_PCT percent or more, real cases going down, or latest dates older than the prior
snapshot's. A flag is a prompt to look, not a finding.

Usage (run with the dev conda env, usually through run_build.ps1 -Step verify):
  python verify_snapshot.py --date 20261115                  # prior = the latest loaded before it
  python verify_snapshot.py --date 20261115 --prior 20261101 --out <month folder>\\verify_20261115.md

Exit codes: 0 = no flags, 1 = error, 6 = written, with flags.
"""

import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT = 'xentity-sandbox-huy'
DATASET = 'blm_seta_dqimp'
STATUS_AND_BOND = ("'0123d0000005ISFAA2'", "'0123d0000004QFQAA2'")   # RECORDTYPEIDs excluded from real cases
FLAG_PCT = 2.0
MLRS_TS = "SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', {col})"


def mlrs_tables():
    folder = os.path.join(os.path.dirname(SCRIPT_DIR), 'schemas', 'mlrs_export_schemas')
    names = sorted(os.path.basename(p)[:-len('_schema.json')] for p in glob.glob(os.path.join(folder, '*_schema.json')))
    if not names:
        print(f'ERROR: no *_schema.json files in {folder}')
        sys.exit(1)
    return names


def bq_query(sql):
    """Run standard SQL through the bq CLI (SQL passed on stdin from a file) and return rows."""
    bq = shutil.which('bq') or shutil.which('bq.cmd')
    if not bq:
        print('ERROR: bq not found on PATH (Google Cloud SDK).')
        sys.exit(1)
    fd, path = tempfile.mkstemp(suffix='.sql')
    with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as f:
        f.write(sql)
    try:
        r = subprocess.run(f'"{bq}" query --nouse_legacy_sql --format=json --max_rows=100000 --quiet < "{path}"',
                           shell=True, capture_output=True, text=True)
    finally:
        os.remove(path)
    if r.returncode != 0:
        print('ERROR: BigQuery query failed. If the message mentions reauthentication, run')
        print('  `gcloud auth login` in a terminal and try again.')
        print('  ' + (r.stderr or r.stdout).strip()[:800])
        sys.exit(1)
    out = r.stdout.strip()
    return json.loads(out[out.index('['):]) if '[' in out else []


def fmt(n):
    return '—' if n is None else f'{int(n):,}'


def pct(new, old):
    if new is None or old in (None, 0):
        return None
    return 100.0 * (new - old) / old


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--date', required=True, help='snapshot to check, YYYYMMDD')
    ap.add_argument('--prior', help='snapshot to compare with (default: the latest loaded before --date)')
    ap.add_argument('--out', help='Markdown file to write (also printed)')
    args = ap.parse_args()
    if not re.fullmatch(r'\d{8}', args.date) or (args.prior and not re.fullmatch(r'\d{8}', args.prior)):
        print('ERROR: dates must be YYYYMMDD')
        return 1

    meta = bq_query(f"SELECT table_id, row_count FROM `{PROJECT}.{DATASET}.__TABLES__` "
                    f"WHERE REGEXP_CONTAINS(table_id, r'_\\d{{8}}$')")
    counts = {m['table_id']: int(m['row_count']) for m in meta}
    loaded = sorted({t[-8:] for t in counts if t.startswith('blm_case_')})
    if args.date not in loaded:
        print(f'ERROR: blm_case_{args.date} is not in {DATASET}; was the snapshot loaded?')
        return 1
    prior = args.prior or next((d for d in reversed(loaded) if d < args.date), None)
    if not prior:
        print('ERROR: no earlier snapshot to compare with; pass --prior')
        return 1

    flags = []
    lines = [f'# Snapshot check: {args.date} compared with {prior}', '',
             f'Dataset `{PROJECT}.{DATASET}`. Row counts come from BigQuery table metadata. '
             f'FLAG = worth a look (missing, empty, moved by {FLAG_PCT:g}% or more, or dates going backwards); '
             f'it is not a finding.', '',
             '## Row counts', '',
             f'| Table | {prior} | {args.date} | Change | % | |', '|---|---:|---:|---:|---:|---|']
    for t in mlrs_tables() + ['nlsdb_case', 'nlsdb_case_land']:
        old, new = counts.get(f'{t}_{prior}'), counts.get(f'{t}_{args.date}')
        p = pct(new, old)
        flag = ''
        if new is None:
            flag = 'FLAG: missing'
        elif new == 0 and (old or 0) > 0:
            flag = 'FLAG: empty'
        elif p is not None and abs(p) >= FLAG_PCT:
            flag = 'FLAG'
        if flag:
            flags.append(f'{t}: {flag.replace("FLAG: ", "").replace("FLAG", f"{p:+.2f}%" if p is not None else "")}'.strip())
        change = '—' if new is None or old is None else f'{new - old:+,}'
        lines.append(f'| {t} | {fmt(old)} | {fmt(new)} | {change} | {"—" if p is None else f"{p:+.2f}"} | {flag} |')

    # --- blm_case: real cases and how recent the data is ---
    def case_stats(d):
        rows = bq_query(
            f"SELECT COUNT(*) AS all_rows, "
            f"COUNTIF(RECORDTYPEID NOT IN ({', '.join(STATUS_AND_BOND)})) AS real_cases, "
            f"CAST(MAX({MLRS_TS.format(col='CREATEDDATE')}) AS STRING) AS latest_created, "
            f"CAST(MAX({MLRS_TS.format(col='LASTMODIFIEDDATE')}) AS STRING) AS latest_modified "
            f"FROM `{PROJECT}.{DATASET}.blm_case_{d}`")
        return rows[0] if rows else {}

    def nlsdb_stats(t, d):
        if f'{t}_{d}' not in counts:
            return {}
        rows = bq_query(f"SELECT CAST(MAX(Created) AS STRING) AS latest_created, "
                        f"CAST(MAX(Modified) AS STRING) AS latest_modified FROM `{PROJECT}.{DATASET}.{t}_{d}`")
        return rows[0] if rows else {}

    cp, cn = case_stats(prior), case_stats(args.date)
    lines += ['', '## MLRS cases (blm_case)', '',
              f'| | {prior} | {args.date} | |', '|---|---:|---:|---|']
    rc_old, rc_new = int(cp.get('real_cases') or 0), int(cn.get('real_cases') or 0)
    f_rc = 'FLAG: fewer real cases' if rc_new < rc_old else ''
    if f_rc:
        flags.append(f'blm_case real cases went from {rc_old:,} to {rc_new:,}')
    lines.append(f'| Real cases (Status and Bond record types excluded) | {rc_old:,} | {rc_new:,} | {f_rc} |')
    for k, lab in (('latest_created', 'Latest CREATEDDATE'), ('latest_modified', 'Latest LASTMODIFIEDDATE')):
        o, n = cp.get(k), cn.get(k)
        f = 'FLAG: older than prior' if o and n and n < o else ('FLAG: none parsed' if not n else '')
        if f:
            flags.append(f'blm_case {lab}: {f.replace("FLAG: ", "")} ({n} vs {o})')
        lines.append(f'| {lab} (parsed) | {o} | {n} | {f} |')

    lines += ['', '## NLSDB dates', '', f'| | {prior} | {args.date} | |', '|---|---:|---:|---|']
    for t in ('nlsdb_case', 'nlsdb_case_land'):
        sp, sn = nlsdb_stats(t, prior), nlsdb_stats(t, args.date)
        for k, lab in (('latest_created', 'Created'), ('latest_modified', 'Modified')):
            o, n = sp.get(k), sn.get(k)
            f = 'FLAG: older than prior' if o and n and n < o else ''
            if f:
                flags.append(f'{t} latest {lab}: older than prior ({n} vs {o})')
            lines.append(f'| {t} latest {lab} | {o} | {n} | {f} |')

    lines += ['', '## Flags', '']
    lines += [f'- {f}' for f in flags] if flags else ['None.']
    text = '\n'.join(lines) + '\n'
    print(text)
    if args.out:
        with open(args.out, 'w', encoding='utf-8') as f:
            f.write(text)
        print(f'Written to {args.out}')
    return 6 if flags else 0


if __name__ == '__main__':
    sys.exit(main())
