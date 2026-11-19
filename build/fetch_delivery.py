"""
fetch_delivery.py — find the next BLM delivery in the shared drop folders, copy its three zips
into the local month folder, and record what was found.

BLM's monthly deliveries are uploaded to two shared folders (one for the MLRS Full and MC
zips, one for the NLSDB GDB zip). This script:

  1. lists the zips in both folders and pairs each MLRS Full zip with its MC zip and the
     nearest NLSDB zip by the dates in their names;
  2. asks BigQuery which snapshots are already loaded (one blm_case_<YYYYMMDD> table each);
  3. picks the oldest delivery newer than the newest loaded snapshot (or the one named with
     --date), copies its three zips into the month folder, and checks the copies;
  4. opens the copied zips and reads the dates of the tar files inside them — the date the
     snapshot is built under (the zip label has been wrong before: an October 2026 Full zip
     labelled 2026-10-26 held tars dated 20261025);
  5. writes delivery_<YYYYMMDD>.json in the month folder, which the later steps read.

Machine-specific paths (the drop folders, the data root) come from monthly_build.local.json
beside this script — see monthly_build.example.json. Nothing here deletes or overwrites a file
that differs from its source: a zip already in the month folder with the same size is reused.

Usage (run with the dev conda env, usually through run_build.ps1 -Step fetch):
  python fetch_delivery.py --list              # show deliveries and what is loaded; copy nothing
  python fetch_delivery.py                     # fetch the next delivery to build
  python fetch_delivery.py --date 20261115     # fetch a specific delivery (MLRS Full zip date)

Exit codes:
  0  fetched (or listed) and nothing needs a person
  1  error (missing config, unreadable folder, bad zip, failed copy)
  3  nothing new to build — every delivery in the drop folder is already in BigQuery
  4  not enough free disk space in the data folder
  5  fetched, but something needs a person before the build continues (listed under REVIEW)
"""

import argparse
import calendar
import datetime as dt
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import zipfile

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(SCRIPT_DIR, 'monthly_build.local.json')
PROJECT = 'xentity-sandbox-huy'
DATASET = 'blm_seta_dqimp'
EXPECTED_STATES = ['AK', 'AZ', 'CA', 'CO', 'ES', 'ID', 'MT', 'NM', 'NV', 'OR', 'UT', 'WY']

# A month folder holds the zips, the unpacked tars and the Parquet files: about 84 GB for the
# 11/1/2026 delivery, whose three zips are about 7.6 GB. Require 15x the zip size free.
SPACE_FACTOR = 15
MC_MAX_DAYS_AFTER_FULL = 3       # MC zip labelled up to 3 days after its Full zip (10/25 -> 10/26)
NLSDB_MAX_DAYS_FROM_FULL = 10    # NLSDB GDB exported within 10 days of the MLRS extract

ZIP_RE = {
    'full':  re.compile(r'^(\d{4}-\d{2}-\d{2})_MLRS_Full\.zip$', re.I),
    'mc':    re.compile(r'^(\d{4}-\d{2}-\d{2})_MLRS_MC\.zip$', re.I),
    'nlsdb': re.compile(r'^(\d{4}-\d{2}-\d{2})_NLSDB\.gdb\.zip$', re.I),
}
TAR_RE = re.compile(r'(?:^|/)(CR_FULL|MC)_([A-Z]{2})_(\d{8})\.tar$')


def load_config(path):
    if not os.path.isfile(path):
        print(f'ERROR: config file not found: {path}')
        print(f'  Copy {os.path.join(SCRIPT_DIR, "monthly_build.example.json")} to that name and fill in this')
        print('  machine\'s paths. (It is git-ignored: it holds local paths only.)')
        sys.exit(1)
    with open(path, encoding='utf-8') as f:
        cfg = json.load(f)
    for key in ('data_root', 'mlrs_drop', 'nlsdb_drop'):
        if not cfg.get(key):
            print(f'ERROR: "{key}" is missing from {path}')
            sys.exit(1)
    return cfg


def to_date(label):
    return dt.datetime.strptime(label, '%Y-%m-%d').date()


def scan_drop(folder, kind):
    """{date: full path} for the zips of one kind in a drop folder."""
    if not os.path.isdir(folder):
        print(f'ERROR: drop folder not found or not reachable: {folder}')
        print('  Is Google Drive for desktop running and signed in?')
        sys.exit(1)
    found = {}
    for name in os.listdir(folder):
        m = ZIP_RE[kind].match(name)
        if m:
            found[to_date(m.group(1))] = os.path.join(folder, name)
    return found


def loaded_snapshots():
    """Dates (YYYYMMDD strings) that have a blm_case_<date> table in BigQuery."""
    bq = shutil.which('bq') or shutil.which('bq.cmd')
    if not bq:
        print('ERROR: bq not found on PATH (Google Cloud SDK).')
        sys.exit(1)
    r = subprocess.run([bq, 'ls', '--max_results=100000', '--format=json', f'{PROJECT}:{DATASET}'],
                       capture_output=True, text=True)
    if r.returncode != 0:
        print('ERROR: could not list the BigQuery dataset. If the message mentions')
        print('  reauthentication, run `gcloud auth login` in a terminal and try again.')
        print('  ' + (r.stderr or r.stdout).strip()[:600])
        sys.exit(1)
    tables = json.loads(r.stdout or '[]')
    out = set()
    for t in tables:
        m = re.fullmatch(r'blm_case_(\d{8})', t.get('tableReference', {}).get('tableId', ''))
        if m:
            out.add(m.group(1))
    return out


def pair_deliveries(full, mc, nlsdb):
    """One entry per MLRS Full zip, with its MC and NLSDB partners and any pairing notes."""
    deliveries = []
    for d in sorted(full):
        notes = []
        mc_d = d if d in mc else next(
            (x for x in sorted(mc) if 0 < (x - d).days <= MC_MAX_DAYS_AFTER_FULL), None)
        if mc_d is None:
            notes.append(f'no MC zip dated {d} to {d + dt.timedelta(days=MC_MAX_DAYS_AFTER_FULL)}')
        elif mc_d != d:
            notes.append(f'MC zip is labelled {mc_d}, Full zip {d}')
        near = sorted(nlsdb, key=lambda x: (abs((x - d).days), x < d))
        nl_d = near[0] if near and abs((near[0] - d).days) <= NLSDB_MAX_DAYS_FROM_FULL else None
        if nl_d is None:
            notes.append(f'no NLSDB zip within {NLSDB_MAX_DAYS_FROM_FULL} days of {d}')
        elif nl_d < d:
            notes.append(f'NLSDB zip ({nl_d}) is dated BEFORE the MLRS Full zip ({d})')
        deliveries.append({
            'label': d, 'full': full[d],
            'mc_label': mc_d, 'mc': mc.get(mc_d) if mc_d else None,
            'nlsdb_label': nl_d, 'nlsdb': nlsdb.get(nl_d) if nl_d else None,
            'notes': notes,
        })
    # An NLSDB zip claimed by two deliveries means one of them has no NLSDB of its own yet.
    used = {}
    for x in deliveries:
        if x['nlsdb_label']:
            used.setdefault(x['nlsdb_label'], []).append(x['label'])
    for x in deliveries:
        others = [str(o) for o in used.get(x['nlsdb_label'], []) if o != x['label']]
        if others:
            x['notes'].append(f'NLSDB zip {x["nlsdb_label"]} is also the nearest for {", ".join(others)}')
    return deliveries


def month_folder_for(data_root, label):
    """<Month>_<YYYY>; <Month>_<YYYY>_<YYYYMMDD> when that folder already holds another delivery."""
    base = os.path.join(data_root, f'{calendar.month_name[label.month]}_{label.year}')
    mine = label.strftime('%Y-%m-%d')
    others = set()
    for p in glob.glob(os.path.join(base, '*_MLRS_Full*')):
        m = re.match(r'(\d{4}-\d{2}-\d{2})_MLRS_Full', os.path.basename(p), re.I)
        if m and m.group(1) != mine:
            others.add(m.group(1))
    if others:
        return base + '_' + label.strftime('%Y%m%d'), sorted(others)
    return base, []


def copy_zip(src, dest_dir):
    """Copy one zip, reusing an identical-size copy already there. Returns the local path."""
    dest = os.path.join(dest_dir, os.path.basename(src))
    size = os.path.getsize(src)
    if os.path.isfile(dest) and os.path.getsize(dest) == size:
        print(f'  {os.path.basename(src)}: already in the month folder ({size / 1e9:.2f} GB) — reused')
        return dest
    if os.path.exists(dest):
        print(f'ERROR: {dest} exists with a different size from the shared copy.')
        print('  Not overwriting it. Check which one is right, move the wrong one aside, and re-run.')
        sys.exit(1)
    print(f'  Copying {os.path.basename(src)} ({size / 1e9:.2f} GB) ...', flush=True)
    tmp = dest + '.partial'
    done, step, next_mark = 0, size // 10 or 1, size // 10 or 1
    with open(src, 'rb') as fi, open(tmp, 'wb') as fo:
        while True:
            buf = fi.read(16 * 1024 * 1024)
            if not buf:
                break
            fo.write(buf)
            done += len(buf)
            if done >= next_mark:
                print(f'    {100 * done // size}%', flush=True)
                next_mark += step
    if os.path.getsize(tmp) != size:
        os.remove(tmp)
        print(f'ERROR: copy of {os.path.basename(src)} is incomplete (size mismatch). Re-run to retry.')
        sys.exit(1)
    os.replace(tmp, dest)
    return dest


def inspect_mlrs_zip(path, kind):
    """Dates and states of the tars inside an MLRS zip (reads the zip's directory only)."""
    try:
        with zipfile.ZipFile(path) as z:
            names = z.namelist()
    except zipfile.BadZipFile:
        print(f'ERROR: {path} is not a readable zip (damaged or incomplete upload).')
        sys.exit(1)
    prefix = 'CR_FULL' if kind == 'full' else 'MC'
    dates, states = set(), set()
    for n in names:
        m = TAR_RE.search(n)
        if m and m.group(1) == prefix:
            states.add(m.group(2))
            dates.add(m.group(3))
    return sorted(dates), sorted(states)


def inspect_nlsdb_zip(path):
    try:
        with zipfile.ZipFile(path) as z:
            tops = {n.split('/')[0] for n in z.namelist() if n.split('/')[0]}
    except zipfile.BadZipFile:
        print(f'ERROR: {path} is not a readable zip (damaged or incomplete upload).')
        sys.exit(1)
    return sorted(t for t in tops if t.lower().endswith('.gdb'))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--config', default=DEFAULT_CONFIG)
    ap.add_argument('--list', action='store_true', help='list deliveries and loaded snapshots; copy nothing')
    ap.add_argument('--date', help='MLRS Full zip date to fetch, YYYYMMDD (default: the next one to build)')
    ap.add_argument('--allow-rebuild', action='store_true',
                    help='fetch a delivery whose snapshot is already loaded (the build would replace it)')
    args = ap.parse_args()
    cfg = load_config(args.config)

    full = scan_drop(cfg['mlrs_drop'], 'full')
    mc = scan_drop(cfg['mlrs_drop'], 'mc')
    nlsdb = scan_drop(cfg['nlsdb_drop'], 'nlsdb')
    deliveries = pair_deliveries(full, mc, nlsdb)
    loaded = loaded_snapshots()
    latest_loaded = max(loaded) if loaded else None

    print(f'Shared MLRS folder : {cfg["mlrs_drop"]}')
    print(f'Shared NLSDB folder: {cfg["nlsdb_drop"]}')
    print(f'Newest snapshot in BigQuery: {latest_loaded or "none"}')
    print()
    print('Most recent deliveries (by MLRS Full zip date):')
    for x in deliveries[-8:]:
        key = x['label'].strftime('%Y%m%d')
        state = 'LOADED' if key in loaded else 'not loaded'
        mc_l, nl_l = str(x['mc_label'] or '-'), str(x['nlsdb_label'] or '-')
        print(f'  {x["label"]}  MC {mc_l:<10}  NLSDB {nl_l:<10}  {state}')
        for n in x['notes']:
            print(f'      note: {n}')
    if args.list:
        return 0

    # --- choose the delivery ---
    if args.date:
        want = dt.datetime.strptime(args.date, '%Y%m%d').date()
        chosen = next((x for x in deliveries if x['label'] == want), None)
        if not chosen:
            print(f'\nERROR: no {want}_MLRS_Full.zip in {cfg["mlrs_drop"]}')
            return 1
    else:
        pending = [x for x in deliveries
                   if x['label'].strftime('%Y%m%d') not in loaded
                   and (latest_loaded is None or x['label'].strftime('%Y%m%d') > latest_loaded)]
        if not pending:
            print('\nNothing new to build: every delivery newer than the latest snapshot is already loaded.')
            return 3
        chosen = pending[0]
        if len(pending) > 1:
            print(f'\nNOTE: {len(pending)} deliveries are waiting; fetching the oldest, {chosen["label"]}. '
                  f'Run again after it is built for the next one.')

    review = []
    label = chosen['label']
    print(f'\nFetching the delivery labelled {label}')
    if not chosen['mc'] or not chosen['nlsdb']:
        for n in chosen['notes']:
            print(f'  {n}')
        print('ERROR: this delivery is incomplete in the shared folders; not fetching it.')
        return 5
    for n in chosen['notes']:
        if 'BEFORE' in n or 'also the nearest' in n:
            review.append(n)

    folder, others = month_folder_for(cfg['data_root'], label)
    if others:
        print(f'  The month folder already holds delivery {", ".join(others)}; using a separate folder.')
    print(f'  Month folder: {folder}')
    os.makedirs(folder, exist_ok=True)

    srcs = [chosen['full'], chosen['mc'], chosen['nlsdb']]
    need = sum(os.path.getsize(s) for s in srcs)
    to_copy = sum(os.path.getsize(s) for s in srcs
                  if not os.path.isfile(os.path.join(folder, os.path.basename(s))))
    free = shutil.disk_usage(folder).free
    if to_copy and free < need * SPACE_FACTOR:
        print(f'ERROR: {free / 1e9:.0f} GB free on the data drive; this delivery needs about '
              f'{need * SPACE_FACTOR / 1e9:.0f} GB once unpacked ({SPACE_FACTOR}x its {need / 1e9:.1f} GB of zips).')
        return 4

    local = [copy_zip(s, folder) for s in srcs]

    # --- what is inside ---
    full_dates, full_states = inspect_mlrs_zip(local[0], 'full')
    mc_dates, mc_states = inspect_mlrs_zip(local[1], 'mc')
    gdbs = inspect_nlsdb_zip(local[2])
    print(f'  Full tars: {len(full_states)} states, dated {", ".join(full_dates) or "?"}')
    print(f'  MC tars  : {len(mc_states)} states, dated {", ".join(mc_dates) or "?"}')
    print(f'  NLSDB    : {", ".join(gdbs) or "no .gdb folder"}')

    if len(full_dates) != 1:
        review.append(f'the Full zip holds tars with {len(full_dates)} different dates: {full_dates}')
    for kind, states in (('Full', full_states), ('MC', mc_states)):
        missing = sorted(set(EXPECTED_STATES) - set(states))
        extra = sorted(set(states) - set(EXPECTED_STATES))
        if missing:
            review.append(f'{kind} zip has no tar for: {", ".join(missing)}')
        if extra:
            review.append(f'{kind} zip has tars for unexpected states: {", ".join(extra)}')
    if len(gdbs) != 1:
        review.append(f'the NLSDB zip holds {len(gdbs)} .gdb folders: {gdbs}')

    snapshot = full_dates[0] if len(full_dates) == 1 else label.strftime('%Y%m%d')
    notes = list(chosen['notes'])
    if snapshot != label.strftime('%Y%m%d'):
        notes.append(f'zip label {label} but tars dated {snapshot}: building as {snapshot}')
    if mc_dates and mc_dates != [snapshot]:
        review.append(f'MC tars are dated {mc_dates}, Full tars {snapshot}')
    if snapshot in loaded and not args.allow_rebuild:
        review.append(f'snapshot {snapshot} is already in BigQuery; building it again would replace '
                      f'its tables (re-run with --allow-rebuild if that is intended)')

    record = {
        'snapshot_date': snapshot,
        'month_folder': folder,
        'fetched_at': dt.datetime.now().isoformat(timespec='seconds'),
        'zips': {
            'full':  {'source': chosen['full'],  'local': local[0], 'bytes': os.path.getsize(local[0]),
                      'tar_dates': full_dates, 'states': full_states},
            'mc':    {'source': chosen['mc'],    'local': local[1], 'bytes': os.path.getsize(local[1]),
                      'tar_dates': mc_dates, 'states': mc_states},
            'nlsdb': {'source': chosen['nlsdb'], 'local': local[2], 'bytes': os.path.getsize(local[2]),
                      'gdb': gdbs},
        },
        'notes': notes,
        'review': review,
    }
    out = os.path.join(folder, f'delivery_{snapshot}.json')
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(record, f, indent=2)

    print(f'\n  Snapshot date: {snapshot}')
    print(f'  Recorded in  : {out}')
    for n in notes:
        print(f'  note: {n}')
    if review:
        print('\nREVIEW — needs a person before the build continues:')
        for r in review:
            print(f'  - {r}')
        return 5
    print('\nFetched. Next: run_build.ps1 -Step prepare -Date ' + snapshot + ' -CheckOnly')
    return 0


if __name__ == '__main__':
    sys.exit(main())
