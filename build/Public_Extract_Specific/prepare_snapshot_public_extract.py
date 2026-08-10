"""
prepare_snapshot_public_extract.py — BLM DQIMP snapshot prep (NLSDB Public Extract variant)

This is a modified version of prepare_snapshot.py for use when BLM does not provide
a standard NLSDB GDB snapshot and the data is instead extracted from the Esri API.
The public extract GDB differs from the standard BLM GDB:
  - Zip naming: YYYY-MM-DD_NLSDB_Public_Extract.zip (not _NLSDB.gdb.zip)
  - GDB naming: nlsdb_public_extract_MMDDYYYY.gdb (not nlsdb_MMDDYYYY.gdb)
  - Layer names: "case" and "case_lands" (lowercase, no date/time suffix)
  - Schema (ogr2ogr path, the standardized go-forward path): fewer fields, dates
    stored as Real (epoch ms) instead of Timestamp; Case has OBJECTID + fid,
    CaseLands has its own 28-field schema; geometry column is named 'geom' (WKB)
    with a junk numeric 'Shape' column alongside. (A Feb 2026-style ArcPro-converted
    GDB instead has OBJECTID_1 / nlsdb_Shape / Shape_Length / Shape_Area and
    Timestamp dates — that path is deprecated.)

RECOMMENDED USAGE (from inside the month folder, no arguments needed):
  python prepare_snapshot_public_extract.py

After this completes, run:
  .\\BLM_DQIMP_OneStop_param_v4_public_extract.ps1 -Date <YYYYMMDD>

GCS layout after upload:
  gs://<bucket>/snapshots/<YYYYMMDD>/
    <TABLE>/
      CR_FULL_<TABLE>_<STATE>.load   (one per table × state combination)
      MC_<TABLE>_<STATE>.load        (one per table × state combination)
    Case_<YYYYMMDD>.parquet          (Case layer, Status Records excluded)
    StatusRecords_<YYYYMMDD>.parquet (Case layer, Status Records only — produced
                                      when the Case layer has a CSE_DISP field)
    CaseLands_<YYYYMMDD>.parquet
"""

import argparse
import glob
import gzip
import json
import os
import shutil
import subprocess
import sys
import tarfile
import zipfile

DEFAULT_BUCKET = 'sandbox-blm-seta-dqimp-qaqc'

# The 24 BQ schema JSONs are located at runtime. NO path to any particular workstation
# appears in this file: the repo's own schemas/mlrs_export_schemas/ is the single source
# of truth. Search order:
#   1. any ancestor directory containing schemas/mlrs_export_schemas  (the repo, at any depth)
#   2. a schemas/ folder sitting next to this script
#   3. $BLM_DQIMP_SCHEMA_DIR — for running from a month folder outside the repo
# --schema-dir overrides the search entirely. If nothing is found the script stops and
# prints what it looked at, rather than guessing.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_ENV_VAR = 'BLM_DQIMP_SCHEMA_DIR'


def _has_schemas(path):
    """True if `path` exists and contains at least one *_schema.json file."""
    return bool(path) and bool(glob.glob(os.path.join(path, '*_schema.json')))


def resolve_schema_dir():
    """Locate the folder holding the 24 BQ schema JSONs. Returns None if not found."""
    # 1. walk up from this script looking for the repo's schemas/mlrs_export_schemas
    directory = _SCRIPT_DIR
    while True:
        candidate = os.path.join(directory, 'schemas', 'mlrs_export_schemas')
        if _has_schemas(candidate):
            return os.path.normpath(candidate)
        parent = os.path.dirname(directory)
        if parent == directory:
            break
        directory = parent
    # 2. a schemas/ folder beside this script
    beside = os.path.join(_SCRIPT_DIR, 'schemas')
    if _has_schemas(beside):
        return os.path.normpath(beside)
    # 3. explicit environment override, for month folders outside the repo
    from_env = os.environ.get(SCHEMA_ENV_VAR)
    if _has_schemas(from_env):
        return os.path.normpath(from_env)
    return None


def schema_dir_search_description():
    """Human-readable list of everything resolve_schema_dir() considered."""
    return [
        f'ancestor directories of {_SCRIPT_DIR} containing schemas\\mlrs_export_schemas',
        os.path.join(_SCRIPT_DIR, 'schemas'),
        f'${SCHEMA_ENV_VAR} (currently: {os.environ.get(SCHEMA_ENV_VAR) or "not set"})',
    ]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def run_cmd(cmd, label):
    """Run an external command, print pass/warn, return exit code."""
    print(f'    Running: {" ".join(str(c) for c in cmd)}')
    # shell=True required on Windows so .cmd/.bat wrappers (gsutil, bq) are found on PATH
    result = subprocess.run(cmd, shell=True)
    if result.returncode == 0:
        print(f'    OK: {label}')
    else:
        print(f'    WARNING: {label} returned exit code {result.returncode}')
    return result.returncode


def find_one(pattern, label):
    """
    Find exactly one file/folder matching glob pattern.
    Returns the match, or None if nothing found.
    Warns (but picks the first) if multiple are found.
    """
    matches = sorted(glob.glob(pattern))
    if not matches:
        return None
    if len(matches) > 1:
        print(f'  WARNING: multiple {label} matches found — using {matches[0]}')
    return matches[0]


def table_name_from_load(filename):
    """Extract table name from CR_FULL_{TABLE}_{STATE}.load or MC_{TABLE}_{STATE}.load."""
    stem = os.path.basename(filename)[:-5]  # strip .load
    if stem.startswith('CR_FULL_'):
        stem = stem[8:]   # strip CR_FULL_
    elif stem.startswith('MC_'):
        stem = stem[3:]   # strip MC_
    return stem[:-3]      # strip _{STATE} (underscore + 2-letter state code)


# ---------------------------------------------------------------------------
# Step 0 — Unzip incoming BLM data
# ---------------------------------------------------------------------------

def unzip_mlrs(zip_path, suffix):
    """
    Unzip an MLRS zip file (Full or MC).

    BLM delivers these as YYYY-MM-DD_MLRS_<suffix>.zip. When unzipped, the
    top-level folder inside is named YYYY-MM-DD (the date only, no suffix).
    This function renames it to YYYY-MM-DD_MLRS_<suffix> so it matches what
    the rest of the script expects.

    Skips the unzip if the renamed folder already exists.
    Returns the final folder name.
    """
    basename = os.path.basename(zip_path)           # e.g. 2026-06-07_MLRS_Full.zip
    date_str = basename.split('_')[0]               # e.g. 2026-06-07
    final_name = f'{date_str}_MLRS_{suffix}'        # e.g. 2026-06-07_MLRS_Full

    if os.path.isdir(final_name):
        print(f'  {final_name} already exists — skipping unzip')
        return final_name

    print(f'  Unzipping {basename} ...')
    with zipfile.ZipFile(zip_path) as z:
        top_items = set(p.split('/')[0] for p in z.namelist() if p.split('/')[0])
        z.extractall('.')

    # The extracted folder is usually just the date string; rename it.
    extracted = top_items.pop() if len(top_items) == 1 else date_str
    if extracted != final_name and os.path.exists(extracted):
        print(f'  Renaming {extracted} -> {final_name}')
        os.rename(extracted, final_name)

    if not os.path.isdir(final_name):
        print(f'  ERROR: expected folder {final_name} not found after unzip of {basename}')
        sys.exit(1)

    return final_name


def unzip_nlsdb(zip_path):
    """
    Unzip an NLSDB GDB zip file.

    BLM delivers this as YYYY-MM-DD_NLSDB.gdb.zip. The folder inside the zip
    is already correctly named nlsdb_MMDDYYYY.gdb (note: MMDDYYYY format, not
    YYYYMMDD). No rename is needed.

    Skips the unzip if the .gdb folder already exists.
    Returns the GDB folder name.
    """
    basename = os.path.basename(zip_path)

    with zipfile.ZipFile(zip_path) as z:
        top_items = set(p.split('/')[0] for p in z.namelist() if p.split('/')[0])
        gdb_folders = [t for t in top_items if t.lower().endswith('.gdb')]

        if not gdb_folders:
            print(f'  ERROR: no .gdb folder found inside {basename}')
            sys.exit(1)

        gdb_name = gdb_folders[0]

        if os.path.isdir(gdb_name):
            print(f'  {gdb_name} already exists — skipping unzip')
            return gdb_name

        print(f'  Unzipping {basename} ...')
        z.extractall('.')

    if not os.path.isdir(gdb_name):
        print(f'  ERROR: expected GDB folder {gdb_name} not found after unzip')
        sys.exit(1)

    return gdb_name


def resolve_inputs(args):
    """
    Determine full_dir, mc_dir, gdb_path, and date from either explicit args
    or by auto-detecting and unzipping zip files in the current directory.
    Returns (full_dir, mc_dir, gdb_path, date).
    """
    full_dir = args.full_dir
    mc_dir   = args.mc_dir
    gdb_path = args.gdb
    date     = args.date

    # --- MLRS Full ---
    if not full_dir:
        zip_path = find_one('*_MLRS_Full.zip', 'MLRS Full zip')
        if zip_path:
            full_dir = unzip_mlrs(zip_path, 'Full')
        else:
            full_dir = find_one('*_MLRS_Full', 'MLRS Full folder')
            if full_dir:
                print(f'  Using existing MLRS Full folder: {full_dir}')
            else:
                print('ERROR: no *_MLRS_Full.zip or *_MLRS_Full folder found in current directory.')
                sys.exit(1)

    # --- MLRS MC ---
    if not mc_dir:
        zip_path = find_one('*_MLRS_MC.zip', 'MLRS MC zip')
        if zip_path:
            mc_dir = unzip_mlrs(zip_path, 'MC')
        else:
            mc_dir = find_one('*_MLRS_MC', 'MLRS MC folder')
            if mc_dir:
                print(f'  Using existing MLRS MC folder: {mc_dir}')
            else:
                print('ERROR: no *_MLRS_MC.zip or *_MLRS_MC folder found in current directory.')
                sys.exit(1)

    # --- NLSDB GDB (public extract variant) ---
    if not gdb_path:
        zip_path = find_one('*_NLSDB_Public_Extract.zip', 'NLSDB Public Extract zip')
        if not zip_path:
            zip_path = find_one('*_NLSDB.gdb.zip', 'NLSDB GDB zip')
        if zip_path:
            gdb_path = unzip_nlsdb(zip_path)
        else:
            gdb_path = find_one('*public_extract*.gdb', 'NLSDB Public Extract GDB folder')
            if not gdb_path:
                gdb_path = find_one('*.gdb', 'NLSDB GDB folder')
            if gdb_path:
                print(f'  Using existing GDB: {gdb_path}')
            else:
                print('ERROR: no NLSDB GDB zip or folder found in current directory.')
                sys.exit(1)

    # --- Date ---
    if not date:
        # Derive from the MLRS Full folder name: 2026-06-07_MLRS_Full -> 20260607
        date_str = os.path.basename(full_dir).split('_')[0]  # "2026-06-07"
        date = date_str.replace('-', '')                      # "20260607"
        print(f'  Auto-derived snapshot date: {date}')

    return full_dir, mc_dir, gdb_path, date


# ---------------------------------------------------------------------------
# Steps 1 & 2 — MLRS tar extraction
# ---------------------------------------------------------------------------

def extract_tars(folder):
    """
    Unzip every .tar in `folder` into a same-named subdirectory, then
    decompress all .gz files inside each subdirectory in place.
    Skips subdirectories that already exist (safe to re-run).
    """
    tar_paths = sorted(glob.glob(os.path.join(folder, '*.tar')))
    if not tar_paths:
        print(f'  WARNING: no .tar files found in {folder}')
        return

    for tar_path in tar_paths:
        out_dir = tar_path[:-4]  # strip ".tar"
        if os.path.exists(out_dir):
            print(f'  Skipping {os.path.basename(tar_path)} — output dir already exists')
            continue
        os.makedirs(out_dir)
        print(f'  Extracting {os.path.basename(tar_path)} ...')
        with tarfile.open(tar_path) as tf:
            tf.extractall(path=out_dir)

        for gz_path in glob.glob(os.path.join(out_dir, '*.gz')):
            out_path = gz_path[:-3]  # strip ".gz" -> .load
            with gzip.open(gz_path, 'rb') as f_in:
                with open(out_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(gz_path)


# ---------------------------------------------------------------------------
# Column-count validation
# ---------------------------------------------------------------------------

def validate_load_files(folder, schema_dir):
    """
    Scan .load files for rows whose pipe count doesn't match the schema's column
    count. Rows with extra pipes contain an embedded delimiter that BQ will
    misparse (BQ has no way to distinguish data pipes from delimiter pipes).
    Runs in seconds — just counting characters per line, no parsing.
    """
    expected_cols = {}
    for schema_file in sorted(glob.glob(os.path.join(schema_dir, '*_schema.json'))):
        with open(schema_file) as f:
            schema = json.load(f)
        tname = os.path.basename(schema_file).replace('_schema.json', '').upper()
        expected_cols[tname] = len(schema)

    if not expected_cols:
        print(f'  WARNING: no schema files found in {schema_dir} — skipping validation')
        return 0

    state_dirs = sorted(d for d in glob.glob(os.path.join(folder, '*')) if os.path.isdir(d))
    total_bad = 0

    for state_dir in state_dirs:
        for load_file in sorted(glob.glob(os.path.join(state_dir, '*.load'))):
            if os.path.getsize(load_file) == 0:
                continue
            tname = table_name_from_load(load_file)
            if tname not in expected_cols:
                continue
            expected = expected_cols[tname]
            fname = os.path.basename(load_file)
            bad_lines = []
            with open(load_file, 'r', encoding='utf-8', errors='replace') as f:
                for i, line in enumerate(f, 1):
                    actual = line.rstrip('\n').rstrip('\r').count('|') + 1
                    if actual != expected:
                        bad_lines.append((i, actual))
            if bad_lines:
                total_bad += len(bad_lines)
                print(f'  WARNING: {fname}: {len(bad_lines)} row(s) with wrong column count '
                      f'(expected {expected}):')
                for line_num, actual_cols in bad_lines[:5]:
                    print(f'    line {line_num}: found {actual_cols} columns')
                if len(bad_lines) > 5:
                    print(f'    ... and {len(bad_lines) - 5} more')

    if total_bad:
        print(f'  TOTAL: {total_bad} row(s) with column count mismatches')
        print(f'  These rows will cause BQ load errors (--max_bad_records=0).')
        print(f'  Fix the affected .load files before continuing, or accept the data loss.')
    else:
        print(f'  All rows have correct column counts.')

    return total_bad


# ---------------------------------------------------------------------------
# Step 3 — Upload .load files to GCS
# ---------------------------------------------------------------------------

def upload_load_files(folder, gcs_prefix):
    """
    Upload per-state .load files to per-table GCS subdirectories, skipping empty files.

    Groups files by table name and uploads each table's files to
    {gcs_prefix}/{table_name}/ so BQ wildcard loads match exactly the right
    table without cross-table name prefix collisions (e.g. ACTION vs ACTION_CASE).
    Empty files (0 bytes) represent states with no records and are skipped.
    """
    state_dirs = sorted(
        d for d in glob.glob(os.path.join(folder, '*')) if os.path.isdir(d)
    )

    by_table = {}
    total_skipped = 0
    for state_dir in state_dirs:
        for f in sorted(glob.glob(os.path.join(state_dir, '*.load'))):
            if os.path.getsize(f) == 0:
                total_skipped += 1
                continue
            tname = table_name_from_load(f)
            by_table.setdefault(tname, []).append(f)

    if not by_table:
        print(f'  WARNING: no non-empty .load files found under {folder}')
        return

    for tname, tfiles in sorted(by_table.items()):
        run_cmd(
            ['gsutil', '-o', 'GSUtil:check_hashes=always', '-m', 'cp'] + tfiles + [f'{gcs_prefix}/{tname}/'],
            f'upload {tname} ({len(tfiles)} files)'
        )

    if total_skipped:
        print(f'  Skipped {total_skipped} empty .load files (states with no records)')
    print(f'  Total uploaded: {sum(len(v) for v in by_table.values())} files')


# ---------------------------------------------------------------------------
# Step 4 — NLSDB GDB → Parquet via ogr2ogr + upload
# ---------------------------------------------------------------------------

def convert_gdb_to_parquet(gdb_path, date):
    """
    Auto-detect the Case_* and CaseLands_* layers in the GDB via fiona,
    convert each to Parquet using ogr2ogr (same flags as original extract_parquet.txt),
    and name the output Case_<YYYYMMDD>.parquet / CaseLands_<YYYYMMDD>.parquet so the
    OneStop PS1 can construct the GCS path from $Date alone.
    Returns (case_parquet_path, case_land_parquet_path).
    """
    try:
        import fiona
        layers = fiona.listlayers(gdb_path)
    except Exception as e:
        print(f'  ERROR listing GDB layers: {e}')
        sys.exit(1)

    # Public extract layer naming varies by month:
    #   - 'case' / 'case_lands' (e.g. February 2026)
    #   - 'nlsdb_public_extract_MMDDYYYY' / 'case_lands' (e.g. November 2025)
    #   - 'Case_MMDDYYYY_HHMM' / 'CaseLands_MMDDYYYY_HHMM' (standard BLM GDB)
    case_layers = [l for l in layers if l.startswith('Case_') or l == 'case'
                   or l.startswith('nlsdb_public_extract_')]
    case_land_layers = [l for l in layers if l.startswith('CaseLands_') or l == 'case_lands']

    if not case_layers:
        print(f'  ERROR: no Case_*/case/nlsdb_public_extract_* layer in GDB. Layers present: {layers}')
        sys.exit(1)
    if not case_land_layers:
        print(f'  ERROR: no CaseLands_*/case_lands layer in GDB. Layers present: {layers}')
        sys.exit(1)

    case_layer      = case_layers[0]
    case_land_layer = case_land_layers[0]
    print(f'  Detected GDB layers: {case_layer}, {case_land_layer}')

    case_parquet      = f'Case_{date}.parquet'
    case_land_parquet = f'CaseLands_{date}.parquet'
    status_parquet    = f'StatusRecords_{date}.parquet'

    # Public extracts include Status Records (CSE_DISP = 'Status Record', ~49% of raw
    # Case rows) which are not present in the standard BLM GDB snapshots. Rather than
    # discard them, the Case layer is split into two parquets:
    #   Case_<date>.parquet          — Status Records excluded, keeps nlsdb_case_<date>
    #                                  consistent with standard-GDB months
    #   StatusRecords_<date>.parquet — Status Records only, loaded to a separate
    #                                  nlsdb_status_records_<date> BQ table
    # The split requires the layer to have a CSE_DISP field (ogr2ogr errors on a WHERE
    # clause referencing a nonexistent field). CaseLands typically lacks CSE_DISP and
    # has no Status Records — it is filtered defensively only when the field exists.
    def layer_has_field(layer, field):
        with fiona.open(gdb_path, layer=layer) as src:
            return field in src.schema['properties']

    base_flags = ['-makevalid', '-lco', 'GEOMETRY=AS_WKB', '-t_srs', 'EPSG:4326']

    if layer_has_field(case_layer, 'CSE_DISP'):
        rc = run_cmd(
            ['ogr2ogr', '-f', 'Parquet', case_parquet, gdb_path, case_layer]
            + base_flags + ['-where', "CSE_DISP <> 'Status Record'", '-progress'],
            f'ogr2ogr {case_layer} -> {case_parquet} (excluding Status Records)'
        )
        if rc != 0:
            print(f'  ERROR: ogr2ogr failed for {case_layer} — is the dev conda env active? Aborting.')
            sys.exit(1)
        rc = run_cmd(
            ['ogr2ogr', '-f', 'Parquet', status_parquet, gdb_path, case_layer]
            + base_flags + ['-where', "CSE_DISP = 'Status Record'", '-progress'],
            f'ogr2ogr {case_layer} -> {status_parquet} (Status Records only)'
        )
        if rc != 0:
            print(f'  ERROR: ogr2ogr failed for {case_layer} Status Records split. Aborting.')
            sys.exit(1)
    else:
        status_parquet = None
        print(f'  WARNING: layer {case_layer} has no CSE_DISP field — the Case layer')
        print(f'  cannot be split. ALL rows (including any Status Records) will go into')
        print(f'  {case_parquet} and load into nlsdb_case_{date} (see the')
        print(f'  nlsdb_case_20251001 incident). Verify before running DQ queries')
        print(f'  against this snapshot.')
        rc = run_cmd(
            ['ogr2ogr', '-f', 'Parquet', case_parquet, gdb_path, case_layer]
            + base_flags + ['-progress'],
            f'ogr2ogr {case_layer} -> {case_parquet} (unfiltered — no CSE_DISP field)'
        )
        if rc != 0:
            print(f'  ERROR: ogr2ogr failed for {case_layer} — is the dev conda env active? Aborting.')
            sys.exit(1)

    if layer_has_field(case_land_layer, 'CSE_DISP'):
        cl_where, cl_label = ['-where', "CSE_DISP <> 'Status Record'"], ' (excluding Status Records)'
    else:
        print(f'  Note: layer {case_land_layer} has no CSE_DISP field — Status Record filter '
              f'not applicable, converting unfiltered.')
        cl_where, cl_label = [], ''
    rc = run_cmd(
        ['ogr2ogr', '-f', 'Parquet', case_land_parquet, gdb_path, case_land_layer]
        + base_flags + cl_where + ['-progress'],
        f'ogr2ogr {case_land_layer} -> {case_land_parquet}{cl_label}'
    )
    if rc != 0:
        print(f'  ERROR: ogr2ogr failed for {case_land_layer} — is the dev conda env active? Aborting.')
        sys.exit(1)

    return case_parquet, case_land_parquet, status_parquet


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            'BLM DQIMP monthly snapshot prep — unzip, extract, upload .load files to GCS.\n'
            'Run with no arguments from the month folder for fully automatic operation.\n'
            'BigQuery wildcard loads are handled by BLM_DQIMP_OneStop_param_v4.ps1.'
        )
    )
    # All args optional — auto-detected from zip files when omitted
    parser.add_argument('--date', default=None,
                        help='Snapshot date YYYYMMDD for BQ table names (auto-derived from '
                             'MLRS zip filename if omitted)')
    parser.add_argument('--bucket', default=DEFAULT_BUCKET,
                        help=f'GCS bucket name (default: {DEFAULT_BUCKET})')
    parser.add_argument('--full-dir', default=None,
                        help='Pre-unzipped MLRS Full folder — skips zip auto-detect and unzip')
    parser.add_argument('--mc-dir', default=None,
                        help='Pre-unzipped MLRS MC folder — skips zip auto-detect and unzip')
    parser.add_argument('--gdb', default=None,
                        help='Pre-unzipped NLSDB GDB path — skips zip auto-detect and unzip')
    parser.add_argument('--schema-dir', default=None,
                        help='Directory containing BQ schema JSON files for column count '
                             'validation (default: auto-detected — the repo\'s '
                             'schemas/mlrs_export_schemas found by walking up from this '
                             'script, then a schemas/ folder beside it, then '
                             f'${SCHEMA_ENV_VAR})')
    args = parser.parse_args()

    # --- Step 0: Unzip / locate inputs ---
    print(f'\n{"="*60}')
    print(f'[0/4] Locating and unzipping inputs')
    print(f'{"="*60}')
    full_dir, mc_dir, gdb_path, date = resolve_inputs(args)
    gcs_prefix = f'gs://{args.bucket}/snapshots/{date}'
    print(f'  Snapshot date : {date}')
    print(f'  MLRS Full     : {full_dir}')
    print(f'  MLRS MC       : {mc_dir}')
    print(f'  NLSDB GDB     : {gdb_path}')
    print(f'  GCS target    : {gcs_prefix}')

    # --- Step 1: Extract MLRS Full tars ---
    print(f'\n{"="*60}')
    print(f'[1/4] Extracting MLRS Full: {full_dir}')
    print(f'{"="*60}')
    extract_tars(full_dir)

    # --- Step 2: Extract MLRS MC tars ---
    print(f'\n{"="*60}')
    print(f'[2/4] Extracting MLRS MC: {mc_dir}')
    print(f'{"="*60}')
    extract_tars(mc_dir)

    # --- Validate column counts against schemas ---
    print(f'\n{"="*60}')
    print(f'Validating .load file column counts against schemas')
    print(f'{"="*60}')
    schema_dir = args.schema_dir or resolve_schema_dir()
    if not schema_dir:
        print('  ERROR: could not locate the BQ schema JSONs. Searched:')
        for entry in schema_dir_search_description():
            print(f'    - {entry}')
        print(f'  Pass --schema-dir <path>, or set {SCHEMA_ENV_VAR}.')
        sys.exit(1)
    print(f'  Schema dir    : {schema_dir}')
    bad_full = validate_load_files(full_dir, schema_dir)
    bad_mc = validate_load_files(mc_dir, schema_dir)

    # --- Step 3: Upload .load files to GCS ---
    print(f'\n{"="*60}')
    print(f'[3/4] Uploading .load files to GCS: {gcs_prefix}')
    print(f'{"="*60}')
    upload_load_files(full_dir, gcs_prefix)
    upload_load_files(mc_dir, gcs_prefix)

    # --- Step 4: NLSDB GDB → Parquet → GCS ---
    print(f'\n{"="*60}')
    print(f'[4/4] Converting NLSDB GDB to Parquet + uploading: {gdb_path}')
    print(f'{"="*60}')
    case_pq, case_land_pq, status_pq = convert_gdb_to_parquet(gdb_path, date)
    upload_rcs = [
        run_cmd(['gsutil', '-o', 'GSUtil:check_hashes=always', 'cp', case_pq,      f'{gcs_prefix}/{case_pq}'],      f'upload {case_pq}'),
        run_cmd(['gsutil', '-o', 'GSUtil:check_hashes=always', 'cp', case_land_pq, f'{gcs_prefix}/{case_land_pq}'], f'upload {case_land_pq}'),
    ]
    if status_pq:
        upload_rcs.append(
            run_cmd(['gsutil', '-o', 'GSUtil:check_hashes=always', 'cp', status_pq, f'{gcs_prefix}/{status_pq}'], f'upload {status_pq}')
        )
    if any(rc != 0 for rc in upload_rcs):
        print(f'  ERROR: Parquet upload to GCS failed (hash check or transfer error).')
        print(f'  Re-run the failed gsutil cp command(s) above before running the PS1. Aborting.')
        sys.exit(1)

    # --- Done ---
    print(f'\n{"="*60}')
    print(f'Preparation complete for snapshot {date}.')
    print(f'All files are in: {gcs_prefix}/')
    print()
    print(f'Next step — run the BigQuery load (PUBLIC EXTRACT version):')
    print(f'  .\\BLM_DQIMP_OneStop_param_v4_public_extract.ps1 -Date {date}')
    print(f'{"="*60}')


if __name__ == '__main__':
    main()
