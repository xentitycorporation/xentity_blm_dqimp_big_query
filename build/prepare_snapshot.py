"""
prepare_snapshot.py — BLM DQIMP monthly snapshot preparation

RECOMMENDED USAGE (from inside the month folder, no arguments needed):
  python prepare_snapshot.py

  The script auto-detects the three zip files in the current directory,
  unzips and renames them correctly, converts the NLSDB GDB to Parquet,
  extracts the MLRS tars, checks that everything still has the layout we
  expect, and only then uploads the per-state .load files and the Parquet
  files to GCS. BigQuery wildcard loads (handled by
  BLM_DQIMP_OneStop_param_v4.ps1) consume them from there.

LAYOUT CHECKS (November 2026):
  Before anything is uploaded, the script stops with exit code 2 if BLM has
  changed the layout of the delivery:
    - NLSDB: the Case / CaseLands Parquet columns and types must match
      schemas/nlsdb_schemas/nlsdb_case.json and nlsdb_case_land.json — the same
      files the PS1 builds its BigQuery tables from.
    - MLRS: every state (AK AZ CA CO ES ID MT NM NV OR UT WY) must deliver exactly
      the 24 tables in schemas/mlrs_export_schemas/, and each file's column count
      must match its schema JSON.
  If the change is expected, update the JSON(s), then re-run: finished steps
  (unzip, tar extraction, Parquet conversion) are skipped on a re-run.

  To run the checks without uploading anything:
    python prepare_snapshot.py --check-only

MANUAL OVERRIDE (if inputs are already unzipped or have non-standard names):
  python prepare_snapshot.py `
    --full-dir "2026-06-07_MLRS_Full" `
    --mc-dir   "2026-06-07_MLRS_MC"   `
    --gdb      "nlsdb_06082026.gdb"   `
    --date     20260607

After this completes, run:
  .\\BLM_DQIMP_OneStop_param_v4.ps1 -Date <YYYYMMDD>

Expected zip file naming conventions (as received from BLM):
  MLRS Full : YYYY-MM-DD_MLRS_Full.zip  e.g. 2026-06-07_MLRS_Full.zip
  MLRS MC   : YYYY-MM-DD_MLRS_MC.zip    e.g. 2026-06-07_MLRS_MC.zip
  NLSDB GDB : YYYY-MM-DD_NLSDB.gdb.zip  e.g. 2026-06-08_NLSDB.gdb.zip
              (the GDB folder inside is named nlsdb_MMDDYYYY.gdb)

GCS layout after upload:
  gs://<bucket>/snapshots/<YYYYMMDD>/
    <TABLE>/
      CR_FULL_<TABLE>_<STATE>.load   (one per table × state combination)
      MC_<TABLE>_<STATE>.load        (one per table × state combination)
    Case_<YYYYMMDD>.parquet
    CaseLands_<YYYYMMDD>.parquet
"""

import argparse
import collections
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

# The NLSDB layout JSONs live beside the MLRS schema folder: schemas/nlsdb_schemas/.
# --nlsdb-schema-dir overrides that.
NLSDB_SCHEMA_FOLDER = 'nlsdb_schemas'
NLSDB_LAYOUT_FILES = {
    'Case':      'nlsdb_case.json',
    'CaseLands': 'nlsdb_case_land.json',
}

# Every MLRS delivery has one tar per BLM state office (ES = Eastern States).
EXPECTED_STATES = ['AK', 'AZ', 'CA', 'CO', 'ES', 'ID', 'MT', 'NM', 'NV', 'OR', 'UT', 'WY']

# Exit code for "BLM changed the layout" — distinct from 1 (tooling/input errors).
EXIT_LAYOUT_CHANGED = 2


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


def state_from_dir(state_dir):
    """Extract the state code from CR_FULL_{STATE}_{YYYYMMDD} or MC_{STATE}_{YYYYMMDD}."""
    return os.path.basename(state_dir).split('_')[-2]


def stop_layout_changed(problems, what_to_update):
    """Print every layout problem found and stop before anything is uploaded."""
    print()
    print('!' * 60)
    print('STOPPED — the delivery does not match the expected layout.')
    print('Nothing has been uploaded to GCS.')
    print('!' * 60)
    for line in problems:
        print(f'  - {line}')
    print()
    print('If BLM changed the export on purpose, update the layout to match:')
    for line in what_to_update:
        print(f'  {line}')
    print('then re-run this script (finished steps are skipped). If the change is')
    print('not expected, raise it with BLM before loading this month.')
    sys.exit(EXIT_LAYOUT_CHANGED)


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

    # --- NLSDB GDB ---
    if not gdb_path:
        zip_path = find_one('*_NLSDB.gdb.zip', 'NLSDB GDB zip')
        if zip_path:
            gdb_path = unzip_nlsdb(zip_path)
        else:
            gdb_path = find_one('*.gdb', 'NLSDB GDB folder')
            if gdb_path:
                print(f'  Using existing GDB: {gdb_path}')
            else:
                print('ERROR: no *_NLSDB.gdb.zip or *.gdb folder found in current directory.')
                sys.exit(1)

    # --- Date ---
    if not date:
        # Derive from the MLRS Full folder name: 2026-06-07_MLRS_Full -> 20260607
        date_str = os.path.basename(full_dir).split('_')[0]  # "2026-06-07"
        date = date_str.replace('-', '')                      # "20260607"
        print(f'  Auto-derived snapshot date: {date}')

    return full_dir, mc_dir, gdb_path, date


# ---------------------------------------------------------------------------
# Expected layouts — loaded up front so a missing or broken file fails fast
# ---------------------------------------------------------------------------

def load_mlrs_column_counts(schema_dir):
    """{TABLE: column count} from the 24 MLRS schema JSONs."""
    expected_cols = {}
    for schema_file in sorted(glob.glob(os.path.join(schema_dir, '*_schema.json'))):
        with open(schema_file) as f:
            schema = json.load(f)
        tname = os.path.basename(schema_file).replace('_schema.json', '').upper()
        expected_cols[tname] = len(schema)
    return expected_cols


def load_nlsdb_layouts(nlsdb_schema_dir):
    """{'Case': [(name, BQ type), ...], 'CaseLands': [...]} from the NLSDB layout JSONs."""
    layouts = {}
    for layer, fname in NLSDB_LAYOUT_FILES.items():
        path = os.path.join(nlsdb_schema_dir, fname)
        try:
            with open(path, encoding='utf-8') as f:
                columns = json.load(f)
            layouts[layer] = [(c['name'], c['type'].upper()) for c in columns]
        except FileNotFoundError:
            print(f'  ERROR: NLSDB layout file not found: {path}')
            print(f'  Pass --nlsdb-schema-dir <path> if it lives elsewhere.')
            sys.exit(1)
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            print(f'  ERROR: NLSDB layout file is not a valid [{{"name", "type"}}, ...] list: {path}')
            print(f'         {e}')
            sys.exit(1)
    return layouts


# ---------------------------------------------------------------------------
# Step 1 — NLSDB GDB → Parquet, then check the Parquet layout
# ---------------------------------------------------------------------------

def parquet_is_complete(path):
    """True if `path` is a readable Parquet file with rows (a crashed ogr2ogr leaves no footer)."""
    if not os.path.exists(path):
        return False
    try:
        import pyarrow.parquet as pq
        return pq.read_metadata(path).num_rows > 0
    except Exception:
        return False


def convert_layer(gdb_path, layer, parquet_path):
    """ogr2ogr one GDB layer to Parquet, reusing a complete Parquet from an earlier run."""
    if parquet_is_complete(parquet_path):
        print(f'  {parquet_path} already exists — reusing it (delete it to force reconversion)')
        return
    if os.path.exists(parquet_path):
        print(f'  {parquet_path} exists but is incomplete — reconverting')
        os.remove(parquet_path)
    rc = run_cmd(
        ['ogr2ogr', '-f', 'Parquet', parquet_path, gdb_path, layer,
         '-makevalid', '-lco', 'GEOMETRY=AS_WKB', '-t_srs', 'EPSG:4326', '-progress'],
        f'ogr2ogr {layer} -> {parquet_path}'
    )
    if rc != 0:
        print(f'  ERROR: ogr2ogr failed for {layer} — is the dev conda env active? Aborting.')
        sys.exit(1)


def convert_gdb_to_parquet(gdb_path, date):
    """
    Auto-detect the Case_* and CaseLands_* layers in the GDB via fiona,
    convert each to Parquet using ogr2ogr (same flags as original extract_parquet.txt),
    and name the output Case_<YYYYMMDD>.parquet / CaseLands_<YYYYMMDD>.parquet so the
    OneStop PS1 can construct the GCS path from $Date alone.
    Returns {'Case': case_parquet_path, 'CaseLands': case_land_parquet_path}.
    """
    try:
        import fiona
        layers = fiona.listlayers(gdb_path)
    except Exception as e:
        print(f'  ERROR listing GDB layers: {e}')
        sys.exit(1)

    case_layers      = [l for l in layers if l.startswith('Case_')]
    case_land_layers = [l for l in layers if l.startswith('CaseLands_')]

    if not case_layers:
        print(f'  ERROR: no Case_* layer in GDB. Layers present: {layers}')
        sys.exit(1)
    if not case_land_layers:
        print(f'  ERROR: no CaseLands_* layer in GDB. Layers present: {layers}')
        sys.exit(1)

    case_layer      = case_layers[0]
    case_land_layer = case_land_layers[0]
    print(f'  Detected GDB layers: {case_layer}, {case_land_layer}')

    parquets = {'Case': f'Case_{date}.parquet', 'CaseLands': f'CaseLands_{date}.parquet'}
    convert_layer(gdb_path, case_layer, parquets['Case'])
    convert_layer(gdb_path, case_land_layer, parquets['CaseLands'])
    return parquets


def bq_type_of(arrow_type):
    """The BigQuery type an external table gives a Parquet column (as written in the layout JSONs)."""
    import pyarrow as pa
    t = pa.types
    if t.is_integer(arrow_type):
        return 'INT64'
    if t.is_floating(arrow_type):
        return 'FLOAT64'
    if t.is_string(arrow_type) or t.is_large_string(arrow_type):
        return 'STRING'
    if t.is_timestamp(arrow_type):
        return 'TIMESTAMP'
    if t.is_date(arrow_type):
        return 'DATE'
    if t.is_boolean(arrow_type):
        return 'BOOL'
    if t.is_binary(arrow_type) or t.is_large_binary(arrow_type):
        return 'BYTES'   # the WKB geometry column, Shape
    return str(arrow_type).upper()


def parquet_layout(parquet_path):
    """[(name, BQ type), ...] for a Parquet file, leaving out GeoParquet *_bbox covering columns."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pq.read_schema(parquet_path)
    layout = []
    for field in schema:
        # ogr2ogr adds a <geometry>_bbox struct (GeoParquet covering). The PS1 DDL omits it on
        # purpose, and it carries no attribute data, so it is not part of the layout.
        if field.name.endswith('_bbox') and pa.types.is_struct(field.type):
            continue
        layout.append((field.name, bq_type_of(field.type)))
    return layout


def compare_layout(found, expected, label):
    """Differences between a found and an expected [(name, type), ...] layout, as readable lines."""
    found_types = dict(found)
    expected_types = dict(expected)
    problems = []
    for name, typ in found:
        if name not in expected_types:
            problems.append(f'{label}: NEW column {name} ({typ})')
    for name, typ in expected:
        if name not in found_types:
            problems.append(f'{label}: column {name} ({typ}) is MISSING')
        elif found_types[name] != typ:
            problems.append(f'{label}: column {name} changed type {typ} -> {found_types[name]}')
    return problems


def check_nlsdb_layout(parquets, layouts, nlsdb_schema_dir):
    """Stop if either Parquet file's columns or types differ from its layout JSON."""
    problems = []
    for layer, parquet_path in parquets.items():
        found = parquet_layout(parquet_path)
        layer_problems = compare_layout(found, layouts[layer], f'NLSDB {layer}')
        if layer_problems:
            # Save what was found, in the layout-JSON format, so updating the JSON is a copy.
            found_file = parquet_path.replace('.parquet', '_layout_found.json')
            with open(found_file, 'w', encoding='utf-8') as f:
                json.dump([{'name': n, 'type': t} for n, t in found], f, indent=4)
            layer_problems.append(f'NLSDB {layer}: the layout found was saved to {found_file}')
        else:
            print(f'  NLSDB {layer}: {len(found)} columns match {NLSDB_LAYOUT_FILES[layer]}')
        problems.extend(layer_problems)
    if problems:
        stop_layout_changed(problems, [
            f'{os.path.join(nlsdb_schema_dir, NLSDB_LAYOUT_FILES["Case"])}',
            f'{os.path.join(nlsdb_schema_dir, NLSDB_LAYOUT_FILES["CaseLands"])}',
            '(the PS1 builds its BigQuery tables from these same files; check the DQ',
            ' tests that use any column that was renamed or removed)',
        ])


# ---------------------------------------------------------------------------
# Steps 2 & 3 — MLRS tar extraction
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
# Step 4 — MLRS layout check (states, tables, column counts)
# ---------------------------------------------------------------------------

def check_mlrs_extract(folder, expected_cols):
    """
    Check one MLRS extract folder (Full or MC) against the expected layout.

    Returns (layout_problems, bad_row_total):
      - layout_problems: a missing or unexpected state, a missing or unexpected table, or a
        file where MOST rows have a different column count than its schema (a column was
        added or removed). These stop the build.
      - bad_row_total: rows with a wrong column count in files whose layout is otherwise
        right — almost always an embedded pipe | in a text field. These are reported, not
        stopped on (fix the line and re-upload that file, as before).

    The .load files have no header row, so only the column COUNT can be checked: a renamed
    or reordered column with the same count is invisible here.
    """
    label = os.path.basename(folder)
    problems = []
    state_dirs = sorted(d for d in glob.glob(os.path.join(folder, '*')) if os.path.isdir(d))
    states = {state_from_dir(d): d for d in state_dirs}

    for state in EXPECTED_STATES:
        if state not in states:
            problems.append(f'{label}: state {state} is MISSING (no extracted tar for it)')
    for state in sorted(set(states) - set(EXPECTED_STATES)):
        problems.append(f'{label}: UNEXPECTED state {state} ({os.path.basename(states[state])})')

    total_bad = 0
    for state, state_dir in sorted(states.items()):
        load_files = sorted(glob.glob(os.path.join(state_dir, '*.load')))
        tables = {table_name_from_load(f): f for f in load_files}

        for tname in sorted(set(expected_cols) - set(tables)):
            problems.append(f'{label}: table {tname} is MISSING for {state}')
        for tname in sorted(set(tables) - set(expected_cols)):
            problems.append(f'{label}: NEW table {tname} for {state} ({os.path.basename(tables[tname])}) '
                            f'— no schema JSON for it')

        for tname, load_file in sorted(tables.items()):
            if tname not in expected_cols or os.path.getsize(load_file) == 0:
                continue
            expected = expected_cols[tname]
            fname = os.path.basename(load_file)
            rows = 0
            bad_lines = []
            bad_counts = collections.Counter()
            with open(load_file, 'r', encoding='utf-8', errors='replace') as f:
                for i, line in enumerate(f, 1):
                    rows += 1
                    actual = line.rstrip('\n').rstrip('\r').count('|') + 1
                    if actual != expected:
                        bad_counts[actual] += 1
                        if len(bad_lines) < 5:
                            bad_lines.append((i, actual))
            n_bad = sum(bad_counts.values())
            if n_bad * 2 > rows:
                # Most rows disagree: the file's layout changed, not a few stray pipes.
                found, found_rows = bad_counts.most_common(1)[0]
                problems.append(f'{label}: {fname} has {found} columns on {found_rows:,} of {rows:,} rows '
                                f'— schema expects {expected}')
            elif n_bad:
                total_bad += n_bad
                print(f'  WARNING: {fname}: {n_bad} row(s) with wrong column count '
                      f'(expected {expected}):')
                for line_num, actual_cols in bad_lines:
                    print(f'    line {line_num}: found {actual_cols} columns')
                if n_bad > 5:
                    print(f'    ... and {n_bad - 5} more')

    if not problems and not total_bad:
        print(f'  {label}: {len(states)} states x {len(expected_cols)} tables, all rows have correct column counts.')
    return problems, total_bad


# ---------------------------------------------------------------------------
# Step 5 — Upload .load files and Parquet files to GCS
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
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            'BLM DQIMP monthly snapshot prep — unzip, convert, check layouts, upload to GCS.\n'
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
                        help='Directory containing the 24 MLRS BQ schema JSON files '
                             '(default: auto-detected — the repo\'s '
                             'schemas/mlrs_export_schemas found by walking up from this '
                             'script, then a schemas/ folder beside it, then '
                             f'${SCHEMA_ENV_VAR})')
    parser.add_argument('--nlsdb-schema-dir', default=None,
                        help='Directory containing nlsdb_case.json and nlsdb_case_land.json '
                             f'(default: the {NLSDB_SCHEMA_FOLDER} folder beside the MLRS schema folder)')
    parser.add_argument('--check-only', action='store_true',
                        help='Unzip, convert and run every layout check, but upload nothing')
    args = parser.parse_args()

    # --- Step 0: Unzip / locate inputs, load the expected layouts ---
    print(f'\n{"="*60}')
    print(f'[0/5] Locating inputs and expected layouts')
    print(f'{"="*60}')
    schema_dir = args.schema_dir or resolve_schema_dir()
    if not schema_dir:
        print('  ERROR: could not locate the BQ schema JSONs. Searched:')
        for entry in schema_dir_search_description():
            print(f'    - {entry}')
        print(f'  Pass --schema-dir <path>, or set {SCHEMA_ENV_VAR}.')
        sys.exit(1)
    nlsdb_schema_dir = args.nlsdb_schema_dir or os.path.join(
        os.path.dirname(os.path.normpath(schema_dir)), NLSDB_SCHEMA_FOLDER)
    expected_cols = load_mlrs_column_counts(schema_dir)
    nlsdb_layouts = load_nlsdb_layouts(nlsdb_schema_dir)

    full_dir, mc_dir, gdb_path, date = resolve_inputs(args)
    gcs_prefix = f'gs://{args.bucket}/snapshots/{date}'
    print(f'  Snapshot date : {date}')
    print(f'  MLRS Full     : {full_dir}')
    print(f'  MLRS MC       : {mc_dir}')
    print(f'  NLSDB GDB     : {gdb_path}')
    print(f'  MLRS schemas  : {schema_dir} ({len(expected_cols)} tables)')
    print(f'  NLSDB layouts : {nlsdb_schema_dir}')
    print(f'  GCS target    : {gcs_prefix}' + ('  (CHECK ONLY — nothing will be uploaded)' if args.check_only else ''))

    # --- Step 1: NLSDB GDB → Parquet, check layout (first, so a change stops us in minutes) ---
    print(f'\n{"="*60}')
    print(f'[1/5] Converting NLSDB GDB to Parquet + checking layout: {gdb_path}')
    print(f'{"="*60}')
    parquets = convert_gdb_to_parquet(gdb_path, date)
    check_nlsdb_layout(parquets, nlsdb_layouts, nlsdb_schema_dir)

    # --- Step 2: Extract MLRS Full tars ---
    print(f'\n{"="*60}')
    print(f'[2/5] Extracting MLRS Full: {full_dir}')
    print(f'{"="*60}')
    extract_tars(full_dir)

    # --- Step 3: Extract MLRS MC tars ---
    print(f'\n{"="*60}')
    print(f'[3/5] Extracting MLRS MC: {mc_dir}')
    print(f'{"="*60}')
    extract_tars(mc_dir)

    # --- Step 4: Check MLRS layout against the schemas ---
    print(f'\n{"="*60}')
    print(f'[4/5] Checking MLRS layout (states, tables, column counts) against the schemas')
    print(f'{"="*60}')
    problems_full, bad_full = check_mlrs_extract(full_dir, expected_cols)
    problems_mc, bad_mc = check_mlrs_extract(mc_dir, expected_cols)
    if problems_full or problems_mc:
        stop_layout_changed(problems_full + problems_mc, [
            f'{schema_dir}  (one <table>_schema.json per table; the PS1 also needs a',
            ' new table added to its $loads list)',
        ])
    if bad_full or bad_mc:
        print(f'  TOTAL: {bad_full + bad_mc} row(s) with column count mismatches')
        print(f'  These rows will cause BQ load errors (--max_bad_records=0).')
        print(f'  Fix the affected .load files before continuing, or accept the data loss.')
    print('  Layout checks passed.')

    if args.check_only:
        print(f'\n{"="*60}')
        print(f'Check only — all layout checks passed for snapshot {date}. Nothing was uploaded.')
        print(f'{"="*60}')
        return

    # --- Step 5: Upload .load files and Parquet files to GCS ---
    print(f'\n{"="*60}')
    print(f'[5/5] Uploading .load files and Parquet files to GCS: {gcs_prefix}')
    print(f'{"="*60}')
    upload_load_files(full_dir, gcs_prefix)
    upload_load_files(mc_dir, gcs_prefix)
    rc1 = run_cmd(['gsutil', '-o', 'GSUtil:check_hashes=always', 'cp', parquets['Case'],      f'{gcs_prefix}/{parquets["Case"]}'],      f'upload {parquets["Case"]}')
    rc2 = run_cmd(['gsutil', '-o', 'GSUtil:check_hashes=always', 'cp', parquets['CaseLands'], f'{gcs_prefix}/{parquets["CaseLands"]}'], f'upload {parquets["CaseLands"]}')
    if rc1 != 0 or rc2 != 0:
        print(f'  ERROR: Parquet upload to GCS failed (hash check or transfer error).')
        print(f'  Re-run the failed gsutil cp command(s) above before running the PS1. Aborting.')
        sys.exit(1)

    # --- Done ---
    print(f'\n{"="*60}')
    print(f'Preparation complete for snapshot {date}.')
    print(f'All files are in: {gcs_prefix}/')
    print()
    print(f'Next step — run the BigQuery load:')
    print(f'  .\\BLM_DQIMP_OneStop_param_v4.ps1 -Date {date}')
    print(f'{"="*60}')


if __name__ == '__main__':
    main()
