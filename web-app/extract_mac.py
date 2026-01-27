# mac-friendly, cross-platform version of extract.py
# Minimal, surgical changes: (1) skip directories & non-archives in extract();
# (2) use os.path functions instead of Windows-only splits; (3) create folders with exist_ok;
# (4) only gunzip *.gz files in the extracted folder.
import gzip, shutil, tarfile, os, glob, pandas as pd

sub_folder = '2026-01-04'

def extract(sub_folder):
    # Cycle through top-level entries and process only tar archives
    for state_archive in glob.glob(os.path.join(sub_folder, '*')):
        # Skip directories
        if os.path.isdir(state_archive):
            continue
        # Process only recognized tar archives
        if not tarfile.is_tarfile(state_archive):
            continue

        # Open the tar file
        with tarfile.open(state_archive, 'r:*') as tar:
            # Define the name of the folder to unzip to
            folder = state_archive
            for ext in ('.tar.gz', '.tgz', '.tar'):
                if folder.endswith(ext):
                    folder = folder[: -len(ext)]
                    break
            # Make that new directory (idempotent)
            os.makedirs(folder, exist_ok=True)
            # Extract all files in the tar archive
            tar.extractall(path=folder)

        # Cycle through the new files... only *.gz need extracted further
        for new_file in glob.glob(os.path.join(folder, '*')):
            if not (os.path.isfile(new_file) and new_file.lower().endswith('.gz')):
                continue
            # Use gzip utility to open compressed file
            out_path = new_file[:-3]  # strip .gz
            with gzip.open(new_file, 'rb') as f_in, open(out_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(new_file)

def consolidate(sub_folder):
    consolidated_folder = os.path.join(sub_folder, 'consolidated_tables')
    # Create the consolidated folder (idempotent)
    os.makedirs(consolidated_folder, exist_ok=True)

    # Helper to compute table base name cross-platform
    def _table_of(path):
        base = os.path.basename(path)         # e.g., MC_CASE_LAND_NM.load
        if base.endswith('.load'):
            base = base[:-5]                   # MC_CASE_LAND_NM
        return base[:-3]                       # MC_CASE_LAND

    # Find all .load parts under subfolders
    load_paths = glob.glob(os.path.join(sub_folder, '*', '*.load'))
    tables = sorted(set(_table_of(x) for x in load_paths))

    for table in tables:
        print(table)
        dft = pd.DataFrame()
        all_related_files = [x for x in load_paths if _table_of(x) == table]
        print(f'consolidating {table} table...')
        for file in all_related_files:
            print(f'\t...reading & saving {file}')
            try:
                df = pd.read_table(file, delimiter='|', low_memory=False, header=None, on_bad_lines='warn')
            except Exception:
                df = pd.DataFrame()
            dft = pd.concat([dft, df], ignore_index=True)
        dft.to_csv(os.path.join(consolidated_folder, f'{table}.csv'), sep='|', index=False)

if __name__ == '__main__':
    extract(sub_folder)
    consolidate(sub_folder)
