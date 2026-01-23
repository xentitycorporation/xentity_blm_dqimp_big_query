# Purpose of this code is to extract the zipped tar and gzip files of the MLRS snapshot
# One must customize the sub_folder variable to the relevent snapshot data in unzipped foler
# This code runs in relative path of the unzipped folder titled "YYYY-MM-DD"
import gzip, shutil, tarfile, os, glob, pandas as pd
sub_folder = '2025-05-04'

def extract(sub_folder):
    # Cycle through tar files
    for state_archive in glob.glob('{}/*'.format(sub_folder)):
        # Open the tar file in the tarfile utility
        tar = tarfile.open(state_archive)
        # Define the name of the folder to unzip to
        folder = '{}'.format(state_archive.replace('.tar', ''))
        # Make that new directory
        os.mkdir(folder)
        # Extract all files in the tar archive
        tar.extractall(path=folder)  
        # Close the tar utility
        tar.close()
        # Cycle through the new files...they need extracted too
        for new_file in glob.glob('{}/*'.format(folder)):
            # Use gzip utility to open compressed file
            with gzip.open(new_file, 'rb') as f_in:
                # Open a new file to write the data to
                with open(new_file.replace('.gz', ''), 'wb') as f_out:
                    # Write the data
                    shutil.copyfileobj(f_in, f_out)
            os.remove(new_file)
            
def consolidate(sub_folder):
    consolidated_folder = '{}/consolidated_tables'.format(sub_folder)
    os.mkdir(consolidated_folder)
    tables = list(set([x.split('\\')[-1].replace('.load','')[:-3] for x in glob.glob('{}/*/*.load'.format(sub_folder))]))
    for table in tables:
        print(table)
        dft = pd.DataFrame()
        all_related_files = [x for x in glob.glob('{}/*/*.load'.format(sub_folder)) if x.split('\\')[-1].replace('.load','')[:-3] == table]
        print(f'consolidating {table} table...')
        for file in all_related_files:
            print(f'\t...reading & saving {file}')
            try:
                df = pd.read_table(file, delimiter='|', low_memory=False, header=None, on_bad_lines='warn')
            except:
                df = pd.DataFrame()
            dft = pd.concat([dft, df], ignore_index=True)
        dft.to_csv('{}/{}.csv'.format(consolidated_folder, table), sep='|', index=False)

extract(sub_folder)
consolidate(sub_folder)
