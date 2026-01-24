# BLM SETA Data Quality Testing Project

---

## How To For BLM DQIMP in BigQuery
Each month a snapshot of MLRS and NLSDB from the BLM and are shared on GDrive @[BLM DQIMP Snapshots](https://drive.google.com/drive/folders/18oZFnOIgRR_K9n4CPTm67lKxpPcq3wsk?usp=drive_link)

### Extract MLRS (Salesforce) Snapshots
1. Download the (2) snaphsot zipped filed with naming convention *Full_* and *MC_*
2. Place [extract.py](../extract_files/extract.py)
3. Change the subfolder date to date of current snapshot
4. Run extract.py
5. Outputs ~24 CSVs into consolidated_files* folder for each snapshot zipped file

### Extract NLSDB data
1. NLSDB snapshots come as zipped FileGDBs; unzip.
2. Use *ogr2ogr* to extract a parquet file of each feature class (there are (2); Case and Case Lands)
3. Use [remove_geom_from_parquet.ipynb](../extract_files/remove_geom_from_parquet.ipynb) to remove geometries from the parquet file
    - This process allows for uploading just the attribute table
    - Future project development still remains to enable Bigquery spatial functionality 

### Load data into Google Cloud Sandbox for BLM DQIMP Project
1. Go to [sandbox-blm-seta-dqimp-qaqc](https://console.cloud.google.com/storage/browser/sandbox-blm-seta-dqimp-qaqc/snapshots?inv=1&invt=AbxeBA&pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22)))
2. Create a subfolder in the *Snapshots* folder titled in format 'YYYYMM01' related to the month the snapshot represents
3. Load all CSVs from extract.py output as well as (2) NLSDB parquet files extracted from the FileGDB

### Load data into BigQuery Tables
1. First step is to consolidate the MLRS Salesforce table parts into a single table
    - In the sandbox the extract.py output provided two versions of each table with naming convention *FULL_* and *MC_*
    - These need to be concatenated together to acheive one CSV per table
2. Open command prompt and *cd* to the [schemas folder](../schemas/)
3. Create a new [load_commands_.txt](../load_commands/) file and search and replace the snapshot date with the new snapshot date being laoded:
    - e.g. for 20250501 snapshot:
    - `gsutil compose gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20250501/MC_ACCOUNT_RPT.csv gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20250501/CR_FULL_ACCOUNT_RPT.csv gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20250501/ACCOUNT_RPT.csv`
        - This code concatenates the two parts of the ACCOUNT_RPT table into a single table that is loaded into the sandbox
    - `bq load --source_format=CSV --field_delimiter="|" --skip_leading_rows=0 --schema=account_rpt_schema.json blm_dqimp_qaqc.account_rpt_20250501 gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20250501/ACCOUNT_RPT.csv`
        - This code references the schema for the specific table from the [schemas folder](../schemas/)
4. Copy and paste all lines of code in the load_commands_.txt file down to line 52 into command prompt to run it all as a batch
    - Note that a requirement for this step is Google Cloud CLI tools
5. Verify that the new tables are loaded into the sandbox and that the count of snaphsot increased in the BigQuery tables
6. Run the cleanup code in load_commands_.txt starting on line 55 to remove the *sub-tables* from the sandbox


