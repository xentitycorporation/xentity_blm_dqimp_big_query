# BLM SETA Data Quality Testing How To and Work Log
This README begins with a [How To]() which is followed by a [Work Log](#blm-data-quality-testing-bigquery-implementation-work-log)

---

## How To For BLM DQIMP in BigQuery
Each month a snapshot of MLRS and NLSDB come in from the BLM and are shared on GDrive @[BLM DQIMP Snapshots](https://drive.google.com/drive/folders/18oZFnOIgRR_K9n4CPTm67lKxpPcq3wsk?usp=drive_link)

### Extract MLRS (Salesforce) Snapshots
1. Download the (2) snaphsot zipped filed with naming convention *Full_* and *MC_*
2. Place [extract.py](../extract_files/extract.py)
3. Change the subfolder date to date of current snapshot
4. Run extract.py
5. Outputs ~24 CSVs into consolidated_files* folder for each snapshot zipped file

### Extract NLSDB data
1. NLSDB snapshots come as zipped FileGDBs; unzip.
2. Use *ogr2ogr* to extract a parquet file of each feature class (there are (2); Case and Case Lands)
3. Use [remove_geom_from_parquet.ipynb](../extract_files/remove_geom_from_parquet.ipynb) to remove geometries from teh parquet file
    - Bigquery spatial enablement not flushed out, so this process allows for uploading just the attribute table

### Load data into Google Cloud Sandbox for BLM DQIMP Project
1. Go to [sandbox-blm-seta-dqimp-qaqc](https://console.cloud.google.com/storage/browser/sandbox-blm-seta-dqimp-qaqc/snapshots?inv=1&invt=AbxeBA&pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22)))
    - Huy is admin for access
2. Create a subfolder in the *Snapshots* folder titled in format 'YYYYMM01' related to the month the snapshot represents
3. Load all CSVs from extract.py output as well as (2) NLSDB parquet files extracted from the FileGDB

### Load data into BigQuery Tables
1. First step is to consolidate the MLRS Salesforce table parts into a single table
    - in the sandbox you'll notice the extract.py output provided two versions of each table with naming convention *FULL_* and *MC_*
    - these need to be concatenated together to acheive one CSV per table
2. Open command prompt and *cd* to the [schemas folder](../schemas/)
3. Create a new [load_commands_.txt](../load_commands/) file and search and replace the snapshot date with the new snapshot date you are laoding:
    - e.g. for 20250501 snapshot:
    - `gsutil compose gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20250501/MC_ACCOUNT_RPT.csv gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20250501/CR_FULL_ACCOUNT_RPT.csv gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20250501/ACCOUNT_RPT.csv`
        - this code concatenates the two parts of the ACCOUNT_RPT table into a single table that is loaded into the sandbox
    - `bq load --source_format=CSV --field_delimiter="|" --skip_leading_rows=0 --schema=account_rpt_schema.json blm_dqimp_qaqc.account_rpt_20250501 gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20250501/ACCOUNT_RPT.csv`
        - this code references the schema for the specific table from the [schemas folder](../schemas/)
4. Copy and paste all lines of code in the load_commands_.txt file down to line 52 into command prompt to run it all as a batch
    - this supposes you are already setup with Google CLoud CLI tools
5. Verify that the new tables are loaded into the sandbox and that the count of snaphsot increased in the BigQuery tables
6. Run the cleanup code in load_commands_.txt starting on line 55 to remove the *sub-tables* from the sandbox

### Update Queries
1. coming soon...

---

## BLM Data Quality Testing BigQuery Implementation Work Log
### 10/7/2025
* NLSDB extract will have a later date than MLRS extract (10/5/25) this month. Need to have Natalie (BLM) set up EGMS access so we can pull NLSDB
  
### 5/21/2025
* Mark F Learned to Git
* Mark F completed all steps in the README up to "Update Queries"

### 5/19/2025 - 5/20/2025
* Productionalized SYT_Basic_CL queries and aggegated into single script
* Created new tests for current and future Jira tickets
* Working on integration of Geoparquet into BigQuery

### 5/15/2025 - 5/16/2025

* Created queries for all SYT_Basic_CL tests (17)
* Created aggregate query for snapshot table results of SYTCL tests
* Added workflow section to README
* Requested repo moved into blm-devs-group

### 5/12/2025 - 5/14/2025

* Created new queries for Case Land test and Jira ticket specific tests for weekly calls
* walked Mark F through extract process for January snapshot
* Mark F loaded Jan snapshot data in sandbox for MLRS and NLSDB
* Elii created how to for loading data from sandbox to BigQuery tables
    * appended to beginning of README

### 05/10/2025

* Converted SYT_Basic tests into SQL queries and saved each as an individual query.
    * this allows for running of individual tests to obtain the actual records which fail the test rather than just receiving the count.

* Created a compiled version of the SYT_Basic query which runs all SYT_Basic attribute tests for all loaded snapshots.
    * output is a table by snapshot date with results counts for each test
    * output table is saved as a BQ table titled "SYT_Basic"

* Created saved query for Jira ticket 15252
    * CSE_DISP 
    * SRC
    * Modified date 
    * case action testing

* Created query for Jira ticket 16052

* Designed custom CSE_DISP waterfall ploty chart in dev.ipynb for tracking case dispositon synchronization changes

### 05/07/2025

* Got new snapshot data, began extracting for upload to Google Cloud  
Completed extract

* Loaded all Feb snapshot data into new vintage snapshot folder `20250201`

* Converted nlsdbs to Parquets in format acceptable to BQ  
Got snapshot pieces consolidated  
Loaded Feb snapshot data into BQ

---

### 05/05/2025

* Removed NLSDB case/case_lands Parquets from April snapshot folder. Discussed with Huy and decided spatially enabling data avoid for now.

* Loaded in CSV version of NLSDB into each snapshot folder  
March snapshot folder needs consolidating still.

* Opened CMD as admin and `cd`’d to schema/ folder  
Sourced the `load_commands_march.txt` file from load commands folder

* Confirmed Google setup still correct with:

```bash
gcloud auth list
```

* Confirmed the correct project is active:

```bash
gcloud config get-value project
```

* Ran the combine and delete of pieces for March snapshots.

* Had a big learning curve on loading NLSDB.  
Process is to extract from FileGDB as Parquet, then use pandas to load in a Notebook and drop the Shape column, then upload that to Cloud Storage.

* Converted the first `SYT_Basic` test into a SQL query and saved as `SYT01`

* Learned you have to run a query in same region of BigQuery project, which in this case is `us-central1`

* Built the following tests and saved as queries:  
`SYT 1, 3, 4, 5`

---

### 05/01/2025

* Elii set up an updated sandbox file structure for snapshots:  
`sandbox-blm-dqimp-qaqc/snapshots/2050401`

* All CSV files have been moved into the subfolder:

```bash
gsutil mv gs://sandbox-blm-seta-dqimp-qaqc/*.csv gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20250401/
```

* Elii created a `.json` file with the `MC_CASE_ACTION` schema.

* Elii used `gsutil compose` to combine `MC_CASE_ACTION.csv` and `CR_FULL_CASE_ACTION.csv` into a single CSV titled `CASE_ACTION.csv`:

```bash
gsutil compose gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20250401/MC_CASE_ACTION.csv \
gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20250401/CR_FULL_CASE_ACTION.csv \
gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20250401/CASE_ACTION.csv
```

* Created a `case_action_schema.json` to load into BigQuery dataset.

* Loaded combined `CASE_ACTION.csv` with a schema as `blm_dqimp_qaqc.CASE_ACTION_20250401`:

```bash
bq load --source_format=CSV --field_delimiter="|" --skip_leading_rows=0 \
--schema=case_action_schema.json blm_dqimp_qaqc.case_action_20250401 \
gs://sandbox-blm-seta-dqimp-qaqc/snapshots/20250401/CASE_ACTION.csv
```

* Date formats non-conformant to BigQuery; loading all as string to `CAST` later.

* Created schemas for all tables and loaded all tables for April snapshot.

* Created new snapshot subfolder for March data titled `20250301/`  
Loaded all CSVs to this subfolder  
Created load scripts for these tables to BigQuery

* ~~Loaded NLSDB case and case_land feature classes as Parquet files with WKT geometry column.  
Will have to `CAST` to GEOMETRY column compatible with BigQuery later for spatial analyses.~~
    * ^^ditching geospatial for now and loaded nlsdb as attribute table only

---

### 04/30/2025

* Huy and Elii met and Huy showed Elii how to load a CSV from sandbox into BigQuery dataset.

* Huy showed how snapshots can be handled with low LOE via date suffix naming convention.

**Setup**

* Huy created a sandbox for this data:  
<https://console.cloud.google.com/storage/browser/sandbox-blm-seta-dqimp-qaqc>

* Elii loaded all CSVs for one snapshot into that sandbox.

---

### Notes

* Huy suggested using **DataForm** instead of **dbt**… this is under the BigQuery left sidebar.
