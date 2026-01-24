# BLM SETA Data Quality Testing How To and Work Log
This README describes the purpose of this project and the associated content in each of the project subfolders. DQIMP is used as the abbreviation for the project, and is derived from the original creation of a Data Quality Improvement Plan presented to the BLM in summer 2023.
---

## Project Purpose
Each month a snapshot of MLRS and NLSDB come in from the BLM and are used to populate a mirror of the RDMS that exists in the BLM environment for operations. The extracts of MLRS and NLSDB do not contain any pii or sensitive information, and as a reult are not a complete mirror of the BLM operational database in Salesforce. The extracts are used to create a database of historical snapshots (Aug 2023 - current) in the Xentity Google Cloud environment, where Big Query is used to interact with the data. The schema files used in the build of the Xentity Google Cloud DQUIM

### Build
This folder contains the instructions and scripts required to build the DQIMP Google Cloud database of extract snapshots. Currently each month a manual process is required to unpack the snapshot files, convert them into an ingestible file format, merge them into single tables that can be loaded into Big Query tables and used for querying. 

### Report
Each month there are a predefined set of data quality tests that have been identified as valuable to the BLM team. These tests were extensively vetted through discovery with the BLM team to ensure the information they produce is accurate, does not produce any false-positive conclusions, and provides insight by monitoring over time. 

This folder contains all of the queries used to generate the report results, as well as the template for the monthly report itself - which is delivered as a pdf to the BLM each month and includes a moving window, or rolling tally, of feature counts. The active working files for all logic diagrams in the report are also stored here, along with detailed descriptions of the report results for interpretation.

### New_Tests
While a select number of tests are included in the monthly report and web app, there are actually a much larger number of tests that have been identified for the broader DQIMP. All of the tests are organized into four categories:
1. UID Trace Tests confirm that all unique cases in MLRS Salesforce are present in NLSDB.
2. Attribute Tests ensure MLRS Salesforce and NLSDB field domains are accurate.
3. Synchronization Tests show if the attributes in MLRS Salesforce are the same as they are in NLSDB and validate that changes over time to MLRS Salesforce and NLSDB are logical.
4. System Validation Tests track record changes over time to provide feedback on system functionality and identify O&M stories needed to fix or improve the system.

This folder contains subfolders with all scripts used to run all of the tests. The associated index for all of the tests, including thier associated short code name, are stored and managed on the Xentity gDrive: https://docs.google.com/spreadsheets/d/1uCg_kqia9dU-r6eLh6LAgq3mlRzYD4F5/edit?gid=1015025729#gid=1015025729

Scripts in this folder are actively under development. The BLM team focused efforts from 2023-2025 on the synchronization of attributes between MLRS and NLSDB. At the end of 2025 all sync tests had been validated and added to the list of tests for the report. Beginning in 2026, new tests are being developed for the remaining three categories UID Trace, Attribute and System Validation.

### Business Rules
Throughout the course of validating tests that follow the four scientifically designed test categories, it was discovered that the MLRS and NLSDB systems do not have clear and complete documentation of the business rules that define the system. These rulese include the data dictionary, the ERDs, the field definitions and the table join FK/PK uniqueness. The development of the business rule documentation was captured through BLM SME interviews throughout the five years of MLRS development, and the details were stored in JIRA cards for the developers to use in the creation of the system. The only way to retro0actively identify the rules is to reverse engineer them from the test development process, validate their implemantation through data tests, and create documentation that SMEs can use and refine.

This folder contains primarily ipynb files that are used to work through the discovery process of each business rule as it is identified.

