# BLM SETA Data Quality Testing Schemas Workspace
This README describes purpose of the schemas and the relationship between the various databases containing MLRS information at the BLM.

MLRS Salesforce
The primary database for the MLRS system is stored in Salesforce, and only a select number of people have API access to this database. 

MLRS Monthly Extract
Each month an extract is run from the MLRS Salesforce database to produce the MLRS Monthly Extract (commonly referred to a sthe MLRS Extract). This extract was created primarily to power the NRS (reporting system at the BLM), however there are a handful of public entities that recieve this extract. It contains no pii, which is how Xentity is able to use this data outside the BLM environment.

NLSDB Monthly Extract.
The NLSDB is the geospatial database that contains all of the geospatial representation of lands associated with each case. This NLSDB Case layer has all geometries assciated with a single case dissolved into one multi-part geometry representing each case. There was an algorithm used to convert the Legal Land Descriptions (LLDs) to their associated geometry and run the dissolve operation. That algorithm produced a "Quality Score" that is a numeric value associated with the sucess of the algorithm matching the Case LLD to its associated PLSS geometry. The NLSDBIt also contains a Case Land layer that has each individual land as a single geometric entry and each land has a many to one relationship with the Case Serial Number. The NLSDB is published using ESRI feature services, and each month the BLM GIS team uses the BLM internal Citrix network to pull a snapshot of the NLSDB to align with the Monthly MLRS Extract for the purposes of testing alignment between the two systems, and to moniter the NLSDB geometry over time. It contains no pii, which is how Xentity is able to use this data outside the BLM environment.
---

## MLRS Extract Schemas 
These json files were created from the excel sheet that is distributed by the BLM with each monthly extract.

## NLSDB Schemas 
These json files were created by converting the NLSDB export attributes.

## Lookup Tables
`lookup_tables/` holds reference tables that Xentity maintains and loads into BigQuery. These are
**not** BLM extract schemas — they originate here, not from the monthly MLRS or NLSDB deliveries,
and they reload only when their content changes rather than on the monthly cycle. For that reason
they are deliberately kept out of `build/`, which is the monthly snapshot pipeline.

**Convention: one subfolder per lookup table**, each self-contained — source data, explicit
BigQuery schema, reload script, and validator together, so a table can be rebuilt from its own
folder without hunting.

| Subfolder | Table |
|---|---|
| `case_type_group_subgroup/` | `Product_Code_Case_Type_Group_Subgroup` — BLM Product Code → Case Type Group / Subgroup, the client's primary reporting dimension |

## Schema Validation Notebooks
These notebooks are the location of 