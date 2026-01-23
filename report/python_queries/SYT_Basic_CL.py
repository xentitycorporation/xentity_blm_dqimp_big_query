## This file outputs attribute tests for Case Land attributes

import pandas as pd, logging, numpy as np, datetime, difflib, dask.dataframe as dd

# Suppress overly verbose logging intermediate tasks by dask
logging.getLogger("fsspec").setLevel(logging.WARNING)
logging.getLogger("distributed").setLevel(logging.WARNING)
logging.getLogger("dask").setLevel(logging.WARNING)

## FUNCTIONS ######################################################################################################################################
def load_case_lands(sf_full_case_lands_fp,sf_mc_case_lands_fp):
    # call in CAE_LANDs to join
    full_case_lands = dd.read_parquet(sf_full_case_lands_fp)
    # print(f'# of CASE_LANDs from Full SF subset: {full_case_lands.shape}')
    mc_case_lands = dd.read_parquet(sf_mc_case_lands_fp)
    # print(f'# of CASE_LANDs from MC SF subset: {mc_case_lands.shape}')
    case_lands = dd.concat([full_case_lands,mc_case_lands], ignore_index=True, axis = 0)
    del full_case_lands
    del mc_case_lands
    case_land_column = [
        'ID','ISDELETED','NAME','CREATEDDATE','CREATEDBYID','LASTMODIFIEDDATE','LASTMODIFIEDBYID','SYSTEMMODSTAMP',
        'LASTACTIVITYDATE','LASTVIEWEDDATE','LASTREFERENCEDDATE','BLM_CASE','ACRES','ADMIN_STATE_CODE',
        'ALIQUOT_PART_DESCRIPTION','ALIQUOT_PART','BLM_DISTRICT_CODE','GEOGRAPHIC_STATE_CODE','LEGACY_ID',
        'LEGACY_SYSTEM_CODE','LEGAL_LAND_DESCRIPTION','MERIDIAN_CODE','MERIDIAN_QUADRANT_CODE','METES_AND_BOUNDS',
        'NLSDB_LAND_OID','RANGE_NUMBER','SECTION_NUMBER','SURVEY_NUMBER','SURVEY_TYPE','TOWNSHIP_NUMBER','META_LOAD_DT',
        'MTR','MTRS','LEGACY_ID_WO_ALIQUOT',
        ## new ones post r4 below
        'ACTION_EFFECTIVE_DATE','ALASKA_LAND_SELECTION','ALIS_LEGACY_ID','FORMATTED_LLD_TXT','LAND_STATUS','PRIORITY']
    # case_lands = case_lands.drop(columns='Unnamed: 0')
    case_lands.columns = case_land_column
    # print(f'Total # of CASE_LANDs combined: {case_lands.shape}')
    return case_lands.compute()

def load_cases(sf_full_cases_fp, sf_mc_cases_fp):
    blm_case_full = dd.read_parquet(sf_full_cases_fp)
    # print(f'# Cases from Salesforce Full snapshot:{blm_case_full.shape[0]}')
    # call in MC subset
    blm_case_mc = dd.read_parquet(sf_mc_cases_fp)
    # print(f'# Cases from Salesforce Mining Claim subset: {blm_case_mc.shape[0]}')
    # combine the subsets
    blm_case_all = dd.concat([blm_case_full, blm_case_mc], ignore_index=True, axis = 0)
    del blm_case_full
    del blm_case_mc
    # columns from the MLRS schema spreadsheet
    blm_case_cols = [
        'ID','OWNERID','ISDELETED','NAME','RECORDTYPEID','CREATEDDATE','CREATEDBYID',
        'LASTMODIFIEDDATE','LASTMODIFIEDBYID','SYSTEMMODSTAMP','LASTACTIVITYDATE',
        'LASTVIEWEDDATE','LASTREFERENCEDDATE','ACCOUNT','ACRES','BLM_ADMIN_STATE__C',
        'BLM_PRODUCT','CASE_GROUP','CASE_ID','CASE_NAME__C','CASE_STATUS','CASE_SUFFIX',
        'CASE_TYPE__C','CLAIM_IN_A_LOT','DATE_FILED','DATE_OF_LOCATION',
        'DISCOVERY_OF_VALUABLE_MINING','DISPOSITION_DATE','HEIGHT','INDEPENDENT_CASE',
        'LEGACY_ID','LEGACY_LEAD_FILE_NUMBER__C','LEGACY_SYSTEM_CODE','LNGTH','NLSDB_CASE_OID',
        'NEXT_PAYMENT_DUE_DATE','PL_359','RADIUS','RELATED_CASE','SERIAL_NUMBER__C','WIDTH',
        'REQUIRES_VOIDED_CLAIM_NOTIF','SRHA','DISP_ACT_CLSD_DT','CLSD_VOID_CLAIM_CUT_OFF_DT_YR',
        'GENERAL_REMARKS_LEGACY','LEGACY_SERIAL_NUMBER','NOITL_SERIAL_NUMBER','SYNCHRONIZATION_STATUS',
        'CASE_NAME_FORMULA','SERIAL_NUMBER_PART','META_LOAD_DT','CURRENT_MAINTENANCE_FEE_DUE',
        'LEAD_FILE_NUMBER','COMMODITY','LEASE_ISSUED_DATE','ACTION_669_FLAG','EFFECTIVE_DATE',
        'EXPIRATION_DATE','AGREEMENT_FED_PERCENT','AGREEMENT_FEE_PERCENT','AGREEMENT_INDIAN_PERCENT',
        'AGREEMENT_NONFED_PERCENT','AGREEMENT_STATE_PERCENT','NFLSS_RENTAL_AMOUNT','PARTICIPATING_FED_PERCENT',
        'PARTICIPATING_FEE_PERCENT','PARTICIPATING_INDIAN_PERCENT','PARTICIPATING_NONFED_PERCENT',
        'PARTICIPATING_STATE_PERCENT','RENTAL_RATE','TOTAL_AMOUNT','APA_APPROVED','AUTHORIZATION_LENGTH',
        'AUTHORIZATION','BILLABLE_ACRES','CASE_CATEGORY','DATE_RECEIVED','GENERATION_START_DATE',
        'GENERATION_TYPE','PRE_FLPMA','PROPOSED_ACRES','PROPOSED_LENGTH','PROPOSED_WIDTH','HOLDER_ACCOUNT',
        'HOLDER_INTEREST_PERCENT','LENGTH_MILES','IS_EXPIRED','DIAMETER_OF_PIPE_INCHES','FREQUENCY_OF_SIGNAL',
        'GEOGRAPHIC_NAME','POWERLINE_VOLTAGE_KV','PROJECT_NAME','STATUS_REASON','COMM_SITE_NAME','DATE_REPORTED',
        'DOCUMENT_CATEGORY','DOCUMENT_NUMBER','FEDERAL_ACRES_INPUT','FEE_ACRES_INPUT','US_RIGHTS_EXCEPTION',
        'US_RIGHTS','INDIAN_ACRES_INPUT','NON_FEDERAL_ACRES_INPUT','STATE_ACRES_INPUT','FEDERAL_INPUT_PERCENTAGE',
        'FEE_INPUT_PERCENTAGE','INDIAN_INPUT_PERCENTAGE','NON_FEDERAL_INPUT_PERCENTAGE','STATE_INPUT_PERCENTAGE','SYSTEM_ID',
        ##4 new attributes atarting with March export (04-08-2024)
        'PRODUCTION_STATUS','BLM_OFFICE','BLM_OFFICE_DESCR','COST_CENTER_CODE',
        ## new ones post r4 below
        'FORMATION_NAME','SALE_DATE','LAST_CASE_DISPOSITION_ACTION',
        ## 2 new  attributes added since december snapshot and defined by Natalie on 1/23 via email
        'AGREEMENT_ACRES', 'PARTICIPATING_AREA_ACRES'] 
    # drop unneeded index column
    # blm_case_all = blm_case_all.drop('Unnamed: 0', axis = 1)
    # assign columns names
    blm_case_all.columns = blm_case_cols
    # print(f'Total Non-Status Cases from Salesforce: {blm_case_all.shape[0]}')
    return blm_case_all.compute()

def load_case_groups(sf_full_case_group_d_fp, sf_mc_case_group_d_fp):
    case_group_full = pd.read_parquet(sf_full_case_group_d_fp)
    case_group_mc = pd.read_parquet(sf_mc_case_group_d_fp)
    case_group_all = pd.concat([case_group_full, case_group_mc], ignore_index=True, axis = 0)
    del case_group_full
    del case_group_mc
    cols = [
        'CASE_GROUP_CD',
        'DESCRIPTION',
        'META_LOAD_DT'
    ]
    case_group_all.columns = cols
    case_group_mapping = dict(zip(case_group_all['CASE_GROUP_CD'], case_group_all['DESCRIPTION']))
    return case_group_mapping

def load_record_types(sf_full_record_type_fp, sf_mc_record_type_fp):
    record_types_full = pd.read_parquet(sf_full_record_type_fp)
    record_types_mc = pd.read_parquet(sf_mc_record_type_fp)
    record_types_all = pd.concat([record_types_full, record_types_mc], ignore_index=True, axis = 0)
    del record_types_full
    del record_types_mc
    cols = [
            'ID',
            'NAME',
            'DEVELOPERNAME',
            'NAMESPACEPREFIX',
            'DESCRIPTION',
            'BUSINESSPROCESSID',
            'SOBJECTTYPE',
            'ISACTIVE',
            'ISPERSONTYPE',
            'CREATEDBYID',
            'CREATEDDATE',
            'LASTMODIFIEDBYID',
            'LASTMODIFIEDDATE',
            'SYSTEMMODSTAMP',
            'META_LOAD_DT'
    ]
    record_types_all.columns = cols
    record_types_mapping = dict(zip(record_types_all['ID'], record_types_all['NAME']))
    return record_types_mapping

def read_blm_prod_tables(sf_full_blm_prod_fp,sf_mc_blm_prod_fp):
    blm_prod_cols = [
        'ID','OWNERID','ISDELETED','NAME','CREATEDBYID','LASTMODIFIEDDATE','LASTMODIFIEDBYID',
        'LASTVIEWEDDATE','LASTREFERENCEDDATE','CASE_GROUP','DESCRIPTION','LEGACY_ID','LEGACY_SYSTEM_CODE',
        'CREATEDDATE','SYSTEMMODSTAMP','META_LOAD_DT','AUTHORITY','CASE_RECORD_TYPES','CASE_TYPE_CODE',
        'CASE_GROUP_DESCR','CASE_COMMODITIES']
    blm_prod_full = pd.read_parquet(sf_full_blm_prod_fp)
    blm_prod_mc = pd.read_parquet(sf_mc_blm_prod_fp) 
    blm_prod = pd.concat([blm_prod_full,blm_prod_mc], ignore_index=True, axis = 0)
    blm_prod.columns = blm_prod_cols
    del blm_prod_mc
    del blm_prod_full
    return blm_prod

    # Similarity function for NLP ranking

def load_us_rights_case_lands(sf_full_us_right_case_land_fp, sf_mc_us_right_case_land_fp):
    df_full = pd.read_parquet(sf_full_us_right_case_land_fp)
    df_mc = pd.read_parquet(sf_mc_us_right_case_land_fp)
    dft = pd.concat([df_full,df_mc], ignore_index=True, axis=0)
    us_right_case_lands_cols = [
        'CASE_LAND',
        'US_RIGHTS_RESERVED',
    ]
    dft.columns = us_right_case_lands_cols
    dft = dft.groupby('CASE_LAND')['US_RIGHTS_RESERVED'].agg(lambda x: ','.join(x.astype(str))).reset_index()
    return dft

def gather_case_action_data(action_name_mapping, sf_full_case_action_fp, sf_mc_case_action_fp):
    case_action_cols = ['ID','ISDELETED','NAME','RECORDTYPEID','CREATEDDATE','CREATEDBYID','LASTMODIFIEDDATE',
                        'LASTMODIFIEDBYID','SYSTEMMODSTAMP','LASTACTIVITYDATE','LASTVIEWEDDATE','LASTREFERENCEDDATE',
                        'BLM_CASE','ACTION_NAME','ACTION','BLM_PRODUCTID','CASE_BATCH_REQUEST_ASSMT_YR','CASE_BATCH_REQUEST_RECORD_TYP',
                        'CASE_BATCH_REQUEST','DATE_OF_NOTICE_DECISION','DEFECT_REASON','DEFECT_TYPE','LEGACY_ID','LEGACY_SYSTEM_CODE',
                        'PAYMENT_AMOUNT','ACTION_REMARKS','DOCUMENT_NUMBER','RECEIPT_NUMBER','DATE_KEY','META_LOAD_DT','ACTION_DATE',
                        'CLSD_VOIDED_CLAIMS_CUTOFF_DTYR','CURABLE_DEFECT_DEADLINE_DATE','NOTICE_RECEIVED_REMINDER_DT','BLM_CASE_PRODUCT',
                        'CASE_PRODUCT','ASSESSMENT_YEAR','CODE','DATE_FILED','CASE_ACTION_STATUS_CD','ACTION_DECISION_DT','ACTION_INFORMATION',
                        'ACTION_CODE','CASE_BATCH_REQUEST_SEL_TYPE','EXPIRATION_DATE','RENTAL_RATE','PUBLICATION_TYPE','MINERAL_SEGREGATION',
                        'SURFACE_SEGREGATION',
                        ## new one after r4
                        'ACQUISITION_FUND']
    df_full = dd.read_csv(sf_full_case_action_fp, low_memory = False, sep='|', dtype= 'str', names= case_action_cols)
    df_mc = dd.read_csv(sf_mc_case_action_fp, low_memory = False, sep='|', dtype= 'str', names= case_action_cols)
    dft = dd.concat([df_full,df_mc], ignore_index = True, axis = 0)
    dft['ACTION_CODE'] = dft['ACTION'].map(action_name_mapping)
    dft = dft[(~dft['MINERAL_SEGREGATION'].isna())|(dft['ACTION_CODE'].isin(['552']))]
    return dft.compute()    

def load_case_action_mapping(sf_full_action_fp, sf_mc_action_fp):
    ## function to load the case action names which are housed in a lookup table
    action_columns = [
        'ID','OWNERID','ISDELETED','NAME','CREATEDDATE','CREATEDBYID','LASTMODIFIEDDATE',
        'LASTMODIFIEDBYID','SYSTEMMODSTAMP','LASTVIEWEDDATE','LASTREFERENCEDDATE','ACTION_CODE',
        'ACTION_TYPE','BLM_PRODUCT','CASE_GROUP','DESCRIPTION','LEGACY_ID','LEGACY_SYSTEM_CODE',
        'META_LOAD_DT']
    df_full = pd.read_parquet(sf_full_action_fp)
    df_mc = pd.read_parquet(sf_mc_action_fp)
    dft = pd.concat([df_full,df_mc], ignore_index = True, axis = 0)
    dft.columns = action_columns
    del df_full
    del df_mc
    mapping = dict(zip(dft['ID'], [dft[['ACTION_CODE','NAME']]]))
    return mapping    
##helper function calc nlp similarirty score of string
def simple_similarity(a, b):
    try:
        return difflib.SequenceMatcher(None, a, b).ratio()
    except Exception:
        return 0.0
##helper function compare csv list order dont matter
def compare_csv_sets(a, b):
    if pd.isna(a): a = ""
    if pd.isna(b): b = ""
    a_list = sorted(x.strip() for x in str(a).split(',') if x.strip())
    b_list = sorted(x.strip() for x in str(b).split(',') if x.strip())
    return a_list == b_list
## test functions below ############################################################################################################
# CSE_NR test
def SYT_CL_01(nlsdb_case_lands, sf_cases):
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'CSE_LND_NR']], 
                    sf_cases[['ID','SERIAL_NUMBER__C']], 
                    left_on = 'SF_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', '_sf'])
    ## drop no matches to BLM_CASE
    results = results[~results['ID_sf'].isna()]
    results['SYT_CL_01'] = results['CSE_NR'] == results['SERIAL_NUMBER__C']
    ## force null == null as True
    results.loc[(results['CSE_NR'].isnull()) & (results['SERIAL_NUMBER__C'].isnull()), 'SYT_CL_01'] = True
    results = results[results['SYT_CL_01']==False]
    results.to_csv('results/SYT_CL_01.csv')
    return results.shape[0] ## return results count
# LEG_CSE_NR test
def SYT_CL_02(nlsdb_case_lands, sf_cases):
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'LEG_CSE_NR', 'CSE_LND_NR']], 
                    sf_cases[['ID','LEGACY_SERIAL_NUMBER']], 
                    left_on = 'SF_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', '_sf'])
    ## drop no matches to BLM_CASE
    results = results[~results['ID_sf'].isna()]
    results['SYT_CL_02'] = results['LEG_CSE_NR'] == results['LEGACY_SERIAL_NUMBER']
    ## force null == null as True
    results.loc[(results['LEG_CSE_NR'].isnull()) & (results['LEGACY_SERIAL_NUMBER'].isnull()), 'SYT_CL_02'] = True
    ## isolate only those records that fail
    results = results[results['SYT_CL_02']==False]
    results.to_csv('results/SYT_CL_02.csv')
    return results.shape[0] ## return results count
# REC_TYPE_CSE_GRP test
def SYT_CL_03(nlsdb_case_lands, sf_cases, sf_record_type_mapping, sf_case_group_mapping):
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'REC_TYPE_CSE_GRP', 'CSE_LND_NR']], 
                    sf_cases[['ID','RECORDTYPEID', 'CASE_GROUP']], 
                    left_on = 'SF_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', '_sf'])
    results['RECORD_TYPE.NAME_sf'] = results['RECORDTYPEID'].map(sf_record_type_mapping)
    results['CASE_GROUP_D.DESCRIPTION_sf'] = results['CASE_GROUP'].map(sf_case_group_mapping)
    results['rec_type_cse_grp_sf'] = results['RECORD_TYPE.NAME_sf'] + " - " + results['CASE_GROUP_D.DESCRIPTION_sf']
    ## drop no matches to BLM_CASE
    results = results[~results['ID_sf'].isna()]
    results['SYT_CL_03'] = results['REC_TYPE_CSE_GRP'] == results['rec_type_cse_grp_sf']
    ## force null == null as True
    results.loc[(results['REC_TYPE_CSE_GRP'].isnull()) & (results['rec_type_cse_grp_sf'].isnull()), 'SYT_CL_03'] = True
    ## isolate only those records that fail
    results = results[results['SYT_CL_03']==False]
    results['REC_TYPE_CSE_GRP_similarity'] = results.apply(lambda row: simple_similarity(row['REC_TYPE_CSE_GRP'], row['rec_type_cse_grp_sf']), axis=1)
    results = results.sort_values('REC_TYPE_CSE_GRP_similarity', ascending=True)
    results.to_csv('results/SYT_CL_03.csv')
    return results.shape[0] ## return results count
# BLM_PROD test
def SYT_CL_04(nlsdb_case_lands, sf_cases, sf_blm_prod):
    results = pd.merge( 
                    sf_cases[['ID','BLM_PRODUCT']], 
                    sf_blm_prod[['ID','NAME']],
                    left_on = 'BLM_PRODUCT',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_sf_blm_case', '_sf_blm_prod'])
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'CSE_LND_NR','BLM_PROD']], 
                    results, 
                    left_on = 'SF_ID',
                    right_on = 'ID_sf_blm_case',
                    how = 'left',
                    suffixes=['_nlsdb', ''])
    ## drop no matches to BLM_CASE
    results = results[~results['ID_sf_blm_case'].isna()]
    results['SYT_CL_04'] = results['BLM_PROD'] == results['NAME']
    ## force null == null as True
    results.loc[(results['BLM_PROD'].isnull()) & (results['NAME'].isnull()), 'SYT_CL_04'] = True
    # isolate only those records that fail
    results = results[results['SYT_CL_04']==False]
    results['BLM_PROD_similarity'] = results.apply(lambda row: simple_similarity(row['NAME'], row['BLM_PROD']), axis=1)
    results.to_csv('results/SYT_CL_04.csv')
    return results.shape[0] ## return results count 
## CSE_TYPE_NR test
def SYT_CL_05(nlsdb_case_lands, sf_cases, sf_blm_prod):
    results = pd.merge( 
                    sf_cases[['ID','BLM_PRODUCT']],
                    sf_blm_prod[['ID','CASE_TYPE_CODE']],
                    left_on = 'BLM_PRODUCT',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_sf_blm_case', '_sf_blm_prod'])
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'CSE_LND_NR', 'BLM_PROD', 'CSE_TYPE_NR']], 
                    results, 
                    left_on = 'SF_ID',
                    right_on = 'ID_sf_blm_case',
                    how = 'left',
                    suffixes=['_nlsdb', ''])
    ## drop no matches to BLM_CASE
    results = results[~results['ID_sf_blm_case'].isna()]
    results['SYT_CL_05'] = results['CSE_TYPE_NR'] == results['CASE_TYPE_CODE']
    ## force null == null as True
    results.loc[(results['CSE_TYPE_NR'].isnull()) & (results['CASE_TYPE_CODE'].isnull()), 'SYT_CL_05'] = True
    # isolate only those records that fail
    results = results[results['SYT_CL_05']==False]
    results.to_csv('results/SYT_CL_05.csv')
    return results.shape[0] ## return results count      
## CSE_LND_STATUS test
def SYT_CL_06(nlsdb_case_lands, sf_case_lands):
    results = pd.merge(nlsdb_case_lands, #[['ID', 'SF_CL_ID', 'SF_ID', 'CSE_NR', 'CSE_LND_STATUS']]
                    sf_case_lands, #[['ID','LAND_STATUS']]
                    left_on = 'SF_CL_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', ''])
    # drop no matches to CASE_LANDS
    results = results[~results['ID'].isna()]
    results['SYT_CL_06'] = results['CSE_LND_STATUS'] == results['LAND_STATUS']
    ## force null == null as True
    results.loc[(results['CSE_LND_STATUS'].isnull()) & (results['LAND_STATUS'].isnull()), 'SYT_CL_06'] = True
    # isolate only those records that fail
    results = results[results['SYT_CL_06']==False]
    results.to_csv('results/SYT_CL_06.csv')
    return results.shape[0] ## return results count   
## CSE_LND_STATUS_DT test
def SYT_CL_07(nlsdb_case_lands, sf_case_lands):
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'CSE_LND_STATUS_DT', 'CSE_LND_NR']], 
                    sf_case_lands[['ID','ACTION_EFFECTIVE_DATE']], 
                    left_on = 'SF_CL_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', ''])
    # drop no matches to CASE_LANDS
    results = results[~results['ID'].isna()]
    ## data conditioning
    results['CSE_LND_STATUS_DT_clean'] = pd.to_datetime(results['CSE_LND_STATUS_DT'], errors = 'coerce').dt.date
    results['ACTION_EFFECTIVE_DATE_clean'] = pd.to_datetime(results['ACTION_EFFECTIVE_DATE'], errors = 'coerce').dt.date
    results['SYT_CL_07'] = results['CSE_LND_STATUS_DT_clean'] == results['ACTION_EFFECTIVE_DATE_clean']
    ## force null == null as True
    results.loc[(results['CSE_LND_STATUS_DT_clean'].isnull()) & (results['ACTION_EFFECTIVE_DATE_clean'].isnull()), 'SYT_CL_07'] = True
    # isolate only those records that fail
    results = results[results['SYT_CL_07']==False]
    results.to_csv('results/SYT_CL_07.csv')
    return results.shape[0] ## return results count     
## CSE_LND_NR test
def SYT_CL_08(nlsdb_case_lands, sf_case_lands):
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'CSE_LND_NR']], 
                    sf_case_lands[['ID','NAME']], 
                    left_on = 'SF_CL_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', ''])
    # drop no matches to CASE_LANDS
    results = results[~results['ID'].isna()]
    results['SYT_CL_08'] = results['CSE_LND_NR'] == results['NAME']
    ## force null == null as True
    results.loc[(results['CSE_LND_NR'].isnull()) & (results['NAME'].isnull()), 'SYT_CL_08'] = True
    # isolate only those records that fail
    results = results[results['SYT_CL_08']==False]
    results.to_csv('results/SYT_CL_08.csv')
    return results.shape[0] ## return results count   
## US_RIGHTS test
def SYT_CL_09(nlsdb_case_lands, sf_us_rights_case_lands):
    results = pd.merge(nlsdb_case_lands[['SF_ID', 'SF_CL_ID', 'CSE_NR', 'CSE_LND_NR', 'US_RIGHTS']], 
                    sf_us_rights_case_lands[['CASE_LAND','US_RIGHTS_RESERVED']], 
                    left_on = 'SF_CL_ID',
                    right_on = 'CASE_LAND',
                    how = 'left',
                    suffixes=['_nlsdb', ''])
    # drop no matches to CASE_LANDS
    results = results[~results['CASE_LAND'].isna()]
    # Apply the comparison
    results['match'] = results.apply(lambda row: compare_csv_sets(row['US_RIGHTS_RESERVED'], row['US_RIGHTS']), axis=1)
    results = results[results['match']==False]
    results.to_csv('results/SYT_CL_09.csv')
    return results.shape[0] ## return results count    
## Document Category test
def SYT_CL_10(nlsdb_case_lands, sf_cases):
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR','CSE_LND_NR', 'DOC_TYPE']], 
                    sf_cases[['ID','DOCUMENT_CATEGORY']], 
                    left_on = 'SF_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', '_sf'])
    ## drop no matches to BLM_CASE
    results = results[~results['ID_sf'].isna()]
    results['SYT_CL_10'] = results['DOC_TYPE'] == results['DOCUMENT_CATEGORY']
    ## force null == null as True
    results.loc[(results['DOC_TYPE'].isnull()) & (results['DOCUMENT_CATEGORY'].isnull()), 'SYT_CL_10'] = True
    results = results[results['SYT_CL_10']==False]
    results.to_csv('results/SYT_CL_10.csv')
    return results.shape[0] ## return results count 
## DOCUMENT_NUMBER test
def SYT_CL_11(nlsdb_case_lands, sf_cases):
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'DOC_NR', 'CSE_LND_NR']], 
                    sf_cases[['ID','DOCUMENT_NUMBER']], 
                    left_on = 'SF_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', '_sf'])
    ## drop no matches to BLM_CASE
    results = results[~results['ID_sf'].isna()]
    results['SYT_CL_11'] = results['DOC_NR'] == results['DOCUMENT_NUMBER']
    ## force null == null as True
    results.loc[(results['DOC_NR'].isnull()) & (results['DOCUMENT_NUMBER'].isnull()), 'SYT_CL_11'] = True
    results = results[results['SYT_CL_11']==False]
    results.to_csv('results/SYT_CL_11.csv')
    return results.shape[0] ## return results count      
## SEG_MIN test
def SYT_CL_13(nlsdb_case_lands, sf_cases, case_actions):
    case_actions_nlsdb = case_actions[case_actions['BLM_CASE'].isin(nlsdb_case_lands['SF_ID'].unique())]
    case_actions_nlsdb['ACTION_DECISION_DT'] = pd.to_datetime(case_actions_nlsdb['ACTION_DECISION_DT'], errors = 'coerce')
    ## get latest action code for a case with many
    case_actions_nlsdb = (case_actions_nlsdb
        .sort_values('ACTION_DECISION_DT')  # Sort by date first
        .groupby('BLM_CASE')
        .first()  # Get first value of each column after sorting
        .reset_index()  # Optional: Convert group_column back to a regular column
    )
    case_actions_nlsdb['MINERAL_SEGREGATION_sf_derived'] = np.where(
        case_actions_nlsdb['ACTION_CODE'] == '552',
        'ALL',  # Value when action_code is 552
        np.where(
            case_actions_nlsdb['MINERAL_SEGREGATION'].notna(),
            case_actions_nlsdb['MINERAL_SEGREGATION'],
            None  # Default value if neither condition is met
        )
    )
    results = pd.merge(sf_cases[['ID','SERIAL_NUMBER__C']], 
                    case_actions_nlsdb[['BLM_CASE','ACTION_CODE','ACTION_DECISION_DT','MINERAL_SEGREGATION', 'MINERAL_SEGREGATION_sf_derived']][1:], 
                    left_on = 'ID',
                    right_on = 'BLM_CASE',
                    how = 'left',
                    suffixes=['_sf_blm_case', '_sf_case_action'])
    print(results.shape[0])
    ## join on NLSDB.CASE_LANDS.SF_ID | SF.BLM_CASE.ID
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'SEG_MIN', 'CSE_LND_NR']], 
                    results, 
                    left_on = 'SF_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', '_sf'])
    ## drop no matches to BLM_CASE
    results = results[~results['ID_sf'].isna()]
    results['SYT_CL_13'] = results['SEG_MIN'] == results['MINERAL_SEGREGATION_sf_derived']
    ## force null == null as True
    results.loc[(results['SEG_MIN'].isnull()) & (results['MINERAL_SEGREGATION_sf_derived'].isnull()), 'SYT_CL_13'] = True
    results = results[(results['SYT_CL_13']==False)& (~results['BLM_CASE'].isna())]
    results.to_csv('results/SYT_CL_13.csv')
    return results.shape[0] ## return results count  
## SEG_SUR test        
def SYT_CL_14(nlsdb_case_lands, sf_cases, case_actions):
    case_actions_nlsdb = case_actions[case_actions['BLM_CASE'].isin(nlsdb_case_lands['SF_ID'].unique())]
    case_actions_nlsdb['ACTION_DECISION_DT'] = pd.to_datetime(case_actions_nlsdb['ACTION_DECISION_DT'], errors = 'coerce')
    ## get latest action code for a case with many
    case_actions_nlsdb = (case_actions_nlsdb
        .sort_values('ACTION_DECISION_DT')  # Sort by date first
        .groupby('BLM_CASE')
        .first()  # Get first value of each column after sorting
        .reset_index()  # Optional: Convert group_column back to a regular column
    )
    case_actions_nlsdb['SURFACE_SEGREGATION_sf_derived'] = np.where(
        case_actions_nlsdb['ACTION_CODE'] == '552',
        'ALL',  # Value when action_code is 552
        np.where(
            case_actions_nlsdb['SURFACE_SEGREGATION'].notna(),
            case_actions_nlsdb['SURFACE_SEGREGATION'],
            None  # Default value if neither condition is met
        )
    )
    results = pd.merge(sf_cases[['ID','SERIAL_NUMBER__C']], 
                    case_actions_nlsdb[['BLM_CASE','ACTION_CODE','ACTION_DECISION_DT','SURFACE_SEGREGATION', 'SURFACE_SEGREGATION_sf_derived']][1:], 
                    left_on = 'ID',
                    right_on = 'BLM_CASE',
                    how = 'left',
                    suffixes=['_sf_blm_case', '_sf_case_action'])
    print(results.shape[0])
    ## join on NLSDB.CASE_LANDS.SF_ID | SF.BLM_CASE.ID
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'SEG_SUR', 'CSE_LND_NR']], 
                    results, 
                    left_on = 'SF_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', '_sf'])
    ## drop no matches to BLM_CASE
    results = results[~results['ID_sf'].isna()]
    results['SYT_CL_14'] = results['SEG_SUR'] == results['SURFACE_SEGREGATION_sf_derived']
    ## force null == null as True
    results.loc[(results['SEG_SUR'].isnull()) & (results['SURFACE_SEGREGATION_sf_derived'].isnull()), 'SYT_CL_14'] = True
    results = results[(results['SYT_CL_14']==False)& (~results['BLM_CASE'].isna())]
    results.to_csv('results/SYT_CL_14.csv')
    return results.shape[0] ## return results count
## LND_SELECTED_BY test
def SYT_CL_15(nlsdb_case_lands, sf_case_lands):
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'LND_SELECTED_BY', 'CSE_LND_NR']], 
                    sf_case_lands[['ID','ALASKA_LAND_SELECTION']], 
                    left_on = 'SF_CL_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', ''])
    # drop no matches to CASE_LANDS
    results = results[~results['ID'].isna()]
    results['SYT_CL_15'] = results['LND_SELECTED_BY'] == results['ALASKA_LAND_SELECTION']
    ## force null == null as True
    results.loc[(results['LND_SELECTED_BY'].isnull()) & (results['ALASKA_LAND_SELECTION'].isnull()), 'SYT_CL_15'] = True
    # isolate only those records that fail
    ## there is logic missing for how sometimes NLSDB has a value where SF doesnt but its not cause for concern unless SF has value and NLSDB does not OR NLSDB != MLRS
    results = results[(results['SYT_CL_15']==False)&(~results['ALASKA_LAND_SELECTION'].isna())]
    results.to_csv('results/SYT_CL_15.csv')
    return results.shape[0] ## return results count   
## PRIORITY test
def SYT_CL_16(nlsdb_case_lands, sf_case_lands):
    ## join on NLSDB.CASE_LANDS.SF_ID | SF.CASE_LANDS.ID
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'PRIORITY', 'CSE_LND_NR']], 
                    sf_case_lands[['ID','PRIORITY']], 
                    left_on = 'SF_CL_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', ''])
    # drop no matches to CASE_LANDS
    results = results[~results['ID'].isna()]
    results['SYT_CL_16'] = results['PRIORITY'].astype('str') == results['PRIORITY_nlsdb'].astype('str')
    ## force null == null as True
    results.loc[(results['PRIORITY'].isnull()) & (results['PRIORITY_nlsdb'].isnull()), 'SYT_CL_16'] = True
    # isolate only those records that fail
    ## there is logic missing for how sometimes NLSDB has a value where SF doesnt but its not cause for concern unless SF has value and NLSDB does not OR NLSDB != MLRS
    results = results[(results['SYT_CL_16']==False)]
    results.to_csv('results/SYT_CL_16.csv')
    return results.shape[0] ## return results count   
## CSE_LND_ACRS test
def SYT_CL_17(nlsdb_case_lands, sf_case_lands):
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'CSE_LND_NR', 'CSE_LND_ACRS']], 
                    sf_case_lands[['ID','ACRES']], 
                    left_on = 'SF_CL_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', ''])
    # drop no matches to CASE_LANDS
    results = results[~results['ID'].isna()]
    results['SYT_CL_17'] = results['ACRES'].astype('float') == results['CSE_LND_ACRS'].astype('float')
    ## force null == null as True
    results.loc[(results['ACRES'].isnull()) & (results['CSE_LND_ACRS'].isnull()), 'SYT_CL_17'] = True
    # isolate only those records that fail
    ## there is logic missing for how sometimes NLSDB has a value where SF doesnt but its not cause for concern unless SF has value and NLSDB does not OR NLSDB != MLRS
    results = results[(results['SYT_CL_17']==False)]
    results['acres_diff'] = abs(results['ACRES'].astype('float') - results['CSE_LND_ACRS'].astype('float'))
    results.to_csv('results/SYT_CL_17.csv')
    return results.shape[0] ## return results count   
## CSE_LND_ACRS test
def SYT_CL_18(nlsdb_case_lands, sf_case_lands):
    results = pd.merge(nlsdb_case_lands[['ID', 'SF_ID', 'SF_CL_ID', 'CSE_NR', 'CSE_LND_ID', 'CSE_LND_NR']], 
                    sf_case_lands[['ID','ALIS_LEGACY_ID']], 
                    left_on = 'SF_CL_ID',
                    right_on = 'ID',
                    how = 'left',
                    suffixes=['_nlsdb', ''])
    # drop no matches to CASE_LANDS
    results = results[~results['ID'].isna()]
    results['SYT_CL_18'] = results['CSE_LND_ID'].astype('str') == results['ALIS_LEGACY_ID'].astype('str')
    ## force null == null as True
    results.loc[(results['CSE_LND_ID'].isnull()) & (results['ALIS_LEGACY_ID'].isnull()), 'SYT_CL_18'] = True
    # isolate only those records that fail
    ## there is logic missing for how sometimes NLSDB has a value where SF doesnt but its not cause for concern unless SF has value and NLSDB does not OR NLSDB != MLRS
    results = results[(results['SYT_CL_18']==False)&(~results['ALIS_LEGACY_ID'].isna())]
    results.to_csv('results/SYT_CL_18.csv')
    return results.shape[0] ## return results count
###################################################################################################################################################
 
def main():
    start = datetime.datetime.now()
    logging.basicConfig(filename=f'results/SYT_CL_Basic.log', encoding='utf-8', level=logging.DEBUG, format='%(message)s: %(asctime)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('******* Start Test *************')

    ## FILE PATHS #####################################################################################################################################
    nlsdb_case_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\NLSDB\2025-05-05_NLSDB.gdb\Case_05052025_0700.csv"
    nlsdb_case_lands_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\NLSDB\2025-05-05_NLSDB.gdb\CaseLands_05052025_0720.csv"
    sf_full_cases_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_BLM_CASE.parquet"
    sf_mc_cases_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_BLM_CASE.parquet"
    sf_full_case_lands_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_CASE_LAND.parquet"
    sf_mc_case_lands_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_CASE_LAND.parquet"
    sf_full_case_group_d_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_CASE_GROUP_D.parquet"
    sf_mc_case_group_d_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_CASE_GROUP_D.parquet"
    sf_full_record_type_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_RECORD_TYPE.parquet"
    sf_mc_record_type_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_RECORD_TYPE.parquet"
    sf_full_blm_prod_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_BLM_PRODUCT.parquet"
    sf_mc_blm_prod_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_BLM_PRODUCT.parquet"
    sf_full_us_right_case_land_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_US_RIGHT_CASE_LAND.parquet"
    sf_mc_us_right_case_land_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_US_RIGHT_CASE_LAND.parquet"
    sf_full_case_action_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_CASE_ACTION.csv"
    sf_mc_case_action_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_CASE_ACTION.csv"
    sf_full_action_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_ACTION.parquet"
    sf_mc_action_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_ACTION.parquet"
    ###################################################################################################################################################
       
    ## DATA LOADS and JOINS ###########################################################################################################################
    SYT_CL_results = {} ##intiiate a dict for results gathering

    logging.info('Loading nlsdb case lands...')
    nlsdb_case_lands = pd.read_csv(nlsdb_case_lands_fp)

    try:
        logging.info('Loading salesforce case lands...')
        sf_case_lands = load_case_lands(sf_full_case_lands_fp,sf_mc_case_lands_fp)  
        logging.info('Running SYT_CL_06...')
        SYT_CL_results['SYT_CL_06'] = SYT_CL_06(nlsdb_case_lands, sf_case_lands)
        logging.info('Running SYT_CL_07...')
        SYT_CL_results['SYT_CL_07'] = SYT_CL_07(nlsdb_case_lands, sf_case_lands)
        logging.info('Running SYT_CL_08...')
        SYT_CL_results['SYT_CL_08'] = SYT_CL_08(nlsdb_case_lands, sf_case_lands)
        logging.info('Running SYT_CL_15...')
        SYT_CL_results['SYT_CL_15'] = SYT_CL_15(nlsdb_case_lands, sf_case_lands)
        logging.info('Running SYT_CL_16...')
        SYT_CL_results['SYT_CL_16'] = SYT_CL_16(nlsdb_case_lands, sf_case_lands)
        logging.info('Running SYT_CL_17...')
        SYT_CL_results['SYT_CL_17'] = SYT_CL_17(nlsdb_case_lands, sf_case_lands)
        logging.info('Running SYT_CL_18...')
        SYT_CL_results['SYT_CL_18'] = SYT_CL_18(nlsdb_case_lands, sf_case_lands)
        del sf_case_lands
    except Exception as e:
        logging.error(f"Error in SYT_CL_18: {e}")
        del sf_case_lands

    try:
        logging.info('Loading salesforce cases...')
        sf_cases = load_cases(sf_full_cases_fp,sf_mc_cases_fp)

        logging.info('Running SYT_CL_01...')
        SYT_CL_results['SYT_CL_01'] = SYT_CL_01(nlsdb_case_lands, sf_cases)
        logging.info('Running SYT_CL_02...')
        SYT_CL_results['SYT_CL_02'] = SYT_CL_02(nlsdb_case_lands, sf_cases)
        logging.info('Running SYT_CL_10...')
        SYT_CL_results['SYT_CL_10'] = SYT_CL_10(nlsdb_case_lands, sf_cases)
        logging.info('Running SYT_CL_11...')
        SYT_CL_results['SYT_CL_11'] = SYT_CL_11(nlsdb_case_lands, sf_cases)
    except Exception as e:
        logging.error(f"Error in SYT_CL_01/SYT_CL_02: {e}")
    try:
        logging.info('\nLoading salesforce action name mapping...')
        action_name_mapping = load_case_action_mapping(sf_full_action_fp, sf_mc_action_fp)
        logging.info('Loading salesforce case action data...\n')
        case_actions = gather_case_action_data(action_name_mapping, sf_full_case_action_fp, sf_mc_case_action_fp)

        logging.info('Running SYT_CL_13...')
        SYT_CL_results['SYT_CL_13'] = SYT_CL_13(nlsdb_case_lands, sf_cases, case_actions)
        logging.info('Running SYT_CL_14...')
        SYT_CL_results['SYT_CL_14'] = SYT_CL_14(nlsdb_case_lands, sf_cases, case_actions)
        del case_actions
    except Exception as e:
        logging.error(f"Error in SYT_CL_13/SYT_CL_14: {e}")
        del case_actions

    try:
        logging.info('Loading salesforce blm prod...')
        sf_blm_prod = read_blm_prod_tables(sf_full_blm_prod_fp,sf_mc_blm_prod_fp)

        logging.info('Running SYT_CL_04...')
        SYT_CL_results['SYT_CL_04'] = SYT_CL_04(nlsdb_case_lands, sf_cases, sf_blm_prod)
        logging.info('Running SYT_CL_05...')
        SYT_CL_results['SYT_CL_05'] = SYT_CL_05(nlsdb_case_lands, sf_cases, sf_blm_prod)
        del sf_blm_prod
    except Exception as e:
        logging.error(f"Error in SYT_CL_04/SYT_CL_05: {e}")
        del sf_blm_prod
    try:
        logging.info('Loading salesforce case group mapping...')
        sf_case_group_mapping = load_case_groups(sf_full_case_group_d_fp, sf_mc_case_group_d_fp)
        logging.info('Loading salesforce record type mapping...')
        sf_record_type_mapping = load_record_types(sf_full_record_type_fp, sf_mc_record_type_fp)

        logging.info('Running SYT_CL_03...')
        SYT_CL_results['SYT_CL_03'] = SYT_CL_03(nlsdb_case_lands, sf_cases, sf_record_type_mapping, sf_case_group_mapping)
        del sf_record_type_mapping, sf_case_group_mapping, sf_cases
    except Exception as e:
        logging.error(f"Error in SYT_CL_03: {e}")
        del sf_record_type_mapping, sf_case_group_mapping, sf_cases

    try:
        logging.info('Loading salesforce us rights case land...')
        sf_us_rights_case_lands = load_us_rights_case_lands(sf_full_us_right_case_land_fp, sf_mc_us_right_case_land_fp)
        logging.info('Running SYT_CL_09...')
        SYT_CL_results['SYT_CL_09'] = SYT_CL_09(nlsdb_case_lands, sf_us_rights_case_lands)
        del sf_us_rights_case_lands
        del nlsdb_case_lands
    except Exception as e:
        logging.error(f"Error in SYT_CL_09: {e}")

    ## logging.info('Running SYT_CL_12...')
    ## SYT_CL_results['SYT_CL_12'] = SYT_CL_12()

    ###################################################################################################################################################
    results_df = pd.DataFrame(SYT_CL_results, index=[0]).T
    logging.info(results_df.to_string())

if __name__ == "__main__":
    main()