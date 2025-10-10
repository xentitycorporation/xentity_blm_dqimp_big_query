# BLM MLRS DQIMP Test: SYT03 BLM_PROD test for null values

import pandas as pd, logging

def main():
    logging.basicConfig(filename='results/SYT03.log', encoding='utf-8', level=logging.DEBUG, format='%(message)s: %(asctime)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('Start Test')

    ## File Paths #####################################################################################################################################
        ## May
    sf_full_cases_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_BLM_CASE.csv"
    sf_mc_cases_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_BLM_CASE.csv"
    sf_full_blm_prod_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_BLM_PRODUCT.csv"
    sf_mc_blm_prod_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_BLM_PRODUCT.csv"
    nlsdb_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\NLSDB\2025-05-05_NLSDB.gdb\Case_05052025_0700.csv"
    record_types_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\ALL_RECORD_TYPES.csv"
    
    ## Functions ######################################################################################################################################
    ## fxn to call in all Salesforce cases and exclude status records
    def call_cases_into_memory(full_case_fp,mc_case_fp):
        blm_case_full = pd.read_csv(full_case_fp, low_memory=False, sep = '|')
        logging.info(f'# Cases from Salesforce Full snapshot:{blm_case_full.shape[0]}')
        # call in MC subset
        blm_case_mc = pd.read_csv(mc_case_fp, low_memory=False, sep = '|')
        logging.info(f'# Cases from Salesfeorce Mining Claim subset: {blm_case_mc.shape[0]}')
        # combine the subsets
        blm_case_all = pd.concat([blm_case_full, blm_case_mc], ignore_index=True, axis = 0)
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
            'PRODUCTION_STATUS','BLM_OFFICE','BLM_OFFICE_DESCR','COST_CENTER_CODE',
            ## new ones post r4 below
            'FORMATION_NAME','SALE_DATE','LAST_CASE_DISPOSITION_ACTION',
            ## 2 new  attributes added since december snapshot and defined by Natalie on 1/23 via email
            'AGREEMENT_ACRES', 'PARTICIPATING_AREA_ACRES']
        # drop unneded column
        # blm_case_all = blm_case_all.drop('Unnamed: 0', axis = 1)
        # assign columns names
        blm_case_all.columns = blm_case_cols
        # # remove status records
        # blm_case_all = blm_case_all[blm_case_all['RECORDTYPEID']!='0123d0000005ISFAA2']
        logging.info(f'Total Non-Status Cases from Salesforce: {blm_case_all.shape[0]}')
        return blm_case_all
    ## fxn to call in blm_prod table
    def read_blm_prod_tables(sf_full_blm_prod_fp,sf_mc_blm_prod_fp):
        blm_prod_cols = [
            'ID','OWNERID','ISDELETED','NAME','CREATEDBYID','LASTMODIFIEDDATE','LASTMODIFIEDBYID',
            'LASTVIEWEDDATE','LASTREFERENCEDDATE','CASE_GROUP','DESCRIPTION','LEGACY_ID','LEGACY_SYSTEM_CODE',
            'CREATEDDATE','SYSTEMMODSTAMP','META_LOAD_DT','AUTHORITY','CASE_RECORD_TYPES','CASE_TYPE_CODE',
            'CASE_GROUP_DESCR','CASE_COMMODITIES']
        blm_prod_full = pd.read_csv(sf_full_blm_prod_fp, usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20], sep='|')
        blm_prod_mc = pd.read_csv(sf_mc_blm_prod_fp, usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20], sep='|')
        blm_prod = pd.concat([blm_prod_full,blm_prod_mc], ignore_index=True, axis = 0)
        blm_prod.columns = blm_prod_cols
        del blm_prod_mc
        del blm_prod_full
        return blm_prod

    ## Tests and dump results #########################################################################################################################
    nlsdb = pd.read_csv(nlsdb_fp, low_memory = False, sep=',')
    cases = call_cases_into_memory(sf_full_cases_fp,sf_mc_cases_fp)
    cases = cases[cases['ID'].isin(nlsdb['SF_ID'])]
    blm_prod = read_blm_prod_tables(sf_full_blm_prod_fp,sf_mc_blm_prod_fp) 
    blm_prod = blm_prod.groupby('ID').first().reset_index()
    cases = pd.merge(cases,blm_prod[['ID','NAME','CASE_TYPE_CODE']],left_on='BLM_PRODUCT',right_on = 'ID', how='left', suffixes = ('','_blm_prod'))
    del blm_prod
    nlsdb = pd.merge(nlsdb,cases, left_on = 'SF_ID', right_on = 'ID', how='left', suffixes = ('','_SalesForce'))
    result = nlsdb[(nlsdb['BLM_PROD'].isna())]#[['SF_ID','SERIAL_NUMBER__C','NAME_blm_prod','BLM_PROD','CSE_TYPE_NR']].sort_values('SERIAL_NUMBER__C')    
    logging.info(f'SYT032a: {result.shape[0]} null values in NLSDB')
    result.to_csv('results/SYT032a.csv')
    
if __name__ == "__main__":
    main()




