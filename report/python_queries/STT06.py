## This test answers the question: How many Salesforce Cases do not have a CASE_LAND record?
## excluding status records and bonds

import pandas as pd, logging

def main():
    logging.basicConfig(filename='results/STT06.log', encoding='utf-8', level=logging.DEBUG, format='%(message)s: %(asctime)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('Start Test')

    ## file paths ############################################################################################################
    sf_full_cases_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_BLM_CASE.csv"
    sf_mc_cases_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_BLM_CASE.csv"
    sf_full_case_lands_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_CASE_LAND.csv"
    sf_mc_case_lands_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_CASE_LAND.csv"

    ## functions ##############################################################################################################
    # function to call in all Salesforce cases and exclude status records
    def call_cases_into_memory(full_case_fp,mc_case_fp):
        blm_case_full = pd.read_csv(full_case_fp, low_memory=False, sep = '|')
        logging.info(f'# Cases from Salesforce Full snapshot:{blm_case_full.shape[0]}')
        # call in MC subset
        blm_case_mc = pd.read_csv(mc_case_fp, low_memory=False, sep = '|')
        logging.info(f'# Cases from Salesforce Mining Claim subset: {blm_case_mc.shape[0]}')
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
        # remove status records
        blm_case_all = blm_case_all[blm_case_all['RECORDTYPEID']!='0123d0000005ISFAA2']
        # remove bond records
        blm_case_all = blm_case_all[blm_case_all['RECORDTYPEID']!='0123d0000004QFQAA2']
        logging.info(f'Total Non-Status Cases from Salesforce: {blm_case_all.shape[0]}')
        # blm_case_all = blm_case_all[['ID','RECORDTYPEID']]
        return blm_case_all
    ## function to call in all case land records
    def load_SF_case_lands(sf_full_case_lands_fp,sf_mc_case_lands_fp):
        # call in CAE_LANDs to join
        full_case_lands = pd.read_csv(sf_full_case_lands_fp, low_memory=False, sep = '|')
        logging.info(f'# of CASE_LANDs from Full SF subset: {full_case_lands.shape}')
        mc_case_lands = pd.read_csv(sf_mc_case_lands_fp, low_memory=False, sep = '|')
        logging.info(f'# of CASE_LANDs from MC SF subset: {mc_case_lands.shape}')
        case_lands = pd.concat([full_case_lands,mc_case_lands], ignore_index=True, axis = 0)
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
        logging.info(f'Total # of CASE_LANDs combined: {case_lands.shape}')
        case_lands = case_lands.groupby('BLM_CASE').count()['ID'].reset_index()
        return case_lands
    ## test function 
    def STT06(sf_cases, sf_case_lands):
        result = sf_cases[~sf_cases['ID'].isin(sf_case_lands['BLM_CASE'])]
        logging.info('STT06 results: SF Cases wihtout CASE_LAND records: {}'.format(sf_cases[~sf_cases['ID'].isin(sf_case_lands['BLM_CASE'])].shape[0]))
        logging.info('SF Non-MC Cases wihtout CASE_LAND records: {} out of {}'.format(result[result['RECORDTYPEID']!='012t00000008bvNAAQ'].shape[0], sf_cases[sf_cases['RECORDTYPEID']!='012t00000008bvNAAQ'].shape[0]))
        logging.info('SF MC Cases wihtout CASE_LAND records: {} out of {}'.format(result[result['RECORDTYPEID']=='012t00000008bvNAAQ'].shape[0], sf_cases[sf_cases['RECORDTYPEID']=='012t00000008bvNAAQ'].shape[0]))
        return result

    ## test and results ######################################################################################################
    sf_cases = call_cases_into_memory(sf_full_cases_fp,sf_mc_cases_fp)
    sf_case_lands = load_SF_case_lands(sf_full_case_lands_fp,sf_mc_case_lands_fp)
    STT06 = STT06(sf_cases,sf_case_lands)
    logging.info('STT06 result: {}'.format(STT06.shape[0]))
    STT06.to_csv('results/STT06.csv')
    STT06c = STT06[(STT06['CASE_STATUS'].isin(['AUTHORIZED','ACTIVE'])) & (STT06['LEGACY_SERIAL_NUMBER'].isna())]
    logging.info('STT06c result: {}'.format(STT06c.shape[0]))
    STT06c.to_csv('results/STT06c.csv')
    STT06d = STT06[(STT06['CASE_STATUS'].isin(['AUTHORIZED','ACTIVE'])) & (~STT06['LEGACY_SERIAL_NUMBER'].isna())]
    logging.info('STT06d result: {}'.format(STT06d.shape[0]))
    STT06d.to_csv('results/STT06d.csv')

if __name__ == "__main__":
    main()
