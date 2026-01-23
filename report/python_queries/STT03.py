## This code runs the SF to NLSDB UID Trace tests for the Report

import pandas as pd, logging

def main():

    logging.basicConfig(filename='results/STT03.log', encoding='utf-8', level=logging.DEBUG, format='%(message)s: %(asctime)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('Start Test')

    nlsdb = pd.read_csv(r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\NLSDB\2025-05-05_NLSDB.gdb\Case_05052025_0700.csv", low_memory=False, sep=',')
    # logging.info(f'# of NLSDB Cases: {nlsdb.shape[0]}')
    sf_full_cases_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_BLM_CASE.csv"
    sf_mc_cases_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_BLM_CASE.csv"
    sf_full_case_lands_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_CASE_LAND.csv"
    sf_mc_case_lands_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_CASE_LAND.csv"
    sf_full_blm_prod_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_BLM_PRODUCT.csv"
    sf_mc_blm_prod_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_BLM_PRODUCT.csv"

    sensitive_case_types = ['7502','7504','8522','210001','210002','210003','210004','210005','210006','210007',
                            '210008','210009','210010','210011','210012','210013','210014','210015','210016','210017',
                            '210018','210019','210020','210026','210030','210099','210500','210501','210502','211000',
                            '212002','212009','213002','213701','214000','214001','214002','214100','214101','215000',
                            '215001','215002','215003','215005','216001','218001','218002','218003','218004','218005',
                            '218006','218007','218008','218009','218010','218011','218012','218013','218014','218015',
                            '218016','218017','218018','218019','218020','218021','218022','218023','218024','218025',
                            '218030','218035','218040','218050','218060','218065','218070','218071','218080','218090',
                            '218091','281001','281003','281007','281008','281009','281011','281099','281100','281130',
                            '281200','281210','281211','281230','281300','281400','284009','285009','286009','286209',
                            '287009','288009','288102','288105','289001','289003','289004','289007','289008','289009',
                            '289011','289099','289100','289101','289102','289103','289104','289105','289106','289107',
                            '289111','289150','289160','289200','289600','289601','289602','289608','289610','289612',
                            '289614','289615','289616','289617','289618','289619','289620','289621','289622','289623',
                            '289624','289701','289702','289800','289801','289900','289902','292801','292802','292803',
                            '293200','310099','323000','342701','360311','360312','360313','415000','810000','811001',
                            '815100','827000','920000','923000','923099','923514','923515','923907','923910','923911',
                            '923930','923950','923951','923952','923953','923961','923990']
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

        logging.info(f'Total Non-Status Non-Mining Claim Cases from Salesforce: {blm_case_all.shape[0]}')
        return blm_case_all

    sf_cases = call_cases_into_memory(sf_full_cases_fp,sf_mc_cases_fp)

    def read_blm_prod_tables(sf_full_blm_prod_fp,sf_mc_blm_prod_fp):
        blm_prod_cols = [
            'ID','OWNERID','ISDELETED','NAME','CREATEDBYID','LASTMODIFIEDDATE','LASTMODIFIEDBYID',
            'LASTVIEWEDDATE','LASTREFERENCEDDATE','CASE_GROUP','DESCRIPTION','LEGACY_ID','LEGACY_SYSTEM_CODE',
            'CREATEDDATE','SYSTEMMODSTAMP','META_LOAD_DT','AUTHORITY','CASE_RECORD_TYPES','CASE_TYPE_CODE',
            'CASE_GROUP_DESCR','CASE_COMMODITIES']
        blm_prod_full = pd.read_csv(sf_full_blm_prod_fp, usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20], sep = '|')
        blm_prod_mc = pd.read_csv(sf_mc_blm_prod_fp, usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20], sep = '|')
        blm_prod = pd.concat([blm_prod_full,blm_prod_mc], ignore_index=True, axis = 0)
        blm_prod.columns = blm_prod_cols
        del blm_prod_mc
        del blm_prod_full
        return blm_prod

    blm_prod = read_blm_prod_tables(sf_full_blm_prod_fp,sf_mc_blm_prod_fp)
    blm_prod = blm_prod.groupby('CASE_TYPE_CODE').first().reset_index()
    blm_prod = blm_prod[['ID','NAME','CASE_TYPE_CODE']].rename(columns={'ID':'BLM_PRODUCT.ID','NAME':'BLM_PRODUCT.NAME','CASE_TYPE_CODE':'BLM_PRODUCT.CASE_TYPE_CODE'})
    sf_cases = pd.merge(sf_cases,blm_prod, left_on='BLM_PRODUCT', right_on = 'BLM_PRODUCT.ID', how='left')

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
            'ACTION_EFFECTIVE_DATE','ALASKA_LAND_SELECTION','ALIS_LEGACY_ID','FORMATTED_LLD_TXT','LAND_STATUS','PRIORITY'
        ]
        # case_lands = case_lands.drop(columns='Unnamed: 0')
        case_lands.columns = case_land_column
        logging.info(f'Total # of CASE_LANDs combined: {case_lands.shape}')
        case_lands = case_lands.groupby('BLM_CASE').count()['ID'].reset_index()
        return case_lands

    sf_case_lands = load_SF_case_lands(sf_full_case_lands_fp,sf_mc_case_lands_fp)

    STT03 = sf_cases[~sf_cases['ID'].isin(nlsdb['SF_ID'])]
    del sf_cases
    logging.info('STT03 result: {}'.format(STT03.shape[0]))
    STT03b1 = STT03[STT03['RECORDTYPEID']=='012t00000008bvNAAQ']
    logging.info('STT03b1 result: {}'.format(STT03b1.shape[0]))

    STT03b2 = STT03b1[(STT03b1['LEGACY_SERIAL_NUMBER'].isna()) & (STT03b1['ID'].isin(sf_case_lands['BLM_CASE'])) & (~STT03b1['BLM_PRODUCT.CASE_TYPE_CODE'].isin(sensitive_case_types))]
    logging.info('STT03b2 result: {}'.format(STT03b2.shape[0]))
    STT03b2.to_csv('results/STT03b2.csv')

    STT03c1 = STT03[STT03['RECORDTYPEID']!='012t00000008bvNAAQ']
    logging.info('STT03c1 result: {}'.format(STT03c1.shape[0]))

    STT03c2 = STT03c1[(STT03c1['LEGACY_SERIAL_NUMBER'].isna()) & (STT03c1['ID'].isin(sf_case_lands['BLM_CASE'])) & (~STT03c1['BLM_PRODUCT.CASE_TYPE_CODE'].isin(sensitive_case_types))]
    logging.info('STT03c2 result: {}'.format(STT03c2.shape[0]))
    STT03c2.to_csv('results/STT03c2.csv')

if __name__ == "__main__":
    main()
