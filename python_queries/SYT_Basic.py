## This file outputs attribute tests for 26 different attributes
## It's long...sorry! BUT its majority repeated logic that can be simplified once time allows

import pandas as pd, logging, numpy as np, datetime

def main():
    start = datetime.datetime.now()
    logging.basicConfig(filename=f'results/SYT_basic.log', encoding='utf-8', level=logging.DEBUG, format='%(message)s: %(asctime)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('******* Start Test *************')

    ## FILE PATHS #####################################################################################################################################
    file_paths = {
        'this_month':{
            'nlsdb_fp':r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\NLSDB\2025-05-05_NLSDB.gdb\Case_05052025_0700.csv",
            'sf_full_cases_fp': r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_BLM_CASE.csv",
            'sf_mc_cases_fp': r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_BLM_CASE.csv",
            'sf_full_case_lands_fp': r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_CASE_LAND.csv",
            'sf_mc_case_lands_fp': r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_CASE_LAND.csv",
            'sf_full_blm_prod_fp': r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_BLM_PRODUCT.csv",
            'sf_mc_blm_prod_fp': r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_BLM_PRODUCT.csv",
            'sf_mc_case_cust_fp': r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_CASE_CUSTOMER.csv",
            'sf_full_case_cust_fp': r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_CASE_CUSTOMER.csv",
            'sf_full_case_action_fp':r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_CASE_ACTION.csv",
            'sf_mc_case_action_fp':r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_CASE_ACTION.csv"
        }
    }
    ###################################################################################################################################################

    ## FUNCTIONS ######################################################################################################################################
    ## function to call in all Salesforce cases and exclude status records and bonds
    def call_cases_into_memory(full_case_fp,mc_case_fp):
        blm_case_full = pd.read_csv(full_case_fp, low_memory=False, on_bad_lines='warn', sep='|')
        logging.info(f'# Cases from Salesforce Full snapshot:{blm_case_full.shape[0]}')
        # call in MC subset
        blm_case_mc = pd.read_csv(mc_case_fp, low_memory=False, on_bad_lines='warn', sep='|')
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
        # remove status records *********
        blm_case_all = blm_case_all[blm_case_all['RECORDTYPEID']!='0123d0000005ISFAA2']
        # remove bond records ***********
        blm_case_all = blm_case_all[blm_case_all['RECORDTYPEID']!='0123d0000004QFQAA2']
        logging.info(f'Total Non-Status Cases from Salesforce: {blm_case_all.shape[0]}')
        return blm_case_all
    ## fxn to call in case_lands table
    def get_case_land_count_per_case(sf_full_case_lands_fp,sf_mc_case_lands_fp):
        # call in CAE_LANDs to join
        full_case_lands = pd.read_csv(sf_full_case_lands_fp, low_memory=False, sep='|')
        logging.info(f'# of CASE_LANDs from Full SF subset: {full_case_lands.shape}')
        mc_case_lands = pd.read_csv(sf_mc_case_lands_fp, low_memory=False, sep='|')
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
        ## below returns you BLM_CASE with count of case lands only
        case_lands = case_lands.groupby('BLM_CASE').count()['ID'].reset_index().rename(columns={'ID':'Case_Land_Count'})
        return case_lands
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
    ## function to call in all Salesforce case customer records and exclude status records and bonds
    def call_case_customers_into_memory(full_case_cust_fp, mc_case_cust_fp):
        case_cust_full = pd.read_csv(full_case_cust_fp, low_memory=False, sep='|')
        logging.info(f'# Case Customers from Salesforce Full snapshot:{case_cust_full.shape[0]}')
        # call in MC subset
        case_cust_mc = pd.read_csv(mc_case_cust_fp, low_memory=False, sep='|')
        logging.info(f'# Cases from Salesforce Mining Claim subset: {case_cust_mc.shape[0]}')
        # combine the subsets
        case_cust_all = pd.concat([case_cust_full, case_cust_mc], ignore_index=True, axis = 0)
        del case_cust_full
        del case_cust_mc
        # columns from the MLRS schema spreadsheet
        case_cust_cols = [
            'ID',
            'ISDELETED',
            'NAME',
            'CREATEDDATE',
            'CREATEDBYID',
            'LASTMODIFIEDDATE',
            'LASTMODIFIEDBYID',
            'SYSTEMMODSTAMP',
            'LASTVIEWEDDATE',
            'LASTREFERENCEDDATE',
            'BLM_CASE',
            'ACCOUNT',
            'INTEREST_PERCENT',
            'INTEREST_RELATIONSHIP',
            'LEGACY_ID',
            'LEGACY_SYSTEM_CODE',
            'BLM_CASE_STATUS',
            'META_LOAD_DT']
        # drop unneded column
        # case_cust_all = case_cust_all.drop('Unnamed: 0', axis = 1)
        # assign columns names
        case_cust_all.columns = case_cust_cols
        logging.info(f'Total Case Customers from Salesforce: {case_cust_all.shape[0]}')
        return case_cust_all
    ## gather all case action data required for tests
    def gather_case_action_data(sf_full_case_action_fp,sf_mc_case_action_fp):
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
        case_action_data = pd.DataFrame()
        chunk_min_seg_all = pd.DataFrame()
        chunk_sur_seg_all = pd.DataFrame()
        chunk_271_all = pd.DataFrame()
        chunk_610_all = pd.DataFrame()
        chunk_865_all = pd.DataFrame()

        for subset in [sf_full_case_action_fp, sf_mc_case_action_fp]:
            logging.info(f'going through {subset} case actions')
            chunks = pd.read_csv(subset, low_memory=False, chunksize = 1000000, sep='|')
            for n, chunk in enumerate(chunks):
                logging.info(f'....calculating chunk {n} for action decision dates')
                # chunk = chunk.drop('Unnamed: 0', axis =1)
                chunk.columns = case_action_cols
                chunk['ACTION_DECISION_DT'] = pd.to_datetime(chunk['ACTION_DECISION_DT'], errors = 'coerce')
                chunk = chunk.sort_values('ACTION_DECISION_DT', ascending=False)
                chunk_min_seg = chunk.sort_values(['MINERAL_SEGREGATION']).groupby('BLM_CASE').first()[['MINERAL_SEGREGATION']].reset_index()
                chunk_min_seg_all = pd.concat([chunk_min_seg_all,chunk_min_seg], ignore_index=True, axis = 0)

                chunk_sur_seg = chunk.sort_values(['SURFACE_SEGREGATION']).groupby('BLM_CASE').first()[['SURFACE_SEGREGATION']].reset_index()
                chunk_sur_seg_all = pd.concat([chunk_sur_seg_all,chunk_sur_seg], ignore_index=True, axis = 0)

                chunk_271 = chunk[(chunk['ACTION_CODE']==271)|(chunk['ACTION_CODE']=='271')|(chunk['ACTION_CODE']=='271.0')]
                chunk_271 = chunk_271.rename(columns={'ACTION_DECISION_DT': 'ACTION_DECISION_DT_271', 'ACTION_REMARKS': 'ACTION_REMARKS_271'})
                chunk_271 = chunk_271.sort_values('ACTION_DECISION_DT_271', ascending=False).groupby('BLM_CASE').first()[['ACTION_DECISION_DT_271','ACTION_REMARKS_271']].reset_index()
                chunk_271_all = pd.concat([chunk_271_all, chunk_271], ignore_index=True, axis = 0)

                chunk_610 = chunk[(chunk['ACTION_CODE']==610)|(chunk['ACTION_CODE']=='610')|(chunk['ACTION_CODE']=='610.0')]
                chunk_610 = chunk_610.rename(columns={'ACTION_DECISION_DT': 'ACTION_DECISION_DT_610', 'PUBLICATION_TYPE': 'PUBLICATION_TYPE_610'})
                chunk_610 = chunk_610.sort_values('ACTION_DECISION_DT_610', ascending=False).groupby('BLM_CASE').first()[['ACTION_DECISION_DT_610','PUBLICATION_TYPE_610']].reset_index()
                chunk_610_all = pd.concat([chunk_610_all, chunk_610], ignore_index=True, axis = 0)

                chunk_865 = chunk[(chunk['ACTION_CODE']==865)|(chunk['ACTION_CODE']=='865')|(chunk['ACTION_CODE']=='865.0')]
                chunk_865 = chunk_865.rename(columns={'ACTION_DECISION_DT': 'ACTION_DECISION_DT_865'})
                chunk_865 = chunk_865.sort_values('ACTION_DECISION_DT_865', ascending=False).groupby('BLM_CASE').first()[['ACTION_DECISION_DT_865']].reset_index()
                chunk_865_all = pd.concat([chunk_865_all, chunk_865], ignore_index=True, axis = 0)
            
            chunk_min_seg_all = chunk_min_seg_all.sort_values(['MINERAL_SEGREGATION']).groupby('BLM_CASE').first()[['MINERAL_SEGREGATION']].reset_index()
            chunk_sur_seg_all = chunk_sur_seg_all.sort_values(['SURFACE_SEGREGATION']).groupby('BLM_CASE').first()[['SURFACE_SEGREGATION']].reset_index()
            chunk_271_all = chunk_271_all.sort_values('ACTION_DECISION_DT_271', ascending=False).groupby('BLM_CASE').first()[['ACTION_DECISION_DT_271','ACTION_REMARKS_271']].reset_index()
            chunk_610_all = chunk_610_all.sort_values('ACTION_DECISION_DT_610', ascending=False).groupby('BLM_CASE').first()[['ACTION_DECISION_DT_610','PUBLICATION_TYPE_610']].reset_index()
            chunk_865_all = chunk_865_all.sort_values('ACTION_DECISION_DT_865', ascending=False).groupby('BLM_CASE').first()[['ACTION_DECISION_DT_865']].reset_index()

            chunk_seg = pd.merge(chunk_min_seg_all, chunk_sur_seg_all,  left_on='BLM_CASE', right_on='BLM_CASE', how='outer')
            chunk = pd.merge(chunk_271_all, chunk_seg, left_on='BLM_CASE',right_on='BLM_CASE',how='outer')
            chunk = pd.merge(chunk_610_all, chunk, left_on='BLM_CASE',right_on='BLM_CASE',how='outer')
            chunk = pd.merge(chunk_865_all, chunk, left_on='BLM_CASE',right_on='BLM_CASE',how='outer')
            case_action_data = pd.concat([case_action_data,chunk], ignore_index=True, axis = 0)
            case_action_data = case_action_data.groupby('BLM_CASE').first().reset_index()
        return case_action_data
    ###################################################################################################################################################
    
    ## cycle through months ################################### deprecate this no need to loop; when developing I was backtesting multi motnhs
    for month in file_paths:
        logging.info(f'************** {month} **************')
        ## extract paths from dictionary of monthly paths
        nlsdb_fp = file_paths[month]['nlsdb_fp']
        sf_full_cases_fp = file_paths[month]['sf_full_cases_fp']
        sf_mc_cases_fp = file_paths[month]['sf_mc_cases_fp']
        sf_full_case_lands_fp = file_paths[month]['sf_full_case_lands_fp']
        sf_mc_case_lands_fp = file_paths[month]['sf_mc_case_lands_fp']
        sf_full_blm_prod_fp = file_paths[month]['sf_full_blm_prod_fp']
        sf_mc_blm_prod_fp = file_paths[month]['sf_mc_blm_prod_fp']
        # full_case_cust_fp = file_paths[month]['sf_full_case_cust_fp']
        # mc_case_cust_fp = file_paths[month]['sf_mc_case_cust_fp']
        sf_full_case_action_fp = file_paths[month]['sf_full_case_action_fp']
        sf_mc_case_action_fp = file_paths[month]['sf_mc_case_action_fp']

        ## DATA LOADS and JOINS #######################################################################################################################
        ## read in NLSDB data
        nlsdb = pd.read_csv(nlsdb_fp, low_memory=False, sep=',')
        logging.info(f'# of NLSDB Cases: {nlsdb.shape[0]}')
        ## call in cases to memory
        sf_cases = call_cases_into_memory(sf_full_cases_fp,sf_mc_cases_fp)
        ## join the nlsdb attributes
        sf_cases = pd.merge(sf_cases, nlsdb, left_on='ID', right_on='SF_ID', how='left', suffixes = ('','_NLSDB'))
        logging.info(f'cases after NLSDB join {sf_cases.shape[0]}')
        del nlsdb

        ## get case land counts
        case_land_counts = get_case_land_count_per_case(sf_full_case_lands_fp,sf_mc_case_lands_fp)
        ## join the case_land counts
        sf_cases = pd.merge(sf_cases, case_land_counts, left_on='ID', right_on='BLM_CASE', how='left', suffixes = ('','_case_land_counts'))
        logging.info(f'cases after Case_Lands join {sf_cases.shape[0]}')
        del case_land_counts

        ## load an join bl_prod data to main table
        blm_prod = read_blm_prod_tables(sf_full_blm_prod_fp,sf_mc_blm_prod_fp)
        blm_prod = blm_prod.groupby('ID').first().reset_index()
        sf_cases = pd.merge(sf_cases,blm_prod[['ID','NAME','CASE_TYPE_CODE']],left_on='BLM_PRODUCT',right_on = 'ID', how='left', suffixes = ('','_blm_prod'))
        logging.info(f'cases after BLM_PROD join {sf_cases.shape[0]}')
        del blm_prod

        ## load and attach latest 610 action code action decision dt for testing PUB_DT
        case_action_data = gather_case_action_data(sf_full_case_action_fp,sf_mc_case_action_fp)
        sf_cases = pd.merge(sf_cases, case_action_data, left_on = 'ID', right_on = 'BLM_CASE',  how='left', suffixes = ('','_case_action'))
        logging.info(f'cases after Case_Action join {sf_cases.shape[0]}')
        del case_action_data

        ### add record types
        record_type_dict = {
            '0123d0000004QFPAA2': 'Agreement',
            '0123d0000004QFQAA2': 'Bond',
            '0123d0000004QFSAA2': 'Geophysical',
            '0123d0000004QFTAA2': 'Lease',
            '0123d0000005ISCAA2': 'Land Use Authoriza',
            '0123d0000005ISEAA2': 'Solid Minerals',
            '0123d0000005ISFAA2': 'Status',
            '012t00000008bvLAAQ': 'Land Tenure',
            '0123d0000004QFRAA2': 'Compensatory Royal',
            '0123d0000004QFUAA2': 'Participating Area',
            '0123d0000005ISGAA2': 'Trespass',
            '0123d0000004QFVAA2': 'Unleased Lands Acc',
            '012t00000008bvNAAQ': 'Mining Claim'
        }
        ###############################################################################################################################################

        ## Data Conditioning ##########################################################################################################################
        ##### includes creating "cleaned" versions of attributes to allow proper comparison; i.e. datetime handling etc
        ##### other cleanup came from refining these tests with the client
        sf_cases['RECORDTYPE'] = sf_cases['RECORDTYPEID'].map(record_type_dict)
        sf_cases['PUB_DT'] = pd.to_datetime(sf_cases['PUB_DT']).dt.strftime('%Y-%m-%d')
        sf_cases['CSE_NAME'].replace(' ', np.nan, inplace=True)
        sf_cases['EFFECTIVE_DATE_clean'] = pd.to_datetime(sf_cases['EFFECTIVE_DATE'], errors = 'coerce').dt.date
        sf_cases['EFF_DT_clean'] = pd.to_datetime(sf_cases['EFF_DT'], errors = 'coerce').dt.date
        sf_cases['EXPIRATION_DATE_clean'] = pd.to_datetime(sf_cases['EXPIRATION_DATE'], errors = 'coerce').dt.date
        sf_cases['EXP_DT_clean'] = pd.to_datetime(sf_cases['EXP_DT'], errors = 'coerce').dt.date
        sf_cases['DISPOSITION_DATE_clean'] = pd.to_datetime(sf_cases['DISPOSITION_DATE'], errors = 'coerce').dt.date
        sf_cases['CSE_DISP_DT_clean'] = pd.to_datetime(sf_cases['CSE_DISP_DT'], errors = 'coerce').dt.date
        sf_cases['CSE_JURIS_DESC_clean'] = sf_cases['CSE_JURIS_DESC'].str.replace(' - BLM', '')
        sf_cases['CSE_JURIS_DESC_clean'] = sf_cases['CSE_JURIS_DESC_clean'].str.replace('OREGON-WASHINGTON', 'OREGON/WASHINGTON')
        sf_cases['CSE_JURIS_DESC_clean'] = sf_cases['CSE_JURIS_DESC_clean'].fillna('-')
        sf_cases['BLM_OFFICE_DESCR_clean'] = sf_cases['BLM_OFFICE_DESCR'].str.replace(' - BLM', '')
        sf_cases['BLM_OFFICE_DESCR_clean'] = sf_cases['BLM_OFFICE_DESCR_clean'].str.replace('OREGON-WASHINGTON', 'OREGON/WASHINGTON')
        sf_cases['CSE_JURIS_CD_clean'] = sf_cases['CSE_JURIS_CD'].fillna('-')
        sf_cases['TITLE_ACC_DT'] = pd.to_datetime(sf_cases['TITLE_ACC_DT'], errors = 'coerce').dt.date
        sf_cases['ACTION_REMARKS_271_clean'] = [str(x).split(';')[0] for x in sf_cases['ACTION_REMARKS_271']]
        sf_cases['PAT_ISS_DT_clean'] = pd.to_datetime(sf_cases['PAT_ISS_DT']).dt.date
        ## value add columns
        sf_cases['Length_match'] = 100*sf_cases['LNGTH'] / sf_cases['CSE_LGTH']
        sf_cases['Width_match'] = 100*sf_cases['WIDTH'] / sf_cases['CSE_WIDTH']
        ###############################################################################################################################################

        ## CREATE TEST RESULT COLUMNS #################################################################################################################
        ## create columns which perform the attribute test for each SYT (SYT01-SYT26) 
        sf_cases['SYT01'] = sf_cases['BLM_ADMIN_STATE__C'] == sf_cases['ADMIN_STATE'] ## this is the simple test for SYT01; you'll see this repeated for each attribute
        sf_cases.loc[(sf_cases['BLM_ADMIN_STATE__C'].isnull()) & (sf_cases['ADMIN_STATE'].isnull()), 'SYT01'] = True ## this gets rid of false positives created bc (null == null) = False 

        sf_cases['SYT03'] = sf_cases['BLM_PROD'].str.lower() == sf_cases['NAME_blm_prod'].str.lower()
        sf_cases.loc[(sf_cases['BLM_PROD'].isnull()) & (sf_cases['NAME_blm_prod'].isnull()), 'SYT03'] = True

        sf_cases['SYT04'] = sf_cases['CASE_STATUS'] == sf_cases['CSE_DISP'].str.upper()
        sf_cases.loc[(sf_cases['CASE_STATUS'].isnull()) & (sf_cases['CSE_DISP'].isnull()), 'SYT04'] = True

        sf_cases['SYT05'] = sf_cases['CASE_TYPE_CODE'].fillna(0).astype('int').astype('str') == sf_cases['CSE_TYPE_NR'].astype('str')
        sf_cases.loc[(sf_cases['CASE_TYPE_CODE'].isnull()) & (sf_cases['CSE_TYPE_NR'].isnull()), 'SYT05'] = True

        sf_cases['SYT06'] = sf_cases['SERIAL_NUMBER__C'] == sf_cases['CSE_NR']
        sf_cases.loc[(sf_cases['SERIAL_NUMBER__C'].isnull()) & (sf_cases['CSE_NR'].isnull()), 'SYT06'] = True

        sf_cases['SYT07'] = sf_cases['LEGACY_SERIAL_NUMBER'] == sf_cases['LEG_CSE_NR']
        sf_cases.loc[(sf_cases['LEGACY_SERIAL_NUMBER'].isnull()) & (sf_cases['LEG_CSE_NR'].isnull()), 'SYT07'] = True

        sf_cases['SYT08'] = sf_cases['CASE_NAME__C'].str.upper() == sf_cases['CSE_NAME'].str.upper()
        sf_cases.loc[(sf_cases['CASE_NAME__C'].isnull()) & (sf_cases['CSE_NAME'].isnull()), 'SYT08'] = True

        sf_cases['SYT09'] = sf_cases['COMMODITY'].str.upper() == sf_cases['CMMDTY'].str.upper()
        sf_cases.loc[(sf_cases['COMMODITY'].isnull()) & (sf_cases['CMMDTY'].isnull()), 'SYT09'] = True

        sf_cases['SYT10'] = sf_cases['EFFECTIVE_DATE_clean'] == sf_cases['EFF_DT_clean']
        sf_cases.loc[(sf_cases['EFFECTIVE_DATE_clean'].isnull()) & (sf_cases['EFF_DT_clean'].isnull()), 'SYT10'] = True

        sf_cases['SYT11'] = sf_cases['EXPIRATION_DATE_clean'] == sf_cases['EXP_DT_clean']
        sf_cases.loc[(sf_cases['EXPIRATION_DATE_clean'].isnull()) & (sf_cases['EXP_DT_clean'].isnull()), 'SYT11'] = True

        sf_cases['SYT12'] = sf_cases['PRODUCTION_STATUS'].str.upper() == sf_cases['PRDCNG'].str.upper()
        sf_cases.loc[(sf_cases['PRODUCTION_STATUS'].isnull()) & (sf_cases['PRDCNG'].isnull()), 'SYT12'] = True

        sf_cases['SYT14'] = sf_cases['DISPOSITION_DATE_clean'] == sf_cases['CSE_DISP_DT_clean']
        sf_cases.loc[(sf_cases['DISPOSITION_DATE_clean'].isnull()) & (sf_cases['CSE_DISP_DT_clean'].isnull()), 'SYT14'] = True

        sf_cases['SYT15'] = sf_cases['COST_CENTER_CODE'].str.upper() == sf_cases['CSE_JURIS_CD_clean'].str.upper()
        sf_cases.loc[(sf_cases['COST_CENTER_CODE'].isnull()) & (sf_cases['CSE_JURIS_CD_clean'].isnull()), 'SYT15'] = True

        sf_cases['SYT16'] = sf_cases['BLM_OFFICE_DESCR_clean'].str.upper() == sf_cases['CSE_JURIS_DESC_clean'].str.upper()
        sf_cases.loc[(sf_cases['BLM_OFFICE_DESCR_clean'].isnull()) & (sf_cases['CSE_JURIS_DESC_clean'].isnull()), 'SYT15'] = True

        sf_cases['SYT17'] = sf_cases['WIDTH'] == sf_cases['CSE_WIDTH']
        sf_cases.loc[(sf_cases['WIDTH'].isnull()) & (sf_cases['CSE_WIDTH'].isnull()), 'SYT17'] = True

        sf_cases['SYT18'] = sf_cases['LNGTH'] == sf_cases['CSE_LGTH']
        sf_cases.loc[(sf_cases['LNGTH'].isnull()) & (sf_cases['CSE_LGTH'].isnull()), 'SYT18'] = True

        sf_cases['SYT19'] = sf_cases['ACTION_REMARKS_271_clean']==sf_cases['PAT_NR']
        sf_cases.loc[(sf_cases['ACTION_REMARKS_271'].isnull()) & (sf_cases['PAT_NR'].isnull()), 'SYT19'] = True

        sf_cases['SYT20'] = sf_cases['ACTION_DECISION_DT_271'] == sf_cases['PAT_ISS_DT_clean']
        sf_cases.loc[(sf_cases['ACTION_DECISION_DT_271'].isnull()) & (sf_cases['PAT_ISS_DT_clean'].isnull()), 'SYT20'] = True

        sf_cases['SYT21'] = sf_cases['MINERAL_SEGREGATION'] == sf_cases['SEG_MIN']
        sf_cases.loc[(sf_cases['MINERAL_SEGREGATION'].isnull()) & (sf_cases['SEG_MIN'].isnull()), 'SYT21'] = True

        sf_cases['SYT22'] = sf_cases['SURFACE_SEGREGATION'] == sf_cases['SEG_SUR']
        sf_cases.loc[(sf_cases['SURFACE_SEGREGATION'].isnull()) & (sf_cases['SEG_SUR'].isnull()), 'SYT22'] = True

        sf_cases['SYT23'] = sf_cases['ACTION_DECISION_DT_610'] == sf_cases['PUB_DT']
        sf_cases.loc[(sf_cases['ACTION_DECISION_DT_610'].isnull()) & (sf_cases['PUB_DT'].isnull()), 'SYT23'] = True

        sf_cases['SYT24'] = sf_cases['PUBLICATION_TYPE_610'] == sf_cases['PUB_TYPE']
        sf_cases.loc[(sf_cases['PUBLICATION_TYPE_610'].isnull()) & (sf_cases['PUB_TYPE'].isnull()), 'SYT24'] = True

        sf_cases['SYT26'] = sf_cases['TITLE_ACC_DT'] == sf_cases['ACTION_DECISION_DT_865']
        sf_cases.loc[(sf_cases['TITLE_ACC_DT'].isnull()) & (sf_cases['ACTION_DECISION_DT_865'].isnull()), 'SYT26'] = True
        ###############################################################################################################################################

        ## PRELIMINARY DATA FILTERING INTO GROUPS #####################################################################################################
        results = {}
        ## isolate only cases with lands that are found in nlsdb
        SYT00 = sf_cases[(~sf_cases['Case_Land_Count'].isna())&(~sf_cases['SF_ID'].isna())]
        results['SYT00'] = SYT00.shape[0]
        ## legacy cases only
        SYT00a = SYT00[~SYT00['LEGACY_SERIAL_NUMBER'].isna()]
        results['SYT00a'] = SYT00a.shape[0]
        ## legacy mining claims only
        SYT00a1 = SYT00a[SYT00a['RECORDTYPEID']=='012t00000008bvNAAQ']
        results['SYT00a1'] = SYT00a1.shape[0]
        ## legacy non-mining claims only
        SYT00a2 = SYT00a[SYT00a['RECORDTYPEID']!='012t00000008bvNAAQ']
        results['SYT00a2'] = SYT00a2.shape[0]

        ## new cases only
        SYT00b = SYT00[SYT00['LEGACY_SERIAL_NUMBER'].isna()]
        results['SYT00b'] = SYT00b.shape[0]
        ## new mining claims only
        SYT00b1 = SYT00b[SYT00b['RECORDTYPEID']=='012t00000008bvNAAQ']
        results['SYT00b1'] = SYT00b1.shape[0]
        ## new non-mining claims only
        SYT00b2 = SYT00b[SYT00b['RECORDTYPEID']!='012t00000008bvNAAQ']
        results['SYT00b2'] = SYT00b2.shape[0]

        ## delete the fluff
        del SYT00
        del SYT00a
        del SYT00b
        ###############################################################################################################################################

        ## GATHER AND DUMP TEST RESULTS AS CSVS #######################################################################################################
        ## SYT01 Case Tests
        logging.info(f'----------SYT01---------------')
        try:
            SYT01a12 = SYT00a1[SYT00a1['SYT01']==False]
            results['SYT01a12'] = SYT01a12.shape[0]
            SYT01a22 = SYT00a2[SYT00a2['SYT01']==False]
            results['SYT01a22'] = SYT01a22.shape[0]
            SYT01b12 = SYT00b1[SYT00b1['SYT01']==False]
            results['SYT01b21'] = SYT01b12.shape[0]
            SYT01b22 = SYT00b2[SYT00b2['SYT01']==False]
            results['SYT01b22'] = SYT01b22.shape[0]
            SYT01a12.to_csv('results/SYT01a12.csv')
            SYT01a22.to_csv('results/SYT01a22.csv')
            SYT01b12.to_csv('results/SYT01b12.csv')
            SYT01b22.to_csv('results/SYT01b22.csv')
            del SYT01a12
            del SYT01a22
            del SYT01b12
            del SYT01b22
        except:
            logging.info('SYT01 failed')

        ## SYT03 Case Tests
        logging.info(f'----------SYT03---------------')
        try:
            SYT03a12 = SYT00a1[SYT00a1['SYT03']==False]
            results['SYT03a12'] = SYT03a12.shape[0]
            SYT03a22 = SYT00a2[SYT00a2['SYT03']==False]
            results['SYT03a22'] = SYT03a22.shape[0]
            SYT03b12 = SYT00b1[SYT00b1['SYT03']==False]
            results['SYT03b21'] = SYT03b12.shape[0]
            SYT03b22 = SYT00b2[SYT00b2['SYT03']==False]
            results['SYT03b22'] = SYT03b22.shape[0]
            SYT03a12.to_csv('results/SYT03a12.csv')
            SYT03a22.to_csv('results/SYT03a22.csv')
            SYT03b12.to_csv('results/SYT03b12.csv')
            SYT03b22.to_csv('results/SYT03b22.csv')
            del SYT03a12
            del SYT03a22
            del SYT03b12
            del SYT03b22
        except:
            logging.info('SYT03 failed')

        ## SYT04 Case Tests
        logging.info(f'----------SYT04---------------')
        try:
            SYT04a12 = SYT00a1[SYT00a1['SYT04']==False]
            results['SYT04a12'] = SYT04a12.shape[0]
            SYT04a22 = SYT00a2[SYT00a2['SYT04']==False]
            results['SYT04a22'] = SYT04a22.shape[0]
            SYT04b12 = SYT00b1[SYT00b1['SYT04']==False]
            results['SYT04b21'] = SYT04b12.shape[0]
            SYT04b22 = SYT00b2[SYT00b2['SYT04']==False]
            results['SYT04b22'] = SYT04b22.shape[0]
            SYT04a12.to_csv('results/SYT04a12.csv')
            SYT04a22.to_csv('results/SYT04a22.csv')
            SYT04b12.to_csv('results/SYT04b12.csv')
            SYT04b22.to_csv('results/SYT04b22.csv')
            del SYT04a12
            del SYT04a22
            del SYT04b12
            del SYT04b22
        except:
            logging.info('SYT04 failed')

        ## SYT05 Case Tests
        logging.info(f'----------SYT05---------------')
        try:
            SYT05a12 = SYT00a1[SYT00a1['SYT05']==False]
            results['SYT05a12'] = SYT05a12.shape[0]
            SYT05a22 = SYT00a2[SYT00a2['SYT05']==False]
            results['SYT05a22'] = SYT05a22.shape[0]
            SYT05b12 = SYT00b1[SYT00b1['SYT05']==False]
            results['SYT05b21'] = SYT05b12.shape[0]
            SYT05b22 = SYT00b2[SYT00b2['SYT05']==False]
            results['SYT05b22'] = SYT05b22.shape[0]
            SYT05a12.to_csv('results/SYT05a12.csv')
            SYT05a22.to_csv('results/SYT05a22.csv')
            SYT05b12.to_csv('results/SYT05b12.csv')
            SYT05b22.to_csv('results/SYT05b22.csv')
            del SYT05a12
            del SYT05a22
            del SYT05b12
            del SYT05b22
        except:
            logging.info('SYT05 failed')

        ## SYT06 Case Tests
        logging.info(f'----------SYT06---------------')
        try:
            SYT06a12 = SYT00a1[SYT00a1['SYT06']==False]
            results['SYT06a12'] = SYT06a12.shape[0]
            SYT06a22 = SYT00a2[SYT00a2['SYT06']==False]
            results['SYT06a22'] = SYT06a22.shape[0]
            SYT06b12 = SYT00b1[SYT00b1['SYT06']==False]
            results['SYT06b21'] = SYT06b12.shape[0]
            SYT06b22 = SYT00b2[SYT00b2['SYT06']==False]
            results['SYT06b22'] = SYT06b22.shape[0]
            SYT06a12.to_csv('results/SYT06a12.csv')
            SYT06a22.to_csv('results/SYT06a22.csv')
            SYT06b12.to_csv('results/SYT06b12.csv')
            SYT06b22.to_csv('results/SYT06b22.csv')
            del SYT06a12
            del SYT06a22
            del SYT06b12
            del SYT06b22
        except:
            logging.info('SYT06 failed')


        ## SYT07 Case Tests
        logging.info(f'----------SYT07---------------')
        try:
            SYT07a12 = SYT00a1[SYT00a1['SYT07']==False]
            results['SYT07a12'] = SYT07a12.shape[0]
            SYT07a22 = SYT00a2[SYT00a2['SYT07']==False]
            results['SYT07a22'] = SYT07a22.shape[0]
            SYT07b12 = SYT00b1[SYT00b1['SYT07']==False]
            results['SYT07b21'] = SYT07b12.shape[0]
            SYT07b22 = SYT00b2[SYT00b2['SYT07']==False]
            results['SYT07b22'] = SYT07b22.shape[0]
            SYT07a12.to_csv('results/SYT07a12.csv')
            SYT07a22.to_csv('results/SYT07a22.csv')
            SYT07b12.to_csv('results/SYT07b12.csv')
            SYT07b22.to_csv('results/SYT07b22.csv')
            del SYT07a12
            del SYT07a22
            del SYT07b12
            del SYT07b22
        except:
            logging.info('SYT07 failed')

        ## SYT08 Case Tests
        logging.info(f'----------SYT08---------------')
        try:
            SYT08a12 = SYT00a1[SYT00a1['SYT08']==False]
            results['SYT08a12'] = SYT08a12.shape[0]
            SYT08a22 = SYT00a2[SYT00a2['SYT08']==False]
            results['SYT08a22'] = SYT08a22.shape[0]
            SYT08b12 = SYT00b1[SYT00b1['SYT08']==False]
            results['SYT08b21'] = SYT08b12.shape[0]
            SYT08b22 = SYT00b2[SYT00b2['SYT08']==False]
            results['SYT08b22'] = SYT08b22.shape[0]
            SYT08a12.to_csv('results/SYT08a12.csv')
            SYT08a22.to_csv('results/SYT08a22.csv')
            SYT08b12.to_csv('results/SYT08b12.csv')
            SYT08b22.to_csv('results/SYT08b22.csv')
            del SYT08a12
            del SYT08a22
            del SYT08b12
            del SYT08b22
        except:
            logging.info('SYT08 failed')

        ## SYT09 Case Tests
        logging.info(f'----------SYT09---------------')
        try:
            SYT09a12 = SYT00a1[SYT00a1['SYT09']==False]
            results['SYT09a12'] = SYT09a12.shape[0]
            SYT09a22 = SYT00a2[SYT00a2['SYT09']==False]
            results['SYT09a22'] = SYT09a22.shape[0]
            SYT09b12 = SYT00b1[SYT00b1['SYT09']==False]
            results['SYT09b21'] = SYT09b12.shape[0]
            SYT09b22 = SYT00b2[SYT00b2['SYT09']==False]
            results['SYT09b22'] = SYT09b22.shape[0]
            SYT09a12.to_csv('results/SYT09a12.csv')
            SYT09a22.to_csv('results/SYT09a22.csv')
            SYT09b12.to_csv('results/SYT09b12.csv')
            SYT09b22.to_csv('results/SYT09b22.csv')
            del SYT09a12
            del SYT09a22
            del SYT09b12
            del SYT09b22
        except:
            logging.info('SYT09 failed')

        ## SYT10 Case Tests
        logging.info(f'----------SYT10---------------')
        try:
            SYT10a12 = SYT00a1[SYT00a1['SYT10']==False]
            results['SYT10a12'] = SYT10a12.shape[0]
            SYT10a22 = SYT00a2[SYT00a2['SYT10']==False]
            results['SYT10a22'] = SYT10a22.shape[0]
            SYT10b12 = SYT00b1[SYT00b1['SYT10']==False]
            results['SYT10b21'] = SYT10b12.shape[0]
            SYT10b22 = SYT00b2[SYT00b2['SYT10']==False]
            results['SYT10b22'] = SYT10b22.shape[0]
            SYT10a12.to_csv('results/SYT10a12.csv')
            SYT10a22.to_csv('results/SYT10a22.csv')
            SYT10b12.to_csv('results/SYT10b12.csv')
            SYT10b22.to_csv('results/SYT10b22.csv')
            del SYT10a12
            del SYT10a22
            del SYT10b12
            del SYT10b22
        except:
            logging.info('SYT10 failed')

        ## SYT11 Case Tests
        logging.info(f'----------SYT11---------------')
        try:
            SYT11a12 = SYT00a1[SYT00a1['SYT11']==False]
            results['SYT11a12'] = SYT11a12.shape[0]
            SYT11a22 = SYT00a2[SYT00a2['SYT11']==False]
            results['SYT11a22'] = SYT11a22.shape[0]
            SYT11b12 = SYT00b1[SYT00b1['SYT11']==False]
            results['SYT11b21'] = SYT11b12.shape[0]
            SYT11b22 = SYT00b2[SYT00b2['SYT11']==False]
            results['SYT11b22'] = SYT11b22.shape[0]
            SYT11a12.to_csv('results/SYT11a12.csv')
            SYT11a22.to_csv('results/SYT11a22.csv')
            SYT11b12.to_csv('results/SYT11b12.csv')
            SYT11b22.to_csv('results/SYT11b22.csv')
            del SYT11a12
            del SYT11a22
            del SYT11b12
            del SYT11b22
        except:
            logging.info('SYT11 failed')

        ## SYT12 Case Tests
        logging.info(f'----------SYT12---------------')
        try:
            SYT12a12 = SYT00a1[SYT00a1['SYT12']==False]
            results['SYT12a12'] = SYT12a12.shape[0]
            SYT12a22 = SYT00a2[SYT00a2['SYT12']==False]
            results['SYT12a22'] = SYT12a22.shape[0]
            SYT12b12 = SYT00b1[SYT00b1['SYT12']==False]
            results['SYT12b21'] = SYT12b12.shape[0]
            SYT12b22 = SYT00b2[SYT00b2['SYT12']==False]
            results['SYT12b22'] = SYT12b22.shape[0]
            SYT12a12.to_csv('results/SYT12a12.csv')
            SYT12a22.to_csv('results/SYT12a22.csv')
            SYT12b12.to_csv('results/SYT12b12.csv')
            SYT12b22.to_csv('results/SYT12b22.csv')
            del SYT12a12
            del SYT12a22
            del SYT12b12
            del SYT12b22
        except Exception as e:
            logging.info(f'SYT12 failed {e}')

        ## SYT14 Case Tests
        logging.info(f'----------SYT14---------------')
        try:
            SYT14a12 = SYT00a1[SYT00a1['SYT14']==False]
            results['SYT14a12'] = SYT14a12.shape[0]
            SYT14a22 = SYT00a2[SYT00a2['SYT14']==False]
            results['SYT14a22'] = SYT14a22.shape[0]
            SYT14b12 = SYT00b1[SYT00b1['SYT14']==False]
            results['SYT14b21'] = SYT14b12.shape[0]
            SYT14b22 = SYT00b2[SYT00b2['SYT14']==False]
            results['SYT14b22'] = SYT14b22.shape[0]
            SYT14a12.to_csv('results/SYT14a12.csv')
            SYT14a22.to_csv('results/SYT14a22.csv')
            SYT14b12.to_csv('results/SYT14b12.csv')
            SYT14b22.to_csv('results/SYT14b22.csv')
            del SYT14a12
            del SYT14a22
            del SYT14b12
            del SYT14b22
        except:
            logging.info('SYT14 failed')

        ## SYT15 Case Tests
        logging.info(f'----------SYT15---------------')
        try:
            SYT15a12 = SYT00a1[SYT00a1['SYT15']==False]
            results['SYT15a12'] = SYT15a12.shape[0]
            SYT15a22 = SYT00a2[SYT00a2['SYT15']==False]
            results['SYT15a22'] = SYT15a22.shape[0]
            SYT15b12 = SYT00b1[SYT00b1['SYT15']==False]
            results['SYT15b21'] = SYT15b12.shape[0]
            SYT15b22 = SYT00b2[SYT00b2['SYT15']==False]
            results['SYT15b22'] = SYT15b22.shape[0]
            SYT15a12.to_csv('results/SYT15a12.csv')
            SYT15a22.to_csv('results/SYT15a22.csv')
            SYT15b12.to_csv('results/SYT15b12.csv')
            SYT15b22.to_csv('results/SYT15b22.csv')
            del SYT15a12
            del SYT15a22
            del SYT15b12
            del SYT15b22
        except:
            logging.info('SYT15 failed')

        ## SYT16 Case Tests
        logging.info(f'----------SYT16---------------')
        try:
            SYT16a12 = SYT00a1[SYT00a1['SYT16']==False]
            results['SYT16a12'] = SYT16a12.shape[0]
            SYT16a22 = SYT00a2[SYT00a2['SYT16']==False]
            results['SYT16a22'] = SYT16a22.shape[0]
            SYT16b12 = SYT00b1[SYT00b1['SYT16']==False]
            results['SYT16b21'] = SYT16b12.shape[0]
            SYT16b22 = SYT00b2[SYT00b2['SYT16']==False]
            results['SYT16b22'] = SYT16b22.shape[0]
            SYT16a12.to_csv('results/SYT16a12.csv')
            SYT16a22.to_csv('results/SYT16a22.csv')
            SYT16b12.to_csv('results/SYT16b12.csv')
            SYT16b22.to_csv('results/SYT16b22.csv')
            del SYT16a12
            del SYT16a22
            del SYT16b12
            del SYT16b22
        except:
            logging.info('SYT16 failed')

        ## SYT17 Case Tests
        logging.info(f'----------SYT17---------------')
        try:
            SYT17a12 = SYT00a1[SYT00a1['SYT17']==False]
            results['SYT17a12'] = SYT17a12.shape[0]
            SYT17a22 = SYT00a2[SYT00a2['SYT17']==False]
            results['SYT17a22'] = SYT17a22.shape[0]
            SYT17b12 = SYT00b1[SYT00b1['SYT17']==False]
            results['SYT17b21'] = SYT17b12.shape[0]
            SYT17b22 = SYT00b2[SYT00b2['SYT17']==False]
            results['SYT17b22'] = SYT17b22.shape[0]
            SYT17a12.to_csv('results/SYT17a12.csv')
            SYT17a22.to_csv('results/SYT17a22.csv')
            SYT17b12.to_csv('results/SYT17b12.csv')
            SYT17b22.to_csv('results/SYT17b22.csv')
            del SYT17a12
            del SYT17a22
            del SYT17b12
            del SYT17b22
        except:
            logging.info('SYT17 failed')

        ## SYT18 Case Tests
        logging.info(f'----------SYT18---------------')
        try:
            SYT18a12 = SYT00a1[SYT00a1['SYT18']==False]
            results['SYT18a12'] = SYT18a12.shape[0]
            SYT18a22 = SYT00a2[SYT00a2['SYT18']==False]
            results['SYT18a22'] = SYT18a22.shape[0]
            SYT18b12 = SYT00b1[SYT00b1['SYT18']==False]
            results['SYT18b21'] = SYT18b12.shape[0]
            SYT18b22 = SYT00b2[SYT00b2['SYT18']==False]
            results['SYT18b22'] = SYT18b22.shape[0]
            SYT18a12.to_csv('results/SYT18a12.csv')
            SYT18a22.to_csv('results/SYT18a22.csv')
            SYT18b12.to_csv('results/SYT18b12.csv')
            SYT18b22.to_csv('results/SYT18b22.csv')
            del SYT18a12
            del SYT18a22
            del SYT18b12
            del SYT18b22
        except:
            logging.info('SYT18 failed')

        ## SYT19 Case Tests
        logging.info(f'----------SYT19---------------')
        try:
            SYT19a12 = SYT00a1[SYT00a1['SYT19']==False]
            results['SYT19a12'] = SYT19a12.shape[0]
            SYT19a22 = SYT00a2[SYT00a2['SYT19']==False]
            results['SYT19a22'] = SYT19a22.shape[0]
            SYT19b12 = SYT00b1[SYT00b1['SYT19']==False]
            results['SYT19b21'] = SYT19b12.shape[0]
            SYT19b22 = SYT00b2[SYT00b2['SYT19']==False]
            results['SYT19b22'] = SYT19b22.shape[0]
            SYT19a12.to_csv('results/SYT19a12.csv')
            SYT19a22.to_csv('results/SYT19a22.csv')
            SYT19b12.to_csv('results/SYT19b12.csv')
            SYT19b22.to_csv('results/SYT19b22.csv')
            del SYT19a12
            del SYT19a22
            del SYT19b12
            del SYT19b22
        except:
            logging.info('SYT19 failed')

        ## SYT20 Case Tests
        logging.info(f'----------SYT20---------------')
        try:
            SYT20a12 = SYT00a1[SYT00a1['SYT20']==False]
            results['SYT20a12'] = SYT20a12.shape[0]
            SYT20a22 = SYT00a2[SYT00a2['SYT20']==False]
            results['SYT20a22'] = SYT20a22.shape[0]
            SYT20b12 = SYT00b1[SYT00b1['SYT20']==False]
            results['SYT20b21'] = SYT20b12.shape[0]
            SYT20b22 = SYT00b2[SYT00b2['SYT20']==False]
            results['SYT20b22'] = SYT20b22.shape[0]
            SYT20a12.to_csv('results/SYT20a12.csv')
            SYT20a22.to_csv('results/SYT20a22.csv')
            SYT20b12.to_csv('results/SYT20b12.csv')
            SYT20b22.to_csv('results/SYT20b22.csv')
            del SYT20a12
            del SYT20a22
            del SYT20b12
            del SYT20b22
        except:
            logging.info('SYT20 failed')
            
        ## SYT21 Case Tests
        logging.info(f'----------SYT21---------------')
        try:
            SYT21a12 = SYT00a1[SYT00a1['SYT21']==False]
            results['SYT21a12'] = SYT21a12.shape[0]
            SYT21a22 = SYT00a2[SYT00a2['SYT21']==False]
            results['SYT21a22'] = SYT21a22.shape[0]
            SYT21b12 = SYT00b1[SYT00b1['SYT21']==False]
            results['SYT21b21'] = SYT21b12.shape[0]
            SYT21b22 = SYT00b2[SYT00b2['SYT21']==False]
            results['SYT21b22'] = SYT21b22.shape[0]
            SYT21a12.to_csv('results/SYT21a12.csv')
            SYT21a22.to_csv('results/SYT21a22.csv')
            SYT21b12.to_csv('results/SYT21b12.csv')
            SYT21b22.to_csv('results/SYT21b22.csv')
            del SYT21a12
            del SYT21a22
            del SYT21b12
            del SYT21b22
        except:
            logging.info('SYT21 failed')

        ## SYT22 Case Tests
        logging.info(f'----------SYT22---------------')
        try:
            SYT22a12 = SYT00a1[SYT00a1['SYT22']==False]
            results['SYT22a12'] = SYT22a12.shape[0]
            SYT22a22 = SYT00a2[SYT00a2['SYT22']==False]
            results['SYT22a22'] = SYT22a22.shape[0]
            SYT22b12 = SYT00b1[SYT00b1['SYT22']==False]
            results['SYT22b21'] = SYT22b12.shape[0]
            SYT22b22 = SYT00b2[SYT00b2['SYT22']==False]
            results['SYT22b22'] = SYT22b22.shape[0]
            SYT22a12.to_csv('results/SYT22a12.csv')
            SYT22a22.to_csv('results/SYT22a22.csv')
            SYT22b12.to_csv('results/SYT22b12.csv')
            SYT22b22.to_csv('results/SYT22b22.csv')
            del SYT22a12
            del SYT22a22
            del SYT22b12
            del SYT22b22
        except:
            logging.info('SYT22 failed')

        ## SYT23 Case Tests
        logging.info(f'----------SYT23---------------')
        try:
            SYT23a12 = SYT00a1[SYT00a1['SYT23']==False]
            results['SYT23a12'] = SYT23a12.shape[0]
            SYT23a22 = SYT00a2[SYT00a2['SYT23']==False]
            results['SYT23a22'] = SYT23a22.shape[0]
            SYT23b12 = SYT00b1[SYT00b1['SYT23']==False]
            results['SYT23b21'] = SYT23b12.shape[0]
            SYT23b22 = SYT00b2[SYT00b2['SYT23']==False]
            results['SYT23b22'] = SYT23b22.shape[0]
            SYT23a12.to_csv('results/SYT23a12.csv')
            SYT23a22.to_csv('results/SYT23a22.csv')
            SYT23b12.to_csv('results/SYT23b12.csv')
            SYT23b22.to_csv('results/SYT23b22.csv')
            del SYT23a12
            del SYT23a22
            del SYT23b12
            del SYT23b22
        except:
            logging.info('SYT23 failed')

        ## SYT24 Case Tests
        logging.info(f'----------SYT24---------------')
        try:
            SYT24a12 = SYT00a1[SYT00a1['SYT24']==False]
            results['SYT24a12'] = SYT24a12.shape[0]
            SYT24a22 = SYT00a2[SYT00a2['SYT24']==False]
            results['SYT24a22'] = SYT24a22.shape[0]
            SYT24b12 = SYT00b1[SYT00b1['SYT24']==False]
            results['SYT24b21'] = SYT24b12.shape[0]
            SYT24b22 = SYT00b2[SYT00b2['SYT24']==False]
            results['SYT24b22'] = SYT24b22.shape[0]
            SYT24a12.to_csv('results/SYT24a12.csv')
            SYT24a22.to_csv('results/SYT24a22.csv')
            SYT24b12.to_csv('results/SYT24b12.csv')
            SYT24b22.to_csv('results/SYT24b22.csv')
            del SYT24a12
            del SYT24a22
            del SYT24b12
            del SYT24b22
        except:
            logging.info('SYT24 failed')

        logging.info(f'----------SYT26---------------')
        try:
            SYT26a12 = SYT00a1[SYT00a1['SYT26']==False]
            results['SYT26a12'] = SYT26a12.shape[0]
            SYT26a22 = SYT00a2[SYT00a2['SYT26']==False]
            results['SYT26a22'] = SYT26a22.shape[0]
            SYT26b12 = SYT00b1[SYT00b1['SYT26']==False]
            results['SYT26b21'] = SYT26b12.shape[0]
            SYT26b22 = SYT00b2[SYT00b2['SYT26']==False]
            results['SYT26b22'] = SYT26b22.shape[0]
            SYT26a12.to_csv('results/SYT26a12.csv')
            SYT26a22.to_csv('results/SYT26a22.csv')
            SYT26b12.to_csv('results/SYT26b12.csv')
            SYT26b22.to_csv('results/SYT26b22.csv')
            del SYT26a12
            del SYT26a22
            del SYT26b12
            del SYT26b22
        except:
            logging.info('SYT26 failed')


        ## end tests and report findings
        end = datetime.datetime.now()
        duration = end-start

        results_df = pd.DataFrame(results, index=[0]).T
        logging.info(f'code duration: {duration}')
        logging.info(f'results for {month}')
        logging.info(results_df.to_string())
        logging.info('--------------------------------')
        ###############################################################################################################################################


if __name__ == "__main__":
    main()