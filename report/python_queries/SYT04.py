# BLM MLRS DQIMP Test: SYT04 (including all subtests)

# Case Disposition Tests which equate to the graphic in report
# SYT04a:	Count of Cases (excluding status records) from SF BLM_CASE table
# SYT04b1:	Count of Mining Claim Cases (excluding status records) from SF BLM_CASE table
# SYT04b2:	Count of Mining Claim Cases (excluding status records) from SF BLM_CASE table WHERE the latest CASE_ACTION is greater than Release 1
# SYT04b3:	Count of Mining Claim Cases (excluding status records) from SF BLM_CASE table WHERE the latest CASE_ACTION is greater than Release 1 AND disposition doesn't match in NLSDB
# SYT04c1:	Count of Fluid Minerals Cases (excluding status records) from SF BLM_CASE table
# SYT04c2:	Count of Fluid Minerals Cases (excluding status records) from SF BLM_CASE table WHERE the latest CASE_ACTION is greater than Release 2
# SYT04c3:	Count of Fluid Minerals Cases (excluding status records) from SF BLM_CASE table WHERE the latest CASE_ACTION is greater than Release 2 AND disposition doesn't match in NLSDB
# SYT04d1:	Count of Fluid Minerals Cases (excluding status records) from SF BLM_CASE table WHERE the latest CASE_ACTION is greater than Release 2 AND disposition doesn't match in NLSDB
# SYT04d2:	Count of Fluid Minerals Cases (excluding status records) from SF BLM_CASE table WHERE the latest CASE_ACTION is greater than Release 3
# SYT04d3:	Count of LUA, LT and SM Cases (excluding status records) from SF BLM_CASE table WHERE the latest CASE_ACTION is greater than Release 3 AND disposition doesn't match in NLSDB
# SYT04e:	Total Count of SYT04b3 + SYT04c3 + SYT04d3: total disposition sync fails

# Mining Claims - Release 1
# 012t00000008bvNAAQ	Mining Claim
# Fluid Minerals - Release 2
# 0123d0000004QFPAA2	Agreement
# 0123d0000004QFTAA2	Lease
# 0123d0000004QFUAA2	Participating Area
# Others - Release 3
# 0123d0000004QFQAA2	Bond
# 0123d0000005ISCAA2	Land Use Authoriza
# 0123d0000005ISEAA2	Solid Minerals
# 0123d0000005ISFAA2	Status
# 012t00000008bvLAAQ	Land Tenure
# 0123d0000004QFRAA2	Compensatory Royal
# 0123d0000005ISGAA2	Trespass
# 0123d0000004QFVAA2	Unleased Lands Acc
# 0123d0000004QFSAA2	Geophysical

import pandas as pd, logging

def main():

    logging.basicConfig(filename='results/SYT04.log', encoding='utf-8', level=logging.DEBUG, format='%(message)s: %(asctime)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('Start Test')

    ## File Paths #####################################################################################
    sf_full_cases_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_BLM_CASE.csv"
    sf_mc_cases_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_BLM_CASE.csv"
    sf_full_case_action_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_CASE_ACTION.csv"
    sf_mc_case_action_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_CASE_ACTION.csv"
    nlsdb_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\NLSDB\2025-05-05_NLSDB.gdb\Case_05052025_0700.csv"
    record_types_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\ALL_RECORD_TYPES.csv"
    ## End File Paths #################################################################################

    ####### Functions #################################################################################
    ## Function to call in all Salesforce cases and exclude status records
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
        # remove status records
        blm_case_all = blm_case_all[blm_case_all['RECORDTYPEID']!='0123d0000005ISFAA2']
        logging.info(f'Total Non-Status Cases from Salesforce: {blm_case_all.shape[0]}')
        return blm_case_all
    ## Function to pull case ids of interest from case_action tables
    def extract_from_case_action(SYT04b1, SYT04c1, SYT04d1, sf_full_case_action_fp, sf_mc_case_action_fp):
        case_action_cols = ['ID','ISDELETED','NAME','RECORDTYPEID','CREATEDDATE','CREATEDBYID','LASTMODIFIEDDATE',
                            'LASTMODIFIEDBYID','SYSTEMMODSTAMP','LASTACTIVITYDATE','LASTVIEWEDDATE','LASTREFERENCEDDATE',
                            'BLM_CASE','ACTION_NAME','ACTION','BLM_PRODUCTID','CASE_BATCH_REQUEST_ASSMT_YR',
                            'CASE_BATCH_REQUEST_RECORD_TYP','CASE_BATCH_REQUEST','DATE_OF_NOTICE_DECISION','DEFECT_REASON',
                            'DEFECT_TYPE','LEGACY_ID','LEGACY_SYSTEM_CODE','PAYMENT_AMOUNT','ACTION_REMARKS','DOCUMENT_NUMBER',
                            'RECEIPT_NUMBER','DATE_KEY','META_LOAD_DT','ACTION_DATE','CLSD_VOIDED_CLAIMS_CUTOFF_DTYR',
                            'CURABLE_DEFECT_DEADLINE_DATE','NOTICE_RECEIVED_REMINDER_DT','BLM_CASE_PRODUCT','CASE_PRODUCT',
                            'ASSESSMENT_YEAR','CODE','DATE_FILED','CASE_ACTION_STATUS_CD','ACTION_DECISION_DT','ACTION_INFORMATION',
                            'ACTION_CODE','NEWFIECASE_BATCH_REQUEST_SEL_TYPELD2','EXPIRATION_DATE','RENTAL_RATE',
                            'PUBLICATION_TYPE','MINERAL_SEGREGATION','SURFACE_SEGREGATION',
                            ## new one past r4
                            'ACQUISITION_FUND']

        action_status_dict = {-999999999999:'-',1:'Accepted',2:'Active',3:'Approved',4:'Approved/Accepted',
                            5:'Cancelled',6:'Denied',7:'Draft',8:'Filed',9:'Inactive',10:'Rejected',
                            11:'Rejected/Denied',12:'Returned',13:'Returned Unapproved',14:'Unacceptable',
                            15:'Under Review',16:'Vacated',17:'Withdrawn',18:'Entered in Error',19:'Lifted'}
        
        case_action_disp_change_codes = ['124','387','388','929','125','8421','130','424','8017','037','039',
                                        '113','134','197','237','242','271','276','279','282','304','307','308',
                                        '334','345','402','438','620','634','705','855','865','868','887','8176',
                                        '8176','199','244','272','804','234','310','967','970','915','8421','8017',
                                        '8284','8307','8334','8244','9311']
        case_action_disp_change_codes_int = [124,387,388,929,125,8421,130,424,8017,37,39,
                                        113,134,197,237,242,271,276,279,282,304,307,308,
                                        334,345,402,438,620,634,705,855,865,868,887,8176,
                                        8176,199,244,272,804,234,310,967,970,915,8421,8017,
                                        8284,8307,8334,8244,9311]
        sf_full_case_action_chunks = pd.read_csv(sf_full_case_action_fp, low_memory=False, chunksize = 1000000, sep = '|')
        sf_mc_case_action_chunks = pd.read_csv(sf_mc_case_action_fp, low_memory=False, chunksize = 1000000, sep = '|')
        snapshots = [sf_full_case_action_chunks,sf_mc_case_action_chunks]
        df_mc_all = pd.DataFrame()
        df_fm_all = pd.DataFrame()
        df_other_all = pd.DataFrame()
        for i, snapshot in enumerate(snapshots):
            for n, df in enumerate(snapshot):
                logging.info('\t\tQuerying chunk {} of subset {}'.format(n+1, i+1))
                # df = df.drop(columns='Unnamed: 0')
                df.columns = case_action_cols
                df[['CREATEDDATE','META_LOAD_DT','DATE_FILED','ACTION_DECISION_DT']] = df[['CREATEDDATE','META_LOAD_DT','DATE_FILED','ACTION_DECISION_DT']].apply(pd.to_datetime, errors='coerce')
                ##### query below ####
                df_mc = df[df['BLM_CASE'].isin(SYT04b1['ID'])]
                df_mc = df_mc[df_mc['CREATEDDATE']>"2021-01-25"]
                df_mc = df_mc[(df_mc['ACTION_CODE'].astype('int', errors='ignore').isin(case_action_disp_change_codes_int))|(df_mc['ACTION_CODE'].astype('str').isin(case_action_disp_change_codes))]

                df_fm = df[df['BLM_CASE'].isin(SYT04c1['ID'])]
                df_fm = df_fm[df_fm['CREATEDDATE']>"2022-03-14"]
                df_fm = df_fm[(df_fm['ACTION_CODE'].astype('int', errors='ignore').isin(case_action_disp_change_codes_int))|(df_fm['ACTION_CODE'].astype('str').isin(case_action_disp_change_codes))]

                df_other = df[df['BLM_CASE'].isin(SYT04d1['ID'])]
                df_other = df_other[df_other['CREATEDDATE']>"2023-06-16"]
                df_other = df_other[(df_other['ACTION_CODE'].astype('int', errors='ignore').isin(case_action_disp_change_codes_int))|(df_other['ACTION_CODE'].astype('str').isin(case_action_disp_change_codes))]
                ### concat chunks
                df_mc_all = pd.concat([df_mc_all,df_mc], ignore_index =True, axis = 0)
                df_fm_all = pd.concat([df_fm_all,df_fm], ignore_index =True, axis = 0)
                df_other_all = pd.concat([df_other_all,df_other], ignore_index =True, axis = 0)
                # break

        df_mc_all = df_mc_all.groupby('BLM_CASE')['CREATEDDATE'].max().reset_index()
        df_fm_all = df_fm_all.groupby('BLM_CASE')['CREATEDDATE'].max().reset_index()
        df_other_all = df_other_all.groupby('BLM_CASE')['CREATEDDATE'].max().reset_index()

        return df_mc_all, df_fm_all, df_other_all

    ## Tests ###############################################################################
    # Call in SF Cases
    SYT04a = call_cases_into_memory(sf_full_cases_fp,sf_mc_cases_fp)
    logging.info(f'SYT04a result: {SYT04a.shape[0]}')

    # call in nlsdb and join dispostion to Sf Cases table
    nlsdb = pd.read_csv(nlsdb_fp, low_memory=False, sep=',')
    nlsdb = nlsdb[['SF_ID','CSE_DISP']]
    nlsdb['CSE_DISP'] = nlsdb['CSE_DISP'].str.upper()
    SYT04a = pd.merge(SYT04a,nlsdb, left_on='ID',right_on='SF_ID',how='left')
    SYT04a = SYT04a[~SYT04a['SF_ID'].isna()]
    # logging.info(f'\t this is the # of SF cases found in NLSDB: {SYT04a.shape[0]}')
    SYT04a['nlsdb_disp_test'] = SYT04a['CASE_STATUS'] == SYT04a['CSE_DISP']
    # logging.info(f'test, this number should equal last output: {SYT04a.shape[0]}')
    SYT04 = SYT04a[SYT04a['nlsdb_disp_test']==False]
    # logging.info('test - {}'.format(SYT04.head().to_string()))
    # logging.info(f'SYT04 result: {SYT04.shape[0]}')
    ## Subset the SF Cases ###############################
    # mining claims - R1
    SYT04b1 = SYT04a[SYT04a['RECORDTYPEID']=='012t00000008bvNAAQ']
    logging.info(f'SYT04b1 result: # of SF Mining Claim Cases: {SYT04b1.shape[0]}')
    # fluid minerals - R2
    SYT04c1 = SYT04a[SYT04a['RECORDTYPEID'].isin(['0123d0000004QFPAA2','0123d0000004QFTAA2','0123d0000004QFUAA2'])]
    logging.info(f'SYT04c1 result: # of SF Fluid Minerals Cases: {SYT04c1.shape[0]}')
    # others - R3
    SYT04d1 = SYT04a[SYT04a['RECORDTYPEID'].isin(['0123d0000004QFQAA2','0123d0000005ISCAA2','0123d0000005ISEAA2',
                                                '0123d0000005ISFAA2','012t00000008bvLAAQ','0123d0000004QFRAA2',
                                                '0123d0000005ISGAA2','0123d0000004QFVAA2','0123d0000004QFSAA2'])]
    logging.info(f'SYT04d1 result: # of SF Other Cases: {SYT04d1.shape[0]}')

    SYT04b2, SYT04c2, SYT04d2 = extract_from_case_action(SYT04b1, SYT04c1, SYT04d1, sf_full_case_action_fp, sf_mc_case_action_fp)
    logging.info(f'SYT04b2 result: {SYT04b2.shape[0]}')
    logging.info(f'SYT04c2 result: {SYT04c2.shape[0]}')
    logging.info(f'SYT04d2 result: {SYT04d2.shape[0]}')

    SYT04b1 = pd.merge(SYT04b1,SYT04b2, left_on='ID',right_on='BLM_CASE',how='left')
    SYT04b2 = SYT04b1[SYT04b1['ID'].isin(SYT04b2['BLM_CASE'])]
    SYT04c1 = pd.merge(SYT04c1,SYT04c2, left_on='ID',right_on='BLM_CASE',how='left')
    SYT04c2 = SYT04c1[SYT04c1['ID'].isin(SYT04c2['BLM_CASE'])]
    SYT04d1 = pd.merge(SYT04d1,SYT04d2, left_on='ID',right_on='BLM_CASE',how='left')    
    SYT04d2 = SYT04d1[SYT04d1['ID'].isin(SYT04d2['BLM_CASE'])]    

    SYT04b2 = SYT04b2[~SYT04b2['SF_ID'].isna()]
    logging.info(f'SYT04b2 result found in NLSDB: {SYT04b2.shape[0]}')
    SYT04c2 = SYT04c2[~SYT04c2['SF_ID'].isna()]
    logging.info(f'SYT04c2 result found in NLSDB: {SYT04c2.shape[0]}')
    SYT04d2 = SYT04d2[~SYT04d2['SF_ID'].isna()]
    logging.info(f'SYT04d2 result found in NLSDB: {SYT04d2.shape[0]}')

    SYT04b3 = SYT04b2[SYT04b2['nlsdb_disp_test']==False]
    logging.info(f'SYT04b3 result: {SYT04b3.shape[0]}')
    SYT04c3 = SYT04c2[SYT04c2['nlsdb_disp_test']==False]
    logging.info(f'SYT04c3 result: {SYT04c3.shape[0]}')
    SYT04d3 = SYT04d2[SYT04d2['nlsdb_disp_test']==False]
    logging.info(f'SYT04d3 result: {SYT04d3.shape[0]}')
    SYT04b3.to_csv('results/SYT042b3.csv')
    SYT04c3.to_csv('results/SYT042c3.csv')
    SYT04d3.to_csv('results/SYT042d3.csv')

    SYT04e = pd.concat([SYT04b3, SYT04c3, SYT04d3], ignore_index=True, axis = 0)
    logging.info(f'SYT04e result: {SYT04e.shape[0]}')

    # add record type names from lookup table
    record_types = pd.read_csv(record_types_fp)
    SYT04e = pd.merge(SYT04e,record_types, left_on='RECORDTYPEID', right_on='RECORDTYPEID', how='left')
    # SYT04e.to_csv('SYT04e.csv')

if __name__ == "__main__":
    main()









