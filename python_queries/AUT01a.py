## ADMIN UNIT tests

## See logic graphic from report example in media folder
## AUT01a:	Total # of cases in Salesforce; excluding status records
## AUT01b:	Subset of the above: How many are new since migrations from LR2000
## AUT01c1:	Subset of the above: How many are Mining Claim records
## AUT01c2:	Subset of the above:  How many have a status of ACTIVE or FILED
## AUT01c3:	Subset of the above: How many have a quality score (in nlsdb) of <10 or ==15
## AUT01c4:	Subset of the above: How many do not have one each of (4) ADMIN_UNIT record types
## AUT01d1:	Subset of the AUT01b: How many are Non-Mining Claim records
## AUT01d2:	Subset of the above:  How many have a status of ACTIVE or FILED
## AUT01d3:	Subset of the above: How many have a quality score (in nlsdb) of <10 or ==15
## AUT01d4:	Subset of the above: How many do not have one each of (4) ADMIN_UNIT record types

import pandas as pd, logging

def main():    
    logging.basicConfig(filename='results/AUT01a.log', encoding='utf-8', level=logging.DEBUG, format='%(message)s: %(asctime)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('Start Test')
    ## File Paths #####################################################################################################################################
    sf_full_cases_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_BLM_CASE.csv"
    sf_mc_cases_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_BLM_CASE.csv"
    nlsdb_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\NLSDB\2025-05-05_NLSDB.gdb\Case_05052025_0700.csv"
    record_types_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\ALL_RECORD_TYPES.csv"
    sf_case_lands_full_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_CASE_LAND.csv"
    sf_case_lands_mc_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_CASE_LAND.csv"
    sf_admin_units_full_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_Full\2025-05-04\consolidated_tables\CR_FULL_ADMIN_UNIT.csv"
    sf_admin_units_mc_fp = r"C:\Users\es422\Documents\xentity\BLM\MLRS\Data\Snapshots\MLRS\2025-05-04_MLRS_MC\2025-05-04\consolidated_tables\MC_ADMIN_UNIT.csv"


    ####### Functions #################################################################################################################################
    ## Function to call in all Salesforce cases and exclude status records
    def call_cases_into_memory(full_case_fp,mc_case_fp):
        blm_case_full = pd.read_csv(full_case_fp, low_memory=False, sep = '|')
        print(f'# Cases from Salesforce Full snapshot:{blm_case_full.shape[0]}')
        # call in MC subset
        blm_case_mc = pd.read_csv(mc_case_fp, low_memory=False, sep = '|')
        print(f'# Cases from Salesfeorce Mining Claim subset: {blm_case_mc.shape[0]}')
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
        print(f'Total Non-Status Cases from Salesforce: {blm_case_all.shape[0]}')
        return blm_case_all
    ## Function to collect land records from sf_df, which must include the 'ID' attribute
    def collect_associated_lands(sf_df, sf_case_lands_full_fp, sf_case_lands_mc_fp):
        sf_full_case_lands = pd.read_csv(sf_case_lands_full_fp, sep = '|')
        sf_mc_case_lands = pd.read_csv(sf_case_lands_mc_fp, sep = '|')
        sf_case_lands = pd.concat([sf_full_case_lands,sf_mc_case_lands], ignore_index=True, axis = 0)
        del sf_full_case_lands
        del sf_mc_case_lands
        # sf_case_lands = sf_case_lands.drop('Unnamed: 0', axis =1)
        case_land_columns = [
            'ID','ISDELETED','NAME','CREATEDDATE','CREATEDBYID','LASTMODIFIEDDATE','LASTMODIFIEDBYID','SYSTEMMODSTAMP',
            'LASTACTIVITYDATE','LASTVIEWEDDATE','LASTREFERENCEDDATE','BLM_CASE','ACRES','ADMIN_STATE_CODE',
            'ALIQUOT_PART_DESCRIPTION','ALIQUOT_PART','BLM_DISTRICT_CODE','GEOGRAPHIC_STATE_CODE','LEGACY_ID',
            'LEGACY_SYSTEM_CODE','LEGAL_LAND_DESCRIPTION','MERIDIAN_CODE','MERIDIAN_QUADRANT_CODE','METES_AND_BOUNDS',
            'NLSDB_LAND_OID','RANGE_NUMBER','SECTION_NUMBER','SURVEY_NUMBER','SURVEY_TYPE','TOWNSHIP_NUMBER','META_LOAD_DT',
            'MTR','MTRS','LEGACY_ID_WO_ALIQUOT',
            ## new ones post r4 below
            'ACTION_EFFECTIVE_DATE','ALASKA_LAND_SELECTION','ALIS_LEGACY_ID','FORMATTED_LLD_TXT','LAND_STATUS','PRIORITY'

        ]
        sf_case_lands.columns = case_land_columns
        result = sf_case_lands[sf_case_lands['BLM_CASE'].isin(sf_df['ID'])]
        return result
    ## Function to collect ADMIN_UNIT record types per CASE_LAND from sf_land_df using 'CASE_LAND' attribute
    def collect_admin_unit_types_per_land(sf_land_df, sf_admin_units_full_fp, sf_admin_units_mc_fp):
        admin_unit_columns = ['ID','ISDELETED','NAME','CREATEDDATE','CREATEDBYID','LASTMODIFIEDDATE',
                        'LASTMODIFIEDBYID','SYSTEMMODSTAMP','CASE_LAND','CODE','INTERSECT_ACRES',
                        'TEXT','TYPE','META_LOAD_DT']
        full_admin_units_chunks = pd.read_csv(sf_admin_units_full_fp, low_memory=False, dtype='str', chunksize = 1000000, sep = '|')
        mc_admin_units_chunks = pd.read_csv(sf_admin_units_mc_fp, low_memory=False, dtype='str', chunksize = 1000000, sep = '|')
        i = 1
        all_case_land_admin_unit_types = pd.DataFrame()
        for source in [full_admin_units_chunks,mc_admin_units_chunks]:
            print(f'subset: {i}')
            i += 1
            z = 1
            for df in source:
                print(f'chunk: {z}')
                z+=1
                # df = df.drop(columns='Unnamed: 0', axis=1)
                df.columns = admin_unit_columns
                df = df[df['CASE_LAND'].isin(sf_land_df['ID'])]
                case_land_admin_unit_types = df.groupby(['CASE_LAND'])['TYPE'].apply(','.join).reset_index()
                all_case_land_admin_unit_types = pd.concat([all_case_land_admin_unit_types,case_land_admin_unit_types], ignore_index=True, axis = 0)
                # break
            all_case_land_admin_unit_types = all_case_land_admin_unit_types.groupby(['CASE_LAND'])['TYPE'].apply(','.join).reset_index()
        return all_case_land_admin_unit_types

    ######### TESTS AND RESULTS DUMPS #################################################################################################################
    ## Quality scores from NLSDB to include in analysis, see infographic
    scores_filter = ['4.1','8.2','0','1','3','8','5','-1','4','2','8.4','15','9','8.1','6','7']
    ## AUT01a:	Total # of cases in Salesforce; excluding status records
    AUT01a = call_cases_into_memory(sf_full_cases_fp,sf_mc_cases_fp)
    logging.info(f'AUT01a result: {AUT01a.shape[0]}')
    ## call in nlsdb and join dispostion to Sf Cases table
    nlsdb = pd.read_csv(nlsdb_fp, low_memory = False, sep = ',')
    nlsdb = nlsdb[['SF_ID','QLTY']]
    nlsdb['QLTY_SCORE_derived'] = [str(x).split()[0].split(':')[0].split(';')[0] for x in nlsdb['QLTY']]
    AUT01a = pd.merge(AUT01a,nlsdb, left_on='ID', right_on='SF_ID', how='left')
    # logging.info(f'**test, # should equal last output: {AUT01a.shape[0]}')
    ## AUT01b:	Subset of the above: How many are new since migrations from LR2000
    AUT01b = AUT01a[AUT01a['LEGACY_SERIAL_NUMBER'].isna()]
    logging.info(f'AUT01b result: {AUT01b.shape[0]}')
    ## AUT01c1:	Subset of the above: How many are Mining Claim records
    AUT01c1 = AUT01b[AUT01b['RECORDTYPEID']=='012t00000008bvNAAQ']
    logging.info(f'AUT01c1 result: {AUT01c1.shape[0]}')
    ## AUT01c2:	Subset of the above:  How many have a status of ACTIVE or FILED
    AUT01c2 = AUT01c1[AUT01c1['CASE_STATUS'].isin(['ACTIVE','FILED'])]
    logging.info(f'AUT01c2 result: {AUT01c2.shape[0]}')
    # AUT01c3:	Subset of the above: How many have a quality score (in nlsdb) of <10 or ==15
    AUT01c3 = AUT01c2[AUT01c2['QLTY_SCORE_derived'].isin(scores_filter)]
    logging.info(f'AUT01c3 result: {AUT01c3.shape[0]}')
    # AUT01c4:	Subset of the above: How many do not have one each of (4) ADMIN_UNIT record types
    ## first collect all land records for each case
    case_lands = collect_associated_lands(AUT01c3, sf_case_lands_full_fp, sf_case_lands_mc_fp)
    admin_units = collect_admin_unit_types_per_land(case_lands, sf_admin_units_full_fp, sf_admin_units_mc_fp)
    case_lands = pd.merge(case_lands,admin_units, left_on='ID', right_on='CASE_LAND', how='left')
    case_lands = case_lands[['BLM_CASE','TYPE']]
    case_lands['SMA_check'] = ['Y' if 'SMA' in x else 'N' for x in case_lands['TYPE'].astype('str')]
    case_lands['District_Office_check'] = ['Y' if 'District Office' in x else 'N' for x in case_lands['TYPE'].astype('str')]
    case_lands['Field_Office_check'] = ['Y' if 'Field Office' in x else 'N' for x in case_lands['TYPE'].astype('str')]
    case_lands['County_check'] = ['Y' if 'County' in x else 'N' for x in case_lands['TYPE'].astype('str')]
    case_lands['ADMIN_UNIT_test'] = ['fail' if 'N' in x else 'pass' for x in (case_lands['SMA_check']+case_lands['District_Office_check']+
                                                                            case_lands['Field_Office_check']+
                                                                            case_lands['County_check'])]

    # the fails
    AUT01c4 = case_lands[case_lands['ADMIN_UNIT_test']=='fail']
    AUT01c4 = AUT01c4.groupby('BLM_CASE').count()[['TYPE']].reset_index().rename(columns={'TYPE':'#_Case_Lands_Fail_ADMIN_UNIT'})
    AUT01c4 = pd.merge(AUT01c3, AUT01c4, left_on='ID',right_on='BLM_CASE', how='left')
    AUT01c4 = AUT01c4[~AUT01c4['#_Case_Lands_Fail_ADMIN_UNIT'].isna()]
    logging.info(f'AUT01c4 result: {AUT01c4.shape[0]}')
    AUT01c4.to_csv('results/AUT01c4.csv')

    # AUT01d1:	Subset of the AUT01b: How many are Non-Mining Claim records
    AUT01d1 = AUT01b[AUT01b['RECORDTYPEID']!='012t00000008bvNAAQ']
    logging.info(f'AUT01d1 result: {AUT01d1.shape[0]}')
    # AUT01d2:	Subset of the above:  How many have a status of ACTIVE or FILED
    AUT01d2 = AUT01d1[AUT01d1['CASE_STATUS'].isin(['AUTHORIZED','PENDING'])]
    logging.info(f'AUT01d2 result: {AUT01d2.shape[0]}')
    # AUT01d3:	Subset of the above: How many have a quality score (in nlsdb) of <10 or ==15
    AUT01d3 = AUT01d2[AUT01d2['QLTY_SCORE_derived'].isin(scores_filter)]
    logging.info(f'AUT01d3 result: {AUT01d3.shape[0]}')

    # AUT01d4:	Subset of the above: How many do not have one each of (4) ADMIN_UNIT record types
    ## first collect all land records for each case
    case_lands = collect_associated_lands(AUT01d3, sf_case_lands_full_fp, sf_case_lands_mc_fp)
    admin_units = collect_admin_unit_types_per_land(case_lands, sf_admin_units_full_fp, sf_admin_units_mc_fp)
    case_lands = pd.merge(case_lands,admin_units, left_on='ID', right_on='CASE_LAND', how='left')
    case_lands = case_lands[['BLM_CASE','TYPE']]
    case_lands['SMA_check'] = ['Y' if 'SMA' in x else 'N' for x in case_lands['TYPE'].astype('str')]
    case_lands['District_Office_check'] = ['Y' if 'District Office' in x else 'N' for x in case_lands['TYPE'].astype('str')]
    case_lands['Field_Office_check'] = ['Y' if 'Field Office' in x else 'N' for x in case_lands['TYPE'].astype('str')]
    case_lands['County_check'] = ['Y' if 'County' in x else 'N' for x in case_lands['TYPE'].astype('str')]
    case_lands['ADMIN_UNIT_test'] = ['fail' if 'N' in x else 'pass' for x in (case_lands['SMA_check']+case_lands['District_Office_check']+
                                                                            case_lands['Field_Office_check']+
                                                                            case_lands['County_check'])]

    # the fails
    AUT01d4 = case_lands[case_lands['ADMIN_UNIT_test']=='fail']
    AUT01d4 = AUT01d4.groupby('BLM_CASE').count()[['TYPE']].reset_index().rename(columns={'TYPE':'#_Case_Lands_Fail_ADMIN_UNIT'})
    AUT01d4 = pd.merge(AUT01d3, AUT01d4, left_on='ID',right_on='BLM_CASE', how='left')
    AUT01d4 = AUT01d4[~AUT01d4['#_Case_Lands_Fail_ADMIN_UNIT'].isna()]
    logging.info(f'AUT01d4 result: {AUT01d4.shape[0]}')
    AUT01d4.to_csv('results/AUT01d4.csv')

if __name__ == "__main__":
    main()










