'''This module is used to load the college scorecard data'''

'''This module hosts the code which allows the scorecard data to be entered 
into our database. The user will input the filename and the year the data was 
collected.'''
import psycopg
from credentials import DB_NAME, DB_USER, DB_PASSWORD
import pandas as pd
import sys
import numpy as np

with open("invalid_college_scorecard.csv", "w", encoding="utf-8") as f:
    FILE = sys.argv[1]
    YEAR = sys.argv[2]


    conn = psycopg.connect(
        host="pinniped.postgres.database.azure.com", dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

    data = pd.read_csv(FILE)
    errors = pd.DataFrame(columns=list(data.columns))
    data = data.where(pd.notnull(data), None)
    data = data.astype("object").replace(np.nan, None)

    data.WDRAW_ORIG_YR2_RT = data.WDRAW_ORIG_YR2_RT.replace('PrivacySuppressed',
                                                            np.nan)

    num_rows_inserted = 0

    cur = conn.cursor()
    num_row = 1

    locations = pd.read_sql_query("SELECT UNITID FROM region", conn)
    with conn.transaction():

        for ind in data.index:
            try:
                with conn.transaction():
                    # university finance table
                    print(num_row)
                    num_row += 1
                    id = data['UNITID'][ind]
                    cdr2 = data['CDR2'][ind]
                    cdr3 = data['CDR3'][ind]
                    avgfacsal = data['AVGFACSAL'][ind]
                    tuitfte = data['TUITFTE'][ind]
                    tuitionfee_in = data['TUITIONFEE_IN'][ind]
                    tuitionfee_out = data['TUITIONFEE_OUT'][ind]
                    tuitionfee_prog = data['TUITIONFEE_PROG'][ind]
                    preddeg = data['PREDDEG'][ind]
                    highdeg = data['HIGHDEG'][ind]
                    control = data['CONTROL'][ind]
                    ccbasic = data['CCBASIC'][ind]
                    adm_rate = data['ADM_RATE'][ind]
                    stufacr = data['STUFACR'][ind]
                    wraw_orig_yr2_rt = data['WDRAW_ORIG_YR2_RT'][ind]
                    region = data['REGION'][ind]
                    ugds_men = data['UGDS_MEN'][ind]
                    ugds_women = data['UGDS_WOMEN'][ind]
                    grads = data['GRADS'][ind]
                    ug = data['UG'][ind]
                    ugds_black = data['UGDS_BLACK'][ind]
                    ugds_white = data['UGDS_WHITE'][ind]
                    ugds_hisp = data['UGDS_HISP'][ind]
                    ugds_asian = data['UGDS_ASIAN'][ind]
                    ugds_aian = data['UGDS_AIAN'][ind]
                    ugds_nhpi = data['UGDS_NHPI'][ind]
                    ugds_unkn = data['UGDS_UNKN'][ind]
                    finance_vars = (id, YEAR, cdr2, cdr3, avgfacsal, tuitfte,
                                    tuitionfee_in, tuitionfee_out, tuitionfee_prog)
                    
                    demographic_vars = (id, YEAR, preddeg, highdeg,
                                        ugds_men, ugds_women, grads, ug,
                                        ugds_black, ugds_white, ugds_hisp,
                                        ugds_asian, ugds_aian, ugds_nhpi,
                                        ugds_unkn)
                    
                    fact_vars = (id, YEAR, control, ccbasic, adm_rate, stufacr,
                                 wraw_orig_yr2_rt)
                    
                    region_vars = (id, region)

                    # entering data into finance table
                    cur.execute('''INSERT INTO finances(
                                UNITID, year_data, CDR2, CDR3, AVGFACSAL,
                                TUITFTE, TUITIONFEE_IN, TUITIONFEE_OUT,
                                TUITIONFEE_PROG) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ''', finance_vars)
                    
                    # entering data into demographics table
                    cur.execute('''INSERT INTO demographics(
                                UNITID, year_data, PREDDEG, HIGHDEG, UGDS_MEN,
                                UGDS_WOMEN, GRADS, UG, UGDS_BLACK, UGDS_WHITE,
                                UGDS_HISP, UGDS_ASIAN, UGDS_AIAN, UGDS_NHPI,
                                UGDS_UNKN)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s)
                                ''', demographic_vars)
                    #entering data into fast facts table
                    cur.execute('''INSERT INTO fast_facts(
                                UNITID, year_data, CONTROL, CCBASIC, ADM_RATE,
                                STUFACR, WRAW_ORIG_YR2_RT)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ''', fact_vars)

                    # checking to see if location is already in the table
                    if not locations.unitid.isin([id]).any():
                        cur.execute('''INSERT INTO region(
                            UNITID, region)
                            VALUES (%s, %s)
                        ''', region_vars)

                    
                    
            except Exception as e:
                print("insert failed", e, "on row ", ind)
                errors = errors.append(data.loc[ind,])

            else:
                num_rows_inserted += 1
    conn.commit()
    errors.to_csv('invalid_college_scorecard.csv')
    print("Successfully submitted ", (num_rows_inserted /
                                      data.shape[0]) * 100, "%")
    print(num_rows_inserted,  "rows inserted out of ", data.shape[0])
