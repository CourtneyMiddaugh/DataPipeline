'''This module will be used to load iped data'''
'''This module hosts the code which allows the ipeds data to be entered 
into our database. The user will input the filename and the year the data was 
collected.'''
import psycopg
from credentials import DB_NAME, DB_USER, DB_PASSWORD
import pandas as pd
import sys
import numpy as np


with open("invalid_ipeds.csv", "w", encoding="utf-8") as f:
    FILE = sys.argv[1]


    conn = psycopg.connect(
        host="pinniped.postgres.database.azure.com", dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

    data = pd.read_csv(FILE, encoding='unicode_escape')

    data = data.where(pd.notnull(data), None)
    data = data.astype("object").replace(np.nan, None)

    errors = pd.DataFrame(columns = list(data.columns))
    num_rows_inserted = 0


    cur = conn.cursor()

    locations = pd.read_sql_query("SELECT UNITID FROM location", conn)

    with conn.transaction():

        for ind in data.index:
            try:
                id = data['UNITID'][ind]
                if locations.unitid.isin([id]).any():
                    continue
                with conn.transaction():
                    # data for location table
                    print(num_rows_inserted)
                    name = data['INSTNM'][ind]
                    addr = data['ADDR'][ind]
                    city = data['CITY'][ind]
                    state = data['STABBR'][ind]
                    zips = str(data['ZIP'][ind]).split("-")[0]
                    latitude = data['LATITUDE'][ind]
                    longitude = data['LONGITUD'][ind]
                    cbsa = data['CBSA'][ind]
                    cbsa_type = data['CBSATYPE'][ind]
                    csa = data['CSA'][ind]
                    fips = data['FIPS'][ind]

                    val = (id, name, addr, city, state, zips, latitude,
                            longitude, cbsa, cbsa_type, csa, fips)
                    
                    cur.execute('''INSERT INTO location VALUES (%s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s)''', val)
 
            except Exception as e:
                print("insert failed", e, "on row ", ind)
                errors = errors.append(data.loc[ind,])

            else:
                num_rows_inserted += 1
    conn.commit()
    errors.to_csv('invalid_ipeds.csv')
    print("Successfully submitted ", (num_rows_inserted /
                                      data.shape[0]) * 100, "%")
    print(num_rows_inserted,  "rows inserted out of ", data.shape[0])