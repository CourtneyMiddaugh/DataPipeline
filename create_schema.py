'''This module will create the schema for data to be loaded from College
Scorecard and IPEDS Data'''
import psycopg
from credentials import DB_NAME, DB_USER, DB_PASSWORD


conn = psycopg.connect(
        host="pinniped.postgres.database.azure.com", dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
)

cur = conn.cursor()
# creating schema for table which will decode control variable
cur.execute('''CREATE TABLE control(
CODE INTEGER PRIMARY KEY,
CONTROL TEXT               
);''')

control_vars = [(1, "Public"), (2, "Private non-profit"),
                (3, "Private for-profit")]
# adding values to control table to decode control variable
for control_type in control_vars:
    cur.execute("INSERT INTO control VALUES (%s, %s);", control_type)


# creates table schema of universities' financial information
cur.execute('''CREATE TABLE finances(
UNITID INTEGER,
year_data INTEGER,
CDR2 FLOAT,
CDR3 FLOAT,
AVGFACSAL INTEGER CHECK(AVGFACSAL > 0),
TUITFTE INTEGER CHECK(TUITFTE > 0),
TUITIONFEE_IN INTEGER CHECK(TUITIONFEE_IN > 0),
TUITIONFEE_OUT INTEGER CHECK(TUITIONFEE_OUT > 0),
TUITIONFEE_PROG INTEGER CHECK(TUITIONFEE_PROG >=0),
PRIMARY KEY (UNITID, year_data)
);''')


# creates the table schema of universities' demographic information
cur.execute('''CREATE TABLE demographics(
UNITID INTEGER,
year_data INTEGER,
PREDDEG INTEGER CHECK(PREDDEG >= 0),
HIGHDEG INTEGER CHECK(HIGHDEG >= 0),
UGDS_MEN FLOAT CHECK(UGDS_MEN >= 0),
UGDS_WOMEN FLOAT CHECK(UGDS_WOMEN >= 0),
GRADS INTEGER CHECK(GRADS >= 0),
UG INTEGER CHECK(UG >= 0),
UGDS_BLACK FLOAT CHECK(UGDS_BLACK >= 0),
UGDS_WHITE FLOAT CHECK(UGDS_WHITE >= 0),
UGDS_HISP FLOAT CHECK(UGDS_HISP >= 0),
UGDS_ASIAN FLOAT CHECK(UGDS_ASIAN >= 0),
UGDS_AIAN FLOAT CHECK(UGDS_AIAN >= 0),
UGDS_NHPI FLOAT CHECK(UGDS_NHPI >= 0),
UGDS_UNKN FLOAT CHECK(UGDS_UNKN >= 0),
PRIMARY KEY (UNITID, year_data)
);''')

# creates the table schema of universities' fast facts
# contains information you would find in a brochure
cur.execute('''CREATE TABLE fast_facts(
UNITID INTEGER,
year_data INTEGER,
CONTROL INTEGER REFERENCES control (CODE),
CCBASIC INTEGER CHECK(CCBASIC >= 0),
ADM_RATE FLOAT CHECK(ADM_RATE >= 0),
STUFACR FLOAT CHECK(STUFACR >= 0),
WRAW_ORIG_YR2_RT FLOAT CHECK(WRAW_ORIG_YR2_RT >= 0),
PRIMARY KEY (UNITID, year_data)       
);''')

# creates the table schema of which region each university is in
# region information comes from college scorecard
cur.execute('''CREATE TABLE region(
UNITID INTEGER PRIMARY KEY,
REGION INTEGER CHECK(REGION >= 0)
);''')

# creates the table schema of universities' location
cur.execute('''CREATE TABLE location(
    UNITID INTEGER PRIMARY KEY,
    INSTNM TEXT,
    ADDR  TEXT,
    CITY TEXT,
    STABBR TEXT CHECK(LENGTH(STABBR)=2),
    ZIP TEXT,
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    CBSA INTEGER,
    CBSATYPE INTEGER,
    CSA INTEGER,
    FIPS INTEGER CHECK(LENGTH(CAST(FIPS AS VARCHAR(50))) =5)
);''')


conn.commit()
