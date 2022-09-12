# -*- coding: utf-8 -*-
import argparse
import cx_Oracle
import datetime
import io
import pandas as pd
import requests
from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description='Loading data to database.')
parser.add_argument('--instantclient', type=str, default=r"C:\oracle\instantclient_19_8_x64")
parser.add_argument('--user', type=str, default="covid")
parser.add_argument('--pwd', type=str, default="covid")
parser.add_argument('--ip', type=str, default="137.204.74.10")
parser.add_argument('--port', type=str, default="1521")
parser.add_argument('--db', type=str, default="research")
args = parser.parse_args()

# MySQL engine
# engine = create_engine("mysql+pymysql://{user}:{pw}@{ip}:{port}/{db}".format(user=args.user, pw=args.pwd, ip=args.ip, port=args.port, db=args.db))
# Oracle engine
cx_Oracle.init_oracle_client(lib_dir=args.instantclient)
sid = cx_Oracle.makedsn(args.ip, args.port, sid=args.db)
engine = create_engine('oracle://{user}:{password}@{sid}?charset=utf8'.format(user=args.user, password=args.pwd, sid=sid))

# Read from REMOTE URL
url = "https://opendata.ecdc.europa.eu/covid19/casedistribution/csv"
s = requests.get(url).content
data = pd.read_csv(io.StringIO(s.decode('utf-8-sig'))) # read from remote URL
data.to_csv("../../../resources/describe/covid.csv", index=False)
data.columns = [x.upper() for x in data.columns]
# Read from FILE
# df = pd.read_csv("../../../resources/describe/covid.csv") # load from csv

df = data
num = df._get_numeric_data()
num[num < 0] = 0
print(df)
df = df[df['COUNTRIESANDTERRITORIES'].notnull() & df['GEOID'].notnull() & df['POPDATA2019'].notnull()]

df["DATEREP"] = df["DATEREP"].apply(lambda x: datetime.datetime.strptime(x, '%d/%m/%Y'))
df["MONTH"] = df["DATEREP"].apply(lambda x: x.strftime('%Y-%m'))
df["DATEREP"] = df["DATEREP"].apply(lambda x: x.strftime('%Y-%m-%d'))

country = df[["GEOID", "COUNTRIESANDTERRITORIES", "COUNTRYTERRITORYCODE", "POPDATA2019", "CONTINENTEXP"]]
country = country.drop_duplicates()

date = df[["DATEREP", "MONTH", "YEAR"]]
date = date.drop_duplicates()

df["CASES100K"] = df["CASES"] / (df["POPDATA2019"] / 100000)
df["CASES1M"] = df["CASES"] / (df["POPDATA2019"] / 1000000)
df["DEATHS100K"] = df["DEATHS"] / (df["POPDATA2019"] / 100000)
df["DEATHS1M"] = df["DEATHS"] / (df["POPDATA2019"] / 1000000)
df["DEATHS14"] = df.apply(lambda x: x["DEATHS"] / (x["POPDATA2019"] / 100000) if x["DATEREP"] >= "2020/05/04" else 0, axis=1)
df["CASES14"] = df.apply(lambda x: x["CASES"] / (x["POPDATA2019"] / 100000) if x["DATEREP"] >= "2020/05/04" else 0, axis=1)

fact = df[["DATEREP", "COUNTRIESANDTERRITORIES",
           "CASES", "CASES100K", "CASES1M", "CASES14",
           "DEATHS", "DEATHS100K", "DEATHS1M", "DEATHS14"]]
fact = fact.drop_duplicates()

query_country = 'INSERT INTO COUNTRY VALUES (:1, :2, :3, :4, :5)'
query_date = 'INSERT INTO COVIDDATE VALUES (:1, :2, :3)'
query_fact = 'INSERT INTO COVIDFACT VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)'

def append(query, df):
    db = cx_Oracle.connect(args.user, args.pwd, args.ip + "/" + args.db, encoding="UTF-8")
    cursor = db.cursor()
    my_data = []
    for row in df.values:
        my_data.append(tuple(row))
        if (len(my_data) > 10000):
            print(my_data[0])
            cursor.executemany(query, my_data)
            db.commit()
            my_data = []
    cursor.executemany(query, my_data)
    cursor.close()
    db.commit()

append(query_country, country)
append(query_date, date)
append(query_fact, fact)
# country.to_sql(con=engine, name='country', if_exists='append', index=False) #, method='multi'
# print("Done country")
# date.to_sql(con=engine, name='coviddate', if_exists='append', index=False)
# print("Done date")
# fact.to_sql(con=engine, name='covidfact', if_exists='append', index=False)
# print("Done fact")