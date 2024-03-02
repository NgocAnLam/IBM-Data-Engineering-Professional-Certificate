import json
import sqlite3
import requests, json
import pandas as pd
import numpy as np
from datetime import datetime 
from bs4 import BeautifulSoup


with open('config.json') as config_file:
    config_data = json.load(config_file)

log_file = config_data["log_file"]
url = config_data["url"]
table_attribs = config_data["table_attribs_extraction"]
table_attribs_final = config_data["table_attribs_final"]
db_name = config_data["db_name"]
table_name = config_data["table_name"]
csv_path = config_data["csv_path"]
output_path = config_data["output_path"]

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n') 


def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[1].find('a') is not None:
                data_dict = {
                    "Name": col[1].contents[2].contents[0],
                    "MC_USD_Billion": float(col[2].contents[0])
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df


def transform(df, csv_path):
    dfExchange = pd.read_csv(csv_path)
    exchange_rate = dfExchange.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df


def load_to_csv(df, output_path):
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_path)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, output_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) from {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)
log_progress('Process Complete')

sql_connection.close()
log_progress('Server Connection closed')