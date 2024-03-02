import json
import sqlite3
from etl_project_gdp import extract, transform, load_to_csv, load_to_db, run_query, log_progress


with open("config.json") as config_file:
    config_data = json.load(config_file)

log_file = config_data["log_file"]
url = config_data["url"]
table_attribs = config_data["table_attribs"]
db_name = config_data["db_name"]
table_name = config_data["table_name"]
csv_path = config_data["csv_path"]


log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)

log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')
sql_connection.close()