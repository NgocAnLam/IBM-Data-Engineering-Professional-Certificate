import json
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup


with open("config.json", "r") as config_file:
    config_data = json.load(config_file)

url = config_data["url"]
csv_path = config_data["csv_path"]
db_name = config_data["db_name"]
table_name = config_data["table_name"]


df = pd.DataFrame(columns=["Film", "Year", "Rotten Tomatoes' Top 100"])
count = 0

html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

for row in rows:
    if count < 25:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {
                "Film": col[1].contents[0],
                "Year": int(col[2].contents[0]),
                "Rotten Tomatoes' Top 100": col[3].contents[0],
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
            count+=1
    else:
        break


df = df[(df['Year'] >= 2000)]
df.to_csv(csv_path)

### Connect SQLite3 database
conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()