import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './exchange_rate.csv'
output_path = './Largest_banks_data.csv'

def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S'  
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    table = data.select_one('.wikitable.sortable')
    rows = table.find_all('tr')[1:]
    for row in rows:
        col = row.find_all('td')
        if len(col) >= 3:
            name = col[1].text.strip()
            mc_usd = col[2].text.strip()
            df = df.append({'Name': name, 'MC_USD_Billion': mc_usd}, ignore_index=True)
    return df

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)

def transform(df, csv_path):
    exchange_rates = pd.read_csv(csv_path)
    dict = exchange_rates.set_index('Currency')['Rate'].to_dict()
    for currency, rate in dict.items():
        if currency in ['GBP', 'EUR', 'INR']:
            new_column = f"MC_{currency}_Billion"
            df[new_column] = np.round(df['MC_USD_Billion'].astype(float) * rate, 2)  # Ubah ke float sebelum dikalikan
    return df

log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df, csv_path)

def load_to_csv(df, output_path):
    df.to_csv(output_path)

log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress('Data loaded to Database as table. Running the query')
query_statements = [
    "SELECT * FROM Largest_banks",
    "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
    "SELECT Name FROM Largest_banks LIMIT 5"
]
for query_statement in query_statements:
    run_query(query_statement, sql_connection)

log_progress('Process Complete.')
sql_connection.close()

