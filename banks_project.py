# This is an ETL process I performed while working at China Everybright Bank.  
# It aims to extract the world's largest banks' market cap in USD billions from a wiki website,  
# transforming it into different countries' currencies based on exchange rates stored in a csv file,  
# and load it into a SQL table named 'Largest_banks' under the database 'Banks.db' for future queries.

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import numpy as np
import sqlite3

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
csv_path = '/home/project/Largest_banks_data.csv'
exchange_rate_path = 'exchange_rate.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'

def log_progress(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(f"{timestamp} : {message}\n")

def extract(url, table_attribs):
    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {"Name": col[1].find_all('a')[1]['title'],
                         "MC_USD_Billion": float(col[2].contents[0][:-1])}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)

    return df

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

def transform(df, exchange_rate_path):
    exchange_rate_df = pd.read_csv(exchange_rate_path)
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df

df = transform(df, exchange_rate_path)

log_progress('Data transformation complete. Initiating loading process')

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, df)

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress('Data loaded to Database as a table, Executing queries')

query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')
sql_connection.close()
