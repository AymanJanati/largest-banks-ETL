#in this project i'm using the csv file below for exchange rate, try wget it or curl or directly downloading it :)
#https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv
#also this project use external librarie as BeautifulSoup and pandas, to locally get it execute: python -m pip install <library name>

import pandas as pd 
import numpy as np
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import sqlite3

table_attribs = ["Name", "MC_USD_Billion"]
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
output_path = "./Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"
conn = sqlite3.connect("Banks.db")

def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + " : " + message + "\n")

    
def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, "html.parser")
    #to avoid selecting nav tables and other non-content tables...
    tables = data.find_all("table", {"class": "wikitable"})
    target_table = tables[0]
    rows = target_table.find_all("tr")
    df = pd.DataFrame(columns=table_attribs)
    for row in rows[1:]: #skipping the header
        cols = row.find_all("td")
        if len(cols) == 0:
            continue
        name = cols[1].text.strip()
        market_cap = float(cols[2].text.strip().replace("\n", ""))
        df1 = pd.DataFrame({"Name": name, "Market cap (US$ billion)": market_cap}, index=[0])
        df1 = df1.rename(columns={"Market cap (US$ billion)": "MC_USD_Billion"})
        df = pd.concat([df, df1], ignore_index = True)
    log_progress("Data extraction complete. Initiating Transformation process")
    return df

def transform(df, csv_path):
    exchange_rate = pd.read_csv(csv_path, index_col=0).to_dict()["Rate"]
    df["MC_GBP_Billion"] = [np.round(x * exchange_rate["GBP"], 2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [np.round(x * exchange_rate["EUR"], 2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [np.round(x * exchange_rate["INR"], 2) for x in df["MC_USD_Billion"]]
    
    log_progress("Data transformation complete. Initiating Loading process")
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)
    log_progress("Data saved to CSV file")

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)
    log_progress("Data loaded to Database as a table, Executing queries")

def run_queries(query_statement, sql_connection):
    print(f"\nQuery:\n{query_statement}\n")
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    log_progress("Process Complete")
    return query_output


df = extract(url, table_attribs)
df1 = transform(df, "./exchange_rate.csv")
load_to_csv(df1, output_path)
load_to_db(df1, conn, table_name)
run_queries("SELECT * FROM Largest_banks", conn)
run_queries("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)
run_queries("SELECT Name FROM Largest_banks LIMIT 5", conn)
conn.close()