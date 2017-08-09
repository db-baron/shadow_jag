import sqlite3, csv
import pandas as pd

con = sqlite3.connect('/Users/Megatron/Documents/EagleCap/conditions_alerts_timedelta.db')
con.text_factory = str

cond_df = pd.read_csv('conditions_table.csv')
td_df = pd.read_csv('time_delta_table.csv')
alert_df = pd.read_csv('alerts_table.csv')
dec_df = pd.read_csv('decisions_table.csv')

cond_df.to_sql('conditions', con, if_exists='replace', index=False)
td_df.to_sql('time_deltas', con, if_exists='replace', index=False)
alert_df.to_sql('alerts', con, if_exists='replace', index=False)
dec_df.to_sql('decisions', con, if_exists='replace', index=False)
 
with con:
    cur = con.cursor()

    # Create tables if they don't already exist
    cur.execute('''CREATE TABLE IF NOT EXISTS conditions (
        condition_id TEXT PRIMARY KEY,
        condition_description TEXT NOT NULL,
        category_id TEXT NOT NULL,
        alert_level TEXT NOT NULL,
        remarks TEXT)''');

    cur.execute('''CREATE TABLE IF NOT EXISTS combined_conditions (
        combined_condition_id TEXT PRIMARY KEY,
        condition TEXT NOT NULL)''');

    cur.execute('''CREATE TABLE IF NOT EXISTS alerts (
        alert_id TEXT PRIMARY KEY,
        category_id TEXT NOT NULL,
        alert_level TEXT NOT NULL)''');

    cur.execute('''CREATE TABLE IF NOT EXISTS alert_combination (
        alert_id TEXT PRIMARY KEY,
        alert_header TEXT NOT NULL,
        behavior TEXT,
        remarks TEXT)''');

    cur.execute('''CREATE TABLE IF NOT EXISTS time_deltas (
        time_delta TEXT PRIMARY KEY,
        category TEXT NOT NULL)''');

    cur.execute('''CREATE TABLE IF NOT EXISTS decisions (
        combined_condition_id TEXT PRIMARY KEY,
        combined_condition_description TEXT NOT NULL,
        combined_category_id TEXT NOT NULL,
        combined_alert_level TEXT NOT NULL,
        combined_remarks TEXT)''');

    con.commit()

    # Load the CSV files into CSV readers
    conditions_csv = open('conditions_table.csv', "rt", encoding="ascii")
    conditions_reader = csv.reader(conditions_csv, delimiter=',', quotechar='|')

    alerts_csv = open('alerts_table.csv', "rt", encoding="ascii")
    alertsreader = csv.reader(alerts_csv, delimiter=',', quotechar='|')

    timedeltas_csv = open('time_delta_table.csv', "rt", encoding="ascii")
    timedetlasreader = csv.reader(timedeltas_csv, delimiter=',', quotechar='|')

    conditions_csv.close()
    alerts_csv.close()
    timedeltas_csv.close()
    con.commit()
    # con.close() don't need to do .close() in a with con: statement
