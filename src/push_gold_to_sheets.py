import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime

# ------------------------
# Centralized logging setup
# ------------------------
LOG_FILE = os.path.join(os.path.dirname(__file__), '../logs/etl_log.txt')

def log_message(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {level} - {message}"
    print(log_line)  # Optional: keep console output
    with open(LOG_FILE, "a") as f:
        f.write(log_line + "\n")

# -----------------------
# Load environment variables
# -----------------------
_ = load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

TARGET_SHEET_ID = os.getenv("TARGET_SHEET_ID")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")  # Path to JSON creds file

# -----------------------
# Setup database engine
# -----------------------
connection_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_str)

# -----------------------
# Setup Google Sheets API client
# -----------------------
def gsheet_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    return client

# -----------------------
# Read gold table
# -----------------------
def read_gold_table(table_name):
    query = f"SELECT * FROM gold.{table_name};"
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

# -----------------------
# Push DataFrame to Google Sheet
# -----------------------
def push_df_to_gsheet(df, worksheet_name):
    client = gsheet_client()
    sh = client.open_by_key(TARGET_SHEET_ID)
    try:
        worksheet = sh.worksheet(worksheet_name)
        worksheet.clear()
        log_message(f"Cleared existing worksheet '{worksheet_name}'")
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=worksheet_name, rows="1000", cols="50")
        log_message(f"Created new worksheet '{worksheet_name}'")

    set_with_dataframe(worksheet, df)
    log_message(f"Pushed data to worksheet '{worksheet_name}' successfully.")

# -----------------------
# Push gold tables to Google Sheets
# -----------------------
def push_gold_tables_to_sheets():
    users_df = read_gold_table("user_aggregate")
    captains_df = read_gold_table("captain_aggregate")
    dashboard_df = read_gold_table("dashboard")

    push_df_to_gsheet(users_df, "users_data")
    push_df_to_gsheet(captains_df, "captains_data")
    push_df_to_gsheet(dashboard_df, "dashboard_data")

    log_message("All gold tables pushed to Google Sheets successfully.")

# -----------------------
# Run standalone
# -----------------------
if __name__ == "__main__":
    push_gold_tables_to_sheets()
