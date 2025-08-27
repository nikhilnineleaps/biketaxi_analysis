import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# Load environment variables
_ = load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

TARGET_SHEET_ID = os.getenv("TARGET_SHEET_ID")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")  # Path to JSON creds file

# Setup the database engine
connection_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_str)

# Setup Google Sheets API client
def gsheet_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    return client

def read_gold_table(table_name):
    query = f"SELECT * FROM gold.{table_name};"
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

def push_df_to_gsheet(df, worksheet_name):
    client = gsheet_client()
    sh = client.open_by_key(TARGET_SHEET_ID)
    try:
        worksheet = sh.worksheet(worksheet_name)
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=worksheet_name, rows="1000", cols="50")

    set_with_dataframe(worksheet, df)
    print(f"Pushed data to worksheet '{worksheet_name}' successfully.")

def push_gold_aggregates_to_sheets():
    # Read gold tables
    users_df = read_gold_table("user_aggregate")
    captains_df = read_gold_table("captain_aggregate")

    # Push to respective sheets
    push_df_to_gsheet(users_df, "users_data")
    push_df_to_gsheet(captains_df, "captains_data")

if __name__ == "__main__":
    push_gold_aggregates_to_sheets()
