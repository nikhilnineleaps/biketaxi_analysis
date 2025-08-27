# extraction.py
import os
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ---------------- LOAD ENV ----------------
_ = load_dotenv()

# Google Sheets creds
SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

# Postgres creds
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Local CSV staging dir
CSV_DIR = "../bronze_inputs"
os.makedirs(CSV_DIR, exist_ok=True)

# SQLAlchemy engine
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ---------------- TABLES ----------------
SHEETS = {
    "users": "users.csv",
    "captains": "captains.csv",
    "rides": "rides.csv",
    "payments": "payments.csv",
    "feedback": "feedback.csv",
}

create_table_queries = {
    "users": """
        CREATE TABLE IF NOT EXISTS {schema}.users (
            user_id TEXT,
            name TEXT,
            gender TEXT,
            age TEXT,
            signup_date TEXT,
            city TEXT
        );""",
    "captains": """
        CREATE TABLE IF NOT EXISTS {schema}.captains (
            captain_id TEXT,
            name TEXT,
            age TEXT,
            experience_years TEXT,
            city TEXT,
            rating TEXT
        );""",
    "rides": """
        CREATE TABLE IF NOT EXISTS {schema}.rides (
            ride_id TEXT,
            user_id TEXT,
            captain_id TEXT,
            ride_date TEXT,
            pickup_loc TEXT,
            drop_loc TEXT,
            distance_km TEXT,
            duration_min TEXT,
            ride_status TEXT
        );""",
    "payments": """
        CREATE TABLE IF NOT EXISTS {schema}.payments (
            payment_id TEXT,
            ride_id TEXT,
            payment_method TEXT,
            fare TEXT,
            discount_percent TEXT,
            discount_amount TEXT,
            final_amount TEXT,
            payment_status TEXT
        );""",
    "feedback": """
        CREATE TABLE IF NOT EXISTS {schema}.feedback (
            feedback_id TEXT,
            ride_id TEXT,
            user_rating TEXT,
            captain_rating TEXT,
            issue_category TEXT,
            comments TEXT
        );""",
}

# ---------------- EXTRACTION ----------------
def export_sheets_to_csv():
    if not SPREADSHEET_ID or not SERVICE_ACCOUNT_FILE:
        raise ValueError("Missing SPREADSHEET_ID or SERVICE_ACCOUNT_FILE in .env")

    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    service = build("sheets", "v4", credentials=creds)

    for sheet_name, csv_file in SHEETS.items():
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=sheet_name
        ).execute()
        values = result.get("values", [])

        if not values:
            # No data found, skip this sheet
            continue

        df = pd.DataFrame(values[1:], columns=values[0])
        output_path = os.path.join(CSV_DIR, csv_file)
        df.to_csv(output_path, index=False)


# -----------`----- LOAD TO BRONZE ----------------
def load_csv_to_db_raw(schema_name, table_name, file_name):
    path = os.path.join(CSV_DIR, file_name)
    if not os.path.exists(path):
        # Skip missing files silently
        return

    df = pd.read_csv(path).astype(str)
    df.to_sql(table_name, engine, schema=schema_name, if_exists="append", index=False)


def load_all(schema_name="bronze"):
    with engine.connect() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
        for t, query in create_table_queries.items():
            conn.execute(text(query.format(schema=schema_name)))
        conn.commit()

    for table, file in SHEETS.items():
        load_csv_to_db_raw(schema_name, table, file)
