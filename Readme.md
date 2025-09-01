project-root/
│── bronze inputs                     #Extracted data is stored here
│   ├── capatains.csv  
│   ├── feedback.csv
│   ├── payments.csv
│   ├── rides.csv
│   └── users.csv
│── config/                          # Configurations & credentials
│   ├── credentials.json             # Service account credentials (not committed)
│   └── credentials_example.json     # Example credentials template
│
│── load_data/                       # Gold layer aggregation scripts
│   ├── captain_aggregate.py         # Aggregates captain-related metrics
│   ├── dashboard.py                 # Prepares dashboard-ready data
│   └── users_aggregate.py           # Aggregates user-related metrics
│
│── logs/
│   └── etl_log.txt                  # Centralized ETL log file
│
│── src/                             # Core ETL pipeline scripts
│   ├── etl.py                       # Main ETL orchestrator
│   ├── extraction.py                # Extracts raw data (bronze layer)
│   ├── push_gold_to_sheets.py       # Pushes gold layer results to Google Sheets
│   └── transform_data.py            # Coordinates transformations
│
│── test/
│   └── reconciliation_report.csv    # Reconciliation report between user & captain data
│
│── transform/                       # Transformation & cleaning scripts
│   ├── __init__.py
│   ├── clean_captains.py            # Cleans captain data
│   ├── clean_feedback.py            # Cleans feedback data
│   ├── clean_payments.py            # Cleans payments data
│   ├── clean_rides.py               # Cleans ride data
│   └── clean_users.py               # Cleans user data
│
│── .env                             # Environment variables (local, not committed)
│── .env.example                     # Example env config for setup


**etl/extraction.py — Data Extraction & Bronze Layer Loader**

This module handles data ingestion from Google Sheets and loads it into the Bronze layer (raw tables) of the PostgreSQL database.

Features

Google Sheets → CSV Export
Reads multiple sheet tabs (users, captains, rides, payments, feedback) from a Google Spreadsheet and saves them locally as CSVs in the bronze_inputs/ directory.

CSV → PostgreSQL Bronze Schema
Creates Bronze schema tables (users, captains, rides, payments, feedback) and loads the extracted CSV data into them.

Functions

export_sheets_to_csv()
Connects to Google Sheets via service account credentials and exports each sheet into local CSVs.

load_csv_to_db_raw(schema_name, table_name, file_name)
Loads a single CSV file into the corresponding Bronze schema table.

load_all(schema_name="bronze")
Creates the Bronze schema (if not exists), ensures all tables exist, and loads all CSVs into the database.

transform/ — Data Cleaning & Transformation

This directory contains Python modules that validate and clean raw (Bronze) data before loading it into the Silver layer. Each script focuses on a specific entity (users, captains, rides, payments, feedback).

Common Features

Rejects invalid records with detailed reason codes.

Creates a df_clean (valid dataset) and df_rejects (invalid dataset with reasons + timestamp).

Handles missing values with median imputation or default placeholders.

Enforces data consistency (e.g., ID formats, valid references).

clean_captains_data(bronze_file_path)

Validates captain_id format (CP00001).

Rejects empty, invalid, or duplicate captain IDs.

Rejects missing names.

Converts age and rating to numeric, imputes missing with median.

Fills missing city with "Unknown".

Returns (df_clean, df_rejects).

clean_users_data(bronze_file_path)

Validates user_id format (user00001).

Rejects null, empty, or invalid user IDs.

Parses and validates signup_date (multiple formats supported: YYYY-MM-DD, DD/MM/YYYY, MM/DD/YYYY).

Rejects invalid dates.

Cleans age with median imputation.

Removes duplicate user_id.

Standardizes signup_date to YYYY-MM-DD.

clean_rides_data(bronze_file_path, valid_user_ids, valid_captain_ids)

Rejects null or empty ride_id, user_id, or captain_id.

Validates ride_date with flexible parsing.

Rejects invalid user or captain references (must exist in cleaned users/captains).

Deduplicates ride_id.

Numeric imputation for distance_km and duration_min.

Fills missing pickup/drop locations with "Unknown".

Ensures ride_status has a valid mode (or defaults to "Unknown").

clean_payments_data(bronze_file_path, valid_ride_ids)

Rejects null/empty ride_id.

Ensures ride_id exists in cleaned rides.

Converts fare to numeric, imputes missing with median.

Fills missing discount fields (discount_percent, discount_amount, final_amount) with 0.

clean_feedback_data(bronze_file_path, valid_ride_ids)

Rejects null/empty feedback_id or ride_id.

Ensures ride_id exists in cleaned rides.

Deduplicates feedback (1 per ride).

Converts user_rating and captain_rating to numeric, imputes missing with median.

Fills missing issue_category and comments with defaults ("No issues", "No comments").

transform_dat.py — Silver Layer Orchestration

This script orchestrates the Bronze → Silver ETL pipeline with integrated audit-safe checks. It applies transformations from transform/ modules, loads cleaned data into the Silver schema, and logs rejected/invalid records into the Audit schema.

Features

Drops and recreates silver and audit schemas each run.

Creates normalized, constraint-enforced tables for users, captains, rides, payments, and feedback.

Uses transform.clean_* modules for entity-level cleaning.

Applies audit-safe business rules, e.g.:

Rides before user signup → moved to audit.rides.

Payments for deleted rides → moved to audit.payments.

Feedback for deleted rides → moved to audit.feedback.

Ensures Silver contains only valid, referentially consistent data.

Schema Overview

Silver Schema (clean, validated data):

users

captains

rides (FK: user_id, captain_id)

payments (FK: ride_id)

feedback (FK: ride_id)

Audit Schema (invalid/rejected records):

Structure mirrors silver tables.

Includes reason and run_ts fields for debugging.

load_data/ — Gold Layer & Dashboard

This directory contains scripts for transforming Silver layer data into Gold aggregates and a dashboard-ready table.

captains_aggregate.py

Builds gold.captain_aggregate table with per-captain KPIs:

Rides, completed/cancelled counts

Distances, durations, earnings

Average ratings from users & captains

Most frequent issue/comment categories

Active/inactive status

Includes reconciliation logic: compares metrics between silver and gold for data integrity.

dashboard.py

Creates a denormalized dashboard table (gold.dashboard) for analytics and visualization.

Combines users, captains, rides, payments, and feedback into a single unified schema.

Deduplicates feedback & payments per ride.

Adds discount behavior classification:

only_with_discount

only_without_discount

mixed

Ensures captains with no rides are still represented.
push_gold_to_sheets.py — Push Gold Data to Google Sheets

This script syncs Gold layer tables from PostgreSQL into Google Sheets for business stakeholders or analysts.

Features

Connects to Postgres Gold schema (user_aggregate, captain_aggregate, dashboard).

Exports each table into a separate Google Sheets worksheet:

users_data → gold.user_aggregate

captains_data → gold.captain_aggregate

dashboard_data → gold.dashboard

Automatically creates sheets if missing or clears existing ones before pushing new data

etl.py — End-to-End Orchestration

This script is the master ETL pipeline runner. It controls the flow of data from Google Sheets → PostgreSQL Bronze → Silver/Audit → Gold → Google Sheets dashboard.

Features

Centralized logging to logs/etl_log.txt.

Extraction: Downloads raw data from Google Sheets, stores as CSV, loads into Bronze schema.

Transformation: Cleans data, applies validation, loads into Silver & Audit schemas.

Gold Layer: Builds aggregates (user_aggregate, captain_aggregate, dashboard).

Reconciliation: Generates consistency reports (../test/reconciliation_report.csv).

Google Sheets export: Pushes Gold data back to business dashboards.

Log Levels

INFO → Normal progress updates.

ERROR → Failures with traceback.

Each step logs successes (✅) and failures (❌).