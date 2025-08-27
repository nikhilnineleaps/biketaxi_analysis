import sys
import traceback
from datetime import datetime
import importlib
import os
import pandas as pd

# Use logs/etl_log.txt for logging
LOG_FILE = os.path.join(os.path.dirname(__file__), '../logs/etl_log.txt')

def log_message(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {level} - {message}"
    print(log_line)
    with open(LOG_FILE, "a") as f:
        f.write(log_line + "\n")

def run_etl():
    try:
        log_message("🚀 ETL Pipeline Started")

        # --- Extraction + Bronze Load ---
        log_message("🔄 Running Extraction + Bronze Dataset Load...")
        extraction = importlib.import_module("src.extraction")
        try:
            extraction.export_sheets_to_csv()
            for sheet_name, csv_file in extraction.SHEETS.items():
                log_message(f"✅ Sheet '{sheet_name}' exported to CSV: {os.path.join(extraction.CSV_DIR, csv_file)}")
        except Exception as e:
            log_message(f"❌ Failed to export sheets: {e}", level="ERROR")
            log_message(traceback.format_exc(), level="ERROR")
            sys.exit(1)

        schema_name = "bronze"
        with extraction.engine.connect() as conn:
            conn.execute(extraction.text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
            for table_name, create_query in extraction.create_table_queries.items():
                try:
                    conn.execute(extraction.text(create_query.format(schema=schema_name)))
                    log_message(f"✅ Table '{table_name}' created or exists in schema '{schema_name}'")
                except Exception as e:
                    log_message(f"❌ Failed to create table '{table_name}': {e}", level="ERROR")
            conn.commit()

        for table_name, csv_file in extraction.SHEETS.items():
            try:
                extraction.load_csv_to_db_raw(schema_name, table_name, csv_file)
                log_message(f"✅ CSV '{csv_file}' loaded into table '{table_name}'")
            except Exception as e:
                log_message(f"❌ Failed to load CSV '{csv_file}' into table '{table_name}': {e}", level="ERROR")

        log_message("✅ Extraction + Bronze Load Completed Successfully")

        # --- transform / Silver Load ---
        log_message("🔄 Running transform + Silver/Audit Load...")
        transform_data = importlib.import_module("src.transform_data")
        try:
            transform_data.main_pipeline()
            log_message("✅ transform + Silver/Audit Load Completed Successfully")
        except Exception as e:
            log_message(f"❌ transform pipeline failed: {e}", level="ERROR")
            log_message(traceback.format_exc(), level="ERROR")
            sys.exit(1)

        # --- Gold Aggregates + Reconciliation ---
        log_message("🔄 Running Gold Aggregates for Users and Captains...")

        user_aggregate = importlib.import_module("load_data.users_aggregate")
        captain_aggregate = importlib.import_module("load_data.captain_aggregate")
        push_to_sheets = importlib.import_module("push_gold_to_sheets")  # New import for sheets push

        try:
            user_aggregate.create_or_replace_gold_user_aggregate()
            captain_aggregate.create_or_replace_captain_aggregate()

            user_report = user_aggregate.reconcile_silver_gold()
            captain_report = captain_aggregate.reconcile_captain_aggregates()

            user_report["Entity"] = "User"
            captain_report["Entity"] = "Captain"
            merged_report = pd.concat([user_report, captain_report], ignore_index=True)
            merged_report_file = "../test/reconciliation_report.csv"
            merged_report.to_csv(merged_report_file, index=False)
            log_message(f"✅ Merged reconciliation report saved as {merged_report_file}")

            # Push gold aggregates to Google Sheets
            try:
                push_to_sheets.push_gold_aggregates_to_sheets()
                log_message("✅ Gold aggregates pushed to Google Sheets successfully.")
            except Exception as e:
                log_message(f"❌ Failed to push gold aggregates to Google Sheets: {e}", level="ERROR")
                log_message(traceback.format_exc(), level="ERROR")

        except Exception as e:
            log_message(f"❌ Failed to generate merged reconciliation report: {e}", level="ERROR")
            log_message(traceback.format_exc(), level="ERROR")

        log_message("✅ ETL Pipeline Finished Successfully")

    except Exception as e:
        log_message(f"❌ ETL Pipeline Failed: {str(e)}", level="ERROR")
        log_message(traceback.format_exc(), level="ERROR")
        sys.exit(1)

if __name__ == "__main__":
    run_etl()
