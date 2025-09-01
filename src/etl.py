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
        log_message("ETL pipeline started")
        # --- Extraction + Bronze Load ---
        log_message("Starting extraction and bronze dataset load")
        extraction = importlib.import_module("src.extraction")
        try:
            extraction.export_sheets_to_csv()
            for sheet_name, csv_file in extraction.SHEETS.items():
                log_message(
                    f"Sheet '{sheet_name}' successfully exported to CSV: {os.path.join(extraction.CSV_DIR, csv_file)}")
        except Exception as e:
            log_message(f"Failed to export sheets: {e}", level="ERROR")
            log_message(traceback.format_exc(), level="ERROR")
            sys.exit(1)

        schema_name = "bronze"
        with extraction.engine.connect() as conn:
            conn.execute(extraction.text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
            for table_name, create_query in extraction.create_table_queries.items():
                try:
                    conn.execute(extraction.text(create_query.format(schema=schema_name)))
                    log_message(f"Table '{table_name}' created or already exists in schema '{schema_name}'")
                except Exception as e:
                    log_message(f"Failed to create table '{table_name}': {e}", level="ERROR")
            conn.commit()

        for table_name, csv_file in extraction.SHEETS.items():
            try:
                extraction.load_csv_to_db_raw(schema_name, table_name, csv_file)
                log_message(f"CSV '{csv_file}' successfully loaded into table '{table_name}'")
            except Exception as e:
                log_message(f"Failed to load CSV '{csv_file}' into table '{table_name}': {e}", level="ERROR")

        log_message("Extraction and bronze load completed successfully")

        # --- Transform / Silver Load ---
        log_message("Starting transform and silver/audit load")
        transform_data = importlib.import_module("src.transform_data")
        try:
            transform_data.main_pipeline()
            log_message("Transform and silver/audit load completed successfully")
        except Exception as e:
            log_message(f"Transform pipeline failed: {e}", level="ERROR")
            log_message(traceback.format_exc(), level="ERROR")
            sys.exit(1)

        # --- Gold Aggregates + Dashboard + Reconciliation ---
        log_message("Starting gold aggregates for users, captains, dashboard, and sheets push")
        user_aggregate = importlib.import_module("load_data.users_aggregate")
        captain_aggregate = importlib.import_module("load_data.captain_aggregate")
        dashboard_module = importlib.import_module("load_data.dashboard")
        push_to_sheets = importlib.import_module("src.push_gold_to_sheets")

        try:
            user_aggregate.create_or_replace_gold_user_aggregate()
            captain_aggregate.create_or_replace_captain_aggregate()
            log_message("Gold user and captain aggregates created successfully")

            dashboard_module.create_or_replace_dashboard(extraction.engine)
            log_message("Gold dashboard table created or updated successfully")

            user_report = user_aggregate.reconcile_silver_gold()
            captain_report = captain_aggregate.reconcile_captain_aggregates()
            dashboard_report = dashboard_module.reconcile_dashboard(extraction.engine)

            user_report["Entity"] = "User"
            captain_report["Entity"] = "Captain"
            dashboard_report["Entity"] = "Dashboard"

            merged_report = pd.concat([user_report, captain_report, dashboard_report], ignore_index=True)
            merged_report_file = "../test/reconciliation_report.csv"
            merged_report.to_csv(merged_report_file, index=False)

            log_message(f"Merged reconciliation report saved as {merged_report_file}")

            try:
                total_rows = len(merged_report)
                ok_rows = (merged_report["Status"] == "OK").sum()
                if total_rows == 18 and ok_rows == total_rows:
                    push_to_sheets.push_gold_tables_to_sheets()
                    log_message(
                        "All reconciliation checks passed. Gold aggregates pushed to Google Sheets successfully")
                else:
                    log_message("Reconciliation validation failed. Data not pushed to Google Sheets.", level="ERROR")
                    log_message(f"Expected rows: 18 | Found rows: {total_rows} | OK rows: {ok_rows}", level="ERROR")
                    failed_report_file = "../test/reconciliation_failed.csv"
                    failed_rows = merged_report[merged_report["Status"] != "OK"]
                    failed_rows.to_csv(failed_report_file, index=False)
                    log_message(f"Failed reconciliation rows saved at {failed_report_file}")
                    sys.exit(1)
            except Exception as e:
                log_message(f"Failed during reconciliation validation: {e}", level="ERROR")
                log_message(traceback.format_exc(), level="ERROR")
                sys.exit(1)
        except Exception as e:
            log_message(f"Error during gold aggregates, dashboard, or sheets push: {e}", level="ERROR")
            log_message(traceback.format_exc(), level="ERROR")
            sys.exit(1)

        log_message("ETL pipeline finished successfully")
    except Exception as e:
        log_message(f"ETL pipeline failed: {str(e)}", level="ERROR")
        log_message(traceback.format_exc(), level="ERROR")
        sys.exit(1)


if __name__ == "__main__":
    run_etl()
