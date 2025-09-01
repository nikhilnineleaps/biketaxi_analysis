import os
import pandas as pd
import numbers
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
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

connection_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_str)

# -----------------------
# Create or replace gold.dashboard
# -----------------------
def create_or_replace_dashboard(engine):
    log_message("Dropping and recreating gold.dashboard table...")

    create_sql = """
    DROP TABLE IF EXISTS gold.dashboard CASCADE;
    CREATE SCHEMA IF NOT EXISTS gold;

    CREATE TABLE gold.dashboard AS
    -- (your existing dashboard SQL goes here, unchanged)
    WITH user_agg AS (
        SELECT r.user_id,
               MIN(r.ride_date) FILTER (WHERE r.ride_status='completed') AS first_ride_date,
               MAX(r.ride_date) FILTER (WHERE r.ride_status='completed') AS last_ride_date
        FROM silver.rides r
        GROUP BY r.user_id
    ),
    feedback_dedup AS (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY feedback_id ASC) AS rn
            FROM silver.feedback
        ) t
        WHERE rn = 1
    ),
    payments_dedup AS (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY payment_id ASC) AS rn
            FROM silver.payments
        ) t
        WHERE rn = 1
    ),
    discount_behavior_cte AS (
        SELECT r.user_id,
               CASE
                   WHEN SUM(CASE WHEN p.discount_amount > 0 THEN 1 ELSE 0 END) > 0
                    AND SUM(CASE WHEN p.discount_amount = 0 OR p.discount_amount IS NULL THEN 1 ELSE 0 END) = 0
                   THEN 'only_with_discount'
                   WHEN SUM(CASE WHEN p.discount_amount = 0 OR p.discount_amount IS NULL THEN 1 ELSE 0 END) > 0
                    AND SUM(CASE WHEN p.discount_amount > 0 THEN 1 ELSE 0 END) = 0
                   THEN 'only_without_discount'
                   ELSE 'mixed'
               END AS discount_behavior
        FROM silver.rides r
        LEFT JOIN payments_dedup p ON r.ride_id = p.ride_id
        GROUP BY r.user_id
    ),
    dashboard_base AS (
        SELECT u.user_id::TEXT,
               u.name::TEXT AS user_name,
               u.gender::TEXT,
               u.age::INTEGER AS user_age,
               u.signup_date::DATE,
               u.city::TEXT AS user_city,
               ua.first_ride_date::DATE,
               ua.last_ride_date::DATE,
               db.discount_behavior,
               c.captain_id::TEXT,
               c.name::TEXT AS captain_name,
               c.age::INTEGER AS captain_age,
               c.city::TEXT AS captain_city,
               c.rating::NUMERIC AS captain_rating_overall,
               r.ride_id::TEXT,
               r.ride_date::TIMESTAMP,
               r.pickup_loc::TEXT,
               r.drop_loc::TEXT,
               r.distance_km::NUMERIC,
               r.duration_min::NUMERIC,
               r.ride_status::TEXT,
               p.payment_id::TEXT,
               p.payment_method::TEXT,
               p.fare::NUMERIC,
               p.discount_percent::NUMERIC,
               p.discount_amount::NUMERIC,
               p.final_amount::NUMERIC,
               p.payment_status::TEXT,
               f.feedback_id::TEXT,
               f.user_rating::NUMERIC,
               f.captain_rating::NUMERIC AS feedback_captain_rating,
               f.issue_category::TEXT,
               f.comments::TEXT
        FROM silver.users u
        LEFT JOIN user_agg ua ON u.user_id = ua.user_id
        LEFT JOIN discount_behavior_cte db ON u.user_id = db.user_id
        LEFT JOIN silver.rides r ON u.user_id = r.user_id
        LEFT JOIN silver.captains c ON r.captain_id = c.captain_id
        LEFT JOIN payments_dedup p ON r.ride_id = p.ride_id
        LEFT JOIN feedback_dedup f ON r.ride_id = f.ride_id
    ),
    captains_no_rides AS (
        SELECT NULL::TEXT AS user_id,
               NULL::TEXT AS user_name,
               NULL::TEXT AS gender,
               NULL::INTEGER AS user_age,
               NULL::DATE AS signup_date,
               NULL::TEXT AS user_city,
               NULL::DATE AS first_ride_date,
               NULL::DATE AS last_ride_date,
               NULL::TEXT AS discount_behavior,
               c.captain_id::TEXT,
               c.name::TEXT AS captain_name,
               c.age::INTEGER AS captain_age,
               c.city::TEXT AS captain_city,
               c.rating::NUMERIC AS captain_rating_overall,
               NULL::TEXT AS ride_id,
               NULL::TIMESTAMP AS ride_date,
               NULL::TEXT AS pickup_loc,
               NULL::TEXT AS drop_loc,
               NULL::NUMERIC AS distance_km,
               NULL::NUMERIC AS duration_min,
               NULL::TEXT AS ride_status,
               NULL::TEXT AS payment_id,
               NULL::TEXT AS payment_method,
               NULL::NUMERIC AS fare,
               NULL::NUMERIC AS discount_percent,
               NULL::NUMERIC AS discount_amount,
               NULL::NUMERIC AS final_amount,
               NULL::TEXT AS payment_status,
               NULL::TEXT AS feedback_id,
               NULL::NUMERIC AS user_rating,
               NULL::NUMERIC AS feedback_captain_rating,
               NULL::TEXT AS issue_category,
               NULL::TEXT AS comments
        FROM silver.captains c
        WHERE NOT EXISTS (
            SELECT 1
            FROM silver.rides r
            WHERE r.captain_id = c.captain_id
        )
    )
    SELECT * FROM dashboard_base
    UNION ALL
    SELECT * FROM captains_no_rides;
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))
        counts = conn.execute(text("""
            SELECT COUNT(DISTINCT user_id)    AS total_users,
                   COUNT(DISTINCT captain_id) AS total_captains,
                   COUNT(*)                   AS total_rows
            FROM gold.dashboard;
        """)).fetchone()
        log_message(f"Dashboard created. Users: {counts.total_users}, Captains: {counts.total_captains}, Total rows: {counts.total_rows}")

# -----------------------
# Reconciliation Queries
# -----------------------
SILVER_DASHBOARD_QUERIES = {
    "total_users": "SELECT COUNT(DISTINCT user_id)::numeric FROM silver.users;",
    "total_captains": "SELECT COUNT(DISTINCT captain_id)::numeric FROM silver.captains;",
    "total_rides": "SELECT COUNT(DISTINCT ride_id)::numeric FROM silver.rides;",
    "total_payments": "SELECT COUNT(DISTINCT payment_id)::numeric FROM silver.payments;",
    "total_feedback": "SELECT COUNT(DISTINCT feedback_id)::numeric FROM silver.feedback;"
}

GOLD_DASHBOARD_QUERIES = {
    "total_users": "SELECT COUNT(DISTINCT user_id)::numeric FROM gold.dashboard WHERE user_id IS NOT NULL;",
    "total_captains": "SELECT COUNT(DISTINCT captain_id)::numeric FROM gold.dashboard WHERE captain_id IS NOT NULL;",
    "total_rides": "SELECT COUNT(DISTINCT ride_id)::numeric FROM gold.dashboard WHERE ride_id IS NOT NULL;",
    "total_payments": "SELECT COUNT(DISTINCT payment_id)::numeric FROM gold.dashboard WHERE payment_id IS NOT NULL;",
    "total_feedback": "SELECT COUNT(DISTINCT feedback_id)::numeric FROM gold.dashboard WHERE feedback_id IS NOT NULL;"
}

# -----------------------
# Reconciliation function
# -----------------------
def reconcile_dashboard(engine):
    log_message("Starting reconciliation between silver.* and gold.dashboard...")
    results = []
    for metric in SILVER_DASHBOARD_QUERIES.keys():
        silver_val = pd.read_sql(SILVER_DASHBOARD_QUERIES[metric], engine).iloc[0, 0]
        gold_val = pd.read_sql(GOLD_DASHBOARD_QUERIES[metric], engine).iloc[0, 0]

        diff = None
        status = "OK"
        if isinstance(silver_val, numbers.Number) and isinstance(gold_val, numbers.Number):
            diff = silver_val - gold_val
            status = "OK" if diff == 0 else "MISMATCH"
        elif silver_val != gold_val:
            status = "MISMATCH"

        results.append({
            "Metric": metric,
            "Silver": silver_val,
            "Gold": gold_val,
            "Difference": diff,
            "Status": status
        })

    return pd.DataFrame(results)

# -----------------------
# Run standalone
# -----------------------
if __name__ == "__main__":
    create_or_replace_dashboard(engine)
    report = reconcile_dashboard(engine)
    log_message("\nReconciliation Report:")
    log_message(report.to_string(index=False))
