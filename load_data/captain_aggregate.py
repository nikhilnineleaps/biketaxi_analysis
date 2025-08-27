import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

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
# SQL to create or replace gold.captain_aggregate table
# -----------------------
CAPTAIN_AGGREGATE_SQL = """
DROP TABLE IF EXISTS gold.captain_aggregate;
CREATE TABLE gold.captain_aggregate AS
WITH captain_feedback AS (
    SELECT r.captain_id,
           AVG(NULLIF(f.captain_rating, 0)) AS avg_captain_rating,
           AVG(NULLIF(f.user_rating, 0)) AS avg_user_rating,
           MODE() WITHIN GROUP (ORDER BY f.issue_category) AS most_frequent_issue,
           MODE() WITHIN GROUP (ORDER BY f.comments) AS most_frequent_comment
    FROM silver.rides r
    LEFT JOIN silver.feedback f ON r.ride_id = f.ride_id
    GROUP BY r.captain_id
),
captain_payment AS (
    SELECT r.captain_id,
           SUM(COALESCE(p.final_amount, 0)) AS total_final_amount
    FROM silver.rides r
    LEFT JOIN silver.payments p ON r.ride_id = p.ride_id
    GROUP BY r.captain_id
)
SELECT
    c.captain_id,
    c.name,
    c.age,
    c.city,
    c.rating AS average_rating,
    COUNT(r.ride_id) AS total_rides,
    COUNT(*) FILTER (WHERE r.ride_status = 'completed') AS completed_rides,
    COUNT(*) FILTER (WHERE r.ride_status = 'cancelled') AS cancelled_rides,
    COALESCE(SUM(r.distance_km), 0) AS total_distance_km,
    COALESCE(SUM(r.duration_min), 0) AS total_duration_min,
    COALESCE(cp.total_final_amount, 0) AS total_final_amount,
    cf.avg_captain_rating,
    cf.avg_user_rating,
    CASE WHEN COUNT(*) FILTER (WHERE r.ride_status IN ('completed', 'cancelled')) > 0
         THEN 'active' ELSE 'inactive' END AS status,
    cf.most_frequent_issue,
    cf.most_frequent_comment
FROM silver.captains c
LEFT JOIN silver.rides r ON c.captain_id = r.captain_id
LEFT JOIN captain_payment cp ON c.captain_id = cp.captain_id
LEFT JOIN captain_feedback cf ON c.captain_id = cf.captain_id
GROUP BY c.captain_id, c.name, c.age, c.city, c.rating,
         cp.total_final_amount, cf.avg_captain_rating, cf.avg_user_rating,
         cf.most_frequent_issue, cf.most_frequent_comment;
"""

# -----------------------
# Function to create or replace the gold captain aggregate table
# -----------------------
def create_or_replace_captain_aggregate():
    print("Creating or replacing gold.captain_aggregate table...")
    with engine.begin() as conn:
        conn.execute(text(CAPTAIN_AGGREGATE_SQL))
    print("✅ gold.captain_aggregate table created/updated successfully.")

# -----------------------
# Reconciliation function (returns DataFrame, no CSV saved)
# -----------------------
def reconcile_captain_aggregates():
    print("Starting captain reconciliation...")

    silver_query = """
    WITH
    ride_counts AS (
        SELECT c.captain_id,
               COUNT(r.ride_id) AS total_rides
        FROM silver.captains c
        LEFT JOIN silver.rides r ON c.captain_id = r.captain_id
        GROUP BY c.captain_id
    ),
    payment_sums AS (
        SELECT r.captain_id,
               SUM(COALESCE(p.final_amount, 0)) AS total_final_amount
        FROM silver.rides r
        LEFT JOIN silver.payments p ON r.ride_id = p.ride_id
        GROUP BY r.captain_id
    ),
    ride_status_counts AS (
        SELECT c.captain_id,
               COUNT(*) FILTER (WHERE r.ride_status = 'completed') AS completed_rides,
               COUNT(*) FILTER (WHERE r.ride_status = 'cancelled') AS cancelled_rides
        FROM silver.captains c
        LEFT JOIN silver.rides r ON c.captain_id = r.captain_id
        GROUP BY c.captain_id
    ),
    distance_duration AS (
        SELECT r.captain_id,
               SUM(COALESCE(r.distance_km, 0)) AS total_distance_km,
               SUM(COALESCE(r.duration_min, 0)) AS total_duration_min
        FROM silver.rides r
        GROUP BY r.captain_id
    ),
    user_ratings AS (
        SELECT r.captain_id,
               AVG(f.user_rating) AS avg_user_rating
        FROM silver.rides r
        LEFT JOIN silver.feedback f ON r.ride_id = f.ride_id
        GROUP BY r.captain_id
    )
    SELECT
        COUNT(DISTINCT c.captain_id) AS total_captains,
        COALESCE(SUM(rc.total_rides), 0) AS total_rides_sum,
        COALESCE(SUM(rsc.completed_rides), 0) AS completed_rides_sum,
        COALESCE(SUM(rsc.cancelled_rides), 0) AS cancelled_rides_sum,
        COALESCE(SUM(dd.total_distance_km), 0) AS total_distance_km_sum,
        COALESCE(SUM(dd.total_duration_min), 0) AS total_duration_min_sum,
        COALESCE(SUM(ps.total_final_amount), 0) AS total_final_amount_sum,
        COALESCE(AVG(ur.avg_user_rating), 0) AS avg_user_rating_avg
    FROM silver.captains c
    LEFT JOIN ride_counts rc ON c.captain_id = rc.captain_id
    LEFT JOIN ride_status_counts rsc ON c.captain_id = rsc.captain_id
    LEFT JOIN distance_duration dd ON c.captain_id = dd.captain_id
    LEFT JOIN payment_sums ps ON c.captain_id = ps.captain_id
    LEFT JOIN user_ratings ur ON c.captain_id = ur.captain_id;
    """
    gold_query = """
    SELECT
        COUNT(DISTINCT captain_id) AS total_captains,
        COALESCE(SUM(total_rides), 0) AS total_rides_sum,
        COALESCE(SUM(completed_rides), 0) AS completed_rides_sum,
        COALESCE(SUM(cancelled_rides), 0) AS cancelled_rides_sum,
        COALESCE(SUM(total_distance_km), 0) AS total_distance_km_sum,
        COALESCE(SUM(total_duration_min), 0) AS total_duration_min_sum,
        COALESCE(SUM(total_final_amount), 0) AS total_final_amount_sum,
        COALESCE(AVG(avg_user_rating), 0) AS avg_user_rating_avg
    FROM gold.captain_aggregate;
    """

    silver_totals = pd.read_sql(silver_query, engine)
    gold_totals = pd.read_sql(gold_query, engine)

    numeric_cols = [
        'total_captains',
        'total_rides_sum',
        'completed_rides_sum',
        'cancelled_rides_sum',
        'total_distance_km_sum',
        'total_duration_min_sum',
        'total_final_amount_sum',
        'avg_user_rating_avg'
    ]

    diffs = silver_totals.iloc[0][numeric_cols] - gold_totals.iloc[0][numeric_cols]
    statuses = ["OK" if abs(d) < 0.01 else "MISMATCH" for d in diffs]

    report = pd.DataFrame({
        "Metric": numeric_cols,
        "Silver": silver_totals.iloc[0][numeric_cols].values,
        "Gold": gold_totals.iloc[0][numeric_cols].values,
        "Difference": diffs.values,
        "Status": statuses
    })

    print("✅ Captain reconciliation completed (returning DataFrame).")
    return report

# -----------------------
# Run full process standalone
# -----------------------
if __name__ == "__main__":
    create_or_replace_captain_aggregate()
    df = reconcile_captain_aggregates()
    print(df)
