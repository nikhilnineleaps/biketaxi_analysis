import os
import numbers
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
# Gold user aggregate SQL
# -----------------------
GOLD_USER_AGGREGATE_SQL = """
DROP TABLE IF EXISTS gold.user_aggregate;
CREATE TABLE gold.user_aggregate AS
WITH ride_payment AS (
    SELECT r.ride_id,
           r.user_id,
           SUM(COALESCE(p.final_amount, 0)) AS total_payment
    FROM silver.rides r
    LEFT JOIN silver.payments p ON r.ride_id = p.ride_id
    GROUP BY r.ride_id, r.user_id
),
ride_feedback AS (
    SELECT r.ride_id,
           r.user_id,
           AVG(NULLIF(f.captain_rating, 0)) AS avg_captain_rating,
           MODE() WITHIN GROUP (ORDER BY f.issue_category::text) AS most_frequent_issue
    FROM silver.rides r
    LEFT JOIN silver.feedback f ON r.ride_id = f.ride_id
    GROUP BY r.ride_id, r.user_id
),
first_ride AS (
    SELECT u.user_id,
           MIN(r.ride_date) AS first_ride_date
    FROM silver.users u
    LEFT JOIN silver.rides r ON u.user_id = r.user_id
    GROUP BY u.user_id
)
SELECT
    u.user_id,
    u.name,
    u.age,
    u.gender,
    u.city,
    u.signup_date,
    min(r.ride_date) as first_ride_date,
    max(r.ride_date) as last_ride_date,
    COUNT(r.ride_id) AS total_rides,
    COALESCE(SUM(rp.total_payment), 0) AS total_revenue,
    CASE WHEN COUNT(r.ride_id) > 0 THEN SUM(rp.total_payment) / COUNT(r.ride_id) ELSE NULL END AS avg_revenue_per_ride,
    COUNT(r.ride_id) FILTER (WHERE r.ride_date >= CURRENT_DATE - INTERVAL '30 days') AS booking_frequency,
    CASE WHEN COUNT(*) FILTER (WHERE r.ride_status IN ('completed', 'cancelled')) > 0 THEN 1 ELSE 0 END AS is_active,
    AVG(rf.avg_captain_rating) AS avg_captain_rating,
    MODE() WITHIN GROUP (ORDER BY rf.most_frequent_issue::text) AS most_frequent_issue
FROM silver.users u
LEFT JOIN silver.rides r ON u.user_id = r.user_id
LEFT JOIN ride_payment rp ON r.ride_id = rp.ride_id
LEFT JOIN ride_feedback rf ON r.ride_id = rf.ride_id
LEFT JOIN first_ride fr ON u.user_id = fr.user_id
GROUP BY u.user_id, u.name, u.age, u.gender, u.city, fr.first_ride_date;
"""

# -----------------------
# Metric Queries
# -----------------------
SILVER_QUERIES = {
    "total_rides": """
                   SELECT COUNT(*)::numeric AS total_rides
                   FROM silver.rides;
                   """,
    "total_revenue": """
                     SELECT SUM(COALESCE(p.final_amount, 0))::numeric AS total_revenue
                     FROM silver.rides r
                     LEFT JOIN silver.payments p ON r.ride_id = p.ride_id;
                     """,
    "avg_revenue_per_ride": """
                            SELECT AVG(NULLIF(p.final_amount, 0))::numeric AS avg_revenue_per_ride
                            FROM silver.payments p
                            JOIN silver.rides r ON r.ride_id = p.ride_id;
                            """,
    "booking_frequency": """
                         SELECT COUNT(*)::numeric AS booking_frequency
                         FROM silver.rides r
                         WHERE r.ride_date >= CURRENT_DATE - INTERVAL '30 days';
                         """,
    "is_active": """
                 SELECT COUNT(DISTINCT r.user_id)::numeric AS is_active
                 FROM silver.rides r
                 WHERE r.ride_status IN ('completed', 'cancelled');
                 """,
    "avg_captain_rating": """
                          SELECT AVG(NULLIF(f.captain_rating, 0))::numeric AS avg_captain_rating
                          FROM silver.feedback f
                          JOIN silver.rides r ON r.ride_id = f.ride_id
                          WHERE f.captain_rating IS NOT NULL
                            AND f.captain_rating <> 0;
                          """,
    "most_frequent_issue": """
                           SELECT MODE() WITHIN GROUP (ORDER BY f.issue_category::text)::text AS most_frequent_issue
                           FROM silver.feedback f
                           JOIN silver.rides r ON r.ride_id = f.ride_id;
                           """
}

GOLD_QUERIES = {
    "total_rides": """
                   SELECT SUM(total_rides)::numeric AS total_rides
                   FROM gold.user_aggregate;
                   """,
    "total_revenue": """
                     SELECT SUM(total_revenue)::numeric AS total_revenue
                     FROM gold.user_aggregate;
                     """,
    "avg_revenue_per_ride": """
                            SELECT SUM(avg_revenue_per_ride * total_rides) / NULLIF(SUM(total_rides),0) AS avg_revenue_per_ride
                            FROM gold.user_aggregate;
                            """,
    "booking_frequency": """
                         SELECT SUM(booking_frequency)::numeric AS booking_frequency
                         FROM gold.user_aggregate;
                         """,
    "is_active": """
                 SELECT SUM(is_active)::numeric AS is_active
                 FROM gold.user_aggregate;
                 """,
    "avg_captain_rating": """
                          SELECT AVG(avg_captain_rating)::numeric AS avg_captain_rating
                          FROM gold.user_aggregate;
                          """,
    "most_frequent_issue": """
                           SELECT MODE() WITHIN GROUP (ORDER BY most_frequent_issue::text)::text AS most_frequent_issue
                           FROM gold.user_aggregate;
                           """,}

# -----------------------
# Create or replace gold.user_aggregate table
# -----------------------
def create_or_replace_gold_user_aggregate():
    print("Dropping and recreating gold.user_aggregate table...")
    with engine.connect() as conn:
        conn.execute(text(GOLD_USER_AGGREGATE_SQL))
        conn.commit()
    print("gold.user_aggregate table created/updated successfully.")

# -----------------------
# Reconciliation function
# -----------------------
def reconcile_silver_gold():
    print("Starting reconciliation...")
    results = []
    for metric in SILVER_QUERIES.keys():
        silver_val = pd.read_sql(SILVER_QUERIES[metric], engine).iloc[0, 0]
        gold_val = pd.read_sql(GOLD_QUERIES[metric], engine).iloc[0, 0]
        if isinstance(silver_val, numbers.Number) and isinstance(gold_val, numbers.Number):
            diff = silver_val - gold_val
            status = "OK" if abs(diff) < 0.01 else "MISMATCH"
        else:
            diff = None
            status = "OK" if silver_val == gold_val else "MISMATCH"
        results.append({
            "Metric": metric,
            "Silver": silver_val,
            "Gold": gold_val,
            "Difference": diff,
            "Status": status
        })
    report = pd.DataFrame(results)
    return report

# -----------------------
# Run standalone
# -----------------------
if __name__ == "__main__":
    create_or_replace_gold_user_aggregate()
    report = reconcile_silver_gold()
    print(report)
