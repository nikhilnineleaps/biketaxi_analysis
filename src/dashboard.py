import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

connection_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_str)

def drop_and_create_gold_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    conn.commit()

def drop_and_create_dashboard_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS gold.dashboard_data CASCADE;")
        cur.execute("""
            CREATE TABLE gold.dashboard_data AS
            WITH user_rides AS (
                SELECT u.user_id,
                       u.name AS user_name,
                       u.gender,
                       u.age AS user_age,
                       u.signup_date,
                       u.city AS user_city,
                       r.ride_id,
                       r.captain_id,
                       r.ride_date,
                       r.pickup_loc,
                       r.drop_loc,
                       r.distance_km,
                       r.duration_min,
                       r.ride_status
                FROM silver.users u
                LEFT JOIN silver.rides r ON u.user_id = r.user_id
            ),
            captain_rides AS (
                SELECT c.captain_id,
                       c.name AS captain_name,
                       c.age AS captain_age,
                       c.city AS captain_city,
                       c.rating AS captain_rating,
                       r.ride_id
                FROM silver.captains c
                LEFT JOIN silver.rides r ON c.captain_id = r.captain_id
            ),
            rides_with_payments AS (
                SELECT r.*,
                       p.payment_id,
                       p.payment_method,
                       p.fare,
                       p.discount_percent,
                       p.discount_amount,
                       p.final_amount,
                       p.payment_status
                FROM user_rides r
                LEFT JOIN silver.payments p ON r.ride_id = p.ride_id
            ),
            rides_with_feedback AS (
                SELECT rwp.*,
                       f.feedback_id,
                       f.user_rating,
                       f.captain_rating AS feedback_captain_rating,
                       f.issue_category,
                       f.comments
                FROM rides_with_payments rwp
                LEFT JOIN silver.feedback f ON rwp.ride_id = f.ride_id
            )
            SELECT rwf.*,
                   cr.captain_name,
                   cr.captain_age,
                   cr.captain_city,
                   cr.captain_rating AS captain_overall_rating
            FROM rides_with_feedback rwf
            FULL OUTER JOIN captain_rides cr
                ON rwf.captain_id = cr.captain_id
            ORDER BY rwf.user_id NULLS LAST, cr.captain_id NULLS LAST;
        """)
    conn.commit()

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )
    drop_and_create_gold_schema(conn)
    drop_and_create_dashboard_table(conn)
    conn.close()
    print("Gold dashboard_data table created and populated.")

if __name__ == "__main__":
    main()
