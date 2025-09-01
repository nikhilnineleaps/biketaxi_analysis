import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Use a common log file path consistent with the ETL pipeline
LOG_FILE = os.path.join(os.path.dirname(__file__), '../logs/etl_log.txt')


def log_message(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {level} - {message}"
    print(log_line)  # Optional: still print to console for immediate feedback
    with open(LOG_FILE, "a") as f:
        f.write(log_line + "\n")


# Add transform to sys.path for module imports if needed (adjust based on your structure)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../transform')))
from clean_users import clean_users_data
from clean_captains import clean_captains_data
from clean_rides import clean_rides_data
from clean_payments import clean_payments_data
from clean_feedback import clean_feedback_data

_ = load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
connection_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_str)


def drop_and_create_schema(conn, schema_name):
    with conn.cursor() as cur:
        cur.execute(sql.SQL(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        cur.execute(sql.SQL(f"CREATE SCHEMA {schema_name}"))
    conn.commit()
    log_message(f"Schema '{schema_name}' dropped and recreated")


def create_tables(conn, schema_name, table_creation_sqls):
    with conn.cursor() as cur:
        for create_sql in table_creation_sqls.values():
            cur.execute(create_sql)
    conn.commit()
    log_message(f"Tables created in schema '{schema_name}'")


def clear_table(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(sql.SQL(f"DELETE FROM {schema}.{table}"))
    conn.commit()
    log_message(f"Cleared table {schema}.{table}")


def load_dataframe_to_postgres(df: pd.DataFrame, schema: str, table: str, conn):
    try:
        clear_table(conn, schema, table)
        df.to_sql(table, engine, schema=schema, if_exists='append', index=False)
        log_message(f"Loaded {len(df)} rows into {schema}.{table}")
    except Exception as e:
        log_message(f"Failed to load data into {schema}.{table}: {e}", level="ERROR")
        raise


def main_pipeline():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        log_message("Database connection established")

        # ---------------- Drop & recreate schemas ----------------
        drop_and_create_schema(conn, 'silver')
        drop_and_create_schema(conn, 'audit')

        create_table_queries_silver = {
            'users': """ CREATE TABLE silver.users
                         (
                             user_id     VARCHAR PRIMARY KEY,
                             name        TEXT NOT NULL,
                             gender      VARCHAR(10),
                             age         INT CHECK (age > 0),
                             signup_date DATE NOT NULL,
                             city        TEXT
                         ); """,
            'captains': """ CREATE TABLE silver.captains
                            (
                                captain_id VARCHAR PRIMARY KEY,
                                name       TEXT NOT NULL,
                                age        INT CHECK (age > 0),
                                city       TEXT,
                                rating     DECIMAL(3, 2) CHECK (rating >= 0 AND rating <= 5)
                            ); """,
            'rides': """ CREATE TABLE silver.rides
                         (
                             ride_id      VARCHAR PRIMARY KEY,
                             user_id      VARCHAR NOT NULL,
                             captain_id   VARCHAR NOT NULL,
                             ride_date    DATE    NOT NULL,
                             pickup_loc   TEXT,
                             drop_loc     TEXT,
                             distance_km  DECIMAL(7, 2) CHECK (distance_km >= 0),
                             duration_min INT CHECK (duration_min >= 0),
                             ride_status  VARCHAR(20),
                             FOREIGN KEY (user_id) REFERENCES silver.users (user_id),
                             FOREIGN KEY (captain_id) REFERENCES silver.captains (captain_id)
                         ); """,
            'payments': """ CREATE TABLE silver.payments
                            (
                                payment_id       VARCHAR PRIMARY KEY,
                                ride_id          VARCHAR NOT NULL,
                                payment_method   VARCHAR(50),
                                fare             DECIMAL(10, 2) CHECK (fare >= 0),
                                discount_percent DECIMAL(5, 2) CHECK (discount_percent >= 0 AND discount_percent <= 100),
                                discount_amount  DECIMAL(10, 2) CHECK (discount_amount >= 0),
                                final_amount     DECIMAL(10, 2) CHECK (final_amount >= 0),
                                payment_status   VARCHAR(20),
                                FOREIGN KEY (ride_id) REFERENCES silver.rides (ride_id)
                            ); """,
            'feedback': """ CREATE TABLE silver.feedback
                            (
                                feedback_id    VARCHAR PRIMARY KEY,
                                ride_id        VARCHAR NOT NULL,
                                user_rating    DECIMAL(2, 1) CHECK (user_rating >= 0 AND user_rating <= 5),
                                captain_rating DECIMAL(2, 1) CHECK (captain_rating >= 0 AND captain_rating <= 5),
                                issue_category TEXT,
                                comments       TEXT,
                                FOREIGN KEY (ride_id) REFERENCES silver.rides (ride_id)
                            ); """
        }
        create_table_queries_audit = {
            'users': """ CREATE TABLE audit.users
                         (
                             user_id     VARCHAR,
                             name        TEXT,
                             gender      VARCHAR(10),
                             age         INT,
                             signup_date TEXT,
                             city        TEXT,
                             reason      TEXT      NOT NULL,
                             run_ts      TIMESTAMP NOT NULL DEFAULT now()
                         ); """,
            'captains': """ CREATE TABLE audit.captains
                            (
                                captain_id       VARCHAR,
                                name             TEXT,
                                age              INT,
                                experience_years INT,
                                city             TEXT,
                                rating           DECIMAL(3, 2),
                                reason           TEXT      NOT NULL,
                                run_ts           TIMESTAMP NOT NULL DEFAULT now()
                            ); """,
            'rides': """ CREATE TABLE audit.rides
                         (
                             ride_id      VARCHAR,
                             user_id      VARCHAR,
                             captain_id   VARCHAR,
                             ride_date    TEXT,
                             pickup_loc   TEXT,
                             drop_loc     TEXT,
                             distance_km  DECIMAL(7, 2),
                             duration_min INT,
                             ride_status  VARCHAR(20),
                             reason       TEXT      NOT NULL,
                             run_ts       TIMESTAMP NOT NULL DEFAULT now()
                         ); """,
            'payments': """ CREATE TABLE audit.payments
                            (
                                payment_id       VARCHAR,
                                ride_id          VARCHAR,
                                payment_method   VARCHAR(50),
                                fare             DECIMAL(10, 2),
                                discount_percent DECIMAL(5, 2),
                                discount_amount  DECIMAL(10, 2),
                                final_amount     DECIMAL(10, 2),
                                payment_status   VARCHAR(20),
                                reason           TEXT      NOT NULL,
                                run_ts           TIMESTAMP NOT NULL DEFAULT now()
                            ); """,
            'feedback': """ CREATE TABLE audit.feedback
                            (
                                feedback_id    VARCHAR,
                                ride_id        VARCHAR,
                                user_rating    DECIMAL(2, 1),
                                captain_rating DECIMAL(2, 1),
                                issue_category TEXT,
                                comments       TEXT,
                                reason         TEXT      NOT NULL,
                                run_ts         TIMESTAMP NOT NULL DEFAULT now()
                            ); """
        }

        create_tables(conn, 'silver', create_table_queries_silver)
        create_tables(conn, 'audit', create_table_queries_audit)

        # ---------------- Users ----------------
        df_users_clean, df_users_rejects = clean_users_data(os.path.join("../bronze_inputs", "users.csv"))
        log_message(f"clean_users_data → {len(df_users_clean)} valid rows, {len(df_users_rejects)} rejected rows.")
        load_dataframe_to_postgres(df_users_clean, 'silver', 'users', conn)
        load_dataframe_to_postgres(df_users_rejects, 'audit', 'users', conn)

        # ---------------- Captains ----------------
        df_captains_clean, df_captains_rejects = clean_captains_data(os.path.join("../bronze_inputs", "captains.csv"))
        log_message(
            f"clean_captains_data → {len(df_captains_clean)} valid rows, {len(df_captains_rejects)} rejected rows.")
        load_dataframe_to_postgres(df_captains_clean, 'silver', 'captains', conn)
        load_dataframe_to_postgres(df_captains_rejects, 'audit', 'captains', conn)

        valid_user_ids = set(df_users_clean['user_id'])
        valid_captain_ids = set(df_captains_clean['captain_id'])

        # ---------------- Rides ----------------
        df_rides_clean, df_rides_rejects = clean_rides_data(
            os.path.join("../bronze_inputs", "rides.csv"),
            valid_user_ids,
            valid_captain_ids,
        )
        log_message(f"clean_rides_data → {len(df_rides_clean)} valid rows, {len(df_rides_rejects)} rejected rows.")
        load_dataframe_to_postgres(df_rides_clean, 'silver', 'rides', conn)
        load_dataframe_to_postgres(df_rides_rejects, 'audit', 'rides', conn)

        # Audit-safe: rides before signup
        with conn.cursor() as cur:
            cur.execute("""
                        INSERT INTO audit.rides (ride_id, user_id, captain_id, ride_date, pickup_loc, drop_loc,
                                                 distance_km, duration_min, ride_status, reason, run_ts)
                        SELECT r.ride_id,
                               r.user_id,
                               r.captain_id,
                               r.ride_date::TEXT, r.pickup_loc,
                               r.drop_loc,
                               r.distance_km,
                               r.duration_min,
                               r.ride_status,
                               'ride_before_signup',
                               now()
                        FROM silver.rides r
                                 JOIN silver.users u ON r.user_id = u.user_id
                        WHERE r.ride_date < u.signup_date;
                        """)
            conn.commit()
            cur.execute("""
                        DELETE
                        FROM silver.rides r USING silver.users u
                        WHERE r.user_id = u.user_id
                          AND r.ride_date
                            < u.signup_date;
                        """)
            conn.commit()
        log_message("Audit-safe cleanup applied: rides before user signup moved to audit")

        with conn.cursor() as cur:
            cur.execute("SELECT ride_id FROM silver.rides")
            valid_ride_ids = set(row[0] for row in cur.fetchall())

        # ---------------- Payments ----------------
        df_payments_clean, df_payments_rejects = clean_payments_data(
            os.path.join("../bronze_inputs", "payments.csv"),
            valid_ride_ids,
        )
        log_message(
            f"clean_payments_data → {len(df_payments_clean)} valid rows, {len(df_payments_rejects)} rejected rows.")
        load_dataframe_to_postgres(df_payments_clean, 'silver', 'payments', conn)
        load_dataframe_to_postgres(df_payments_rejects, 'audit', 'payments', conn)

        # Audit-safe: payments for deleted rides
        with conn.cursor() as cur:
            cur.execute("""
                        INSERT INTO audit.payments (payment_id, ride_id, payment_method, fare, discount_percent,
                                                    discount_amount,
                                                    final_amount, payment_status, reason, run_ts)
                        SELECT p.payment_id,
                               p.ride_id,
                               p.payment_method,
                               p.fare,
                               p.discount_percent,
                               p.discount_amount,
                               p.final_amount,
                               p.payment_status,
                               'ride_before_signup',
                               now()
                        FROM silver.payments p
                                 LEFT JOIN silver.rides r ON p.ride_id = r.ride_id
                        WHERE r.ride_id IS NULL;
                        """)
            conn.commit()
            cur.execute("""
                        DELETE
                        FROM silver.payments
                        WHERE ride_id NOT IN (SELECT ride_id FROM silver.rides);
                        """)
            conn.commit()
        log_message("Audit-safe cleanup applied: payments linked to deleted rides moved to audit")

        # ---------------- Feedback ----------------
        df_feedback_clean, df_feedback_rejects = clean_feedback_data(
            os.path.join("../bronze_inputs", "feedback.csv"),
            valid_ride_ids
        )
        log_message(
            f"clean_feedback_data → {len(df_feedback_clean)} valid rows, {len(df_feedback_rejects)} rejected rows.")
        load_dataframe_to_postgres(df_feedback_clean, 'silver', 'feedback', conn)
        load_dataframe_to_postgres(df_feedback_rejects, 'audit', 'feedback', conn)

        # Audit-safe: feedback for deleted rides
        with conn.cursor() as cur:
            cur.execute("""
                        INSERT INTO audit.feedback (feedback_id, ride_id, user_rating, captain_rating,
                                                    issue_category, comments, reason, run_ts)
                        SELECT f.feedback_id,
                               f.ride_id,
                               f.user_rating,
                               f.captain_rating,
                               f.issue_category,
                               f.comments,
                               'ride_before_signup',
                               now()
                        FROM silver.feedback f
                                 LEFT JOIN silver.rides r ON f.ride_id = r.ride_id
                        WHERE r.ride_id IS NULL;
                        """)
            conn.commit()
            cur.execute("""
                        DELETE
                        FROM silver.feedback
                        WHERE ride_id NOT IN (SELECT ride_id FROM silver.rides);
                        """)
            conn.commit()
        log_message("Audit-safe cleanup applied: feedback linked to deleted rides moved to audit")

        conn.close()
        log_message("Data pipeline completed successfully. All audit-safe checks applied.")

    except Exception as e:
        log_message(f"Data pipeline failed: {e}", level="ERROR")
        exc_type, exc_value, exc_traceback = sys.exc_info()
        import traceback
        log_message(''.join(traceback.format_tb(exc_traceback)), level="ERROR")
        raise


if __name__ == '__main__':
    main_pipeline()
