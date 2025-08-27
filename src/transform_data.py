import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

# Add transform to sys.path for module imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../transform')))

from transform.clean_users import clean_users_data
from transform.clean_captains import clean_captains_data
from transform.clean_rides import clean_rides_data
from transform.clean_payments import clean_payments_data
from transform.clean_feedback import clean_feedback_data

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

def create_tables(conn, schema_name, table_creation_sqls):
    with conn.cursor() as cur:
        for create_sql in table_creation_sqls.values():
            cur.execute(create_sql)
    conn.commit()

def clear_table(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(sql.SQL(f"DELETE FROM {schema}.{table}"))
    conn.commit()

def load_dataframe_to_postgres(df: pd.DataFrame, schema: str, table: str, conn):
    clear_table(conn, schema, table)
    df.to_sql(table, engine, schema=schema, if_exists='append', index=False)
    print(f"Loaded {len(df)} rows into {schema}.{table}")

def main_pipeline():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

    # Drop and recreate schemas
    drop_and_create_schema(conn, 'silver')
    drop_and_create_schema(conn, 'audit')

    create_table_queries_silver = {
        'users': """
            CREATE TABLE silver.users (
                user_id VARCHAR PRIMARY KEY,
                name TEXT NOT NULL,
                gender VARCHAR(10),
                age INT CHECK (age > 0),
                signup_date DATE NOT NULL,
                city TEXT
            );
        """,
        'captains': """
            CREATE TABLE silver.captains (
                captain_id VARCHAR PRIMARY KEY,
                name TEXT NOT NULL,
                age INT CHECK (age > 0),
                experience_years INT CHECK (experience_years >= 0),
                city TEXT,
                rating DECIMAL(3,2) CHECK (rating >= 0 AND rating <= 5)
            );
        """,
        'rides': """
            CREATE TABLE silver.rides (
                ride_id VARCHAR PRIMARY KEY,
                user_id VARCHAR NOT NULL,
                captain_id VARCHAR NOT NULL,
                ride_date DATE NOT NULL,
                pickup_loc TEXT,
                drop_loc TEXT,
                distance_km DECIMAL(7,2) CHECK (distance_km >= 0),
                duration_min INT CHECK (duration_min >= 0),
                ride_status VARCHAR(20),
                FOREIGN KEY (user_id) REFERENCES silver.users(user_id),
                FOREIGN KEY (captain_id) REFERENCES silver.captains(captain_id)
            );
        """,
        'payments': """
            CREATE TABLE silver.payments (
                payment_id VARCHAR PRIMARY KEY,
                ride_id VARCHAR NOT NULL,
                payment_method VARCHAR(50),
                fare DECIMAL(10,2) CHECK (fare >= 0),
                discount_percent DECIMAL(5,2) CHECK (discount_percent >= 0 AND discount_percent <= 100),
                discount_amount DECIMAL(10,2) CHECK (discount_amount >= 0),
                final_amount DECIMAL(10,2) CHECK (final_amount >= 0),
                payment_status VARCHAR(20),
                FOREIGN KEY (ride_id) REFERENCES silver.rides(ride_id)
            );
        """,
        'feedback': """
            CREATE TABLE silver.feedback (
                feedback_id VARCHAR PRIMARY KEY,
                ride_id VARCHAR NOT NULL,
                user_rating DECIMAL(2,1) CHECK (user_rating >= 0 AND user_rating <= 5),
                captain_rating DECIMAL(2,1) CHECK (captain_rating >= 0 AND captain_rating <= 5),
                issue_category TEXT,
                comments TEXT,
                FOREIGN KEY (ride_id) REFERENCES silver.rides(ride_id)
            );
        """
    }

    create_table_queries_audit = {
        'users': """
            CREATE TABLE audit.users (
                user_id VARCHAR,
                name TEXT,
                gender VARCHAR(10),
                age INT,
                signup_date TEXT,
                city TEXT,
                reason TEXT NOT NULL,
                run_ts TIMESTAMP NOT NULL DEFAULT now()
            );
        """,
        'captains': """
            CREATE TABLE audit.captains (
                captain_id VARCHAR,
                name TEXT,
                age INT,
                experience_years INT,
                city TEXT,
                rating DECIMAL(3,2),
                reason TEXT NOT NULL,
                run_ts TIMESTAMP NOT NULL DEFAULT now()
            );
        """,
        'rides': """
            CREATE TABLE audit.rides (
                ride_id VARCHAR,
                user_id VARCHAR,
                captain_id VARCHAR,
                ride_date TEXT,
                pickup_loc TEXT,
                drop_loc TEXT,
                distance_km DECIMAL(7,2),
                duration_min INT,
                ride_status VARCHAR(20),
                reason TEXT NOT NULL,
                run_ts TIMESTAMP NOT NULL DEFAULT now()
            );
        """,
        'payments': """
            CREATE TABLE audit.payments (
                payment_id VARCHAR,
                ride_id VARCHAR,
                payment_method VARCHAR(50),
                fare DECIMAL(10,2),
                discount_percent DECIMAL(5,2),
                discount_amount DECIMAL(10,2),
                final_amount DECIMAL(10,2),
                payment_status VARCHAR(20),
                reason TEXT NOT NULL,
                run_ts TIMESTAMP NOT NULL DEFAULT now()
            );
        """,
        'feedback': """
            CREATE TABLE audit.feedback (
                feedback_id VARCHAR,
                ride_id VARCHAR,
                user_rating DECIMAL(2,1),
                captain_rating DECIMAL(2,1),
                issue_category TEXT,
                comments TEXT,
                reason TEXT NOT NULL,
                run_ts TIMESTAMP NOT NULL DEFAULT now()
            );
        """
    }

    # Create all tables
    create_tables(conn, 'silver', create_table_queries_silver)
    create_tables(conn, 'audit', create_table_queries_audit)

    df_users_clean, df_users_rejects = clean_users_data(os.path.join("../bronze_inputs", "users.csv"))
    load_dataframe_to_postgres(df_users_clean, 'silver', 'users', conn)
    load_dataframe_to_postgres(df_users_rejects, 'audit', 'users', conn)

    df_captains_clean, df_captains_rejects = clean_captains_data(os.path.join("../bronze_inputs", "captains.csv"))
    load_dataframe_to_postgres(df_captains_clean, 'silver', 'captains', conn)
    load_dataframe_to_postgres(df_captains_rejects, 'audit', 'captains', conn)

    valid_user_ids = set(df_users_clean['user_id'])
    valid_captain_ids = set(df_captains_clean['captain_id'])

    df_rides_clean, df_rides_rejects = clean_rides_data(
        os.path.join("../bronze_inputs", "rides.csv"),
        valid_user_ids,
        valid_captain_ids,
    )
    load_dataframe_to_postgres(df_rides_clean, 'silver', 'rides', conn)
    load_dataframe_to_postgres(df_rides_rejects, 'audit', 'rides', conn)

    valid_ride_ids = set(df_rides_clean['ride_id'])
    df_payments_clean, df_payments_rejects = clean_payments_data(
        os.path.join("../bronze_inputs", "payments.csv"),
        valid_ride_ids,
    )
    load_dataframe_to_postgres(df_payments_clean, 'silver', 'payments', conn)
    load_dataframe_to_postgres(df_payments_rejects, 'audit', 'payments', conn)

    df_feedback_clean, df_feedback_rejects = clean_feedback_data(
        os.path.join("../bronze_inputs", "feedback.csv"),
        valid_ride_ids
    )
    load_dataframe_to_postgres(df_feedback_clean, 'silver', 'feedback', conn)
    load_dataframe_to_postgres(df_feedback_rejects, 'audit', 'feedback', conn)

    conn.close()

if __name__ == '__main__':
    main_pipeline()
