import os
from sqlalchemy import create_engine
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

def create_or_replace_gold_user_aggregate():
    print("Dropping and recreating gold.user_aggregate table...")
    with engine.connect() as conn:
        # Execute the entire SQL block from users_aggregate.py
        conn.execute(text(GOLD_USER_AGGREGATE_SQL))
        conn.commit()
    print("gold.user_aggregate table created/updated successfully.")

def create_or_replace_gold_captain_aggregate():
    print("Dropping and recreating gold.captain_aggregate table...")
    with engine.connect() as conn:
        # Execute the entire SQL block from captain_aggregate_sql.py
        conn.execute(text(CAPTAIN_AGGREGATE_SQL))
        conn.commit()
    print("gold.captain_aggregate table created/updated successfully.")
# -----------------------
# Run
# -----------------------
if __name__ == "__main__":
    create_or_replace_gold_user_aggregate()
    create_or_replace_gold_captain_aggregate()
    import os
    from sqlalchemy import create_engine, text
    from dotenv import load_dotenv

    # Import the gold user aggregate SQL from users_aggregate.py
    from load_data.users_aggregate import GOLD_USER_AGGREGATE_SQL
    from load_data.captain_aggregate import CAPTAIN_AGGREGATE_SQL

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


    def create_or_replace_gold_user_aggregate():
        print("Dropping and recreating gold.user_aggregate table...")
        with engine.connect() as conn:
            # Execute the entire SQL block from users_aggregate.py
            conn.execute(text(GOLD_USER_AGGREGATE_SQL))
            conn.commit()
        print("gold.user_aggregate table created/updated successfully.")


    def create_or_replace_gold_captain_aggregate():
        print("Dropping and recreating gold.captain_aggregate table...")
        with engine.connect() as conn:
            # Execute the entire SQL block from captain_aggregate_sql.py
            conn.execute(text(CAPTAIN_AGGREGATE_SQL))
            conn.commit()
        print("gold.captain_aggregate table created/updated successfully.")


    # -----------------------
    # Run
    # -----------------------
    if __name__ == "__main__":
        create_or_replace_gold_user_aggregate()
        import os
        from sqlalchemy import create_engine, text
        from dotenv import load_dotenv

        # Import the gold user aggregate SQL from users_aggregate.py
        from load_data.users_aggregate import GOLD_USER_AGGREGATE_SQL
        from load_data.captain_aggregate import CAPTAIN_AGGREGATE_SQL

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


        def create_or_replace_gold_user_aggregate():
            print("Dropping and recreating gold.user_aggregate table...")
            with engine.connect() as conn:
                # Execute the entire SQL block from users_aggregate.py
                conn.execute(text(GOLD_USER_AGGREGATE_SQL))
                conn.commit()
            print("gold.user_aggregate table created/updated successfully.")


        def create_or_replace_gold_captain_aggregate():
            print("Dropping and recreating gold.captain_aggregate table...")
            with engine.connect() as conn:
                # Execute the entire SQL block from captain_aggregate_sql.py
                conn.execute(text(CAPTAIN_AGGREGATE_SQL))
                conn.commit()
            print("gold.captain_aggregate table created/updated successfully.")


        # -----------------------
        # Run
        # -----------------------
        if __name__ == "__main__":
            create_or_replace_gold_user_aggregate()
            create_or_replace_gold_captain_aggregate()
