import pandas as pd
from datetime import datetime

# ---------------- SAFE CONCAT FUNCTION ----------------
def safe_concat(df1, df2):
    """Concatenate two DataFrames safely, avoiding FutureWarning from empty/all-NA DataFrames."""
    if df1.empty:
        return df2.reset_index(drop=True)
    if df2.empty:
        return df1.reset_index(drop=True)
    # Ensure all columns exist in both DataFrames
    for col in df1.columns:
        if col not in df2.columns:
            df2[col] = pd.NA
    for col in df2.columns:
        if col not in df1.columns:
            df1[col] = pd.NA
    # Reorder columns to match df1
    df2 = df2[df1.columns]
    return pd.concat([df1, df2], ignore_index=True)



# ---------------- DATE PARSING ----------------
def parse_date_str(date_str):
    formats = ["%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y", "%m/%d/%Y"]
    for fmt in formats:
        try:
            return pd.to_datetime(date_str, format=fmt)
        except (ValueError, TypeError):
            continue
    return pd.NaT


# ---------------- CLEAN USERS ----------------
def clean_users_data(bronze_file_path):
    df = pd.read_csv(bronze_file_path)

    # Prepare rejects DataFrame
    rejects_cols = list(df.columns) + ['reason', 'run_ts']
    df_rejects = pd.DataFrame(columns=rejects_cols)

    # 1️⃣ Reject rows with null user_id
    null_userid_mask = df['user_id'].isna()
    if null_userid_mask.any():
        null_userid = df[null_userid_mask].copy()
        null_userid['reason'] = 'null_user_id'
        null_userid['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, null_userid)

    df_clean = df[~null_userid_mask].copy()

    # 2️⃣ Parse signup_date
    df_clean['signup_date'] = df_clean['signup_date'].apply(parse_date_str)

    # 3️⃣ Reject invalid dates
    invalid_dates_mask = df_clean['signup_date'].isna()
    if invalid_dates_mask.any():
        invalid_dates = df_clean[invalid_dates_mask].copy()
        invalid_dates['reason'] = 'invalid_signup_date'
        invalid_dates['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, invalid_dates)

    df_clean = df_clean[~invalid_dates_mask].copy()

    # 4️⃣ Format date to YYYY-MM-DD
    df_clean['signup_date'] = df_clean['signup_date'].dt.strftime('%Y-%m-%d')

    # 5️⃣ Remove duplicates in user_id (keep first)
    df_clean = df_clean.drop_duplicates(subset=['user_id'], keep='first')

    return df_clean.reset_index(drop=True), df_rejects.reset_index(drop=True)

