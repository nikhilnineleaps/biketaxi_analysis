import pandas as pd
from datetime import datetime
import re

# ---------------- SAFE CONCAT FUNCTION ----------------
def safe_concat(df1, df2):
    """Concatenate two DataFrames safely."""
    if df1.empty:
        return df2.reset_index(drop=True)
    if df2.empty:
        return df1.reset_index(drop=True)
    for col in df1.columns:
        if col not in df2.columns:
            df2[col] = pd.NA
    for col in df2.columns:
        if col not in df1.columns:
            df1[col] = pd.NA
    df2 = df2[df1.columns]
    return pd.concat([df1, df2], ignore_index=True)


# ---------------- DATE PARSING ----------------
def parse_date_str(date_str):
    """
    Parses signup_date in these formats:
    - M/D/YYYY or MM/DD/YYYY → US-style
    - YYYY-MM-DD → ISO
    """
    if pd.isna(date_str):
        return pd.NaT

    s = str(date_str).strip()

    # ISO format
    if re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", s):
        return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")

    # US-style M/D/YYYY
    if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", s):
        return pd.to_datetime(s, format="%m/%d/%Y", errors="coerce")

    return pd.NaT


# ---------------- CLEAN USERS ----------------
def clean_users_data(bronze_file_path):
    df = pd.read_csv(bronze_file_path)
    df_rejects = pd.DataFrame(columns=list(df.columns) + ['reason', 'run_ts'])

    # 1️⃣ Reject null or empty user_id
    empty_userid_mask = df['user_id'].isna() | (df['user_id'].astype(str).str.strip() == "")
    if empty_userid_mask.any():
        rejected = df[empty_userid_mask].copy()
        rejected['reason'] = 'null_or_empty_user_id'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df[~empty_userid_mask].copy()

    # 2️⃣ Reject user_ids not matching "user00001" format
    valid_userid_mask = df_clean['user_id'].str.match(r'^user\d{5}$')
    if (~valid_userid_mask).any():
        rejected = df_clean[~valid_userid_mask].copy()
        rejected['reason'] = 'invalid_user_id_format'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[valid_userid_mask].copy()

    # 3️⃣ Parse signup_date
    df_clean['signup_date'] = df_clean['signup_date'].apply(parse_date_str)

    # 4️⃣ Reject invalid dates
    invalid_dates_mask = df_clean['signup_date'].isna()
    if invalid_dates_mask.any():
        rejected = df_clean[invalid_dates_mask].copy()
        rejected['reason'] = 'invalid_signup_date'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~invalid_dates_mask].copy()

    # 5️⃣ Clean age (replace missing with median, force int)
    median_age = df_clean['age'].median()
    df_clean['age'] = df_clean['age'].fillna(median_age).astype(int)

    # 6️⃣ Remove duplicates in user_id
    df_clean = df_clean.drop_duplicates(subset=['user_id'], keep='first')

    # 7️⃣ Format signup_date for export
    df_clean['signup_date'] = df_clean['signup_date'].dt.strftime('%Y-%m-%d')


    return df_clean.reset_index(drop=True), df_rejects.reset_index(drop=True)
