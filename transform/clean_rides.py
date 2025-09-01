import pandas as pd
from datetime import datetime
import re

# ---------------- SAFE CONCAT ----------------
def safe_concat(df1, df2):
    """Concatenate two DataFrames safely, aligning columns and avoiding empty concat issues."""
    if df1.empty and df2.empty:
        return pd.DataFrame(columns=df1.columns if not df1.empty else df2.columns)
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


# ---------------- SAFE DATE PARSE ----------------
def parse_date(date_str):
    """Parse dates with multiple formats: YYYY-MM-DD, DD/MM/YYYY, DD.MM.YYYY, M/d/yyyy, M-d-yyyy"""
    if pd.isna(date_str):
        return pd.NaT

    s = str(date_str).strip()
    try:
        # ISO
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
        # DD.MM.YYYY
        if re.match(r"^\d{2}\.\d{2}\.\d{4}$", s):
            return pd.to_datetime(s, format="%d.%m.%Y", errors="coerce")
        # Slash: DD/MM/YYYY or M/d/yyyy
        if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", s):
            parts = s.split("/")
            if int(parts[0]) > 12:
                return pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")
            else:
                return pd.to_datetime(s, format="%m/%d/%Y", errors="coerce")
        # Dash: M-d-yyyy
        if re.match(r"^\d{1,2}-\d{1,2}-\d{4}$", s):
            return pd.to_datetime(s, format="%m-%d-%Y", errors="coerce")
    except Exception:
        return pd.NaT

    return pd.NaT


# ---------------- CLEAN USERS ----------------
def clean_users_data(bronze_file_path):
    df = pd.read_csv(bronze_file_path)
    df_rejects = pd.DataFrame(columns=list(df.columns)+['reason','run_ts'])

    # Null/empty user_id
    mask = df['user_id'].isna() | (df['user_id'].astype(str).str.strip() == "")
    if mask.any():
        rejected = df[mask].copy()
        rejected['reason'] = 'null_or_empty_user_id'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df[~mask].copy()

    # Invalid user_id format
    mask = ~df_clean['user_id'].str.match(r'^user\d{5}$')
    if mask.any():
        rejected = df_clean[mask].copy()
        rejected['reason'] = 'invalid_user_id_format'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~mask].copy()

    # Parse signup_date
    df_clean['signup_date'] = df_clean['signup_date'].apply(parse_date)

    # Reject invalid dates
    mask = df_clean['signup_date'].isna()
    if mask.any():
        rejected = df_clean[mask].copy()
        rejected['reason'] = 'invalid_signup_date'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~mask].copy()

    # Clean age
    median_age = df_clean['age'].median()
    df_clean['age'] = df_clean['age'].fillna(median_age).astype(int)

    # Deduplicate user_id
    df_clean = df_clean.drop_duplicates(subset=['user_id'], keep='first')

    # Format date
    df_clean['signup_date'] = df_clean['signup_date'].dt.strftime('%Y-%m-%d')

    return df_clean.reset_index(drop=True), df_rejects.reset_index(drop=True)


# ---------------- CLEAN RIDES ----------------
def clean_rides_data(bronze_file_path, valid_user_ids, valid_captain_ids):
    df = pd.read_csv(bronze_file_path)
    df_rejects = pd.DataFrame(columns=list(df.columns)+['reason','run_ts'])

    # Null/empty ride_id
    mask = df['ride_id'].isna() | (df['ride_id'].astype(str).str.strip() == '')
    if mask.any():
        rejected = df[mask].copy()
        rejected['reason'] = 'null_or_empty_ride_id'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df[~mask].copy()

    # Null/empty user_id
    mask = df_clean['user_id'].isna() | (df_clean['user_id'].astype(str).str.strip() == '')
    if mask.any():
        rejected = df_clean[mask].copy()
        rejected['reason'] = 'null_or_empty_user_id'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~mask].copy()

    # Null/empty captain_id
    mask = df_clean['captain_id'].isna() | (df_clean['captain_id'].astype(str).str.strip() == '')
    if mask.any():
        rejected = df_clean[mask].copy()
        rejected['reason'] = 'null_or_empty_captain_id'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~mask].copy()

    # Parse ride_date
    df_clean['ride_date'] = df_clean['ride_date'].apply(parse_date)

    # Reject invalid ride_date
    mask = df_clean['ride_date'].isna()
    if mask.any():
        rejected = df_clean[mask].copy()
        rejected['reason'] = 'null_or_invalid_ride_date'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~mask].copy()

    # Reject invalid user_id / captain_id
    mask = ~df_clean['user_id'].isin(valid_user_ids)
    if mask.any():
        rejected = df_clean[mask].copy()
        rejected['reason'] = 'invalid_user_id_not_in_users'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~mask].copy()

    mask = ~df_clean['captain_id'].isin(valid_captain_ids)
    if mask.any():
        rejected = df_clean[mask].copy()
        rejected['reason'] = 'invalid_captain_id_not_in_captains'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~mask].copy()

    # Deduplicate ride_id
    mask = df_clean.duplicated(subset=['ride_id'], keep='first')
    if mask.any():
        rejected = df_clean[mask].copy()
        rejected['reason'] = 'duplicate_ride_id'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~mask].copy()

    # Numeric columns median imputation
    for col in ['distance_km', 'duration_min']:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        df_clean[col] = df_clean[col].fillna(df_clean[col].median())

    # Fill empty pickup/drop locations & ride_status
    df_clean['pickup_loc'] = df_clean['pickup_loc'].replace('', pd.NA).fillna('Unknown')
    df_clean['drop_loc'] = df_clean['drop_loc'].replace('', pd.NA).fillna('Unknown')
    df_clean['ride_status'] = df_clean['ride_status'].replace('', pd.NA)
    df_clean['ride_status'] = df_clean['ride_status'].fillna(df_clean['ride_status'].mode()[0] if not df_clean['ride_status'].mode().empty else 'Unknown')

    # Format ride_date
    df_clean['ride_date'] = df_clean['ride_date'].dt.strftime('%Y-%m-%d')

    return df_clean.reset_index(drop=True), df_rejects.reset_index(drop=True)
