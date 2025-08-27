import pandas as pd
from datetime import datetime

# ---------------- SAFE CONCAT ----------------
def safe_concat(df1, df2):
    if df1.empty:
        return df2.reset_index(drop=True)
    if df2.empty:
        return df1.reset_index(drop=True)
    # Ensure columns exist in both dataframes
    for col in df1.columns:
        if col not in df2.columns:
            df2[col] = pd.NA
    for col in df2.columns:
        if col not in df1.columns:
            df1[col] = pd.NA
    # Reorder columns of df2 to match df1
    df2 = df2[df1.columns]
    return pd.concat([df1, df2], ignore_index=True)


# ---------------- SAFE DATE PARSE ----------------
def parse_ride_date(date_str):
    if pd.isna(date_str):
        return pd.NaT
    formats = ["%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y", "%m-%d-%Y"]
    s = str(date_str).strip()
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except:
            continue
    return pd.NaT

# ---------------- CLEAN RIDES ----------------
def clean_rides_data(bronze_file_path, valid_user_ids, valid_captain_ids):
    df = pd.read_csv(bronze_file_path)

    # Prepare rejects DataFrame
    rejects_cols = list(df.columns) + ['reason', 'run_ts']
    df_rejects = pd.DataFrame(columns=rejects_cols)

    # 1️⃣ Reject null/empty ride_id
    mask = df['ride_id'].isna() | (df['ride_id'].astype(str).str.strip() == '')
    if mask.any():
        rejected = df[mask].copy()
        rejected['reason'] = 'null_or_empty_ride_id'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df[~mask].copy()

    # 2️⃣ Reject null/empty user_id
    mask = df_clean['user_id'].isna() | (df_clean['user_id'].astype(str).str.strip() == '')
    if mask.any():
        rejected = df_clean[mask].copy()
        rejected['reason'] = 'null_or_empty_user_id'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~mask].copy()

    # 3️⃣ Reject null/empty captain_id
    mask = df_clean['captain_id'].isna() | (df_clean['captain_id'].astype(str).str.strip() == '')
    if mask.any():
        rejected = df_clean[mask].copy()
        rejected['reason'] = 'null_or_empty_captain_id'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~mask].copy()

    # 4️⃣ Parse ride_date and reject invalid dates
    df_clean['ride_date'] = df_clean['ride_date'].apply(parse_ride_date)
    mask = df_clean['ride_date'].isna()
    if mask.any():
        rejected = df_clean[mask].copy()
        rejected['reason'] = 'null_or_invalid_ride_date'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~mask].copy()

    # 5️⃣ Reject invalid user_id and captain_id
    invalid_user_mask = ~df_clean['user_id'].isin(valid_user_ids)
    if invalid_user_mask.any():
        rejected = df_clean[invalid_user_mask].copy()
        rejected['reason'] = 'invalid_user_id_not_in_users'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~invalid_user_mask].copy()

    invalid_captain_mask = ~df_clean['captain_id'].isin(valid_captain_ids)
    if invalid_captain_mask.any():
        rejected = df_clean[invalid_captain_mask].copy()
        rejected['reason'] = 'invalid_captain_id_not_in_captains'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~invalid_captain_mask].copy()

    # 6️⃣ Deduplicate ride_id
    duplicate_mask = df_clean.duplicated(subset=['ride_id'], keep='first')
    if duplicate_mask.any():
        rejected = df_clean[duplicate_mask].copy()
        rejected['reason'] = 'duplicate_ride_id'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~duplicate_mask].copy()

    # 7️⃣ Numeric columns median imputation
    for col in ['distance_km', 'duration_min']:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        df_clean[col] = df_clean[col].fillna(df_clean[col].median())

    # 8️⃣ Fill empty pickup/drop locations and ride_status
    df_clean['pickup_loc'] = df_clean['pickup_loc'].replace('', pd.NA).fillna('Unknown')
    df_clean['drop_loc'] = df_clean['drop_loc'].replace('', pd.NA).fillna('Unknown')
    mode_val = df_clean['ride_status'].mode()[0] if not df_clean['ride_status'].mode().empty else 'Unknown'
    df_clean['ride_status'] = df_clean['ride_status'].fillna(mode_val)

    # 9️⃣ Format ride_date as string for DB
    df_clean['ride_date'] = df_clean['ride_date'].dt.strftime('%Y-%m-%d')

    return df_clean.reset_index(drop=True), df_rejects.reset_index(drop=True)
