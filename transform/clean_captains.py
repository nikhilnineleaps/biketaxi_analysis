import os
import pandas as pd
from datetime import datetime
import re

def clean_captains_data(bronze_file_path):
    # ---------------- SAFE CONCAT ----------------
    def safe_concat(df1, df2):
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

    # ---------------- CHECK FILE ----------------
    if not os.path.exists(bronze_file_path):
        raise FileNotFoundError(f"Bronze file not found: {bronze_file_path}")

    df = pd.read_csv(bronze_file_path)
    df_rejects = pd.DataFrame(columns=list(df.columns) + ['reason', 'run_ts'])

    # 1️⃣ Null or empty captain_id
    empty_cid_mask = df['captain_id'].isna() | (df['captain_id'].astype(str).str.strip() == '')
    if empty_cid_mask.any():
        empty_cid = df[empty_cid_mask].copy()
        empty_cid['reason'] = 'null_or_empty_captain_id'
        empty_cid['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, empty_cid)
    df_clean = df[~empty_cid_mask].copy()

    # 2️⃣ Invalid captain_id format (e.g., must be CP00001)
    valid_cid_mask = df_clean['captain_id'].str.match(r'^CP\d{5}$')
    if (~valid_cid_mask).any():
        invalid_cid = df_clean[~valid_cid_mask].copy()
        invalid_cid['reason'] = 'invalid_captain_id_format'
        invalid_cid['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, invalid_cid)
    df_clean = df_clean[valid_cid_mask].copy()

    # 3️⃣ Drop duplicate captain_id (keep first)
    duplicate_mask = df_clean['captain_id'].duplicated(keep='first')
    if duplicate_mask.any():
        duplicates = df_clean[duplicate_mask].copy()
        duplicates['reason'] = 'duplicate_captain_id'
        duplicates['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, duplicates)
    df_clean = df_clean[~duplicate_mask].copy()

    # 4️⃣ Null or empty name
    null_name_mask = df_clean['name'].isna() | (df_clean['name'].str.strip() == '')
    if null_name_mask.any():
        null_names = df_clean[null_name_mask].copy()
        null_names['reason'] = 'null_or_empty_name'
        null_names['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, null_names)
    df_clean = df_clean[~null_name_mask].copy()

    # 5️⃣ Age and rating numeric conversion
    df_clean['age'] = pd.to_numeric(df_clean['age'], errors='coerce')
    df_clean['rating'] = pd.to_numeric(df_clean['rating'], errors='coerce')

    # 6️⃣ Fill missing city with 'Unknown' and trim
    df_clean['city'] = df_clean['city'].fillna('Unknown').astype(str).str.strip()
    df_clean['city'] = df_clean['city'].replace('', 'Unknown')

    # 7️⃣ Fill null age/rating with median
    median_age = df_clean['age'].median()
    median_rating = df_clean['rating'].median()
    df_clean['age'] = df_clean['age'].fillna(median_age).astype(int)
    df_clean['rating'] = df_clean['rating'].fillna(median_rating).round(1)

    # 8️⃣ Keep only required columns
    df_clean = df_clean[['captain_id', 'name', 'age', 'city', 'rating']]

    return df_clean.reset_index(drop=True), df_rejects.reset_index(drop=True)
