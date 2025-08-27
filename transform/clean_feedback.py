import pandas as pd
from datetime import datetime

def clean_feedback_data(bronze_file_path, valid_ride_ids):
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

    df = pd.read_csv(bronze_file_path)

    rejects_cols = list(df.columns) + ['reason', 'run_ts']
    df_rejects = pd.DataFrame(columns=rejects_cols)

    # Reject null or empty feedback_id
    null_feedback_id_mask = df['feedback_id'].isna() | (df['feedback_id'].astype(str).str.strip() == '')
    if null_feedback_id_mask.any():
        rejected = df[null_feedback_id_mask].copy()
        rejected['reason'] = 'null_or_empty_feedback_id'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df[~null_feedback_id_mask].copy()

    # Reject null or empty ride_id
    null_ride_id_mask = df_clean['ride_id'].isna() | (df_clean['ride_id'].astype(str).str.strip() == '')
    if null_ride_id_mask.any():
        rejected = df_clean[null_ride_id_mask].copy()
        rejected['reason'] = 'null_or_empty_ride_id'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~null_ride_id_mask].copy()

    # Reject if ride_id not in valid rides
    invalid_ride_mask = ~df_clean['ride_id'].isin(valid_ride_ids)
    if invalid_ride_mask.any():
        rejected = df_clean[invalid_ride_mask].copy()
        rejected['reason'] = 'ride_id_not_in_rides'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df_clean[~invalid_ride_mask].copy()

    # Fill user_rating, captain_rating missing/invalid with median
    for col in ['user_rating', 'captain_rating']:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        median_val = df_clean[col].median()
        df_clean[col] = df_clean[col].fillna(median_val)

    # Fill issue_category and comments missing/empty with defaults
    df_clean['issue_category'] = df_clean['issue_category'].replace('', pd.NA).fillna('No issues')
    df_clean['comments'] = df_clean['comments'].replace('', pd.NA).fillna('No comments')

    return df_clean.reset_index(drop=True), df_rejects.reset_index(drop=True)
