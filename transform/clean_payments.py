import pandas as pd
from datetime import datetime

def safe_concat(df1, df2):
    """Concatenate two DataFrames safely, avoiding FutureWarning from empty/all-NA DataFrames."""
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


def clean_payments_data(bronze_file_path, valid_ride_ids):
    df = pd.read_csv(bronze_file_path)

    rejects_cols = list(df.columns) + ['reason', 'run_ts']
    df_rejects = pd.DataFrame(columns=rejects_cols)

    # Reject null or empty ride_id
    null_ride_id_mask = df['ride_id'].isna() | (df['ride_id'].astype(str).str.strip() == '')
    if null_ride_id_mask.any():
        rejected = df[null_ride_id_mask].copy()
        rejected['reason'] = 'null_or_empty_ride_id'
        rejected['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, rejected)
    df_clean = df[~null_ride_id_mask].copy()

    # Reject payments with ride_id not in cleaned rides
    invalid_ride_mask = ~df_clean['ride_id'].isin(valid_ride_ids)
    if invalid_ride_mask.any():
        invalid_payments = df_clean[invalid_ride_mask].copy()
        invalid_payments['reason'] = 'invalid_ride_id_not_in_rides'
        invalid_payments['run_ts'] = datetime.now()
        df_rejects = safe_concat(df_rejects, invalid_payments)
    df_clean = df_clean[~invalid_ride_mask].copy()

    # Convert fare to numeric and fill NA with median
    df_clean['fare'] = pd.to_numeric(df_clean['fare'], errors='coerce')
    median_fare = df_clean['fare'].median()
    df_clean['fare'] = df_clean['fare'].fillna(median_fare)

    # Fill null discount_percent, discount_amount, final_amount with 0
    for col in ['discount_percent', 'discount_amount', 'final_amount']:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)

    return df_clean.reset_index(drop=True), df_rejects.reset_index(drop=True)
