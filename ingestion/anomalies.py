import logging
from typing import Tuple
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def split_anomalies(
    df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
        clean_df, anomalies_df
    """
    if df is None or df.empty:
        logging.info("Input dataframe valid_df is empty. Returning empty clean and anomalies dataframes.")
        return pd.DataFrame(), pd.DataFrame()

    logging.info("Detecting anomalies in the dataframe")
    df = df.copy()
    anomalies_mask = pd.Series(False, index=df.index)

    print(df.columns)
    print(df.head())
    print(df["orderAmount"])
    print(df["orderAmount"].describe())
    # negative / zero amount
    # not required as orderAmount is defined as PositiveFloat in the schema
    # anomalies_mask |= df["orderAmount"] <= 0

    # future date
    anomalies_mask |= df["orderDate"] > datetime.now(tz=df["orderDate"].dt.tz)

    # z-score outliers
    if len(df) >= 5:
        z = np.abs(stats.zscore(df["orderAmount"]))
        anomalies_mask |= z > 3

    # country code anomalies (e.g., non-ISO codes)
    valid_countries = {"US", "CA", "GB", "FR", "DE"}  # example set
    anomalies_mask |= ~df["country"].isin(valid_countries)

    anomalies_df = df[anomalies_mask]
    clean_df = df[~anomalies_mask]

    logging.info(f"Anomalies detected: {len(anomalies_df)}, Clean records: {len(clean_df)}")

    return clean_df, anomalies_df