import pandas as pd
import requests
import logging

logging.basicConfig(level=logging.INFO)

def read_csv_source(path: str) -> pd.DataFrame:
    logging.info(f"Reading CSV source from: {path}")
    return pd.read_csv(path)


def read_api_source(url: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return pd.DataFrame(data)