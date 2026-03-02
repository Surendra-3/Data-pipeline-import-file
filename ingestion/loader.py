import logging
import pandas as pd
import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy import types

logging.basicConfig(level=logging.INFO)

def delete_by_run_id(engine, table_name: str, run_id: str):
    sql = f"DELETE FROM {table_name} WHERE run_id = :run_id"
    with engine.begin() as conn:
        conn.execute(text(sql), {"run_id": run_id})
        
def load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine
):
    if df.empty:
        logging.info(f"DataFrame is empty. Skipping loading for table {table_name}")
        return

    print(f"Loading {len(df)} records into {table_name}")
    print(df.head())
    print(df.dtypes)
    print(df.describe(include='all'))

    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        method=None,
    )
    print(f"Finished loading {len(df)} records into {table_name}")
