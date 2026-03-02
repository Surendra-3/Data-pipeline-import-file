import logging
from typing import Tuple, List, Dict
import pandas as pd
from pydantic import ValidationError
from datetime import datetime, timezone


from .schema import Order

logging.basicConfig(level=logging.INFO)


def validate_dataframe(
    df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
        valid_df, rejected_df
    """
    logging.info("Validating dataframe against schema")
    valid_rows: List[Dict] = []
    rejected_rows: List[Dict] = []
    df['orderDate'] = df['orderDate'].apply(lambda x: datetime.strptime(str(x), "%Y%m%d").date() if pd.notna(x) else None)
    
    # pd.to_datetime(df['orderDate'],
    #                                  errors='coerce',
    #                                  utc=True)  # produces tz-aware UTC timestamps
    for row in df.to_dict(orient="records"):
        try:
            obj = Order(**row)
            print(f"obj: {obj}")
            valid_rows.append(obj.model_dump())
        except ValidationError as ex:
            rejected_rows.append(
                {
                    "raw_record": str(row),
                    "error": str(ex)
                }
            )
    print(f"Valid rows: {len(valid_rows)}, Rejected rows: {len(rejected_rows)}")
    return pd.DataFrame(valid_rows), pd.DataFrame(rejected_rows)