import logging
import uuid

from pathlib import Path
from .source_reader import read_csv_source
from .validator import validate_dataframe
from .anomalies import split_anomalies
from .db import get_engine
from .loader import load_dataframe, delete_by_run_id


logging.basicConfig(level=logging.INFO)


def run_pipeline(csv_path: str, run_id: str | None = None):
    logging.info(f"Path of the input file: {csv_path}")
    if run_id is None:
        run_id = str(uuid.uuid4())
    logging.info(f"Starting pipeline run: {run_id}")

    logging.info("Reading source")
    raw_df = read_csv_source(csv_path)

    print(raw_df.head())

    logging.info("Schema validation")
    valid_df, rejected_df = validate_dataframe(raw_df)

    print(valid_df.head())
    print(rejected_df.head())

    logging.info("Anomaly detection")
    clean_df, anomalies_df = split_anomalies(valid_df)

    clean_df["run_id"] = run_id
    anomalies_df["run_id"] = run_id
    rejected_df["run_id"] = run_id

    engine = get_engine()

    delete_by_run_id(engine, "orders_clean", run_id)
    delete_by_run_id(engine, "orders_anomalies", run_id)
    delete_by_run_id(engine, "orders_rejected", run_id)

    logging.info("Loading clean records")
    load_dataframe(clean_df, "orders_clean", engine)

    logging.info("Loading anomalies")
    load_dataframe(anomalies_df, "orders_anomalies", engine)

    logging.info("Loading rejected records")
    load_dataframe(rejected_df, "orders_rejected", engine)

    logging.info("Pipeline completed")

    return {
        "run_id": run_id,
        "total": len(raw_df),
        "valid": len(clean_df),
        "anomalies": len(anomalies_df),
        "rejected": len(rejected_df)
    }


if __name__ == "__main__":
    #run_pipeline("ingestion\\sample_orders.csv")
    run_pipeline("C:\\Learning\\Py_ETL\\data-pipeline-mini\\ingestion\\sample_orders.csv")
    # sample_path = Path(__file__).resolve().parent / "sample_orders.csv"
    # run_pipeline(str(sample_path))