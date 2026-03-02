from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from ingestion.db import get_engine
from ingestion.schema import Order

app = FastAPI()
engine = get_engine()


@app.get("/orders/{orderId}", response_model=Order)
def get_order(orderId: int):

    with engine.connect() as conn:
        row = conn.execute(
            text("""
                 SELECT 
                    orderId,
                    customerId,
                    orderAmount,
                    orderDate,
                    country 
                    FROM orders_clean WHERE orderId = :id
                    ORDER BY run_id DESC"""),
            {"id": orderId}
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Order not found")

    return Order(**row)


@app.get("/orders")
def get_orders(limit: int = 100):

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM orders_clean ORDER BY orderId OFFSET 0 ROWS FETCH NEXT :l ROWS ONLY"),
            {"l": limit}
        ).mappings().all()

    return rows


@app.get("/anomalies")
def get_anomalies(limit: int = 100):

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM orders_anomalies ORDER BY orderId OFFSET 0 ROWS FETCH NEXT :l ROWS ONLY"),
            {"l": limit}
        ).mappings().all()

    return rows


@app.get("/metrics")
def get_metrics():

    sql = """
    SELECT
        (SELECT COUNT(*) FROM orders_clean)     AS clean,
        (SELECT COUNT(*) FROM orders_anomalies) AS anomalies,
        (SELECT COUNT(*) FROM orders_rejected)  AS rejected
    """

    with engine.connect() as conn:
        row = conn.execute(text(sql)).mappings().first()

    return dict(row)