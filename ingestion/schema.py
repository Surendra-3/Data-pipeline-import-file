from pydantic import BaseModel, Field
from datetime import datetime


class Order(BaseModel):
    orderId: int
    customerId: int
    orderAmount: float = Field(gt=0)
    orderDate: datetime
    country: str