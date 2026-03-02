from sqlalchemy import create_engine
from config import (
    DB_TYPE,
    SQLSERVER_CONN_STR,
    POSTGRES_CONN_STR
)


def get_engine():

    if DB_TYPE == "sqlserver":
        return create_engine(SQLSERVER_CONN_STR, fast_executemany=True)

    if DB_TYPE == "postgres":
        return create_engine(POSTGRES_CONN_STR)

    raise ValueError("Unsupported DB_TYPE")