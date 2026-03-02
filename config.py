import os

DB_TYPE = os.getenv("DB_TYPE", "sqlserver")  # or postgres

SQLSERVER_CONN_STR = os.getenv(
    "SQLSERVER_CONN_STR",
    #"mssql+pyodbc://user:password@server/db?driver=ODBC+Driver+17+for+SQL+Server"
    "mssql+pyodbc://@MY-DIGITAL-WORL/AdventureWorks2025?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

POSTGRES_CONN_STR = os.getenv(
    "POSTGRES_CONN_STR",
    "postgresql+psycopg2://user:password@localhost:5432/db"
)