###  🚀 Python Data Validation & Ingestion Pipeline with API

A production-style mini data platform built to demonstrate Python for data engineering + backend integration with strong data quality, lineage and operational design.

This project is intentionally designed to mirror how real ETL / ingestion systems are built in SQL Server–centric environments.

### 📌 Project Highlights

This project implements a complete ingestion and serving flow:

Source → Validation → Anomaly detection → Persistence → API access

It focuses on:

-strict schema validation

-data quality separation

-operational traceability

-idempotent batch ingestion

-backend-ready API layer

### 🧱 What this project does

The pipeline:

Reads structured data from a CSV file (or REST API)

Enforces a strict business schema using Pydantic

Separates invalid records from valid records

Detects business and statistical anomalies

Loads data into SQL Server or Postgres

Exposes curated data and metrics through a FastAPI service

### 🧩 Core Use Case

Input data:

order_id, customer_id, order_amount, order_date, country

Output datasets:

clean orders

anomalous orders

rejected (schema-invalid) records

Each dataset is persisted independently.

### 🏗️ Architecture
ingestion/
   source_reader      → IO only
   schema             → business contract
   validator          → schema enforcement
   anomalies          → data quality rules
   loader             → persistence
   main_ingest        → orchestration only

api/
   app                → read-only access layer

This strict separation allows:

easy testing

easier future orchestration (Airflow / ADF / cron)

independent evolution of validation, quality rules and persistence

### 🗃️ Storage Model (Data Quality Layers)

The pipeline persists three datasets:

Table	Purpose
orders_clean	validated and anomaly-free records
orders_anomalies	business/statistical outliers
orders_rejected	schema and parsing failures

This directly mirrors:

bronze → silver → gold style quality layers

No bad data is dropped.

### 🔐 Schema Enforcement

Schema is defined using Pydantic:

strict typing

automatic parsing

explicit validation errors

shared contract with API layer

Validation is performed row-by-row to ensure:

partial batch success

full visibility into rejected rows

detailed error capture

### 🚨 Anomaly Detection

Anomalies are treated as data quality issues, not schema errors.

Implemented rules:

non-positive order amounts

future order dates

extreme statistical outliers (z-score)

business rules (country whitelist)

This cleanly separates:

schema correctness vs business correctness

### 🔁 Run-Level Idempotency & Lineage

Every ingestion execution generates a unique:

run_id (UUID)

Each output row includes:

run_id

This enables:

run-level traceability

safe retries

batch auditing

operational debugging

The pipeline follows a standard idempotent pattern:

delete by run_id
→ insert new data for that run_id

This is the same pattern used in enterprise ETL systems and batch pipelines.

### 📊 Operational Metrics

The ingestion job returns structured metrics:

total records read

valid records loaded

anomalies detected

rejected records

These metrics can later be persisted into a control table for operational monitoring.

### 🌐 API Layer

A FastAPI service exposes curated data for downstream consumers.

Endpoints include:

GET /orders/{order_id}
GET /orders
GET /anomalies
GET /metrics

Key design principles:

read-only serving layer

reuses the same Pydantic schema used during ingestion

clean separation between ingestion and consumption

Built-in OpenAPI documentation is automatically available.

### 🧠 Design Principles Used

Separation of concerns

Schema-first validation

Quarantine instead of deletion

Explicit data quality layers

Run-level lineage

Backend-ready API contracts

Clear orchestration boundaries

### ⚙️ Technology Stack
Area	Technology
Data processing	pandas
Schema validation	pydantic
Anomaly detection	pandas, scipy
Database access	SQLAlchemy
SQL Server driver	pyodbc
Postgres driver	psycopg2
API	FastAPI
Runtime	Python 3.x
### ▶️ How to Run
Ingestion job

From project root:

python -m ingestion.main_ingest

Optional input file:

python -m ingestion.main_ingest myfile.csv
API service
uvicorn api.app:app --reload

API documentation:

http://127.0.0.1:8000/docs
### 🧪 Example Data Scenarios Covered

The project correctly handles:

invalid data types

missing fields

negative or zero monetary values

extreme outliers

future timestamps

business rule violations

### 📐 Why this project is interview-ready

This is not a toy script.

It demonstrates:

Python-based ingestion pipelines

enterprise data quality handling

operational traceability

SQL-first backend integration

API exposure for downstream systems

production-style architecture

### 🗣️ Talking Points

You can confidently state:

Implemented schema enforcement using Pydantic before persistence

Designed quarantine tables for rejected and anomalous data

Implemented run-level idempotent ingestion using run identifiers

Built a backend API layer reusing validation models

Separated ingestion, validation, quality rules and persistence

### 🧩 Future Extensions (easy to add)

pipeline_runs control table (start time, end time, status, row counts)

incremental load support

deduplication strategies

data drift monitoring

scheduled orchestration with Airflow / ADF

### 🎯 Project Goal

This project demonstrates how Python can be used to build a reliable, auditable and backend-ready data ingestion system for relational platforms such as SQL Server and Postgres, using modern validation, quality and API patterns aligned with real production data engineering workflows.
Python data ingestion pipeline with:

- schema validation using Pydantic
- anomaly detection
- quarantine tables
- SQL Server / Postgres persistence
- FastAPI data access layer

-From the boiler plate code
1) Updated connection string for Windows authentication
2) File path issues fixed.
3) All test records were moved to rejected dataframe leaving valid rows dataframe empty, which when attempted to process threw error, that was tracing back to the file path. Added logs and understood where the error, and added a condition to start validating the rows if some exist.
4) The rejected data frame structure (raw_record, error, run_id) is different than what I initially had (orderId, customerId, orderAmount, date, country). Identified and recreated the DB table accordingly.
5) Then came the data type mismatch error. raw_record was object type, DB has varchar. So updated the raw_record to string.
6) DB has varchar without specifying the lengths. It defaulted to 1 length. So threw error '(pyodbc.ProgrammingError) ('String data, right truncation: length 72 buffer 2', 'HY000')'. Resolved by setting lengths
7) Now, all records were pushed to rejected data frame, even two of them met the specifications. Schema.py has a class Orders with columns defined as order_Id, customer_Id, order_Amount.. with an underscore, but the csv has column headers without the underscores. This inconsistent column names led to the rejection of all the rows.
8) Errors: 'Cannot compare tz-naive and tz-aware datetime-like objects', TypeError: Invalid comparison between dtype=datetime64[us, UTC] and datetime. I have 20260220 as date in in the file to be imported. while the data frame has 1970-08-23 11:50:20+00:00.
import pandas as pd
Fixed. converted the incoming Date value to timezone-aware timestamp so comparisons don't mix tz-naive and tz-aware datetimes
```
df['orderDate'] = pd.to_datetime(df['orderDate'].astype(str),
                                 format='%Y%m%d',
                                 errors='coerce',
                                 utc=True)   # produces tz-aware UTC timestamps
```
9) I still see the input dates were not handled properly. I see 1970 year while I fed 2026. fixed by 
```df['orderDate'] = df['orderDate'].apply(lambda x: datetime.strptime(str(x), "%Y%m%d").date() if pd.notna(x) else None)```
10) end point `/orders/{orderId} threw Internal Server Error for some order Ids but was returning expected for other order Ids. compared the order Ids' details from the good and bad response. Identified that the datetime values from initial loads are of the format '1970-01-01 00:00:00.020260220+0000' rather than '2026-03-01 00:00:00.0000000'. The error was resolved after correcting the values to the standard format.
11) /anamolies end point was also throwing error. upon inspecting the method, the SQL query (received by boiler plate) had syntax error. ORDER by clause is required when OFFSET FETCH NEXT are used. But the order by clause was missing.