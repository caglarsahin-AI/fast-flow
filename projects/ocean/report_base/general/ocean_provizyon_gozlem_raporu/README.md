# Ocean Provizyon Gözlem Raporu ETL

This project implements an ETL (Extract, Transform, Load) pipeline that extracts transaction data from the Ocean database, transforms it by appending audit metadata, and loads it into a PostgreSQL DWH target table.

The project is designed to work both:
- **Locally via `.env` file**
- **In Airflow via Airflow Variables**

---

## 📌 Features

- Extracts transactional data via a complex SQL query from Ocean DB (ODBC/replica)
- Transforms data by:
  - Adding `etl_date_time`
  - Casting or cleaning types when required (e.g., for numeric dates)
- Loads data into target PostgreSQL DWH table: `edw.ocean_provizyon_gozlem_raporu`
- Deletes old records before insert
- Handles column type mismatches and log-friendly output

---
