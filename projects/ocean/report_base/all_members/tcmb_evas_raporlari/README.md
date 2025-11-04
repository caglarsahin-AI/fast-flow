# TCMB EVAS RAPORLARI - Ocean to Oracle ETL Project

## 📊 Project Summary

This ETL project extracts transactional data from a PostgreSQL (Ocean Replica) database, transforms it, and loads it into Oracle (PaysOnlineServ) target tables for reporting purposes.

- **Source Database**: PostgreSQL (Ocean Replica)
- **Target Database**: Oracle (PaysOnlineServ)
- **Language**: Python
- **Libraries Used**: 
  - `psycopg2` (PostgreSQL connector)
  - `cx_Oracle` (Oracle connector)
  - `pandas` (Data processing)
  - `dotenv` (Environment variable management)

---

## 📂 Directory Structure

project_root/
│
├── sql/
│ ├── acq_resp.sql
│ ├── iss_resp.sql
│ ├── stat_tmp_hour.sql
│ └── stat_tmp_minute.sql
│
├── etl_oracle.py (Main ETL script)
├── .env (Optional local config)
└── README.md




---

## 🔧 SQL Logic

The project reads 4 different SQL files located in the `sql/` directory:

| SQL File | Target Oracle Table | Column Mapping |
| -------- | ------------------- | --------------- |
| `acq_resp.sql` | `CLEARING.TCMB_REPORT_STAT` | `TRX_SOURCE, TRX_DATETIME, BANK_CODE, REPORT_CODE, TRX_RESP_COUNT, TRX_RESP_CODE` |
| `iss_resp.sql` | `CLEARING.TCMB_REPORT_STAT` | `TRX_SOURCE, TRX_DATETIME, BANK_CODE, REPORT_CODE, TRX_RESP_COUNT, TRX_RESP_CODE` |
| `stat_tmp_hour.sql` | `CLEARING.TCMB_REPORT_STAT_TMP_HOUR` | `TRX_SOURCE, TRX_DATETIME, TRX_HOUR, BANK_CODE, TRX_RESP_COUNT` |
| `stat_tmp_minute.sql` | `CLEARING.TCMB_REPORT_STAT_TMP_MINUTE` | `TRX_SOURCE, TRX_DATETIME, BANK_CODE, TRX_RESP_COUNT` |

---

## 🔐 Environment Variables

The project reads database connection configs dynamically either:

- From **Airflow Variables** if running inside Airflow
- From **local `.env` file** if running standalone

### PostgreSQL Ocean Connection (Source)

```dotenv
OCEAN_HOST=your_postgres_host
OCEAN_PORT=5432
OCEAN_DB=your_postgres_db
OCEAN_USER=your_postgres_user
OCEAN_PASSWORD=your_postgres_password
