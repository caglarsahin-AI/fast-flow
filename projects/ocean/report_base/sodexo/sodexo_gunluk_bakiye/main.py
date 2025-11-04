import os
import psycopg2
import warnings
import json
import calendar
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import oracledb

warnings.filterwarnings("ignore")

# ==================== UTILITIES ====================

def load_env_or_airflow_variable(variable_key: str, is_airflow_env: bool) -> dict:
    if is_airflow_env:
        from airflow.models import Variable
        config = json.loads(Variable.get(variable_key))
    else:
        load_dotenv()
        config = {
            "host": os.getenv(f"{variable_key.upper()}_HOST"),
            "port": os.getenv(f"{variable_key.upper()}_PORT"),
            "database": os.getenv(f"{variable_key.upper()}_NAME"),
            "user": os.getenv(f"{variable_key.upper()}_USER"),
            "password": os.getenv(f"{variable_key.upper()}_PASSWORD"),
        }
    return config


class PostgreDatabaseConnection:
    def __init__(self, host, port, database, user, password):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

    def execute_query(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query, self.conn)

    def execute_non_query(self, query: str):
        with self.conn.cursor() as cur:
            cur.execute(query)
        self.conn.commit()

    def insert_dataframe(self, df: pd.DataFrame, table_name: str):
        # Float -> Int fix (312321.0 -> 312321)
        for col in df.columns:
            if (pd.api.types.is_float_dtype(df[col])
                and df[col].dropna().astype(int).eq(df[col].dropna()).all()):
                df[col] = df[col].astype('Int64')

        # 1) Buffer’a yaz, artık na_rep kullanmıyoruz
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # 2) Kopyalama SQL’i; artık NULL parametresi yok
        cols = ', '.join(df.columns)
        sql = f"""
        COPY {table_name} ({cols})
        FROM STDIN
        WITH (FORMAT csv)
        """
        with self.conn.cursor() as cur:
            cur.copy_expert(sql, buffer)
        self.conn.commit()

    def close(self):
        self.conn.close()

class OracleDatabaseConnection:
    def __init__(self, config):
        # pure-Python driver, Oracle Instant Client gerekmez / ?
        dsn = oracledb.makedsn(config['host'], config['port'], service_name=config['database'])
        self.conn = oracledb.connect(
            user=config['user'],
            password=config['password'],
            dsn=dsn,
        )

    def close(self):
        self.conn.close()

def load_sql(file_name: str):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, 'sql', file_name)
    with open(file_path, 'r', encoding='utf-8') as f:
        sql = f.read()
        return sql

# ==================== CORE LOGIC ====================

def truncate_tables(dwh_db: PostgreDatabaseConnection, base_table: str, months: list[date]):
        print(f"[INFO] Truncate table start: {base_table}")
        dwh_db.execute_non_query(f"TRUNCATE TABLE {base_table};")
        print(f"[INFO] Truncated {base_table}")
        

def run_daily_etl(ocean_config: dict,dwh_config: dict,base_table: str):
    print(f"  -> run_daily_etl ETL for {base_table}")
    sodexo_db = OracleDatabaseConnection(**ocean_config)
    dwh_db = PostgreDatabaseConnection(**dwh_config)

    try:
        truncate_tables(dwh_db, base_table)
        base_query = load_sql('sodexo_gunluk_bakiye.sql')
        df = sodexo_db.execute_query(base_query)

        if df.empty:
            print(f" No data for source {base_query}.")
            return

        df['etl_date_time'] = datetime.now().date()
        print(f"     Loaded {len(df)} rows from source.")
        dwh_db.insert_dataframe(df, base_table)
        print(f"     Inserted {len(df)} rows into {base_table}.")
    finally:
        sodexo_db.close()
        dwh_db.close()

def main():
    print(f"[INFO] Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    is_airflow_env = os.getenv("AIRFLOW_CTX_DAG_ID") is not None
    ocean_config = load_env_or_airflow_variable("SODEXO" if is_airflow_env else "SODEXO_DB_IST", is_airflow_env)
    dwh_config   = load_env_or_airflow_variable("DWH"   if is_airflow_env else "OCEAN_DWH_DB_CORLU",       is_airflow_env)
    base_table = "edw.rp_sodexo_kart_bakiye_bilgileri"

    run_daily_etl(ocean_config, dwh_config, base_table)

    print("\n[INFO] ETL Finished.")

if __name__ == "__main__":
    main()
