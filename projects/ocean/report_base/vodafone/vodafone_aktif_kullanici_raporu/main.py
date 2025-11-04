import os
import psycopg2
import warnings
import json
from datetime import date
from dateutil.relativedelta import relativedelta
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
        env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        load_dotenv(env_path)
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

    def execute_non_query(self, sql: str, params: tuple | list = None):
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)  # parametreli çalıştır
            self.conn.commit()
            print("[INFO] Transaction committed.")
        except Exception as e:
            self.conn.rollback()
            print(f"[ERROR] Query failed, rollback executed. Error: {e}")
            raise

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
"""
def load_sql(file_name: str, etl_date: str = None, start_date: str = None, finish_date: str = None, trx_date_3: str = None, txn_def_text: str = None, ):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, 'sql', file_name)
    with open(file_path, 'r', encoding='utf-8') as f:
        sql = f.read()
        if etl_date:
            sql = sql.replace("{{etl_date}}", etl_date)
        if start_date:
            sql = sql.replace("{{start_date}}", start_date)
        if finish_date:
            sql = sql.replace("{{finish_date}}", finish_date)
        if trx_date_3:
            sql = sql.replace("{{trx_date_3}}", trx_date_3)
        if txn_def_text:
            sql = sql.replace("{{txn_def_text}}", txn_def_text)
        return sql
"""
def load_sql(file_name: str, **kwargs):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, 'sql', file_name)

    with open(file_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    return sql.format(**kwargs)
# ==================== CORE LOGIC ====================

def truncate_tables(dwh_db: PostgreDatabaseConnection, base_table: str):
        print(f"[INFO] Truncate table start: {base_table}")
        dwh_db.execute_non_query(f"TRUNCATE TABLE {base_table};")
        print(f"[INFO] Truncated {base_table}")

def delete_tables(dwh_db: PostgreDatabaseConnection, base_table: str, months: list[date]): 
    print(f"[INFO] Delete table start:{base_table} - {months} ")
    
    months_placeholders = ",".join(["%s"] * len(months))
    sql = f"DELETE FROM {base_table} WHERE trx_date IN ({months_placeholders})"

    dwh_db.execute_non_query(f"DELETE FROM {base_table} WHERE trx_date in {months} ")     
    print(f"[INFO] Deleted table data:{base_table} - {months} ") 

def run_daily_etl(dwh_config: dict,base_table: str):
    print(f"  -> run_daily_etl ETL for {base_table}")
    dwh_db = PostgreDatabaseConnection(**dwh_config)

    # bugünün tarihi
    today = date.today()
    # önceki ayın ilk günü
    prev_month_first_day = (today.replace(day=1) - relativedelta(months=1))
    # mevcut ayın ilk günü
    this_month_first_day = today.replace(day=1)
    # değişkenler
    start_date   = prev_month_first_day.strftime("%Y%m%d")     # YYYYMMDD
    trx_date    = prev_month_first_day.strftime("%Y%m")       # YYYYMM
    finish_date  = this_month_first_day.strftime("%Y%m%d")     # YYYYMMDD
    # önceki ayın ilk gününden 3, 6, 12 ay geri
    trx_date_3  = (prev_month_first_day - relativedelta(months=3)).strftime("%Y%m")
    trx_date_6  = (prev_month_first_day - relativedelta(months=6)).strftime("%Y%m")
    trx_date_12 = (prev_month_first_day - relativedelta(months=12)).strftime("%Y%m")

    # debug print
    print("start_date :", start_date)
    print("trx_date  :", trx_date)
    print("finish_date:", finish_date)
    print("trx_date_3:", trx_date_3)
    print("trx_date_6:", trx_date_6)
    print("trx_date_12:", trx_date_12)

    try:
        delete_tables(dwh_db, base_table,trx_date)

        base_detail_query = load_sql('aktif_kullanici_detay_data.sql',trx_date=trx_date ,start_date=start_date , finish_date=finish_date)
        df = dwh_db.execute_query(base_detail_query)
        if df.empty:
            print(f" No data for source {base_detail_query}.")
            return

        base_sub_sum_query = load_sql('aktif_kullanici_alt_toplam.sql')
        df_3_month = dwh_db.execute_query(base_sub_sum_query,trx_date=trx_date,  trx_date_3=trx_date_3, txn_def_text='out 12 months')
        if df_3_month.empty:
            print(f" No data for source {base_sub_sum_query}.")
            return

        df['etl_date_time'] = datetime.now().date()
        print(f"     Loaded {len(df)} rows from source.")
        dwh_db.insert_dataframe(df, base_table)
        print(f"     Inserted {len(df)} rows into {base_table}.")
    finally:
        dwh_db.close()

def main():
    print(f"[INFO] Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    is_airflow_env = os.getenv("AIRFLOW_CTX_DAG_ID") is not None
    dwh_config   = load_env_or_airflow_variable("DWH"   if is_airflow_env else "OCEAN_DWH_DB_CORLU",       is_airflow_env)
    base_table = "edw.vodafone_aktif_kullanici_raporu"

    run_daily_etl( dwh_config, base_table)

    print("\n[INFO] ETL Finished.")

if __name__ == "__main__":
    main()
