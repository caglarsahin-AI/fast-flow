# count_audit.py COMPARE COUNT
import os
import re
import json
import time
import logging
import pymssql
import psycopg2
from dotenv import load_dotenv
from typing import List, Tuple, Optional

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("count_audit")

# ---- Ayarlar ----
MAX_RETRIES          = int(os.getenv("MAX_RETRIES", "5"))
RETRY_BASE_SEC       = int(os.getenv("RETRY_BASE_SEC", "3"))
MSSQL_READ_UNCOMMITTED = os.getenv("MSSQL_READ_UNCOMMITTED", "1") == "1"

# Ağır tabloları en sona atmak istersen (opsiyonel)
HEAVY_TABLES = {
    "tblessfhrk", "tbliso8583", "tblbkmdrfstr", "tblharktstr",
    "tblgenkartsay", "tblkklogstr_new", "tblaqslpstr",
}

# ---- Yardımcılar ----
def normalize_qualified(qualified: str) -> Tuple[Optional[str], str, str]:
    """db.schema.table | schema.table | table → (db?, schema, table)"""
    parts = qualified.split(".")
    if len(parts) == 3: return parts[0], parts[1], parts[2]
    if len(parts) == 2: return None, parts[0], parts[1]
    return None, "dbo", parts[0]

def reorder_tables_for_heavy_last(table_names: List[str]) -> List[str]:
    def _basename(qname: str) -> str:
        _, _, t = normalize_qualified(qname); return (t or "").lower()
    normal = [t for t in table_names if _basename(t) not in HEAVY_TABLES]
    heavy  = [t for t in table_names if _basename(t) in HEAVY_TABLES]
    return normal + heavy

# ---- Config yükleme ----
def load_env_or_airflow_variable(variable_key: str, is_airflow_env: bool) -> dict:
    if is_airflow_env:
        from airflow.models import Variable
        return json.loads(Variable.get(variable_key))
    load_dotenv()
    return {
        "host": os.getenv(f"{variable_key.upper()}_HOST"),
        "port": os.getenv(f"{variable_key.upper()}_PORT"),
        "database": os.getenv(f"{variable_key.upper()}_NAME") or os.getenv(f"{variable_key.upper()}_DATABASE"),
        "user": os.getenv(f"{variable_key.upper()}_USER"),
        "password": os.getenv(f"{variable_key.upper()}_PASSWORD"),
    }

def load_configs():
    is_airflow_env = os.getenv("AIRFLOW_CTX_DAG_ID") is not None
    ms = load_env_or_airflow_variable("mssql", is_airflow_env)
    pg = load_env_or_airflow_variable("BISReportsDB", is_airflow_env)
    if ms.get("port"): 
        try: ms["port"] = int(ms["port"])
        except: pass
    if pg.get("port"):
        try: pg["port"] = int(pg["port"])
        except: pass
    return {"mssql": ms, "pg": pg}

# ---- MSSQL ----
class MssqlClient:
    def __init__(self, server: str, database: str, user: str, password: str):
        self.server_raw = server.strip()
        self.database = database
        self.user = user
        self.password = password
        self._connect()

    def _connect(self):
        host, port = self.server_raw, None
        m = re.match(r"^([^,:]+)[,:](\d+)$", self.server_raw)
        if m: host, port = m.group(1), int(m.group(2))

        tds_ver = os.getenv("TDS_VERSION", "7.4")
        login_timeout = int(os.getenv("MSSQL_LOGIN_TIMEOUT", "15"))
        query_timeout = int(os.getenv("MSSQL_QUERY_TIMEOUT", "0"))  # 0: sınırsız

        self.cn = pymssql.connect(
            server=host, port=port or 1433, user=self.user, password=self.password,
            database=self.database, login_timeout=login_timeout, timeout=query_timeout,
            charset="UTF-8", tds_version=tds_ver, as_dict=False,
        )
        try: self.cn.autocommit(True)
        except: pass
        if MSSQL_READ_UNCOMMITTED:
            try:
                cur = self.cn.cursor()
                cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            except Exception as e:
                logger.warning(f"[MSSQL] READ UNCOMMITTED set failed: {e}")

    def reconnect(self):
        try: self.cn.close()
        except: pass
        time.sleep(1)
        self._connect()

    def _execute_with_retry(self, sql: str, params: tuple = ()):
        last = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                cur = self.cn.cursor()
                cur.execute(sql, params)
                return cur.fetchall()
            except Exception as e:
                msg = str(e).lower()
                transient = any(s in msg for s in [
                    "dbprocess is dead", "timed out", "connection is closed",
                    "login timeout", "write to the server failed"
                ])
                if not transient or attempt == MAX_RETRIES:
                    last = e; break
                wait = RETRY_BASE_SEC * (2 ** (attempt - 1))
                logger.warning(f"[MSSQL RETRY {attempt}] {e} → {wait}s")
                self.reconnect(); time.sleep(wait)
        if last: raise last

    def fast_rowcount(self, schema: str, table: str) -> int:
        # sadece direkt COUNT. READ UNCOMMITTED açıksa NOLOCK kullan
        if MSSQL_READ_UNCOMMITTED:
            q = f"SELECT COUNT_BIG(*) FROM [{schema}].[{table}] WITH (NOLOCK)"
        else:
            q = f"SELECT COUNT_BIG(*) FROM [{schema}].[{table}]"
        rows = self._execute_with_retry(q)
        return int(rows[0][0])

    def close(self):
        try: self.cn.close()
        except: pass

# ---- PostgreSQL ----
class PgClient:
    def __init__(self, host: str, database: str, user: str, password: str, port: int = 5432):
        self.cn = psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password)
        self.cn.autocommit = True
        with self.cn.cursor() as cur:
            cur.execute("SET search_path TO public")

    def table_exists(self, table: str) -> bool:
        with self.cn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{table}",))
            return bool(cur.fetchone()[0])

    def exact_count(self, table: str) -> int:
        with self.cn.cursor() as cur:
            cur.execute(f'SELECT COUNT(1) FROM "{table}"')
            return int(cur.fetchone()[0])

    def close(self):
        try: self.cn.close()
        except: pass

# ---- IO ----
def read_tables_from_txt(path: str) -> List[str]:
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"): continue
            out.append(s)
    return out

# ---- Ana iş ----
def audit_counts(table_names: List[str]):
    if not table_names:
        raise SystemExit("tables.txt boş veya tablo yok.")

    env = load_configs()
    ms_server = env["mssql"]["host"] if not env["mssql"].get("port") else f"{env['mssql']['host']},{env['mssql']['port']}"

    ms = MssqlClient(
        server=ms_server,
        database=env["mssql"]["database"],
        user=env["mssql"]["user"],
        password=env["mssql"]["password"],
    )
    pg = PgClient(
        host=env["pg"]["host"],
        database=env["pg"]["database"],
        user=env["pg"]["user"],
        password=env["pg"]["password"],
        port=int(env["pg"].get("port") or 5432),
    )

    try:
        table_names = reorder_tables_for_heavy_last(table_names)
        cfg_db = env["mssql"].get("database")
        mismatched = []
        missing_pg = []
        ok = 0

        print("table_name | mssql_count | pg_count | delta")
        print("-"*60)

        for qname in table_names:
            src_db, src_schema, src_table = normalize_qualified(qname)
            if src_db and cfg_db and src_db != cfg_db:
                logger.error(f"[SKIP] Config MSSQL DB '{cfg_db}' != '{src_db}' for '{qname}'")
                continue

            table_lower = (src_table or "").lower()

            # MSSQL COUNT
            try:
                src_cnt = ms.fast_rowcount(src_schema, src_table)
            except Exception as e:
                logger.error(f"[MSSQL COUNT FAIL] {src_schema}.{src_table}: {e}")
                continue

            # PG COUNT (tablo varsa)
            if not pg.table_exists(table_lower):
                print(f"{table_lower} | {src_cnt:,} | (PG: not exists) | -")
                missing_pg.append(table_lower)
                continue

            try:
                dst_cnt = pg.exact_count(table_lower)
            except Exception as e:
                logger.error(f"[PG COUNT FAIL] public.{table_lower}: {e}")
                continue

            delta = src_cnt - dst_cnt
            if delta != 0:
                print(f"{table_lower} | {src_cnt:,} | {dst_cnt:,} | {delta:+,}")
                mismatched.append(table_lower)
            else:
                ok += 1  # sessiz de olabilirdi; toplama koyduk

        print("\n--- Summary ---")
        print(f"Matched (Δ=0): {ok}")
        print(f"Mismatched    : {len(mismatched)} → {mismatched}")
        print(f"Missing in PG : {len(missing_pg)} → {missing_pg}")

    finally:
        ms.close(); pg.close()

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    tables_file = os.path.join(script_dir, "tables.txt")
    if not os.path.exists(tables_file):
        raise SystemExit("tables.txt bulunamadı.")
    table_list = read_tables_from_txt(tables_file)
    audit_counts(table_list)

if __name__ == "__main__":
    main()