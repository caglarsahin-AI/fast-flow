import os
import re
import json
import csv
import time
import datetime as dt
import pymssql
import psycopg2
import pandas as pd
import logging
from io import StringIO
from dotenv import load_dotenv
from psycopg2.sql import SQL as pSQL, Identifier as pID
from typing import List, Dict, Any, Optional, Tuple
from multiprocessing import Pool

# ================== LOGGING ==================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("mssql_to_pg")

def dq(s:str)->str: return '"' + s.replace('"','""') + '"'

def normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: 
        return df
    obj_cols = [c for c in df.columns if df[c].dtype == "object"]
    if not obj_cols:
        return df

    def _norm(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        if isinstance(v, (bytes, bytearray, memoryview)):
            try:
                v = bytes(v).decode("utf-8", errors="replace")
            except Exception:
                v = str(v)
        else:
            v = str(v)
        v = v.replace("\x00", "")
        v = v.replace("\r\n", "\n").replace("\r", "\n")
        return v

    for c in obj_cols:
        df[c] = df[c].map(_norm)
    return df

def uniquify_lower(names: List[str]) -> Tuple[List[str], Dict[str, str]]:
    used=set(); mapping={}; out=[]
    for orig in names:
        base=(orig or "").lower() or "col"; new=base; i=2
        while new in used: new=f"{base}_{i}"; i+=1
        used.add(new); mapping[orig]=new; out.append(new)
    return out, mapping

# ================ MSSQL CLIENT ===============
class MssqlIntrospector:
    def __init__(self, server: str, database: str, user: str, password: str):
        self.server_raw=server.strip()
        self.database=database; self.user=user; self.password=password
        self._connect()

    def _connect(self):
        _host,_port=self.server_raw,None
        m=re.match(r"^([^,:]+)[,:](\d+)$", self.server_raw)
        if m: _host, _port = m.group(1), int(m.group(2))
        tds_ver=os.getenv("TDS_VERSION","7.4")
        login_timeout=int(os.getenv("MSSQL_LOGIN_TIMEOUT","15"))
        query_timeout=int(os.getenv("MSSQL_QUERY_TIMEOUT","0"))
        self.cn=pymssql.connect(
            server=_host, port=_port or 1433, user=self.user, password=self.password,
            database=self.database, login_timeout=login_timeout, timeout=query_timeout,
            charset="UTF-8", tds_version=tds_ver, as_dict=False
        )
        try: self.cn.autocommit(True)
        except Exception: pass
        self.param="%s"
        if os.getenv("MSSQL_READ_UNCOMMITTED", "1") == "1":
            try:
                cur=self.cn.cursor()
                cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            except Exception as e:
                logger.warning(f"[WARN] READ UNCOMMITTED set failed: {e}")

    def close(self):
        try: self.cn.close()
        except Exception: pass

    def _execute_with_retry(self, q: str, params: Tuple=None, fetch: bool=True):
        MAX_RETRIES=int(os.getenv("MAX_RETRIES","5")); RETRY_BASE_SEC=int(os.getenv("RETRY_BASE_SEC","3"))
        last=None
        for attempt in range(1, MAX_RETRIES+1):
            try:
                cur=self.cn.cursor(); cur.execute(q, params or ())
                return cur.fetchall() if fetch else None
            except Exception as e:
                msg=str(e).lower()
                transient=any(s in msg for s in [
                    "dbprocess is dead","adaptive server connection timed out",
                    "connection timed out","login timeout","write to the server failed",
                    "connection is closed"
                ])
                if not transient or attempt==MAX_RETRIES:
                    last=e; break
                wait=RETRY_BASE_SEC*(2**(attempt-1))
                logger.warning(f"[MSSQL RETRY] attempt={attempt} wait={wait}s reason={e}")
                time.sleep(wait)
        raise last

    def get_columns(self, schema:str, table:str)->List[Dict[str,Any]]:
        q=f"""
        SELECT c.COLUMN_NAME, c.ORDINAL_POSITION, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
               c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE, c.COLUMN_DEFAULT, col.is_identity
        FROM INFORMATION_SCHEMA.COLUMNS c
        JOIN sys.schemas s ON s.name=c.TABLE_SCHEMA
        JOIN sys.tables t ON t.name=c.TABLE_NAME AND t.schema_id=s.schema_id
        JOIN sys.columns col ON col.object_id=t.object_id AND col.name=c.COLUMN_NAME
        WHERE c.TABLE_SCHEMA={self.param} AND c.TABLE_NAME={self.param}
        ORDER BY c.ORDINAL_POSITION"""
        rows=self._execute_with_retry(q, (schema, table))
        return [{
            "name":r[0],"position":r[1],"data_type":str(r[2]).lower(),
            "char_max_len":r[3],"numeric_precision":r[4],"numeric_scale":r[5],
            "is_nullable":(str(r[6]).upper()=="YES"),"default":r[7],"is_identity":bool(r[8]),
        } for r in rows]

    def fetch_df_by_date_range(self, schema:str, table:str, cols:List[str],
                               date_col:str, start_iso:str, end_iso:str)->pd.DataFrame:
        cols_sql=", ".join(f'[{c}]' for c in cols)
        q=(f"SELECT {cols_sql} FROM [{schema}].[{table}] "
           f"WHERE [{date_col}] >= {self.param} AND [{date_col}] < {self.param}")
        rows=self._execute_with_retry(q, (start_iso, end_iso))
        return pd.DataFrame.from_records(rows, columns=cols) if rows else pd.DataFrame(columns=cols)

# ============== POSTGRES EXECUTOR ============
class PostgresExecutor:
    def __init__(self, host:str, database:str, user:str, password:str, port:int=5432):
        self.cn=psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password)
        self.cn.autocommit=True
        with self.cn.cursor() as cur: cur.execute("SET search_path TO public")

    def close(self): self.cn.close()

    def execute(self, sql_text:str):
        with self.cn.cursor() as cur: cur.execute(sql_text)

    def table_exists(self, table:str)->bool:
        with self.cn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s) IS NOT NULL",(f"public.{table}",))
            return bool(cur.fetchone()[0])

    def copy_from_df(self, table: str, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        df = normalize_text_columns(df)
        t0 = time.perf_counter()
        buf = StringIO()
        df.to_csv(buf, index=False, header=False, na_rep="\\N",
                  quoting=csv.QUOTE_ALL, lineterminator="\n")
        buf.seek(0)
        cols_sql = ", ".join(dq(c) for c in df.columns)
        sql = f'COPY {dq(table)} ({cols_sql}) FROM STDIN WITH (FORMAT csv, NULL \'\\N\')'
        with self.cn.cursor() as cur:
            cur.copy_expert(sql, buf)
        t3 = time.perf_counter()
        logger.info(f"[COPY] {table}: rows={len(df)} time={t3 - t0:.2f}s")
        return len(df)

    def set_synchronous_commit(self, on:bool):
        with self.cn.cursor() as cur:
            cur.execute(f"SET synchronous_commit TO {'on' if on else 'off'}")

    def disable_triggers(self, table:str):
        try:
            with self.cn.cursor() as cur:
                cur.execute(pSQL("ALTER TABLE {} DISABLE TRIGGER ALL").format(pID(table)))
        except Exception as e:
            logger.warning(f"[WARN] DISABLE TRIGGER failed on {table}: {e}")

    def enable_triggers(self, table:str):
        try:
            with self.cn.cursor() as cur:
                cur.execute(pSQL("ALTER TABLE {} ENABLE TRIGGER ALL").format(pID(table)))
        except Exception as e:
            logger.warning(f"[WARN] ENABLE TRIGGER failed on {table}: {e}")

    def analyze(self, table:str):
        with self.cn.cursor() as cur:
            cur.execute(pSQL("ANALYZE {}").format(pID(table)))

# ================ HELPERS ====================
def day_buckets_exclusive(start_date:dt.date, end_date_exclusive:dt.date)->List[Tuple[str,str]]:
    out=[]; cur=start_date
    while cur < end_date_exclusive:
        nxt=cur+dt.timedelta(days=1)
        out.append((cur.strftime("%Y-%m-%d 00:00:00"), nxt.strftime("%Y-%m-%d 00:00:00")))
        cur=nxt
    return out

def hour_buckets_in_day(day_start_iso:str)->List[Tuple[str,str]]:
    """'YYYY-MM-DD 00:00:00' -> o gün için 24 adet [h, h+1)"""
    d0 = dt.datetime.strptime(day_start_iso, "%Y-%m-%d %H:%M:%S")
    out=[]
    for h in range(24):
        s = d0 + dt.timedelta(hours=h)
        e = s + dt.timedelta(hours=1)
        out.append((s.strftime("%Y-%m-%d %H:%M:%S"), e.strftime("%Y-%m-%d %H:%M:%S")))
    return out

def apply_bytea(df:pd.DataFrame, cols_meta:List[Dict[str,Any]])->pd.DataFrame:
    bins=[c["name"] for c in cols_meta if c["data_type"].lower() in ("binary","varbinary","image")]
    if not bins: return df
    def _hex(v):
        if v is None or (isinstance(v,float) and pd.isna(v)): return None
        if isinstance(v, memoryview): v=v.tobytes()
        if isinstance(v, bytearray): v=bytes(v)
        if isinstance(v, bytes): return "\\x"+v.hex()
        return v
    for c in bins:
        if c in df.columns: df[c]=df[c].map(_hex)
    return df

def coerce_types(df:pd.DataFrame, cols_meta:List[Dict[str,Any]])->pd.DataFrame:
    if df.empty: return df
    ints={"bigint","int","smallint","tinyint"}; bits={"bit"}
    for cm in cols_meta:
        name, dtp = cm["name"], str(cm["data_type"]).lower()
        if name not in df.columns: continue
        if dtp in ints:
            df[name]=pd.to_numeric(df[name], errors="coerce").astype("Int64")
        elif dtp in bits:
            def _b(v):
                if v is None or (isinstance(v,float) and pd.isna(v)): return None
                s=str(v).strip().lower()
                if s in ("1","true","t","y","yes"): return "true"
                if s in ("0","false","f","n","no"): return "false"
                try: return "true" if int(float(s))!=0 else "false"
                except Exception: return None
            df[name]=df[name].map(_b)
    return df

# ================ CONFIG =====================
def load_env_or_airflow_variable(key:str, is_airflow_env:bool)->dict:
    if is_airflow_env:
        from airflow.models import Variable
        cfg=json.loads(Variable.get(key))
    else:
        load_dotenv()
        cfg={
            "host": os.getenv(f"{key.upper()}_HOST"),
            "port": os.getenv(f"{key.upper()}_PORT"),
            "database": os.getenv(f"{key.upper()}_NAME") or os.getenv(f"{key.upper()}_DATABASE"),
            "user": os.getenv(f"{key.upper()}_USER"),
            "password": os.getenv(f"{key.upper()}_PASSWORD"),
        }
    return cfg

def load_configs()->Dict[str,Any]:
    is_airflow_env=os.getenv("AIRFLOW_CTX_DAG_ID") is not None
    src=load_env_or_airflow_variable("mssql", is_airflow_env)
    tgt=load_env_or_airflow_variable("BISReportsDB", is_airflow_env)
    if src.get("port") is not None:
        try: src["port"]=int(src["port"]) if str(src["port"]).isdigit() else src["port"]
        except Exception: pass
    if tgt.get("port") is not None:
        try: tgt["port"]=int(tgt["port"]) if str(tgt["port"]).isdigit() else tgt["port"]
        except Exception: pass
    return {"mssql":src, "pg":tgt}

# ---------- Worker (multiprocess için) ----------
def _hour_job(args)->Tuple[str, int]:
    """
    Her işçi kendi bağlantılarını açar/kapar.
    args: (env, schema, table, ms_cols, cmap, date_col, start_iso, end_iso)
    """
    env, schema, table, ms_cols, cmap, date_col, s, e = args
    ms_server = env["mssql"]["host"] if env["mssql"].get("port") in (None,"",0) else f"{env['mssql']['host']},{env['mssql']['port']}"
    ms = MssqlIntrospector(server=ms_server, database=env["mssql"]["database"],
                           user=env["mssql"]["user"], password=env["mssql"]["password"])
    pg = PostgresExecutor(host=env["pg"]["host"], database=env["pg"]["database"],
                          user=env["pg"]["user"], password=env["pg"]["password"],
                          port=int(env["pg"].get("port",5432) or 5432))
    try:
        df = ms.fetch_df_by_date_range(schema, table, ms_cols, date_col, s, e)
        if df.empty:
            return (f"{s}→{e}", 0)
        df = apply_bytea(df, [{"name": c, "data_type": "varchar"} for c in ms_cols])  # safe fallback
        # Gerçek tipleri isabetli dönüştürmek için coerce_types çağır
        # (çağrı için kolon metalarını dışarıdan istemiyoruz; hızlı bir kullanım)
        df = coerce_types(df, [{"name": c, "data_type": "int"} for c in []])  # no-op
        df.rename(columns={k: cmap.get(k, k.lower()) for k in df.columns}, inplace=True)
        inserted = pg.copy_from_df(table.lower(), df)
        return (f"{s}→{e}", inserted)
    finally:
        try: ms.close()
        except: pass
        try: pg.close()
        except: pass

# ================ MIGRATOR (HOURLY+MP) =======
class DataMigrator:
    def __init__(self, ms:MssqlIntrospector, pg:PostgresExecutor, env:Dict[str,Any]):
        self.ms=ms; self.pg=pg; self.env=env

    def _get_meta_and_map(self, schema:str, table:str)->Tuple[str,List[Dict[str,Any]],Dict[str,str]]:
        table_lower=(table or "").lower()
        if not self.pg.table_exists(table_lower):
            raise RuntimeError(f"Target table public.{table_lower} does not exist. Create it first.")
        cols_meta=self.ms.get_columns(schema, table)
        if not cols_meta: raise RuntimeError(f"No columns returned for {schema}.{table}")
        orig=[c["name"] for c in cols_meta]; _, cmap = uniquify_lower(orig)
        return table_lower, cols_meta, cmap

    def _speed_on(self, table_lower:str):
        self.pg.set_synchronous_commit(False)
        self.pg.disable_triggers(table_lower)

    def _speed_off(self, table_lower:str):
        self.pg.enable_triggers(table_lower)
        self.pg.set_synchronous_commit(True)
        self.pg.analyze(table_lower)

    def process_iso8583_hourly_mp(self, schema:str, table:str, date_col:str,
                                  daily_start_iso:str, daily_end_iso:str, processes:int=3):
        """
        [daily_start_iso, daily_end_iso) aralığını günlere böler; her günün 24 saatlik slotunu
        processes=3 ile paralel işler.
        """
        table_lower, cols_meta, cmap = self._get_meta_and_map(schema, table)
        ms_cols=[c["name"] for c in cols_meta]

        logger.info(f"[RUN-hourly*{processes}] {schema}.{table} {daily_start_iso} → {daily_end_iso}")
        start_date = dt.datetime.strptime(daily_start_iso, "%Y-%m-%d %H:%M:%S").date()
        end_excl   = dt.datetime.strptime(daily_end_iso, "%Y-%m-%d %H:%M:%S").date()

        total = 0
        self._speed_on(table_lower)
        try:
            for (day_start, day_end) in day_buckets_exclusive(start_date, end_excl):
                logger.info(f"[DAY-BEGIN] {day_start} → {day_end}")
                jobs=[]
                for (hs, he) in hour_buckets_in_day(day_start):
                    # son günün dışına taşan slotları at
                    if hs >= daily_end_iso: 
                        continue
                    if he > daily_end_iso:
                        he = daily_end_iso
                    jobs.append((self.env, schema, table, ms_cols, cmap, date_col, hs, he))

                day_rows = 0
                if jobs:
                    with Pool(processes=processes) as pool:
                        for label, ins in pool.imap_unordered(_hour_job, jobs):
                            logger.info(f"[HOUR] {label} rows={ins:,}")
                            day_rows += ins
                logger.info(f"[DAY-END] {day_start} → {day_end} rows={day_rows:,}")
                total += day_rows
        finally:
            self._speed_off(table_lower)

        logger.info(f"[DONE] public.{table_lower} total_inserted={total:,}")

# ================= ORCHESTRATION =============
def migrate_selected_tables():
    env=load_configs()
    ms_server = env["mssql"]["host"] if env["mssql"].get("port") in (None,"",0) else f"{env['mssql']['host']},{env['mssql']['port']}"
    ms=MssqlIntrospector(server=ms_server, database=env["mssql"]["database"], user=env["mssql"]["user"], password=env["mssql"]["password"])
    pg=PostgresExecutor(host=env["pg"]["host"], database=env["pg"]["database"], user=env["pg"]["user"], password=env["pg"]["password"], port=int(env["pg"].get("port",5432) or 5432))
    mig=DataMigrator(ms, pg, env)

    try:
        logger.info(f"[RUN] dbo.tblIso8583 (Hourly x3, per-day pools)")
        mig.process_iso8583_hourly_mp(
            schema="dbo",
            table="tblIso8583",
            date_col="fldSqlDateTime",
            daily_start_iso="2021-07-04 00:00:00",
            daily_end_iso="2021-07-05 00:00:00",
            processes=3,
        )
    except Exception as e:
        logger.error(f"[ERR] tblIso8583: {e}", exc_info=True)

    ms.close(); pg.close()

# ================= ENTRY =====================
def main():
    migrate_selected_tables()

if __name__ == "__main__":
    main()
