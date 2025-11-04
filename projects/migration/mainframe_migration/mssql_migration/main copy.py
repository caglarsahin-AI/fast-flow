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
from multiprocessing import Pool, get_context, current_process

# ================== LOGGING ==================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("mssql_to_pg")

# ================== PARAMS ===================
MAX_RETRIES            = int(os.getenv("MAX_RETRIES", "5"))
RETRY_BASE_SEC         = int(os.getenv("RETRY_BASE_SEC", "3"))
MSSQL_READ_UNCOMMITTED = os.getenv("MSSQL_READ_UNCOMMITTED", "1") == "1"
ISO8583_MIN_DATE       = os.getenv("ISO8583_MIN_DATE", "2000-01-01 00:00:00")
PARALLEL_WORKERS       = int(os.getenv("PARALLEL_WORKERS", "1"))

# ================= TYPE MAP ==================
MSSQL_TO_PG_TYPE = {
    "bigint":"BIGINT","int":"INTEGER","smallint":"SMALLINT","tinyint":"SMALLINT",
    "bit":"BOOLEAN","decimal":"NUMERIC","numeric":"NUMERIC",
    "money":"NUMERIC(19,4)","smallmoney":"NUMERIC(10,4)",
    "float":"DOUBLE PRECISION","real":"REAL",
    "datetime":"TIMESTAMP","datetime2":"TIMESTAMP","smalldatetime":"TIMESTAMP",
    "date":"DATE","time":"TIME","datetimeoffset":"TIMESTAMP",
    "char":"CHAR","varchar":"VARCHAR","text":"TEXT",
    "nchar":"CHAR","nvarchar":"VARCHAR","ntext":"TEXT",
    "binary":"BYTEA","varbinary":"BYTEA","image":"BYTEA",
    "xml":"XML","uniqueidentifier":"UUID","sql_variant":"TEXT",
    "hierarchyid":"TEXT","geography":"TEXT","geometry":"TEXT","json":"JSONB",
}

def dq(s:str)->str: return '"' + s.replace('"','""') + '"'

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
        if MSSQL_READ_UNCOMMITTED:
            try:
                cur=self.cn.cursor()
                cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            except Exception as e:
                logger.warning(f"[WARN] READ UNCOMMITTED set failed: {e}")

    def reconnect(self):
        try: self.close()
        finally:
            time.sleep(1); self._connect()

    def _execute_with_retry(self, q: str, params: Tuple=None, fetch: bool=True):
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
                self.reconnect(); time.sleep(wait)
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

    def get_primary_key(self, schema:str, table:str)->List[str]:
        q=f"""
        SELECT kcu.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
          ON kcu.CONSTRAINT_NAME=tc.CONSTRAINT_NAME
         AND kcu.TABLE_SCHEMA=tc.TABLE_SCHEMA AND kcu.TABLE_NAME=tc.TABLE_NAME
        WHERE tc.TABLE_SCHEMA={self.param} AND tc.TABLE_NAME={self.param}
          AND tc.CONSTRAINT_TYPE='PRIMARY KEY'
        ORDER BY kcu.ORDINAL_POSITION"""
        rows=self._execute_with_retry(q, (schema,table))
        return [r[0] for r in rows]

    def fetch_df_by_date_range(self, schema:str, table:str, cols:List[str],
                               date_col:str, start_iso:str, end_iso:str)->pd.DataFrame:
        cols_sql=", ".join(f'[{c}]' for c in cols)
        q=(f"SELECT {cols_sql} FROM [{schema}].[{table}] "
           f"WHERE [{date_col}] >= {self.param} AND [{date_col}] < {self.param}")
        rows=self._execute_with_retry(q, (start_iso, end_iso))
        return pd.DataFrame.from_records(rows, columns=cols) if rows else pd.DataFrame(columns=cols)

    def fetch_df_by_int_yyyymmdd_range(self, schema:str, table:str, cols:List[str],
                                       int_col:str, start_int:int, end_int:int)->pd.DataFrame:
        cols_sql=", ".join(f'[{c}]' for c in cols)
        q=(f"SELECT {cols_sql} FROM [{schema}].[{table}] "
           f"WHERE [{int_col}] >= {self.param} AND [{int_col}] < {self.param}")
        rows=self._execute_with_retry(q, (start_int, end_int))
        return pd.DataFrame.from_records(rows, columns=cols) if rows else pd.DataFrame(columns=cols)

    def close(self):
        try: self.cn.close()
        except Exception: pass

# ============== POSTGRES EXECUTOR ============
class PostgresExecutor:
    def __init__(self, host:str, database:str, user:str, password:str, port:int=5432):
        self.cn=psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password)
        self.cn.autocommit=True
        with self.cn.cursor() as cur: cur.execute("SET search_path TO public")

    def execute(self, sql_text:str):
        with self.cn.cursor() as cur: cur.execute(sql_text)

    def table_exists(self, table:str)->bool:
        with self.cn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s) IS NOT NULL",(f"public.{table}",))
            return bool(cur.fetchone()[0])

    def truncate_table(self, table:str):
        with self.cn.cursor() as cur:
            cur.execute(pSQL("TRUNCATE TABLE {}").format(pID(table)))

    def copy_from_df(self, table:str, df:pd.DataFrame)->int:
        if df.empty: return 0
        t0=time.perf_counter()
        buf=StringIO()
        df.to_csv(buf, index=False, header=False, na_rep="\\N", quoting=csv.QUOTE_MINIMAL)
        buf.seek(0)
        cols_sql=", ".join(dq(c) for c in df.columns)
        sql=f'COPY {dq(table)} ({cols_sql}) FROM STDIN WITH (FORMAT csv, NULL \'\\N\')'
        with self.cn.cursor() as cur: cur.copy_expert(sql, buf)
        t3=time.perf_counter()
        logger.info(f"[COPY pid={os.getpid()}] {table}: rows={len(df)} time={t3-t0:.2f}s")
        return len(df)

    def set_synchronous_commit(self, on:bool):
        with self.cn.cursor() as cur:
            cur.execute(f"SET synchronous_commit TO {'on' if on else 'off'}")

    def set_unlogged(self, table:str, unlogged:bool):
        try:
            with self.cn.cursor() as cur:
                cur.execute(pSQL(f"ALTER TABLE {{}} SET {'UNLOGGED' if unlogged else 'LOGGED'}").format(pID(table)))
        except Exception as e:
            logger.warning(f"[WARN] UNLOGGED toggle failed on {table}: {e}")

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

    def fix_identity_sequences(self, table:str):
        q_cols="""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s AND identity_generation IS NOT NULL"""
        with self.cn.cursor() as cur:
            cur.execute(q_cols,(table,)); id_cols=[r[0] for r in cur.fetchall()]
        if not id_cols: return
        with self.cn.cursor() as cur:
            for col in id_cols:
                sql=pSQL("""
                    SELECT setval(pg_get_serial_sequence(%s, %s), COALESCE(MAX({col})+1,1), TRUE)
                    FROM public.{tbl}
                """).format(col=pID(col), tbl=pID(table))
                cur.execute(sql, (f"public.{table}", col))

    def close(self): self.cn.close()

# ================== DDL ======================
class DdlTranslator:
    def _map_type(self, col:Dict[str,Any])->str:
        t=col["data_type"].lower()
        if t in ("varchar","nvarchar","char","nchar"):
            L=col.get("char_max_len")
            return "TEXT" if L in (None,-1) else f"VARCHAR({int(L)})"
        if t in ("decimal","numeric"):
            p=col.get("numeric_precision") or 38
            s=col.get("numeric_scale") or 0
            return f"NUMERIC({int(p)},{int(s)})"
        return MSSQL_TO_PG_TYPE.get(t,"TEXT")

    def _clean_default(self, default_expr:Optional[str])->Optional[str]:
        if not default_expr: return None
        expr=str(default_expr).strip()
        while expr.startswith('(') and expr.endswith(')'): expr=expr[1:-1].strip()
        u=expr.upper()
        if u in ("GETDATE()","SYSDATETIME()","CURRENT_TIMESTAMP"): return "now()"
        if u.startswith("N'"): expr=expr[1:]
        return expr

    def build_create_table(self, table:str, cols:List[Dict[str,Any]], pk_cols_lower:List[str],
                           cmap:Dict[str,str])->str:
        lines=[]
        for c in cols:
            new_name=cmap[c["name"]]
            pg_type=self._map_type(c)
            identity=" GENERATED BY DEFAULT AS IDENTITY" if c.get("is_identity") else ""
            null_sql="NOT NULL" if not c.get("is_nullable") else "NULL"
            default_expr=self._clean_default(c.get("default"))
            default_sql=f" DEFAULT {default_expr}" if default_expr else ""
            lines.append(f"    {dq(new_name)} {pg_type}{identity} {null_sql}{default_sql}")
        pk_sql=f",\n    PRIMARY KEY ({', '.join(dq(c) for c in pk_cols_lower)})" if pk_cols_lower else ""
        return f"CREATE TABLE IF NOT EXISTS {dq(table)} (\n"+",\n".join(lines)+pk_sql+"\n);"

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

# ================ BUCKET HELPERS =============
def day_buckets(start_date:dt.date, end_date:dt.date)->List[Tuple[str,str]]:
    out=[]; cur=start_date
    while cur<=end_date:
        nxt=cur+dt.timedelta(days=1)
        out.append((cur.strftime("%Y-%m-%d 00:00:00"), nxt.strftime("%Y-%m-%d 00:00:00")))
        cur=nxt
    return out

def day_buckets_int(start_date:dt.date, end_date:dt.date)->List[Tuple[int,int]]:
    def ymd_int(d:dt.date): return d.year*10000 + d.month*100 + d.day
    out=[]; cur=start_date
    while cur<=end_date:
        nxt=cur+dt.timedelta(days=1)
        out.append((ymd_int(cur), ymd_int(nxt)))
        cur=nxt
    return out

# ============== DF TRANSFORM HELPERS =========
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

# ================ WORKER FUNCS ===============
def worker_datetime(args)->int:
    (env, schema, table_lower, src_table, date_col, cols_meta, cmap, start_iso, end_iso) = args
    pid=current_process().pid
    ms_server = env["mssql"]["host"] if env["mssql"].get("port") in (None,"",0) else f"{env['mssql']['host']},{env['mssql']['port']}"
    ms = MssqlIntrospector(ms_server, env["mssql"]["database"], env["mssql"]["user"], env["mssql"]["password"])
    pg = PostgresExecutor(env["pg"]["host"], env["pg"]["database"], env["pg"]["user"], env["pg"]["password"], int(env["pg"].get("port",5432) or 5432))
    try:
        cols=[c["name"] for c in cols_meta]
        df=ms.fetch_df_by_date_range(schema, src_table, cols, date_col, start_iso, end_iso)
        if not df.empty:
            df=apply_bytea(df, cols_meta)
            df=coerce_types(df, cols_meta)
            df.rename(columns={k:cmap.get(k, k.lower()) for k in df.columns}, inplace=True)
            ins=pg.copy_from_df(table_lower, df)
        else:
            ins=0
        logger.info(f"[PERF pid={pid}] {table_lower} {start_iso}→{end_iso} rows={ins:,}")
        return ins
    finally:
        ms.close(); pg.close()

def worker_int(args)->int:
    (env, schema, table_lower, src_table, int_col, cols_meta, cmap, start_int, end_int) = args
    pid=current_process().pid
    ms_server = env["mssql"]["host"] if env["mssql"].get("port") in (None,"",0) else f"{env['mssql']['host']},{env['mssql']['port']}"
    ms = MssqlIntrospector(ms_server, env["mssql"]["database"], env["mssql"]["user"], env["mssql"]["password"])
    pg = PostgresExecutor(env["pg"]["host"], env["pg"]["database"], env["pg"]["user"], env["pg"]["password"], int(env["pg"].get("port",5432) or 5432))
    try:
        cols=[c["name"] for c in cols_meta]
        df=ms.fetch_df_by_int_yyyymmdd_range(schema, src_table, cols, int_col, start_int, end_int)
        if not df.empty:
            df=apply_bytea(df, cols_meta)
            df=coerce_types(df, cols_meta)
            df.rename(columns={k:cmap.get(k, k.lower()) for k in df.columns}, inplace=True)
            ins=pg.copy_from_df(table_lower, df)
        else:
            ins=0
        logger.info(f"[PERF pid={pid}] {table_lower} {start_int}→{end_int-1} rows={ins:,}")
        return ins
    finally:
        ms.close(); pg.close()

# ================ MIGRATOR (PARALLEL) ========
class DataMigrator:
    def __init__(self, ms:MssqlIntrospector, pg:PostgresExecutor, translator:DdlTranslator, env:Dict[str,Any]):
        self.ms=ms; self.pg=pg; self.translator=translator; self.env=env

    def _prep_table(self, schema:str, table:str, *, truncate:bool=True)\
            ->Tuple[str,List[Dict[str,Any]],Dict[str,str],List[str]]:
        table_lower=(table or "").lower()
        cols=self.ms.get_columns(schema, table)
        if not cols: raise RuntimeError(f"No columns for {schema}.{table}")
        pk=self.ms.get_primary_key(schema, table)
        orig=[c["name"] for c in cols]; _, cmap = uniquify_lower(orig)
        pk_lower=[cmap[c] for c in pk if c in cmap]
        ddl=self.translator.build_create_table(table_lower, cols, pk_lower, cmap)
        self.pg.execute(ddl)
        if truncate and self.pg.table_exists(table_lower):
            self.pg.truncate_table(table_lower)
        return table_lower, cols, cmap, pk_lower

    def _speed_on(self, table_lower:str):
        self.pg.set_synchronous_commit(False); self.pg.set_unlogged(table_lower, True); self.pg.disable_triggers(table_lower)

    def _speed_off(self, table_lower:str):
        self.pg.enable_triggers(table_lower); self.pg.set_unlogged(table_lower, False); self.pg.set_synchronous_commit(True)
        self.pg.analyze(table_lower); self.pg.fix_identity_sequences(table_lower)

    def process_datetime_daily_parallel(self, schema:str, table:str, date_col:str, buckets:List[Tuple[str,str]]):
        table_lower, cols_meta, cmap, _ = self._prep_table(schema, table, truncate=True)
        self._speed_on(table_lower)
        total=0
        try:
            args_list = [(self.env, schema, table_lower, table, date_col, cols_meta, cmap, s, e) for (s,e) in buckets]
            with get_context("fork").Pool(processes=PARALLEL_WORKERS) as pool:
                for ins in pool.imap_unordered(worker_datetime, args_list, chunksize=1):
                    total += ins
        finally:
            self._speed_off(table_lower)
        logger.info(f"[DONE] public.{table_lower} total={total:,}")

    def process_int_daily_parallel(self, schema:str, table:str, int_col:str, ranges:List[Tuple[int,int]]):
        table_lower, cols_meta, cmap, _ = self._prep_table(schema, table, truncate=True)
        self._speed_on(table_lower)
        total=0
        try:
            args_list = [(self.env, schema, table_lower, table, int_col, cols_meta, cmap, s, e) for (s,e) in ranges]
            with get_context("fork").Pool(processes=PARALLEL_WORKERS) as pool:
                for ins in pool.imap_unordered(worker_int, args_list, chunksize=1):
                    total += ins
        finally:
            self._speed_off(table_lower)
        logger.info(f"[DONE] public.{table_lower} total={total:,}")

    def process_iso8583_special(self, schema:str, table:str, date_col:str,
                                cutoff_start_iso:str, cutoff_end_iso:str,
                                *, lower_bound_iso:str):
        """
        1) [lower_bound_iso, cutoff_start_iso) tek seferde
        2) [cutoff_start_iso, cutoff_end_iso) günlük paralel
        """
        # Hazırlık ve hız ayarları
        table_lower, cols_meta, cmap, _ = self._prep_table(schema, table, truncate=True)
        self._speed_on(table_lower)
        total=0
        try:
            # --- (1) Tek seferde bulk
            logger.info(f"[RUN-1shot] {schema}.{table} < {cutoff_start_iso}")
            ms_cols=[c["name"] for c in cols_meta]
            df=self.ms.fetch_df_by_date_range(schema, table, ms_cols, date_col, lower_bound_iso, cutoff_start_iso)
            if not df.empty:
                df=apply_bytea(df, cols_meta)
                df=coerce_types(df, cols_meta)
                df.rename(columns={k:cmap.get(k, k.lower()) for k in df.columns}, inplace=True)
                total += self.pg.copy_from_df(table_lower, df)
            logger.info(f"[DONE-1shot] rows={len(df):,}")

            # --- (2) Günlük paralel
            logger.info(f"[RUN-daily] {schema}.{table} {cutoff_start_iso} → {cutoff_end_iso} (daily)")
            start_date = dt.datetime.strptime(cutoff_start_iso, "%Y-%m-%d %H:%M:%S").date()
            end_date   = (dt.datetime.strptime(cutoff_end_iso, "%Y-%m-%d %H:%M:%S") - dt.timedelta(seconds=1)).date()
            buckets = day_buckets(start_date, end_date)
            args_list = [(self.env, schema, table_lower, table, date_col, cols_meta, cmap, s, e) for (s,e) in buckets]
            with get_context("fork").Pool(processes=PARALLEL_WORKERS) as pool:
                for ins in pool.imap_unordered(worker_datetime, args_list, chunksize=1):
                    total += ins
        finally:
            self._speed_off(table_lower)
        logger.info(f"[DONE] public.{table_lower} total={total:,}")

# ================= ORCHESTRATION =============
def migrate_selected_tables():
    env=load_configs()
    ms_server = env["mssql"]["host"] if env["mssql"].get("port") in (None,"",0) else f"{env['mssql']['host']},{env['mssql']['port']}"
    ms=MssqlIntrospector(server=ms_server, database=env["mssql"]["database"], user=env["mssql"]["user"], password=env["mssql"]["password"])
    pg=PostgresExecutor(host=env["pg"]["host"], database=env["pg"]["database"], user=env["pg"]["user"], password=env["pg"]["password"], port=int(env["pg"].get("port",5432) or 5432))
    translator=DdlTranslator()
    mig=DataMigrator(ms, pg, translator, env)

    """
    # ---- 1) tblgenkartsay — günlük 1999-09-01 → 2008-02-29
    try:
        logger.info(f"[RUN] dbo.tblgenkartsay (daily int parallel, workers={PARALLEL_WORKERS})")
        start = dt.date(1999,9,1)
        end   = dt.date(2008,2,29)  # 2008 Şubat sonu
        mig.process_int_daily_parallel("dbo","tblgenkartsay","KASTARIH", day_buckets_int(start, end))
    except Exception as e:
        logger.error(f"[ERR] tblgenkartsay: {e}", exc_info=True)

    # ---- 2) tblkklogstr_new — günlük 2015-01-01 → 2016-12-31 (2017-01-01’e kadar)
    try:
        logger.info(f"[RUN] dbo.tblkklogstr_new (daily int parallel, workers={PARALLEL_WORKERS})")
        start = dt.date(2015,1,1)
        end   = dt.date(2016,12,31)
        mig.process_int_daily_parallel("dbo","tblkklogstr_new","KLTARIH", day_buckets_int(start, end))
    except Exception as e:
        logger.error(f"[ERR] tblkklogstr_new: {e}", exc_info=True)
    """
    # ---- 3) tblIso8583 — özel akış
    try:
        logger.info(f"[RUN] dbo.tblIso8583 (special)")
        # (a) tek seferde: < 2018-12-01 00:00:00
        cutoff_start = "2018-12-01 00:00:00"
        # (b) günlük: 2018-12-01 00:00:00 → 2021-10-01 00:00:00 (exclusive)
        cutoff_end   = "2021-10-01 00:00:00"
        # alt limit env’den ya da defaulttan:
        lower_bound  = ISO8583_MIN_DATE  # ör. "2000-01-01 00:00:00"
        mig.process_iso8583_special(
            "dbo","tblIso8583","fldSqlDateTime",
            cutoff_start_iso=cutoff_start,
            cutoff_end_iso=cutoff_end,
            lower_bound_iso=lower_bound
        )
    except Exception as e:
        logger.error(f"[ERR] tblIso8583: {e}", exc_info=True)

    ms.close(); pg.close()

# ================= ENTRY =====================
def main():
    migrate_selected_tables()

if __name__ == "__main__":
    main()
