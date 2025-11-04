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
from multiprocessing import get_context, current_process

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
PARALLEL_WORKERS       = int(os.getenv("PARALLEL_WORKERS", "3"))  # ihtiyaca göre arttır
TRIM_VARCHAR_OVERFLOW  = os.getenv("TRIM_VARCHAR_OVERFLOW", "0") == "0"

# ================== YARDIMCILAR ==============
CONTROL_CHARS_RE = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')  # \n ve \r hariç

def _sanitize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].map(
                lambda v: (
                    CONTROL_CHARS_RE.sub('',
                    str(v)
                      .replace('\x08', '')
                      .replace('\x00', '')
                      .replace('\t', ' ')
                      .replace('\r\n', '\n')
                      .replace('\r', '\n'))
                      .replace('\\', '')
                ) if isinstance(v, str) else v
            )
    return df


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

# Destructive guard
DESTRUCTIVE_OK = (os.getenv("ALLOW_DESTRUCTIVE_SQL","").upper() == "YES")
_DESTRUCTIVE_PATTERN = re.compile(
    r"\b(TRUNCATE|DROP\s+(TABLE|SCHEMA|DATABASE)|DELETE\s+FROM\s+\w+\s*;?\s*$)\b",
    re.IGNORECASE
)
def _guard_sql(sql_text:str):
    if not DESTRUCTIVE_OK and _DESTRUCTIVE_PATTERN.search(sql_text or ""):
        raise RuntimeError("Destructive SQL blocked by guard (set ALLOW_DESTRUCTIVE_SQL=YES to override).")

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

    def select_df(self, sql: str, params: Tuple = None) -> pd.DataFrame:
        last = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                cur = self.cn.cursor()
                cur.execute(sql, params or ())
                cols = [d[0] for d in cur.description]
                data = cur.fetchall()
                return pd.DataFrame.from_records(data, columns=cols)
            except Exception as e:
                msg = (str(e) or "").lower()
                transient = any(s in msg for s in [
                    "dbprocess is dead","connection timed out","login timeout",
                    "write to the server failed","connection is closed"
                ])
                if transient and attempt < MAX_RETRIES:
                    wait = RETRY_BASE_SEC * (2 ** (attempt-1))
                    logger.warning(f"[MSSQL RETRY select_df] attempt={attempt} wait={wait}s reason={e}")
                    self.reconnect()
                    time.sleep(wait)
                    continue
                last = e
                break
        raise last

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
        rows=self._execute_with_retry(q, ("dbo", table))
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
        rows=self._execute_with_retry(q, ("dbo",table))
        return [r[0] for r in rows]

    def fetch_df_by_date_range(self, schema:str, table:str, cols:List[str],
                               date_col:str, start_iso:str, end_iso:str) -> pd.DataFrame:
        projected=[]
        for c in cols:
            if c.lower() in ("fld063","fld038"):
                expr=("LEFT("
                      f"REPLACE(REPLACE(REPLACE(CAST([{c}] AS NVARCHAR(MAX)), CHAR(0), ''), "
                      "CHAR(13)+CHAR(10), CHAR(10)), CHAR(13), CHAR(10)), "
                      "800)"
                      f" AS [{c}]")
                projected.append(expr)
            else:
                projected.append(f'[{c}]')
        cols_sql=", ".join(projected)
        q=(f"SELECT {cols_sql} FROM [{schema}].[{table}] "
           f"WHERE [{date_col}] >= CONVERT(datetime, {self.param}, 120) "
           f"  AND [{date_col}] <  CONVERT(datetime, {self.param}, 120)")
        rows=self._execute_with_retry(q, (start_iso, end_iso))
        return pd.DataFrame.from_records(rows, columns=cols) if rows else pd.DataFrame(columns=cols)

    def get_min_max_datetime(self, schema:str, table:str, date_col:str) -> Tuple[Optional[dt.datetime], Optional[dt.datetime]]:
        """Tablodaki date_col için min/max datetime'ı döndürür."""
        q = f"SELECT MIN([{date_col}]), MAX([{date_col}]) FROM [{schema}].[{table}]"
        rows = self._execute_with_retry(q, ())
        if not rows or (rows[0][0] is None and rows[0][1] is None):
            return None, None
        mn, mx = rows[0][0], rows[0][1]
        # pymssql genelde datetime.datetime döndürür
        return mn, mx

    def close(self):
        try: self.cn.close()
        except Exception: pass

# ============== POSTGRES EXECUTOR ============
class PostgresExecutor:
    def __init__(self, host:str, database:str, user:str, password:str, port:int=5432):
        self._params = dict(host=host, port=port, dbname=database, user=user, password=password)
        self._connect()

    def _connect(self):
        self.cn = psycopg2.connect(**self._params)
        self.cn.autocommit = True
        with self.cn.cursor() as cur:
            cur.execute("SET search_path TO public")

    def reconnect(self):
        try:
            self.cn.close()
        except Exception:
            pass
        time.sleep(1)
        self._connect()

    def _should_reconnect(self, exc: Exception) -> bool:
        msg = (str(exc) or "").lower()
        needles = [
            "server closed the connection unexpectedly",
            "connection already closed",
            "terminating connection due to administrator command",
            "could not receive data from server",
            "connection reset by peer",
            "ssl syscall error",
        ]
        return isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError)) and any(n in msg for n in needles)

    def _exec_with_retry(self, fn, *args, **kwargs):
        last = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                if self._should_reconnect(e) and attempt < MAX_RETRIES:
                    wait = RETRY_BASE_SEC * (2 ** (attempt - 1))
                    logging.warning(f"[PG RETRY] attempt={attempt} wait={wait}s reason={e}")
                    self.reconnect()
                    time.sleep(wait)
                    continue
                last = e
                break
        raise last

    def ensure_alive(self):
        def _probe():
            with self.cn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        self._exec_with_retry(_probe)

    def execute(self, sql_text:str):
        _guard_sql(sql_text)
        def _run():
            with self.cn.cursor() as cur:
                cur.execute(sql_text)
        self._exec_with_retry(_run)

    def table_exists(self, table:str)->bool:
        def _run():
            with self.cn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s) IS NOT NULL",(f"public.{table}",))
                return bool(cur.fetchone()[0])
        return self._exec_with_retry(_run)

    def copy_from_df(self, table:str, df:pd.DataFrame)->int:
        if df.empty:
            return 0
        t0 = time.perf_counter()
        df = _sanitize_text_columns(df)

        buf = StringIO(newline="")
        df.to_csv(
            buf,
            index=False,
            header=False,
            sep='\t',
            na_rep="",                # Boş string → aşağıda NULL '' ile NULL'a döner
            quoting=csv.QUOTE_MINIMAL,
            escapechar='\\',
            lineterminator='\n'
        )
        buf.seek(0)

        cols_sql = ", ".join(dq(c) for c in df.columns)
        sql = (
            f"COPY {dq(table)} ({cols_sql}) "
            f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', ESCAPE '\\', QUOTE '\"', NULL '')"
        )

        def _run():
            with self.cn.cursor() as cur:
                cur.copy_expert(sql, buf)

        self._exec_with_retry(_run)
        logger.info(f"[COPY pid={os.getpid()}] {table}: rows={len(df)} time={time.perf_counter()-t0:.2f}s")
        return len(df)

    def analyze(self, table:str):
        def _run():
            with self.cn.cursor() as cur:
                cur.execute(pSQL("ANALYZE {}").format(pID(table)))
        self._exec_with_retry(_run)

    def fix_identity_sequences(self, table:str):
        def _run_cols():
            with self.cn.cursor() as cur:
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=%s AND identity_generation IS NOT NULL
                """, (table,))
                return [r[0] for r in cur.fetchall()]
        id_cols = self._exec_with_retry(_run_cols)
        if not id_cols: return
        for col in id_cols:
            def _run_one():
                with self.cn.cursor() as cur:
                    sql=pSQL("""
                        SELECT setval(pg_get_serial_sequence(%s, %s), COALESCE(MAX({col})+1,1), TRUE)
                        FROM public.{tbl}
                    """).format(col=pID(col), tbl=pID(table))
                    cur.execute(sql, (f"public.{table}", col))
            self._exec_with_retry(_run_one)

    def close(self):
        try: self.cn.close()
        except Exception: pass

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

def date_range_inclusive(start_date: dt.date, end_date: dt.date) -> List[dt.date]:
    days = []
    cur = start_date
    while cur <= end_date:
        days.append(cur)
        cur += dt.timedelta(days=1)
    return days

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
            df = _sanitize_text_columns(df)
            # df = _enforce_varchar_limits(df, cols_meta, cmap)
            ins=pg.copy_from_df(table_lower, df)
        else:
            ins=0
        logger.info(f"[PERF pid={pid}] {table_lower} {start_iso}→{end_iso} rows={ins:,}")
        return ins
    finally:
        ms.close(); pg.close()

# ================ MIGRATOR (APPEND) ==========
class DataMigrator:
    def __init__(self, ms:MssqlIntrospector, pg:PostgresExecutor, translator:DdlTranslator, env:Dict[str,Any]):
        self.ms=ms; self.pg=pg; self.translator=translator; self.env=env

    def _prep_table(self, schema:str, table:str)\
            ->Tuple[str,List[Dict[str,Any]],Dict[str,str],List[str]]:
        table_lower=(table or "").lower()
        cols=self.ms.get_columns(schema, table)
        if not cols: raise RuntimeError(f"No columns for {schema}.{table}")
        pk=self.ms.get_primary_key(schema, table)
        orig=[c["name"] for c in cols]; _, cmap = uniquify_lower(orig)
        pk_lower=[cmap[c] for c in pk if c in cmap]
        ddl=self.translator.build_create_table(table_lower, cols, pk_lower, cmap)
        self.pg.execute(ddl)
        return table_lower, cols, cmap, pk_lower

    def process_datetime_daily_parallel(self, schema:str, table:str, date_col:str, buckets:List[Tuple[str,str]]):
        table_lower, cols_meta, cmap, _ = self._prep_table(schema, table)
        total = 0
        args_list = [(self.env, schema, table_lower, table, date_col, cols_meta, cmap, s, e) for (s,e) in buckets]

        # >>> REVIZE: Tek işçi ise Pool KULLANMA, düz for ile çalış
        if PARALLEL_WORKERS <= 1:
            for a in args_list:
                ins = worker_datetime(a)
                total += ins
        else:
            from multiprocessing import get_context
            with get_context("fork").Pool(processes=PARALLEL_WORKERS, maxtasksperchild=1) as pool:
                for ins in pool.imap_unordered(worker_datetime, args_list, chunksize=1):
                    total += ins
                pool.close()
                pool.join()
        # <<<
        logger.info(f"[DONE] public.{table_lower} total_appended={total:,}")
        try:
            self.pg.analyze(table_lower)
        except Exception as e:
            logger.warning(f"[ANALYZE WARN] {table_lower}: {e}")

# ================= ORCHESTRATION =============
"""
ARCHIVE_TABLES = [
    "tblProvzArsivstr_2018_1","tblProvzArsivstr_2018_2",
    "tblProvzArsivstr_2018_3","tblProvzArsivstr_2018_4","tblProvzArsivstr_2019_1","tblProvzArsivstr_2019_2",
    "tblProvzArsivstr_2019_3","tblProvzArsivstr_2019_4"
]
"""
ARCHIVE_TABLES = [
    "tblProvzArsivstr_2021_3","tblProvzArsivstr_2021_4",
    "tblProvzArsivstr_2022_1","tblProvzArsivstr_2022_4",
    "tblProvzArsivstr_2022_5","tblProvzArsivstr_2022_6","tblProvzArsivstr_2022_7","tblProvzArsivstr_2022_8",
    "tblProvzArsivstr_2022_9","tblProvzArsivstr_2022_10","tblProvzArsivstr_2022_11","tblProvzArsivstr_2022_12",
]


CUSTOM_DATE_RANGES = {
    # Örnek: 2018-02-10'dan başla, bitişi otomatik (MAX)
    "tblProvzArsivstr_2021_3": {"start": dt.date(2021, 7, 20)},
    #"tblProvzArsivstr_2018_2": {"start": dt.date(2018, 3, 1)},
    #"tblProvzArsivstr_2018_3": {"start": dt.date(2018, 6, 1)},
    #"tblProvzArsivstr_2018_4": {"start": dt.date(2018, 9, 1)},

    # Örnek: hem başlangıç hem bitiş verilebilir
    # "tblProvzArsivstr_2019_2": {"start": dt.date(2019, 5, 1), "end": dt.date(2019, 12, 31)},
}



def _resolve_date_range(ms: MssqlIntrospector, schema: str, table: str, date_col: str) -> Optional[Tuple[dt.date, dt.date]]:
    """Tablonun MIN/MAX tarihlerini alır; CUSTOM_DATE_RANGES varsa uygular; geçerli (start<=end) aralığı döner."""
    mn, mx = ms.get_min_max_datetime(schema, table, date_col)
    if not mn or not mx:
        return None  # tablo boş

    start_day = mn.date()
    end_day   = mx.date()

    cfg = CUSTOM_DATE_RANGES.get(table, {})
    if cfg.get("start"):
        start_day = max(start_day, cfg["start"])
    if cfg.get("end"):
        end_day = min(end_day, cfg["end"])

    if start_day > end_day:
        return None  # override, min/max'ı dışarı attıysa

    return start_day, end_day



def migrate_provz_archives_daily():
    env = load_configs()
    ms_server = env["mssql"]["host"] if env["mssql"].get("port") in (None,"",0) else f"{env['mssql']['host']},{env['mssql']['port']}"
    ms = MssqlIntrospector(server=ms_server, database=env["mssql"]["database"],
                           user=env["mssql"]["user"], password=env["mssql"]["password"])
    pg = PostgresExecutor(host=env["pg"]["host"], database=env["pg"]["database"],
                          user=env["pg"]["user"], password=env["pg"]["password"],
                          port=int(env["pg"].get("port",5432) or 5432))
    translator = DdlTranslator()
    mig = DataMigrator(ms, pg, translator, env)

    schema   = "dbo"
    date_col = "PROPDATE"

    for tbl in ARCHIVE_TABLES:
        try:
            logger.info(f"[RUN] {schema}.{tbl} — CREATE IF NOT EXISTS + daily ETL by {date_col}")

            # 1) Tarih aralığını çöz (MIN/MAX + override)
            rng = _resolve_date_range(ms, schema, tbl, date_col)
            if not rng:
                logger.warning(f"[SKIP] {schema}.{tbl} — {date_col} için geçerli aralık bulunamadı (boş tablo veya override dışına çıktı).")
                continue

            start_day, end_day = rng
            logger.info(f"[RANGE] {schema}.{tbl} — {start_day} → {end_day}")

            # 2) Günlük bucketlar
            buckets = day_buckets(start_day, end_day)

            # 3) ETL
            mig.process_datetime_daily_parallel(schema=schema, table=tbl, date_col=date_col, buckets=buckets)

        except Exception as e:
            logger.error(f"[ERR] {schema}.{tbl}: {e}", exc_info=True)

    ms.close(); pg.close()

# ================= ENTRY =====================
def main():
    migrate_provz_archives_daily()

if __name__ == "__main__":
    main()
