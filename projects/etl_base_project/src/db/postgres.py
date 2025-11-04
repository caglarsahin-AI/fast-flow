# projects/etl_base_project/src/db/postgres.py
from __future__ import annotations
import psycopg2, psycopg2.extras
from typing import Any, Dict, Iterable, List, Optional, Tuple
from .base import Dialect, Driver

_TEXT = {"character varying","varchar","text","character","bpchar"}
_NUM  = {"smallint","integer","bigint","numeric","decimal","real","double precision"}
_DATE = {"date"}
_TS   = {"timestamp without time zone","timestamp with time zone"}

class PostgresDialect(Dialect):
    name = "postgres"

    def qident(self, ident: str) -> str: return '"' + ident.replace('"','""') + '"'
    def qualify(self, schema: str, table: str) -> str: return f'{self.qident(schema)}.{self.qident(table)}'
    def wrap_sql_source(self, sql: str) -> str: return f"({sql}) AS _src"
    def limit_clause(self, n: int) -> str: return f"LIMIT {int(n)}"
    def offset_clause(self, n: int) -> str: return f"OFFSET {int(n)}"
    def order_by(self, order_by: Optional[str]) -> str: return f"ORDER BY {order_by}" if order_by else ""

    def column_datatype_query(self) -> str:
        return """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """

    def column_list_query(self) -> str:
        return """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """

    def min_max_sql(self, schema, table, column, where):
        w = []
        if where and where.strip(): w.append(f"({where})")
        w.append(f'{self.qident(column)} IS NOT NULL')
        wsql = "WHERE " + " AND ".join(w)
        return f'SELECT MIN({self.qident(column)}), MAX({self.qident(column)}) FROM {self.qualify(schema,table)} {wsql}'

    def distinct_sql(self, schema, table, column, where, limit):
        w = []
        if where and where.strip(): w.append(f"({where})")
        w.append(f'{self.qident(column)} IS NOT NULL')
        wsql = "WHERE " + " AND ".join(w)
        lim = f" LIMIT {int(limit)}" if limit else ""
        return f'SELECT DISTINCT {self.qident(column)} FROM {self.qualify(schema,table)} {wsql} ORDER BY {self.qident(column)} DESC{lim}'

    def normalize_type(self, db_type: str) -> str: return (db_type or "").lower()

    def literal_for_type(self, val: Any, db_type: str) -> str:
        if val is None: return "NULL"
        t = self.normalize_type(db_type)
        if t in _TEXT: return f"{str(val)!r}::varchar"
        if t in _NUM:  return str(val)
        if t in _DATE: return f"{str(val)!r}::date"
        if t in _TS:   return f"{str(val)!r}::timestamp"
        return f"{str(val)!r}::{t}"

    def pred_eq(self, schema, table, column, v, col_type):
        return f'{self.qident(column)} = {self.literal_for_type(v, col_type)}'

    def pred_ge_lt(self, schema, table, column, lo, hi, col_type):
        return f'{self.qident(column)} >= {self.literal_for_type(lo, col_type)} AND {self.qident(column)} < {self.literal_for_type(hi, col_type)}'

    def pred_ge_le(self, schema, table, column, lo, hi, col_type):
        return f'{self.qident(column)} >= {self.literal_for_type(lo, col_type)} AND {self.qident(column)} <= {self.literal_for_type(hi, col_type)}'

    def ntile_range_sql(self, schema, table, column, parts, where):
        w = []
        if where and where.strip(): w.append(f"({where})")
        w.append(f'{self.qident(column)} IS NOT NULL')
        wsql = "WHERE " + " AND ".join(w)
        c = self.qident(column)
        qt = self.qualify(schema, table)
        return f"""
            SELECT bucket, MIN({c}) AS lo, MAX({c}) AS hi
            FROM (
                SELECT {c}, NTILE({int(parts)}) OVER (ORDER BY {c}) AS bucket
                FROM {qt}
                {wsql}
            ) t
            GROUP BY bucket
            ORDER BY bucket
        """

    def mod_expr(self, column: str, parts: int, remainder: int) -> str:
        return f"({self.qident(column)} % {int(parts)}) = {int(remainder)}"


class PostgresDriver(Driver):
    dialect: Dialect = PostgresDialect()

    def connect(self, conf: Dict[str,Any]):
        return psycopg2.connect(
            host=conf["host"], port=conf["port"],
            dbname=conf["database"], user=conf["user"], password=conf["password"]
        )

    def close(self, conn): conn.close()

    def fetchall(self, conn, sql: str, params=None):
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        return rows, cols

    def server_cursor_iter(self, conn, sql: str, arraysize: int):
        with conn.cursor(name="csr", cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.itersize = arraysize; cur.arraysize = arraysize
            cur.execute(sql)
            while True:
                rows = cur.fetchmany(arraysize)
                if not rows: break
                yield [dict(r) for r in rows]

    def copy_to_filelike(self, conn, inner_select_sql: str, file_obj, *, binary: bool):
        with conn.cursor() as cur:
            if binary:
                cur.copy_expert(f"COPY ({inner_select_sql}) TO STDOUT WITH (FORMAT binary)", file_obj)
            else:
                cur.copy_expert(f"COPY ({inner_select_sql}) TO STDOUT WITH (FORMAT csv, HEADER false, NULL '\\N')", file_obj)

    def get_column_types(self, conn, schema: str, table: str) -> Dict[str,str]:
        sql = self.dialect.column_datatype_query()
        with conn.cursor() as cur:
            cur.execute(sql, (schema, table))
            return {r[0]: r[1] for r in cur.fetchall()}
