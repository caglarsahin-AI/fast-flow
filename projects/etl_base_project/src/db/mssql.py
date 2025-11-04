# projects/etl_base_project/src/db/mssql.py
from __future__ import annotations
import pyodbc
from typing import Any, Dict, Iterable, List, Optional, Tuple
from .base import Dialect, Driver

class MssqlDialect(Dialect):
    name = "mssql"
    def qident(self, ident: str) -> str: return f'[{ident.replace("]", "]]")}]'
    def qualify(self, schema: str, table: str) -> str: return f'{self.qident(schema)}.{self.qident(table)}'
    def wrap_sql_source(self, sql: str) -> str: return f"({sql}) AS _src"
    def limit_clause(self, n: int) -> str: return f"OFFSET 0 ROWS FETCH NEXT {int(n)} ROWS ONLY"  # SQL Server 2012+
    def offset_clause(self, n: int) -> str: return f"OFFSET {int(n)} ROWS"
    def order_by(self, order_by: Optional[str]) -> str: return f"ORDER BY {order_by}" if order_by else "ORDER BY (SELECT 1)"

    def column_datatype_query(self) -> str:
        return """
        SELECT c.name AS column_name, t.name AS data_type
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        JOIN sys.tables tb ON tb.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = tb.schema_id
        WHERE s.name = ? AND tb.name = ?
        """

    def column_list_query(self) -> str:
        return """
        SELECT c.name
        FROM sys.columns c
        JOIN sys.tables tb ON tb.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = tb.schema_id
        WHERE s.name = ? AND tb.name = ?
        ORDER BY c.column_id
        """

    def min_max_sql(self, schema, table, column, where):
        w = []
        if where and where.strip(): w.append(f"({where})")
        w.append(f'{self.qident(column)} IS NOT NULL')
        wsql = "WHERE " + " AND ".join(w)
        return f"SELECT MIN({self.qident(column)}), MAX({self.qident(column)}) FROM {self.qualify(schema,table)} {wsql}"

    def distinct_sql(self, schema, table, column, where, limit):
        w = []
        if where and where.strip(): w.append(f"({where})")
        w.append(f'{self.qident(column)} IS NOT NULL')
        wsql = "WHERE " + " AND ".join(w)
        top = f"TOP {int(limit)} " if limit else ""
        return f"SELECT {top}DISTINCT {self.qident(column)} FROM {self.qualify(schema,table)} {wsql} ORDER BY {self.qident(column)} DESC"

    def normalize_type(self, db_type: str) -> str: return (db_type or "").lower()

    def literal_for_type(self, v: Any, t: str) -> str:
        t = self.normalize_type(t)
        if v is None: return "NULL"
        if t in {"varchar","nvarchar","char","nchar","text"}: return f"N{str(v)!r}"  # NVARCHAR güvenli
        if t in {"int","bigint","smallint","tinyint","decimal","numeric","float","real"}: return str(v)
        if t in {"date"}: return f"{str(v)!r}"
        if t in {"datetime","datetime2","smalldatetime"}: return f"{str(v)!r}"
        return f"{str(v)!r}"

    def pred_eq(self, s,t,c,v,ct):  return f'{self.qident(c)} = {self.literal_for_type(v, ct)}'
    def pred_ge_lt(self,s,t,c,lo,hi,ct): return f'{self.qident(c)} >= {self.literal_for_type(lo, ct)} AND {self.qident(c)} < {self.literal_for_type(hi, ct)}'
    def pred_ge_le(self,s,t,c,lo,hi,ct): return f'{self.qident(c)} >= {self.literal_for_type(lo, ct)} AND {self.qident(c)} <= {self.literal_for_type(hi, ct)}'

    def ntile_range_sql(self, schema, table, column, parts, where):
        # SQL Server destekli
        w = f"WHERE {where} AND {self.qident(column)} IS NOT NULL" if (where and where.strip()) else f"WHERE {self.qident(column)} IS NOT NULL"
        qt = self.qualify(schema, table); c = self.qident(column)
        return f"""
            WITH t AS (
              SELECT {c}, NTILE({int(parts)}) OVER (ORDER BY {c}) AS bucket
              FROM {qt} {w}
            )
            SELECT bucket, MIN({c}) AS lo, MAX({c}) AS hi
            FROM t GROUP BY bucket ORDER BY bucket
        """

    def mod_expr(self, column: str, parts: int, remainder: int) -> str:
        return f"({self.qident(column)} % {int(parts)}) = {int(remainder)}"


class MssqlDriver(Driver):
    dialect: Dialect = MssqlDialect()

    def connect(self, conf: Dict[str,Any]):
        # örnek: DRIVER={ODBC Driver 18 for SQL Server};SERVER=host,1433;DATABASE=db;UID=...;PWD=...
        return pyodbc.connect(conf["dsn"], autocommit=False)

    def close(self, conn): conn.close()

    def fetchall(self, conn, sql, params=None):
        cur = conn.cursor()
        cur.execute(sql, params or ())
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        cur.close()
        return rows, cols

    def server_cursor_iter(self, conn, sql: str, arraysize: int):
        cur = conn.cursor()
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(arraysize)
            if not rows: break
            dicts = [dict(zip([c[0] for c in cur.description], r)) for r in rows]
            yield dicts
        cur.close()

    def copy_to_filelike(self, conn, inner_select_sql: str, file_obj, *, binary: bool):
        # İlk aşamada CSV fallback (binary yok)
        cur = conn.cursor()
        cur.execute(inner_select_sql)
        cols = [c[0] for c in cur.description]
        # başlık yok, NULL = \N
        for row in cur:
            line = ",".join("\\N" if v is None else str(v) for v in row) + "\n"
            file_obj.write(line.encode() if binary else line)  # binary istenirse utf-8 bytes
        cur.close()

    def get_column_types(self, conn, schema: str, table: str) -> Dict[str,str]:
        sql = self.dialect.column_datatype_query()
        cur = conn.cursor()
        cur.execute(sql, (schema, table))
        m = {r[0]: r[1] for r in cur.fetchall()}
        cur.close()
        return m
