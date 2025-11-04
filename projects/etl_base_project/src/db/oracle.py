# projects/etl_base_project/src/db/oracle.py
from __future__ import annotations
import oracledb
from typing import Any, Dict, Iterable, List, Optional, Tuple
from .base import Dialect, Driver

class OracleDialect(Dialect):
    name = "oracle"
    def qident(self, ident: str) -> str: return f'"{ident.upper()}"'  # Oracle case-insensitive ama gelen isimleri büyük harf yap
    def qualify(self, schema: str, table: str) -> str: return f'{self.qident(schema)}.{self.qident(table)}'
    def wrap_sql_source(self, sql: str) -> str: return f"({sql}) _SRC"
    def limit_clause(self, n: int) -> str: return f"FETCH FIRST {int(n)} ROWS ONLY"
    def offset_clause(self, n: int) -> str: return f"OFFSET {int(n)} ROWS"
    def order_by(self, order_by: Optional[str]) -> str: return f"ORDER BY {order_by}" if order_by else ""

    def column_datatype_query(self) -> str:
        return """
        SELECT column_name, data_type
        FROM all_tab_columns
        WHERE owner = :1 AND table_name = :2
        """

    def column_list_query(self) -> str:
        return """
        SELECT column_name
        FROM all_tab_columns
        WHERE owner = :1 AND table_name = :2
        ORDER BY column_id
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
        lim = f" FETCH FIRST {int(limit)} ROWS ONLY" if limit else ""
        return f"SELECT DISTINCT {self.qident(column)} FROM {self.qualify(schema,table)} {wsql} ORDER BY {self.qident(column)} DESC{lim}"

    def normalize_type(self, db_type: str) -> str: return (db_type or "").lower()

    def literal_for_type(self, v: Any, t: str) -> str:
        if v is None: return "NULL"
        t = self.normalize_type(t)
        if "char" in t or "clob" in t: return f"{str(v)!r}"
        if "number" in t or t in {"integer","float"}: return str(v)
        if "date" == t: return f"DATE '{str(v)}'" if len(str(v))==10 else f"TO_DATE({str(v)!r}, 'YYYYMMDD')"
        if "timestamp" in t: return f"TO_TIMESTAMP({str(v)!r}, 'YYYY-MM-DD HH24:MI:SS.FF')"
        return f"{str(v)!r}"

    def pred_eq(self,s,t,c,v,ct):  return f'{self.qident(c)} = {self.literal_for_type(v, ct)}'
    def pred_ge_lt(self,s,t,c,lo,hi,ct): return f'{self.qident(c)} >= {self.literal_for_type(lo, ct)} AND {self.qident(c)} < {self.literal_for_type(hi, ct)}'
    def pred_ge_le(self,s,t,c,lo,hi,ct): return f'{self.qident(c)} >= {self.literal_for_type(lo, ct)} AND {self.qident(c)} <= {self.literal_for_type(hi, ct)}'

    def ntile_range_sql(self, schema, table, column, parts, where):
        w = f"WHERE {where} AND {self.qident(column)} IS NOT NULL" if (where and where.strip()) else f"WHERE {self.qident(column)} IS NOT NULL"
        qt = self.qualify(schema, table); c = self.qident(column)
        return f"""
            SELECT bucket, MIN({c}) lo, MAX({c}) hi FROM (
              SELECT {c}, NTILE({int(parts)}) OVER (ORDER BY {c}) AS bucket
              FROM {qt} {w}
            ) t
            GROUP BY bucket
            ORDER BY bucket
        """

    def mod_expr(self, column: str, parts: int, remainder: int) -> str:
        return f"MOD({self.qident(column)}, {int(parts)}) = {int(remainder)}"


class OracleDriver(Driver):
    dialect: Dialect = OracleDialect()

    def connect(self, conf: Dict[str,Any]):
        # örn: user/pass@host:1521/service
        return oracledb.connect(conf["dsn"])

    def close(self, conn): conn.close()

    def fetchall(self, conn, sql: str, params=None):
        cur = conn.cursor()
        cur.execute(sql, params or ())
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
        return rows, cols

    def server_cursor_iter(self, conn, sql: str, arraysize: int):
        cur = conn.cursor()
        cur.arraysize = arraysize
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(arraysize)
            if not rows: break
            dicts = [dict(zip([c[0] for c in cur.description], r)) for r in rows]
            yield dicts
        cur.close()

    def copy_to_filelike(self, conn, inner_select_sql: str, file_obj, *, binary: bool):
        # CSV fallback
        cur = conn.cursor()
        cur.execute(inner_select_sql)
        for row in cur:
            line = ",".join("\\N" if v is None else str(v) for v in row) + "\n"
            file_obj.write(line.encode() if binary else line)
        cur.close()

    def get_column_types(self, conn, schema: str, table: str) -> Dict[str,str]:
        sql = self.dialect.column_datatype_query()
        cur = conn.cursor()
        cur.execute(sql, (schema.upper(), table.upper()))
        m = {r[0].lower(): r[1].lower() for r in cur.fetchall()}
        cur.close()
        return m
