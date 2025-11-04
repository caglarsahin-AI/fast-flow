# projects/etl_base_project/src/etl/extract.py
from __future__ import annotations
import math
import pandas as pd
import psycopg2.extras
from typing import List, Optional, Iterator, Dict, Any, Tuple, Sequence, IO
from datetime import timedelta, datetime, date

from psycopg2.extensions import connection as _Connection
from projects.etl_base_project.src.common.logger import logger
from projects.etl_base_project.src.common.helpers import Helpers


class Extractor:
    def __init__(self,
                 src_conn: _Connection,
                 tgt_conn: _Connection,                 
                 target_schema: str,
                 target_table: str,
                 *,
                 source_schema: Optional[str],
                 source_table: Optional[str],
                 sql_text: Optional[str]) -> None:
        self.src_conn      = src_conn
        self.tgt_conn      = tgt_conn
        self.source_schema = source_schema
        self.source_table  = source_table
        self.target_schema = target_schema
        self.target_table  = target_table
        self.sql_text      = sql_text

    @staticmethod
    def _qident(ident: str) -> str:
        return '"' + ident.replace('"', '""') + '"'

    @classmethod
    def _qual(cls, schema: str, table: str) -> str:
        return f"{cls._qident(schema)}.{cls._qident(table)}"
    
    # ---- genel ----
    def run(self, **kwargs) -> pd.DataFrame:
        where = (kwargs.get("where") or "").strip()
        order_by = (kwargs.get("order_by") or "").strip()

        if self.sql_text and self.sql_text.strip():
            # SQL kaynağı: alt sorgu olarak sar, WHERE/ORDER dışarıda uygula
            sql = f"SELECT * FROM ({self.sql_text}) AS _src"
            if where:
                sql += f" WHERE {where}"
            if order_by:
                sql += f" ORDER BY {order_by}"
            logger.info(f"[INFO] run(sql_text):\n{sql}")
        else:
            sql = f'SELECT * FROM "{self.source_schema}"."{self.source_table}"'
            if where:
                sql += f" WHERE {where}"
            if order_by:
                sql += f" ORDER BY {order_by}"
            logger.info(f"[INFO] run(table):\n{sql}")

        rows, cols = self.__execute_query(self.src_conn, sql)
        return pd.DataFrame(rows, columns=cols)

    def get_column_metadata(self) -> Dict[str, Dict[str, Any]]:
        sql = """
            SELECT column_name, data_type, character_maximum_length,
                   numeric_precision, numeric_scale, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """
        with self.src_conn.cursor() as cur:
            cur.execute(sql, (self.source_schema, self.source_table))
            return {
                r[0]: {
                    "data_type": r[1],
                    "char_length": r[2],
                    "num_precision": r[3],
                    "num_scale": r[4],
                    "is_nullable": r[5],
                    "default": r[6],
                }
                for r in cur.fetchall()
            }

    def get_column_order(self) -> List[str]:
        sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        with self.src_conn.cursor() as cur:
            cur.execute(sql, (self.source_schema, self.source_table))
            return [r[0] for r in cur.fetchall()]

    # ---- stream (server-side cursor) + optional WHERE ----
    def stream_iter(self, batch_size: int, columns: List[str], where: Optional[str] = None, order_by: Optional[str] = None):
        col_sql = ", ".join(f'"{c}"' for c in columns)
        sql = f'SELECT {col_sql} FROM "{self.source_schema}"."{self.source_table}"'
        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        name = f"csr_{self.source_schema}_{self.source_table}"
        with self.src_conn.cursor(name=name, cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.itersize = batch_size
            cur.arraysize = batch_size
            logger.info("[stream_iter] SQL:\n%s", sql)
            cur.execute(sql)
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                yield [dict(r) for r in rows]

    # ---- gün bazlı ----
    def _get_sample_value(self, date_column: str) -> str:
        sql = f"""
            SELECT {date_column}
            FROM "{self.source_schema}"."{self.source_table}"
            WHERE {date_column} IS NOT NULL
            LIMIT 1
        """
        with self.src_conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
        return str(row[0]) if row else ""

    def _to_midnight_dt(self, v) -> datetime:
        if isinstance(v, datetime):
            return datetime(v.year, v.month, v.day)
        if isinstance(v, date):
            return datetime(v.year, v.month, v.day)
        return Helpers.parse_date_threshold(str(v))
    
    # 1) Günlük iterator: extra_where + order_by destekli
    def iter_by_day(
        self,
        date_column: str,
        date_start: str,
        date_end: str,
        inner_page_size: Optional[int] = None,
        *,
        extra_where: Optional[str] = None,
        order_by: Optional[str] = None,
    ):

        dt_start = Helpers.parse_date_threshold(date_start)
        dt_end   = Helpers.parse_date_threshold(date_end)

        sample_val = self._get_sample_value(date_column) or ""
        col_len    = Helpers.infer_column_length(sample_val)

        cur = dt_start
        while cur < dt_end:
            nxt = cur + timedelta(days=1)
            start_int = Helpers.convert_datetime_to_numeric(cur, col_len)
            end_int   = Helpers.convert_datetime_to_numeric(nxt, col_len)

            # gün aralığı where
            parts = [f"{date_column} >= {start_int}", f"{date_column} < {end_int}"]
            if extra_where and extra_where.strip():
                parts.append(f"({extra_where.strip()})")
            where_sql = " AND ".join(parts)

            # toplam say
            count_sql = f'''
                SELECT COUNT(*)
                FROM "{self.source_schema}"."{self.source_table}"
                WHERE {where_sql}
            '''
            with self.src_conn.cursor() as c:
                c.execute(count_sql)
                total = c.fetchone()[0]

            if total == 0:
                yield (cur.date(), pd.DataFrame(columns=[]))
                cur = nxt
                continue

            if inner_page_size and total > inner_page_size:
                pages = math.ceil(total / inner_page_size)
                for i in range(pages):
                    offset = i * inner_page_size
                    page_sql = f'''
                        SELECT *
                        FROM "{self.source_schema}"."{self.source_table}"
                        WHERE {where_sql}
                        {'ORDER BY ' + order_by if order_by else f'ORDER BY {date_column} ASC'}
                        LIMIT {inner_page_size} OFFSET {offset}
                    '''
                    rows, cols = self.__execute_query(self.src_conn, page_sql)
                    yield (cur.date(), pd.DataFrame(rows, columns=cols))
            else:
                day_sql = f'''
                    SELECT *
                    FROM "{self.source_schema}"."{self.source_table}"
                    WHERE {where_sql}
                    {'ORDER BY ' + order_by if order_by else f'ORDER BY {date_column} ASC'}
                '''
                rows, cols = self.__execute_query(self.src_conn, day_sql)
                yield (cur.date(), pd.DataFrame(rows, columns=cols))

            cur = nxt

    # 4) COPY (passthrough) için günlük export: extra_where + order_by
    def copy_day_to_filelike(
        self,
        date_column: str,
        day: datetime,
        columns: List[str],
        file_obj,
        key_column: Optional[str] = None,
        *,
        extra_where: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> None:
        from projects.etl_base_project.src.common.helpers import Helpers
        from datetime import timedelta

        sample_val = self._get_sample_value(date_column) or ""
        col_len    = Helpers.infer_column_length(sample_val)
        start_int  = Helpers.convert_datetime_to_numeric(day, col_len)
        end_int    = Helpers.convert_datetime_to_numeric(day + timedelta(days=1), col_len)

        col_sql = ", ".join(f'"{c}"' for c in columns)
        order   = order_by if (order_by and order_by.strip()) else (key_column or date_column)

        base_where = f"{date_column} >= {start_int} AND {date_column} < {end_int}"
        where_sql = base_where if not (extra_where and extra_where.strip()) else f"{base_where} AND ({extra_where.strip()})"

        sql = f"""
            COPY (
                SELECT {col_sql}
                FROM "{self.source_schema}"."{self.source_table}"
                WHERE {where_sql}
                ORDER BY {order} ASC
            ) TO STDOUT WITH (FORMAT csv, NULL '\\N')
        """
        with self.src_conn.cursor() as cur:
            cur.copy_expert(sql, file_obj)

    # 2) Gün verisini tek sorguda alan fonksiyon: extra_where + order_by
    def get_day_data(self, date_column, day: datetime, *, extra_where: Optional[str] = None, order_by: Optional[str] = None):
        from projects.etl_base_project.src.common.helpers import Helpers
        from datetime import timedelta

        sample_val = self._get_sample_value(date_column) or ""
        col_len = Helpers.infer_column_length(sample_val)
        start_int = Helpers.convert_datetime_to_numeric(day, col_len)
        end_int   = Helpers.convert_datetime_to_numeric(day + timedelta(days=1), col_len)

        clauses = [f"{date_column} >= {start_int}", f"{date_column} < {end_int}"]
        if extra_where and extra_where.strip():
            clauses.append(f"({extra_where.strip()})")
        where_sql = " AND ".join(clauses)

        sql = f'''
            SELECT *
            FROM "{self.source_schema}"."{self.source_table}"
            WHERE {where_sql}
            {'ORDER BY ' + order_by if order_by else f'ORDER BY {date_column} ASC'}
        '''
        rows, cols = self.__execute_query(self.src_conn, sql)
        return pd.DataFrame(rows, columns=cols)

    # 3) Günlük keyset paging: extra_where destekli (order_by = key_column)
    def iter_day_keyset(self, date_column: str, key_column: str, day: datetime, page_size: int, *, extra_where: Optional[str] = None):
        from projects.etl_base_project.src.common.helpers import Helpers
        from datetime import timedelta
        import psycopg2.extras

        sample_val = self._get_sample_value(date_column) or ""
        col_len = Helpers.infer_column_length(sample_val)

        start_int = Helpers.convert_datetime_to_numeric(day, col_len)
        end_int   = Helpers.convert_datetime_to_numeric(day + timedelta(days=1), col_len)

        last_key = None
        while True:
            base_where = f"{date_column} >= %s AND {date_column} < %s"
            extra = f" AND ({extra_where.strip()})" if extra_where and extra_where.strip() else ""
            if last_key is None:
                sql = f"""
                    SELECT *
                    FROM "{self.source_schema}"."{self.source_table}"
                    WHERE {base_where}{extra}
                    ORDER BY {key_column} ASC
                    LIMIT %s
                """
                params = (start_int, end_int, page_size)
            else:
                sql = f"""
                    SELECT *
                    FROM "{self.source_schema}"."{self.source_table}"
                    WHERE {base_where}{extra} AND {key_column} > %s
                    ORDER BY {key_column} ASC
                    LIMIT %s
                """
                params = (start_int, end_int, last_key, page_size)

            with self.src_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                if not rows:
                    break
                cols = [d[0] for d in cur.description]

            df = pd.DataFrame(rows, columns=cols)
            last_key = df[key_column].iloc[-1]
            yield df


    def get_incremental_range(self, date_column: str):
        today = datetime.utcnow().date()
        t_minus_1 = datetime(today.year, today.month, today.day) - timedelta(days=1)
        with self.tgt_conn.cursor() as cur:
            cur.execute(f'SELECT MAX({date_column}) FROM "{self.target_schema}"."{self.target_table}"')
            max_val = cur.fetchone()[0]
        if max_val is None:
            return None, (t_minus_1 + timedelta(days=1))
        start = self._to_midnight_dt(max_val) + timedelta(days=1)
        end = t_minus_1 + timedelta(days=1)
        return start, end

    # ---- low-level ----
    def __execute_query(self, conn: _Connection, query: str):
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
            return rows, cols
        except Exception as e:
            logger.error(f"Error executing query:\n{query}\n→ {e}")
            raise
    
    def _qual(self, schema: str, table: str) -> str:
        return f'"{schema}"."{table}"'    

    def copy_select_to_filelike(
        self,
        columns: List[str] | None,
        file_obj,
        *,
        where: Optional[str] = None,
        order_by: Optional[str] = None,
        fmt: str = "binary",
        null: str = r"\N",
        select_items: List[str] | None = None,
    ) -> None:
        """
        Kaynaktan SELECT'i COPY TO STDOUT ile file_obj'a döker.
        - self.sql_text varsa: FROM (<sql_text>) AS _src
        - yoksa: FROM "schema"."table"
        """
        # 1) FROM kaynağı
        if self.sql_text and self.sql_text.strip():
            from_clause = f"({self.sql_text}) AS _src"
            quote_cols = False  # alt sorguda alias'lar zaten uygun isimlerle gelir
        else:
            if not self.source_schema or not self.source_table:
                raise ValueError("Extractor: table kaynağı için source_schema/source_table zorunlu.")
            from_clause = self._qual(self.source_schema, self.source_table)
            quote_cols = True

        # 2) SELECT listesi
        if select_items and len(select_items) > 0:
            select_sql = ", ".join(select_items)
        else:
            assert columns is not None and len(columns) > 0, "columns veya select_items sağlanmalı"
            if quote_cols:
                select_sql = ", ".join(f'"{c}"' for c in columns)
            else:
                select_sql = ", ".join(f"{c}" for c in columns)

        # 3) WHERE / ORDER BY
        where_sql = f"WHERE {where}" if (where and where.strip()) else ""
        order_sql = f"ORDER BY {order_by}" if (order_by and order_by.strip()) else ""

        inner_select = f"""
            SELECT {select_sql}
            FROM {from_clause}
            {where_sql}
            {order_sql}
        """

        # 4) COPY
        if fmt == "binary":
            copy_sql = f"COPY ({inner_select}) TO STDOUT WITH (FORMAT binary)"
        else:
            copy_sql = f"COPY ({inner_select}) TO STDOUT WITH (FORMAT csv, HEADER false, NULL '{null}')"

        logger.info("[passthrough] COPY SELECT:\n%s", inner_select)
        with self.src_conn.cursor() as cur:
            cur.copy_expert(copy_sql, file_obj)
    
    def copy_select_to_filelike_binary(
        self,
        columns: List[str],
        file_obj,
        where: Optional[str] = None,
        order_by: Optional[str] = None,
        statement_timeout_ms: Optional[int] = None,
    ) -> None:
        """
        Kaynak veriyi BINARY olarak STDOUT'a kopyalar.
        self.sql_text varsa alt-sorgu olarak kullanır.
        """
        if self.sql_text and self.sql_text.strip():
            from_clause = f"({self.sql_text}) AS _src"
            cols_sql = ", ".join(f"{c}" for c in columns)  # alt-sorguda çıplak
        else:
            from_clause = self._qual(self.source_schema, self.source_table)
            cols_sql = ", ".join(self._qident(c) for c in columns)

        where_sql = f"WHERE {where}" if (where and where.strip()) else ""
        order_sql = f"ORDER BY {order_by}" if (order_by and order_by.strip()) else ""

        inner = f"""
            SELECT {cols_sql}
            FROM {from_clause}
            {where_sql}
            {order_sql}
        """
        sql = f"COPY ({inner}) TO STDOUT WITH (FORMAT binary)"

        logger.info("[passthrough-binary] COPY TO STDOUT:\n%s", inner)
        with self.src_conn.cursor() as cur:
            if statement_timeout_ms:
                cur.execute(f"SET LOCAL statement_timeout = {int(statement_timeout_ms)}")
            cur.copy_expert(sql, file_obj)