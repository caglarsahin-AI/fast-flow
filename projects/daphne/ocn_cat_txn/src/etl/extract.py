# extract.py
import math
import pandas as pd
import psycopg2.extras
from typing import List, Optional, Iterator, Dict, Any, Tuple
from psycopg2.extensions import connection as _Connection
from common.logger import logger
from common.helpers import Helpers
from datetime import timedelta, datetime, date

class Extractor:
    def __init__(self,
                 src_conn: _Connection,
                 tgt_conn: _Connection,
                 source_schema: str,
                 source_table: str,
                 target_schema: str,
                 target_table: str
                 ) -> None:
        self.src_conn      = src_conn
        self.tgt_conn      = tgt_conn
        self.source_schema = source_schema
        self.source_table  = source_table
        self.target_schema = target_schema
        self.target_table  = target_table

    def run(self, **kwargs) -> pd.DataFrame:
        """
        Tek seferlik çekimler:
         - insert_recent_records
         - insert_between_dates  (ARTIK yarı-açık aralık)
         - veya full table
        """
        load_method   = kwargs.get("load_method")
        date_column   = kwargs.get("date_column")
        date_threshold= kwargs.get("date_threshold")
        date_start    = kwargs.get("date_start")
        date_end      = kwargs.get("date_end")

        # 1) SQL oluştur
        if load_method == "create_if_not_exists":
            # create_table için sadece tablo yapısını alır
            sql = f"""
                SELECT *
                  FROM {self.source_schema}.{self.source_table}
                 WHERE 1=0
            """
        else:
            sql = f"SELECT * FROM {self.source_schema}.{self.source_table}"


        logger.info(f"Executing query: {sql.strip()}")
        rows, cols = self.__execute_query(self.src_conn, sql)

        df = pd.DataFrame(rows, columns=cols)
        logger.info(f"[{self.source_schema}.{self.source_table}] Extracted {len(df)} rows.")
        return df

    def _get_sample_value(self, date_column: str) -> str:
        """
        Date sütunundan bir örnek değer döndürür.
        """
        sample_sql = f"""
            SELECT {date_column}
              FROM {self.source_schema}.{self.source_table}
             WHERE {date_column} IS NOT NULL
             LIMIT 1
        """
        logger.debug(f"Sample SQL: {sample_sql.strip()}")

        with self.src_conn.cursor() as cur:
            cur.execute(sample_sql)
            row = cur.fetchone()

        return str(row[0]) if row else ""

    def get_column_metadata(self) -> Dict[str, Dict[str, Any]]:
        """
        information_schema'dan kolon meta verisini çeker.
        """
        meta_sql = f"""
            SELECT column_name,
                   data_type,
                   character_maximum_length,
                   numeric_precision,
                   numeric_scale,
                   is_nullable,
                   column_default
              FROM information_schema.columns
             WHERE table_schema = '{self.source_schema}'
               AND table_name   = '{self.source_table}'
        """

        with self.src_conn.cursor() as cur:
            cur.execute(meta_sql)
            meta = {
                row[0]: {
                    "data_type":     row[1],
                    "char_length":   row[2],
                    "num_precision": row[3],
                    "num_scale":     row[4],
                    "is_nullable":   row[5],
                    "default":       row[6],
                }
                for row in cur.fetchall()
            }
        return meta

    def get_paged_data(
        self,
        date_column: str,
        date_start: str,
        date_end: str,
        page_size: int
    ) -> Iterator[pd.DataFrame]:
        """
        - date_start == "max_date" ise hedef tablo conn’dan MAX(date_column) alır,
          ve başlangıç filtresini `>` (exclusive) yapar.
        - Aksi halde `>=` kullanır.
        - Bitişi her zaman `< end_int` ile alır.
        """
        # --- 1) max_date mantığı ---
        exclusive = False
        if date_start.lower() == "max_date":
            with self.tgt_conn.cursor() as cur:
                cur.execute(
                    f"SELECT MAX({date_column}) "
                    f"FROM {self.target_schema}.{self.target_table}"
                )
                max_val = cur.fetchone()[0]
            if max_val is not None:
                exclusive  = True
                date_start = str(max_val)
            else:
                date_start = None  # tablo boş → full range

        # --- 2) parse & numeric ---
        dt_start = (Helpers.parse_date_threshold(date_start)
                    if date_start else None)
        dt_end   = Helpers.parse_date_threshold(date_end)

        sample_val = self._get_sample_value(date_column) or ""
        col_len    = Helpers.infer_column_length(sample_val)

        start_int = (Helpers.convert_datetime_to_numeric(dt_start, col_len)
                     if dt_start else None)
        end_int   = Helpers.convert_datetime_to_numeric(dt_end,   col_len)

        # --- 3) WHERE operatörleri ---
        op_start = ">" if exclusive else ">="
        clauses = []
        if start_int is not None:
            clauses.append(f"{date_column} {op_start} {start_int}")
        if end_int is not None:
            clauses.append(f"{date_column} <  {end_int}")
        where_sql = " AND ".join(clauses)

        # --- 4) toplam say ---
        count_sql = f"""
            SELECT COUNT(*) 
              FROM {self.source_schema}.{self.source_table}
             WHERE {where_sql}
        """
        with self.src_conn.cursor() as cur:
            cur.execute(count_sql)
            total = cur.fetchone()[0]
        if total == 0:
            return

        pages = math.ceil(total / page_size)
        logger.info(f"Total {total} rows → {pages} pages of {page_size} each.")

        # --- 5) sayfa döngüsü ---
        for i in range(pages):
            offset = i * page_size
            page_sql = f"""
                SELECT *
                  FROM {self.source_schema}.{self.source_table}
                 WHERE {where_sql}
                 ORDER BY {date_column} ASC
                 LIMIT {page_size} OFFSET {offset}
            """
            logger.info(f"Page {i+1}/{pages}: {page_sql.strip()}")
            rows, cols = self.__execute_query(self.src_conn, page_sql)
            yield pd.DataFrame(rows, columns=cols)

    def get_full_paged_data(
        self,
        page_size: int,
        order_by: str
    ) -> Iterator[pd.DataFrame]:
        # Tam tablo sayfalama (offset-based)
        count_sql = f"SELECT COUNT(*) FROM {self.source_schema}.{self.source_table}"
        with self.src_conn.cursor() as cur:
            cur.execute(count_sql)
            total = cur.fetchone()[0]
        if total == 0:
            return

        pages = math.ceil(total / page_size)
        logger.info(f"Total {total} rows → {pages} pages of {page_size} each.")

        for i in range(pages):
            offset = i * page_size
            page_sql = f"""
                SELECT *
                  FROM {self.source_schema}.{self.source_table}
                 ORDER BY {order_by} ASC
                 LIMIT {page_size} OFFSET {offset}
            """
            logger.info(f"Page {i+1}/{pages}: {page_sql.strip()}")
            rows, cols = self.__execute_query(self.src_conn, page_sql)
            yield pd.DataFrame(rows, columns=cols)

    def _to_midnight_dt(self, v) -> datetime:
        if isinstance(v, datetime):
            return datetime(v.year, v.month, v.day)
        if isinstance(v, date):
            return datetime(v.year, v.month, v.day)
        # Decimal / int / str hepsi stringe çevrilip Helpers ile parse edilir
        return Helpers.parse_date_threshold(str(v))
    
    def get_day_range(self, start_date, end_date):
        dt_start = self._to_midnight_dt(start_date)
        dt_end   = self._to_midnight_dt(end_date)
        cur = dt_start
        while cur < dt_end:
            yield cur
            cur += timedelta(days=1)

    def get_day_data(self, date_column, day: datetime):
        sample_val = self._get_sample_value(date_column) or ""
        col_len = Helpers.infer_column_length(sample_val)
        start_int = Helpers.convert_datetime_to_numeric(day, col_len)
        end_int   = Helpers.convert_datetime_to_numeric(day + timedelta(days=1), col_len)
        sql = f"""
            SELECT *
              FROM {self.source_schema}.{self.source_table}
             WHERE {date_column} >= {start_int} AND {date_column} < {end_int}
             ORDER BY {date_column} ASC
        """
        rows, cols = self.__execute_query(self.src_conn, sql)
        return pd.DataFrame(rows, columns=cols)
    
    def delete_target_day(self, target_schema, target_table, date_column, day: datetime):
        sample_val = self._get_sample_value(date_column) or ""
        col_len = Helpers.infer_column_length(sample_val)
        start_int = Helpers.convert_datetime_to_numeric(day, col_len)
        end_int   = Helpers.convert_datetime_to_numeric(day + timedelta(days=1), col_len)
        with self.tgt_conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {target_schema}.{target_table} "
                f"WHERE {date_column} >= {start_int} AND {date_column} < {end_int}"
            )
        self.tgt_conn.commit()

    def get_incremental_range(self, date_column):
        """
        DWH target'tan MAX(date_column) alır.
        Dönüş: (start_day_inclusive, end_exclusive)
        start = max_date + 1 gün, end = (bugün-1) + 1 gün
        """
        today = datetime.utcnow().date()
        t_minus_1 = datetime(today.year, today.month, today.day) - timedelta(days=1)

        with self.tgt_conn.cursor() as cur:
            cur.execute(f"SELECT MAX({date_column}) FROM {self.target_schema}.{self.target_table}")
            max_val = cur.fetchone()[0]

        if max_val is None:
            return None, (t_minus_1 + timedelta(days=1))

        max_dt = self._to_midnight_dt(max_val)
        start  = max_dt + timedelta(days=1)          
        end    = t_minus_1 + timedelta(days=1)       

        return start, end

    def iter_by_day(
        self,
        date_column: str,
        date_start: str,
        date_end: str,
        inner_page_size: Optional[int] = None,   
    ):
        """
        [date_start, date_end) aralığını GÜN GÜN iterate eder.
        - Tarih kolonu numeric YYYYMMDD ya da benzeri ise Helpers ile numeriğe çevirir.
        - Her gün için [day, day+1) yarı-açık aralığı kullanır.
        - inner_page_size verilirse gün içi paging de yapar.
        """
        # 1) parse to datetime
        dt_start = Helpers.parse_date_threshold(date_start)
        dt_end   = Helpers.parse_date_threshold(date_end)

        # 2) kolon uzunluğu (numeric mi, kaç hane vs) kestir
        sample_val = self._get_sample_value(date_column) or ""
        col_len    = Helpers.infer_column_length(sample_val)

        # 3) gün gün dön
        cur = dt_start
        while cur < dt_end:
            nxt = cur + timedelta(days=1)
            start_int = Helpers.convert_datetime_to_numeric(cur, col_len)
            end_int   = Helpers.convert_datetime_to_numeric(nxt, col_len)

            where_sql = f"{date_column} >= {start_int} AND {date_column} < {end_int}"

            # Gün toplamı
            count_sql = f"""
                SELECT COUNT(*) 
                FROM {self.source_schema}.{self.source_table}
                WHERE {where_sql}
            """
            with self.src_conn.cursor() as c:
                c.execute(count_sql)
                total = c.fetchone()[0]

            if total == 0:
                logger.info(f"[{self.source_schema}.{self.source_table}] {cur.date()} - 0 satır (atlandı)")
                cur = nxt
                continue

            logger.info(f"[{self.source_schema}.{self.source_table}] {cur.date()} - {total} satır")

            if inner_page_size and total > inner_page_size:
                pages = math.ceil(total / inner_page_size)
                for i in range(pages):
                    offset = i * inner_page_size
                    page_sql = f"""
                        SELECT *
                        FROM {self.source_schema}.{self.source_table}
                        WHERE {where_sql}
                        ORDER BY {date_column} ASC
                        LIMIT {inner_page_size} OFFSET {offset}
                    """
                    rows, cols = self.__execute_query(self.src_conn, page_sql)
                    yield (cur.date(), pd.DataFrame(rows, columns=cols))
            else:
                day_sql = f"""
                    SELECT *
                    FROM {self.source_schema}.{self.source_table}
                    WHERE {where_sql}
                    ORDER BY {date_column} ASC
                """
                rows, cols = self.__execute_query(self.src_conn, day_sql)
                yield (cur.date(), pd.DataFrame(rows, columns=cols))

            cur = nxt
    def iter_day_keyset(self,
                    date_column: str,
                    key_column: str,
                    day: datetime,
                    page_size: int):
        sample_val = self._get_sample_value(date_column) or ""
        col_len = Helpers.infer_column_length(sample_val)

        start_int = Helpers.convert_datetime_to_numeric(day, col_len)
        end_int   = Helpers.convert_datetime_to_numeric(day + timedelta(days=1), col_len)

        last_key = None
        while True:
            if last_key is None:
                sql = f"""
                    SELECT *
                    FROM {self.source_schema}.{self.source_table}
                    WHERE {date_column} >= %s AND {date_column} < %s
                    ORDER BY {key_column} ASC
                    LIMIT %s
                """
                params = (start_int, end_int, page_size)
            else:
                sql = f"""
                    SELECT *
                    FROM {self.source_schema}.{self.source_table}
                    WHERE {date_column} >= %s AND {date_column} < %s
                    AND {key_column} > %s
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
    
    def get_column_order(self) -> List[str]:
        sql = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{self.source_schema}'
            AND table_name   = '{self.source_table}'
            ORDER BY ordinal_position
        """
        with self.src_conn.cursor() as cur:
            cur.execute(sql)
            return [r[0] for r in cur.fetchall()]

    def copy_day_to_filelike(
        self,
        date_column: str,
        day: datetime,
        columns: List[str],
        file_obj,                     # binary mode'da, write edilebilir
        key_column: Optional[str] = None,
    ) -> None:
        sample_val = self._get_sample_value(date_column) or ""
        col_len    = Helpers.infer_column_length(sample_val)
        start_int  = Helpers.convert_datetime_to_numeric(day, col_len)
        end_int    = Helpers.convert_datetime_to_numeric(day + timedelta(days=1), col_len)

        cols_sql = ", ".join(f'"{c}"' for c in columns)
        order_by = key_column or date_column

        sql = f"""
            COPY (
                SELECT {cols_sql}
                FROM {self.source_schema}.{self.source_table}
                WHERE {date_column} >= {start_int} AND {date_column} < {end_int}
                ORDER BY {order_by} ASC
            ) TO STDOUT WITH (FORMAT csv, NULL '\\N')
        """
        with self.src_conn.cursor() as cur:
            cur.copy_expert(sql, file_obj)

    def __execute_query(
        self,
        conn: _Connection,
        query: str
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Cursor bloğu içinde fetchall + description alıp
        (rows, cols) olarak döner.
        """
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query)
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
            return rows, cols
        except Exception as e:
            logger.error(f"Error executing query:\n{query}\n→ {e}")
            raise
