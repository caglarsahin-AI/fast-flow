# extract.py
import math
import pandas as pd
import psycopg2.extras
from typing import Iterator, List, Dict, Any, Tuple
from psycopg2.extensions import connection as _Connection
from common.logger import logger
from common.helpers import Helpers

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
