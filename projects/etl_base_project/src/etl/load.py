from __future__ import annotations
import io
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import psycopg2.extras

from psycopg2.extensions import connection as _Connection
from projects.etl_base_project.src.common.logger import logger


def q_ident(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


class Loader:
    def __init__(self, conn: _Connection, column_metadata: Dict[str, str], **kwargs) -> None:
        self.conn = conn
        self.column_metadata = column_metadata
        self.target_schema = kwargs.get("target_schema")
        self.target_table  = kwargs.get("target_table")
        self.load_method   = kwargs.get("load_method")
        self._is_prepared = False

    @staticmethod
    def _qident(ident: str) -> str:
        return '"' + str(ident).replace('"', '""') + '"'

    @classmethod
    def _qual(cls, schema: str, table: str) -> str:
        return f"{cls._qident(schema)}.{cls._qident(table)}"
    
    # --- DataFrame yolu (gerekirse) ---
    def run(self, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return
        if not self._is_prepared:
            self.prepare(df)
            self._is_prepared = True
        self.insert_data(df)

    def prepare(self, df: pd.DataFrame) -> None:
        
        """
        Performans için minimum kontrol:
        - create_if_not_exists_or_truncate: CREATE IF NOT EXISTS + TRUNCATE
        - create_if_not_exists            : CREATE IF NOT EXISTS
        - drop_and_create / truncate_table / delete_from_table destekli
        """
        logger.info(f"Warn prepare başladı ")
        method = self.load_method
        logger.info(f"Warn prepare 1.1 self.load_method= {self.load_method}")
        if method == "drop_and_create":
            with self.conn.cursor() as cur:
                cur.execute(f"DROP TABLE {q_ident(self.target_schema, self.target_table)}")
                cur.execute(self.__generate_create_table_sql(df))
            self.conn.commit()
            logger.info(f"Table {self.target_table} dropped and recreated.")
            return
        
        if method == "drop_if_exists_and_create":
            with self.conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {q_ident(self.target_schema, self.target_table)}")
                cur.execute(self.__generate_create_table_sql(df))
            self.conn.commit()
            logger.info(f"Table {self.target_table} dropped and recreated.")
            return

        if method in ("create_if_not_exists_or_truncate", "create_if_not_exists"):
            with self.conn.cursor() as cur:
                cur.execute(self.__generate_create_table_sql(df))  # IF NOT EXISTS
                if method == "create_if_not_exists_or_truncate":
                    cur.execute(f"TRUNCATE TABLE {q_ident(self.target_schema, self.target_table)}")
            self.conn.commit()
            if method == "create_if_not_exists_or_truncate":
                logger.info(f"Table {self.target_table} ensured & truncated.")
            else:
                logger.info(f"Table {self.target_table} ensured (if not exists).")
            return

        if method == "truncate_table":
            with self.conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {q_ident(self.target_schema, self.target_table)}")
            self.conn.commit()
            logger.info(f"Table {self.target_table} truncated.")
            return

        if method == "delete_from_table":
            with self.conn.cursor() as cur:
                cur.execute(f"DELETE FROM {q_ident(self.target_schema, self.target_table)}")
            self.conn.commit()
            logger.info(f"All records in {self.target_table} deleted.")
            return
        logger.info(f"Warn prepare bitti ")

    def insert_data(self, df: pd.DataFrame, page_size: int = 20_000) -> None:
        cols = list(df.columns)
        col_sql = ", ".join(f'"{c}"' for c in cols)
        sql = f"INSERT INTO {q_ident(self.target_schema, self.target_table)} ({col_sql}) VALUES %s"
        it = df.itertuples(index=False, name=None)
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, it, page_size=page_size)
        self.conn.commit()
        logger.info(f"{len(df)} rows inserted into {self.target_table}.")

    # --- COPY (rows → TEXT STDIN) explicit schema/tablo ile (copy_expert) ---
    def copy_from_rows_expert(
        self,
        rows: List[Dict[str, Any]],
        *,
        columns: Sequence[str],
        target_schema: str,
        target_table: str,
        sep: str = "\t",
        null: str = r"\N",
        newline: str = "\n",
    ) -> int:
        if not rows:
            return 0
        buf = io.StringIO()
        write = buf.write
        join = sep.join
        _TRANSLATE = str.maketrans({"\t": " ", "\n": " ", "\r": " "})
        for r in rows:
            parts = []
            for c in columns:
                v = r.get(c) if isinstance(r, dict) else r[columns.index(c)]
                parts.append(null if v is None else str(v).translate(_TRANSLATE))
            write(join(parts)); write(newline)
        buf.seek(0)

        col_sql = ", ".join(f'"{c}"' for c in columns)
        sql = (
            f'COPY "{target_schema}"."{target_table}" ({col_sql}) '
            "FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
        )
        with self.conn.cursor() as cur:
            cur.copy_expert(sql, buf)
        # commit ETLManager içinde periyodik/sonda
        return len(rows)
    
    def _set_fast_session_gucs(self, cur) -> None:
        # COPY süresince session ayarları (sadece oturum scope)
        try:
            #print
            cur.execute("SET LOCAL synchronous_commit = off") 
            cur.execute("SET work_mem = '512MB'") 
            cur.execute("SET maintenance_work_mem = '256MB'") 
        except Exception as e: 
            logger.warning(f"GUC set failed (eski Postgres?): {e}")

    
    def copy_from_filelike(
        self,
        file_obj,
        columns: Sequence[str],
        *,
        fmt: str = "binary",          # "binary" | "csv"
        null: str = r"\N",
    ) -> None:
        """
        file_obj (seek(0) yapılmış) üzerinden COPY FROM STDIN.
        fmt="binary" → PostgreSQL'e binary protokolü, en hızlı yol.
        """
        cols_sql = ", ".join(f'"{c}"' for c in columns)
        qual_tgt = f'"{self.target_schema}"."{self.target_table}"'

        if fmt == "binary":
            copy_sql = f'COPY {qual_tgt} ({cols_sql}) FROM STDIN WITH (FORMAT binary)'
            # file_obj: binary mode ("r+b" / "w+b")
        else:
            copy_sql = f'COPY {qual_tgt} ({cols_sql}) FROM STDIN WITH (FORMAT csv, NULL \'{null}\')'
            # file_obj: text ya da binary fark etmez

        with self.conn.cursor() as cur:
            # Şemanın doğru hedeflenmesi için (copy_from kısa isim davranışına takılmayalım)
            cur.execute(f'SET LOCAL search_path TO "{self.target_schema}"')
            cur.copy_expert(copy_sql, file_obj)
        self.conn.commit()
        logger.info(f"[COPY] {self.target_schema}.{self.target_table} ← filelike ({fmt})") 


    # --- DDL üretimi (CREATE TABLE IF NOT EXISTS) ---
    def __generate_create_table_sql(self, df: pd.DataFrame) -> str:
        cols_sql = []
        for col in df.columns:
            col_name = f'"{col}"'
            meta = self.column_metadata.get(col, {})
            data_type = (meta.get("data_type") or "TEXT")
            dt = str(data_type).lower()

            if dt in ("character varying", "varchar"):
                char_length = meta.get("char_length")
                pg_type = f"VARCHAR({int(char_length)})" if char_length and not pd.isna(char_length) else "TEXT"
            elif dt == "text":
                pg_type = "TEXT"
            elif dt == "numeric":
                precision = meta.get("num_precision")
                scale = meta.get("num_scale", 0)
                pg_type = f"NUMERIC({precision}, {scale})" if precision else "NUMERIC"
            elif dt in ("timestamp", "timestamp without time zone"):
                pg_type = "TIMESTAMP"
            elif dt in ("timestamp with time zone", "timestamptz"):
                pg_type = "TIMESTAMPTZ"
            elif dt in ("integer", "int4"):
                pg_type = "INTEGER"
            elif dt in ("bigint", "int8"):
                pg_type = "BIGINT"
            elif dt in ("boolean", "bool"):
                pg_type = "BOOLEAN"
            else:
                pg_type = str(data_type).upper()

            not_null = "NOT NULL" if meta.get("is_nullable") == "NO" else ""
            default_val = meta.get("default")
            default = f"DEFAULT {default_val}" if default_val else ""
            cols_sql.append(f"{col_name} {pg_type} {not_null} {default}".strip())

        return f"CREATE TABLE IF NOT EXISTS {q_ident(self.target_schema, self.target_table)} ({', '.join(cols_sql)});"

    def copy_from_filelike_binary(
        self,
        file_obj,                      # binary mode'da: read() verecek
        columns: Sequence[str],
        set_search_path: bool = True,
        statement_timeout_ms: Optional[int] = None,
    ) -> int:
        """
        Hedef DB'ye BINARY COPY FROM STDIN.
        Python tarafında neredeyse sıfır CPU.
        """
        qual  = self._qual(self.target_schema, self.target_table)
        cols_sql = ", ".join(self._qident(c) for c in columns)
        sql = f"COPY {qual} ({cols_sql}) FROM STDIN WITH (FORMAT binary)"

        with self.conn.cursor() as cur:
            self._set_fast_session_gucs(cur) 
            if set_search_path:
                # sizin ortamda hızlı çözüm olmuştu:
                cur.execute(f'SET LOCAL search_path TO {self._qident(self.target_schema)}, public')
            if statement_timeout_ms:
                cur.execute(f"SET LOCAL statement_timeout = {int(statement_timeout_ms)}")

            cur.copy_expert(sql, file_obj)  # psycopg2 read() çağırır

        # autocommit kapalıysa commit'i dışarıda kontrollü atmak da mümkün
        self.conn.commit()
        logger.info("[passthrough-binary] COPY FROM STDIN → %s tamam.", qual)
        # Satır sayısını COPY binary’den alamıyoruz; bilgiyi upstream logluyoruz.
        return -1
