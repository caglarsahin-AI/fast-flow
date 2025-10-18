import pandas as pd
import psycopg2.extras
from io import StringIO
from typing import Any, Dict, List
from common.logger import logger
from psycopg2.extensions import connection as _Connection

class Loader:
    def __init__(self, conn: _Connection, column_metadata: Dict[str, str], **kwargs) -> None:
        self.conn = conn
        self.column_metadata = column_metadata
        self.target_schema = kwargs.get("target_schema")
        self.target_table = kwargs.get("target_table")
        self.load_method = kwargs.get("load_method")
        self.unique_key_column = kwargs.get("unique_key_column")
        self.date_column = kwargs.get("date_column")
        self.date_threshold = kwargs.get("date_threshold")
        self.bulk_method = kwargs.get("bulk_method", "copy")  
        self._is_prepared = False

    def run(self, df: pd.DataFrame) -> None:
        """Executes prepare on first call, then bulk inserts each chunk."""
        self.__update_column_metadata(df)
        
        if not self._is_prepared:
            self.prepare(df)
            self._is_prepared = True
        self.insert_data(df)
    
    def prepare(self, df: pd.DataFrame) -> None:
        """Handles initial table prep (create/truncate/delete) based on load_method."""
        method_map = {
            'drop_and_create': self.__drop_and_create,
            'create_if_not_exists_or_truncate': self.__create_if_not_exists_or_truncate,
            'create_if_not_exists': self.__create_if_not_exists,
            'truncate_table': lambda _: self.__truncate_table(),
            'delete_from_table': lambda _: self.__delete_from_table(),
        }
        prep = method_map.get(self.load_method)
        if prep:
            prep(df)
        else:
            logger.debug(f"No prep action for load_method={self.load_method}")

    def __drop_and_create(self, df: pd.DataFrame) -> None:
        """Drops and recreates the table, then inserts data."""
        with self.conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {self.target_schema}.{self.target_table}")
            cur.execute(self.__generate_create_table_sql(df))
            logger.info(f"Table {self.target_table} dropped and recreated.")
        self.conn.commit()

    def __create_if_not_exists_or_truncate(self, df: pd.DataFrame) -> None:
        """Creates the table if not exists, otherwise truncates it, then inserts data.""" 
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = '{self.target_schema}' 
                    AND table_name = '{self.target_table}'
                )
            """)
            exists = cur.fetchone()[0]
            if exists:
                cur.execute(f"TRUNCATE TABLE {self.target_schema}.{self.target_table}")
                logger.info(f"Table {self.target_table} truncated.")
            else:
                cur.execute(self.__generate_create_table_sql(df))
                logger.info(f"Table {self.target_table} created.")
        self.conn.commit()

    def __create_if_not_exists(self, df: pd.DataFrame) -> None:
        """Creates the table if it does not exist, otherwise inserts data."""
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = '{self.target_schema}' 
                    AND table_name = '{self.target_table}'
                )
            """)
            exists = cur.fetchone()[0]
            if not exists:
                cur.execute(self.__generate_create_table_sql(df))
                logger.info(f"Table {self.target_table} created.")
            else:
                logger.info(f"Table {self.target_table} already exists. No action taken.")
        self.conn.commit()

    def __truncate_table(self) -> None:
        """Truncates the table, keeping its structure."""
        with self.conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {self.target_schema}.{self.target_table}")
            logger.info(f"Table {self.target_table} truncated.")
        self.conn.commit()

    def __delete_from_table(self, df: pd.DataFrame) -> None:
        """Deletes all records and inserts new data."""
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {self.target_schema}.{self.target_table}")
            logger.info(f"All records in {self.target_table} deleted.")
        self.conn.commit()

    def insert_data(self, df: pd.DataFrame) -> None:
        """Inserts data into the target table with COPY (default) or execute_values."""
        if df is None or df.empty:
            logger.info(f"No rows to insert into {self.target_table}. Skipping.")
            return

        for col in df.columns:
            if (pd.api.types.is_float_dtype(df[col])
                and df[col].dropna().astype(int).eq(df[col].dropna()).all()):
                df[col] = df[col].astype('Int64')

        if self.bulk_method == "copy":
            self.__copy_insert(df)
        else:
            self.__values_insert(df)

    def __copy_insert(self, df: pd.DataFrame) -> None:
        """Fast path: COPY FROM STDIN WITH CSV."""
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep="\\N")
        buffer.seek(0)

        cols_sql = ", ".join(f'"{c}"' for c in df.columns)  # mixed-case güvenli
        sql = (
            f'COPY {self.target_schema}.{self.target_table} ({cols_sql}) '
            "FROM STDIN WITH (FORMAT csv, NULL '\\N')"
        )

        with self.conn.cursor() as cur:
            cur.copy_expert(sql, buffer)
        self.conn.commit()
        logger.info(f"{len(df)} rows inserted into {self.target_table}.")

    def __values_insert(self, df: pd.DataFrame) -> None:
        """Fallback: psycopg2.extras.execute_values."""
        columns = ", ".join(f'"{c}"' for c in df.columns)
        query = f"INSERT INTO {self.target_schema}.{self.target_table} ({columns}) VALUES %s"
        data_tuples = list(df.itertuples(index=False, name=None))
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, query, data_tuples)
        self.conn.commit()
        logger.info(f"{len(df)} rows inserted (execute_values) into {self.target_table}.")

    def __generate_create_table_sql(self, df: pd.DataFrame) -> str:
        """Source ve transform sonrası kolon metadata bilgilerine göre CREATE TABLE SQL üretir."""

        columns = []

        for col in df.columns:
            col_name = f'"{col}"'  
            col_meta = self.column_metadata.get(col, {})

            data_type = col_meta.get("data_type", "TEXT")
            data_type_lower = data_type.lower()

            if data_type_lower in ("character varying", "varchar"):
                char_length = col_meta.get("char_length")
                if char_length and not pd.isna(char_length):
                    pg_type = f"VARCHAR({int(char_length)})"
                else:
                    pg_type = "TEXT"  # Eğer uzunluk belirsizse, güvenli yol TEXT
            elif data_type_lower == "text":
                pg_type = "TEXT"
            elif data_type_lower == "numeric":
                precision = col_meta.get("num_precision")
                scale = col_meta.get("num_scale", 0)
                if precision:
                    pg_type = f"NUMERIC({precision}, {scale})"
                else:
                    pg_type = "NUMERIC"
            else:
                pg_type = data_type.upper()

            not_null = "NOT NULL" if col_meta.get("is_nullable") == "NO" else ""

            default_val = col_meta.get("default")
            default = f"DEFAULT {default_val}" if default_val else ""

            col_def = f"{col_name} {pg_type} {not_null} {default}".strip()
            columns.append(col_def)

        columns_sql = ", ".join(columns)
        create_sql = f'CREATE TABLE IF NOT EXISTS {self.target_schema}.{self.target_table} ({columns_sql});'
        return create_sql

    def __update_column_metadata(self, df: pd.DataFrame) -> None:
        """Source DB'den gelen column_metadata listesine, Transform sırasında eklenen yeni kolonları da ekler."""
        
        for col in df.columns:
            if col not in self.column_metadata:  
                inferred_type = self.__infer_column_type(df[col])  
                self.column_metadata[col] = {
                    "data_type": inferred_type,
                    "is_nullable": "YES", 
                    "default": None 
                }

    def __infer_column_type(self, series: pd.Series) -> str:
        """Verilen pandas Series’e göre PostgreSQL tipi tahmin eder."""
        if pd.api.types.is_integer_dtype(series):
            return "INTEGER"
        elif pd.api.types.is_float_dtype(series):
            return "NUMERIC"
        elif pd.api.types.is_bool_dtype(series):
            return "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(series):
            return "TIMESTAMP"
        else:
            max_len = series.dropna().astype(str).map(len).max()
            if pd.isna(max_len):
                max_len = 100 
            if max_len > 10000:
                return "TEXT"
            else:
                return f"VARCHAR({int(max_len)})"
    
    def copy_from_filelike(self, file_obj, columns: List[str]) -> None:
        """file_obj (binary, seek(0) yapılmış) üzerinden COPY FROM STDIN."""
        cols_sql = ", ".join(f'"{c}"' for c in columns)
        sql = (
            f'COPY {self.target_schema}.{self.target_table} ({cols_sql}) '
            "FROM STDIN WITH (FORMAT csv, NULL '\\N')"
        )
        with self.conn.cursor() as cur:
            cur.copy_expert(sql, file_obj)
        self.conn.commit()
        logger.info(f"COPY FROM filelike → {self.target_table} tamam.")