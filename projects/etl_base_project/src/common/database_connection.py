# projects/etl_base_project/src/common/database_connection.py
from __future__ import annotations

import io
from typing import Optional, Any, Iterable, Generator, List, Dict, Sequence, Tuple, Union

import psycopg2
import psycopg2.extras

from psycopg2.extensions import connection as _Connection

from .logger import logger


Row = Dict[str, Any]


class DatabaseConnection:
    """
    Backward compatible:
      with DatabaseConnection(**cfg) as conn:
          with conn.cursor() as cur:
              cur.execute("SELECT 1")
              cur.fetchall()

    New helpers:
      - conn.cursor(server_side=True, name="big_cur", itersize=20000)
      - for batch in conn.stream("SELECT * FROM ...", batch_size=20000): ...
      - conn.copy_from_rows(rows, "schema.table", columns=[...])
      - conn.copy_from_file(file_obj, "schema.table", columns=[...])
      - conn.execute(sql, params)
      - conn.query(sql, params) -> List[Row]
      - conn.commit(), conn.rollback()
    """

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        *,
        autocommit: bool = False,
        connect_timeout: int = 30,
        application_name: Optional[str] = "etl_app",
        sslmode: Optional[str] = None,
    ) -> None:
        self.host: str = host
        self.port: int = port
        self.database: str = database
        self.user: str = user
        self.password: str = password
        self.autocommit: bool = autocommit
        self.connect_timeout: int = connect_timeout
        self.application_name: Optional[str] = application_name
        self.sslmode: Optional[str] = sslmode

        self.conn: Optional[_Connection] = None

    @property
    def raw(self) -> _Connection:
        if not self.conn:
            raise RuntimeError("Connection is not established.")
        return self.conn

    # ---- Context manager (backward compatible) ----
    def __enter__(self) -> "DatabaseConnection":   # <— DÖNÜŞ TİPİ wrapper
        try:
            self.conn = self.__create_connection(
                self.host,
                self.port,
                self.database,
                self.user,
                self.password,
                autocommit=self.autocommit,
                connect_timeout=self.connect_timeout,
                application_name=self.application_name,
                sslmode=self.sslmode,
            )
            return self                            # <— ÖNEMLİ: artık wrapper dönüyor
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        try:
            if self.conn:
                if exc_type is not None:
                    # rollback on exception if not autocommit
                    if not self.conn.autocommit:
                        self.conn.rollback()
                self.conn.close()
        finally:
            self.conn = None

    # ---- Connection creation ----
    @staticmethod
    def __create_connection(
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        *,
        autocommit: bool = False,
        connect_timeout: int = 30,
        application_name: Optional[str] = None,
        sslmode: Optional[str] = None,
    ) -> _Connection:
        kwargs = dict(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            connect_timeout=connect_timeout,
            application_name=application_name,
        )
        if sslmode:
            kwargs["sslmode"] = sslmode

        conn: _Connection = psycopg2.connect(**kwargs)  # type: ignore[arg-type]
        conn.autocommit = autocommit
        return conn

    # ---- Basic helpers (optional) ----
    def commit(self) -> None:
        if self.conn and not self.conn.autocommit:
            self.conn.commit()

    def rollback(self) -> None:
        if self.conn and not self.conn.closed:
            self.conn.rollback()

    # ---- Cursor helpers ----
    def cursor(     
        self,
        server_side: bool = False,
        name: Optional[str] = None,
        *,
        dict_cursor: bool = True,
        itersize: Optional[int] = None,
        **kwargs,
    ):
        """
        Returns a cursor.
        - server_side=True -> named server-side cursor (streaming)
        - itersize -> batch fetch size hint (e.g. 20_000)
        """
        if not self.conn:
            raise RuntimeError("Connection is not established.")

        factory = psycopg2.extras.DictCursor if dict_cursor else None
        if server_side:
            cur = self.conn.cursor(name=name or "srv_cur", cursor_factory=factory, **kwargs)
            if itersize:
                cur.itersize = itersize  # type: ignore[attr-defined]
            return cur
        return self.conn.cursor(cursor_factory=factory, **kwargs)

    # ---- Execute / Query convenience ----
    def execute(self, sql: str, params: Optional[Union[Sequence[Any], Dict[str, Any]]] = None) -> None:
        if not self.conn:
            raise RuntimeError("Connection is not established.")
        with self.cursor() as cur:
            cur.execute(sql, params)

    def query(self, sql: str, params: Optional[Union[Sequence[Any], Dict[str, Any]]] = None) -> List[Row]:
        if not self.conn:
            raise RuntimeError("Connection is not established.")
        with self.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    # ---- Streaming read (server-side cursor) ----
    def stream(
        self,
        sql: str,
        params: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
        *,
        batch_size: int = 20_000,
        name: Optional[str] = None,
        dict_cursor: bool = True,
    ) -> Generator[List[Row], None, None]:
        """
        Yields rows in batches using a server-side named cursor.
        Usage:
            for batch in conn.stream("SELECT * FROM tbl", batch_size=20000):
                ...
        """
        if not self.conn:
            raise RuntimeError("Connection is not established.")

        with self.cursor(server_side=True, name=name, dict_cursor=dict_cursor, itersize=batch_size) as cur:
            cur.execute(sql, params)
            while True:
                part = cur.fetchmany(batch_size)
                if not part:
                    break
                yield [dict(r) for r in part]

    # ---- COPY helpers (fast load to PostgreSQL) ----
    def copy_from_file(
        self,
        file_obj: io.TextIOBase,
        table: str,
        *,
        sep: str = "\t",
        null: str = r"\N",
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        """
        COPY FROM STDIN using an open file-like object (text mode).
        Commit is NOT implicit when autocommit=False (call commit() yourself).
        """
        if not self.conn:
            raise RuntimeError("Connection is not established.")
        with self.conn.cursor() as cur:
            cur.copy_from(file=file_obj, table=table, sep=sep, null=null, columns=columns)  # type: ignore[arg-type]

    def copy_from_rows(
        self,
        rows: Iterable[Row],
        table: str,
        *,
        columns: Sequence[str],
        sep: str = "\t",
        null: str = r"\N",
        newline: str = "\n",
        sanitizer: Optional[callable] = None,
    ) -> int:
        """
        Quickly loads rows via COPY by materializing a small in-memory buffer.
        Returns number of rows written to COPY stream.
        """
        buf = io.StringIO()
        count = 0
        for r in rows:
            line_parts: List[str] = []
            for col in columns:
                v = r.get(col)
                if v is None:
                    line_parts.append(null)
                else:
                    s = str(v)
                    if sanitizer:
                        s = sanitizer(s)
                    # basic hygiene to keep single-line TSV; adjust if you need CSV
                    s = s.replace("\t", " ").replace("\r", " ").replace("\n", " ")
                    line_parts.append(s)
            buf.write(sep.join(line_parts) + newline)
            count += 1
        buf.seek(0)
        self.copy_from_file(buf, table, sep=sep, null=null, columns=list(columns))
        return count
