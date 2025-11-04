import psycopg2
import psycopg2.extras
from typing import Optional, Any
from psycopg2.extensions import connection as _Connection
from .logger import logger

class DatabaseConnection:
    def __init__(self, host: str, port: int, database: str, user: str, password: str) -> None:
        self.host: str = host
        self.port: int = port
        self.database: str = database
        self.user: str = user
        self.password: str = password
        self.conn: Optional[_Connection] = None

    def __enter__(self) -> _Connection:
        try:
            self.conn = DatabaseConnection.__create_connection(
                self.host, self.port, self.database, self.user, self.password
            )
            return self.conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        if self.conn:
            self.conn.close()

    @staticmethod
    def __create_connection(host: str, port: int, database: str, user: str, password: str) -> _Connection:
        return psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )