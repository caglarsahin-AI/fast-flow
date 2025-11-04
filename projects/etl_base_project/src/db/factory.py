# projects/etl_base_project/src/db/factory.py
from __future__ import annotations
from typing import Dict, Any, Tuple
from .postgres import PostgresDriver
from .mssql import MssqlDriver
from .oracle import OracleDriver
from .base import Driver

def make_driver(kind: str) -> Driver:
    k = (kind or "").lower()
    if k in {"pg","postgres","postgresql"}: return PostgresDriver()
    if k in {"mssql","sqlserver","sql_server"}: return MssqlDriver()
    if k in {"oracle"}: return OracleDriver()
    raise ValueError(f"Unknown db kind: {kind}")

def connect_driver(conf: Dict[str,Any]) -> Tuple[Driver, any]:
    drv = make_driver(conf["kind"])
    conn = drv.connect(conf)
    return drv, conn