# projects/etl_base_project/src/common/dynamic_filters.py
from __future__ import annotations
import re
from decimal import Decimal
from typing import Any, Dict, Optional, Callable, Union

from psycopg2.extensions import adapt
from ..common.logger import logger
from ..common.database_connection import DatabaseConnection  # dict gelirse kısa ömürlü conn açmak için

_PARAM_RE = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)")

ConnLike = Union[None, Any, Dict[str, Any]]  # psycopg2 conn | None | config dict

def _as_sql_literal(val: Any) -> str:
    """
    Sayısal değerleri tırnaksız, metin/tarih değerleri güvenli şekilde tırnaklı döndür.
    """
    if val is None:
        return "NULL"
    if isinstance(val, (int, float, Decimal)):
        return str(val)
    # kalan her şey adapt ile güvenli
    return adapt(val).getquoted().decode()

def _fetch_sql_conn(conn, sql: str) -> Optional[Any]:
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return None if row is None else row[0]

def _fetch_sql(conn_or_conf: ConnLike, sql: str) -> Optional[Any]:
    """
    - Eğer gerçek psycopg2 connection ise direkt kullan.
    - Eğer dict (db config) ise kısa ömürlü connection aç-kapa.
    """
    if conn_or_conf is None:
        raise ValueError("DB connection/config is required to evaluate binding SQL.")

    # gerçek connection mı?
    if hasattr(conn_or_conf, "cursor"):
        return _fetch_sql_conn(conn_or_conf, sql)

    # config dict ise
    if isinstance(conn_or_conf, dict):
        # autocommit gerekli değil; kısa ömürlü select için yeterli
        with DatabaseConnection(**conn_or_conf) as tmp_conn:
            return _fetch_sql_conn(tmp_conn, sql)

    raise TypeError(f"Unsupported connection type for bindings: {type(conn_or_conf)}")

def resolve_where_with_bindings(
    where_template: Optional[str],
    bindings_conf: Optional[Dict[str, Dict[str, Any]]],
    *,
    src_conn: ConnLike = None,
    tgt_conn: ConnLike = None,
    airflow_vars_get: Optional[Callable[[str], Optional[str]]] = None,  # callable(name)->str|None
) -> Optional[str]:
    """
    where_template içindeki :param adlarını bindings_conf'a göre doldurur.
    Dönüş: literal’ları inline edilmiş hazır WHERE string.
    - src_conn/tgt_conn: psycopg2 connection veya config dict olabilir.
    - airflow_var için name verilmezse placeholder adı kullanılır.
    """
    logger.info("[dynamic_filter] bindings_conf= %s ",bindings_conf)
    logger.info("[dynamic_filter] where_template= %s ",where_template)

    if not where_template:
        return None
    if not bindings_conf:
        return where_template

    def resolve_one(param_name: str) -> str:
        spec = bindings_conf.get(param_name)
        logger.debug(f"[dyn-where] resolving :{param_name} spec={spec}")
        if not spec:
            raise ValueError(f"[dyn-where] Param not defined in bindings: {param_name}")

        origin = (spec.get("from") or "target").lower()
        default = spec.get("default", None)

        if origin == "target":
            sql = spec.get("sql")
            if not sql:
                raise ValueError(f"[dyn-where] Missing SQL for :{param_name} (from=target)")
            val = _fetch_sql(tgt_conn, sql)
            if val is None:
                val = default
            return _as_sql_literal(val)

        if origin == "source":
            sql = spec.get("sql")
            if not sql:
                raise ValueError(f"[dyn-where] Missing SQL for :{param_name} (from=source)")
            val = _fetch_sql(src_conn, sql)
            if val is None:
                val = default
            return _as_sql_literal(val)

        if origin == "airflow_var":
            if airflow_vars_get is None:
                raise ValueError(f"[dyn-where] airflow_vars_get callable required for :{param_name}")
            var_name = spec.get("name") or param_name  # name yoksa placeholder adı
            val = airflow_vars_get(var_name)
            if val is None:
                val = default
            return _as_sql_literal(val)

        raise ValueError(f"[dyn-where] Unknown binding source: {origin}")

    def replacer(m: re.Match) -> str:
        pname = m.group(1)
        lit = resolve_one(pname)
        logger.debug(f"[dyn-where] {pname} -> {lit}")
        return lit

    

    logger.debug(f"[dyn-where] template: {where_template}")
    resolved = _PARAM_RE.sub(replacer, where_template)
    logger.info(f"[dyn-where] resolved WHERE: {resolved}")
    return resolved
