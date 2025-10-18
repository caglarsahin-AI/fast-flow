#partition strategy
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple, Protocol, Callable
import math
import psycopg2

# dynamic WHERE binding çözümü
from projects.etl_base_project.src.utilities.dynamic_filters import resolve_where_with_bindings
from projects.etl_base_project.src.common.logger import logger
# -----------------------------
# Result nesnesi
# -----------------------------
@dataclass(frozen=True)
class PartitionSpec:
    where: Optional[str]  # None => full
    label: str

# -----------------------------
# Ortak arayüz
# -----------------------------
class PartitionStrategy(Protocol):
    def plan(self) -> List[PartitionSpec]:
        ...

# -----------------------------
# Yardımcı DB fonksiyonları
# -----------------------------
def _connect(conf: Dict[str, Any]):
    return psycopg2.connect(
        host=conf["host"],
        port=conf["port"],
        dbname=conf["database"],
        user=conf["user"],
        password=conf["password"],
    )

def _get_min_max(conn, schema: str, table: str, column: str, base_where: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    where = []
    if base_where and base_where.strip():
        where.append(f"({base_where.strip()})")
    where.append(f'"{column}" IS NOT NULL')
    wsql = "WHERE " + " AND ".join(where) if where else ""
    sql = f'SELECT MIN("{column}"), MAX("{column}") FROM "{schema}"."{table}" {wsql}'
    with conn.cursor() as cur:
        cur.execute(sql)
        lo, hi = cur.fetchone()
        return (None if lo is None else int(lo), None if hi is None else int(hi))

def _get_distinct(conn, schema: str, table: str, column: str, limit: Optional[int], base_where: Optional[str]) -> List[int]:
    where = []
    if base_where and base_where.strip():
        where.append(f"({base_where.strip()})")
    where.append(f'"{column}" IS NOT NULL')
    wsql = "WHERE " + " AND ".join(where) if where else ""
    
    sql = f'SELECT DISTINCT "{column}" FROM "{schema}"."{table}" {wsql} ORDER BY "{column}" DESC'
    if limit:
        sql += f" LIMIT {int(limit)}"

    with conn.cursor() as cur:
        cur.execute(sql)
        return [int(r[0]) for r in cur.fetchall() if r[0] is not None]

# -----------------------------
# Somut stratejiler
# -----------------------------
class AutoNumericPartition:
    """
    min–max aralığını `parts` eşit dilime böler (numeric sütun).
    base_where → MIN/MAX sorgusuna push-down edilir.
    """
    def __init__(self, src_conf: Dict[str, Any], schema: str, table: str, conf: Dict[str, Any], base_where: Optional[str]) -> None:
        self.src_conf, self.schema, self.table, self.conf, self.base_where = src_conf, schema, table, conf, (base_where or None)

    def plan(self) -> List[PartitionSpec]:
        column = self.conf["column"]
        parts = int(self.conf.get("parts", 5))
        conn = _connect(self.src_conf)
        try:
            lo, hi = _get_min_max(conn, self.schema, self.table, column, self.base_where)
        finally:
            conn.close()
        if lo is None or hi is None:
            return [PartitionSpec(where="1=0", label="empty")]
        if hi < lo:
            lo, hi = hi, lo
        width = max(1, math.ceil((hi - lo + 1) / parts))
        out: List[PartitionSpec] = []
        start = lo
        while start <= hi:
            end_excl = min(hi + 1, start + width)
            range_clause = f'"{column}" >= {start} AND "{column}" < {end_excl}'
            part_where = f"({self.base_where}) AND ({range_clause})" if self.base_where else range_clause
            out.append(PartitionSpec(where=part_where, label=f"{column}:[{start},{end_excl})"))
            start = end_excl
        return out or [PartitionSpec(where="1=0", label="empty")]

class ExplicitRangePartition:
    """
    Konfigürasyondan min/max alınır, eşit aralıklara bölünür.
    base_where → her parçaya eklenir.
    """
    def __init__(self, src_conf: Dict[str, Any], schema: str, table: str, conf: Dict[str, Any], base_where: Optional[str]) -> None:
        self.conf, self.base_where = conf, (base_where or None)

    def plan(self) -> List[PartitionSpec]:
        column = self.conf["column"]
        parts = int(self.conf.get("parts", 5))
        lo = int(self.conf["min"])
        hi = int(self.conf["max"])
        if hi < lo:
            lo, hi = hi, lo
        width = max(1, math.ceil((hi - lo + 1) / parts))
        out: List[PartitionSpec] = []
        start = lo
        while start <= hi:
            end_excl = min(hi + 1, start + width)
            range_clause = f'"{column}" >= {start} AND "{column}" < {end_excl}'
            part_where = f"({self.base_where}) AND ({range_clause})" if self.base_where else range_clause
            out.append(PartitionSpec(where=part_where, label=f"{column}:[{start},{end_excl})"))
            start = end_excl
        return out

class DistinctValuePartition:
    """
    DISTINCT değer başına parça (numeric kolonda).
    base_where → DISTINCT sorgusuna push-down edilir; çıkan her parçaya tekrar eklenir.
    """
    def __init__(self, src_conf: Dict[str, Any], schema: str, table: str, conf: Dict[str, Any], 
        base_where: Optional[str],source_sql: Optional[str] = None) -> None:
        self.src_conf, self.schema, self.table, self.conf= src_conf, schema, table, conf
        self.base_where = (base_where or "").strip() or None
        self.source_sql = (source_sql or "").strip() or None

    def plan(self) -> List[PartitionSpec]:
        column = self.conf["column"]
        limit = self.conf.get("distinct_limit")
        conn = _connect(self.src_conf)
        try:
            if self.source_sql:
                # SQL kaynağından distinct
                sql = f'SELECT DISTINCT {column} FROM ({self.source_sql}) AS _src'
                if self.base_where:
                    sql += f" WHERE {self.base_where}"
                if limit:
                    sql += f" LIMIT {int(limit)}"
                with conn.cursor() as cur:
                    cur.execute(sql)
                    values = [int(r[0]) for r in cur.fetchall() if r[0] is not None]
            else:
                values = _get_distinct(conn, self.schema, self.table, column, limit, self.base_where)
        finally:
            conn.close()
        if not values:
            return [PartitionSpec(where="1=0", label="empty")]
        out: List[PartitionSpec] = []
        for v in values:
            clause = f'"{column}" = {v}'
            part_where = f"({self.base_where}) AND ({clause})" if self.base_where else clause
            out.append(PartitionSpec(where=part_where, label=f"{column}={v}"))
        return out

class HashModuloPartition:
    """
    Numeric sütunda `col % parts = k` ile N kalem parça.
    base_where → her parçaya eklenir (plan sorgusu yok).
    """
    def __init__(self, src_conf: Dict[str, Any], schema: str, table: str, conf: Dict[str, Any], base_where: Optional[str]) -> None:
        self.conf, self.base_where = conf, (base_where or None)

    def plan(self) -> List[PartitionSpec]:
        column = self.conf["column"]
        parts = int(self.conf.get("parts", 5))
        out: List[PartitionSpec] = []
        for r in range(parts):
            clause = f'("{column}" % {parts}) = {r}'
            part_where = f"({self.base_where}) AND ({clause})" if self.base_where else clause
            out.append(PartitionSpec(where=part_where, label=f"{column}%{parts}={r}"))
        return out

class PercentileRangePartition:
    """
    NTILE tabanlı aralıklar. base_where → NTILE iç select'ine push-down edilir
    ve üretilen aralıklar her parçaya tekrar eklenir.
    """
    def __init__(self, src_conf: Dict[str, Any], schema: str, table: str, conf: Dict[str, Any], base_where: Optional[str]) -> None:
        self.src_conf, self.schema, self.table, self.conf, self.base_where = src_conf, schema, table, conf, (base_where or None)

    def plan(self) -> List[PartitionSpec]:
        column = self.conf["column"]
        parts = int(self.conf.get("parts", 5))
        conn = _connect(self.src_conf)
        try:
            extra = []
            if self.base_where:
                extra.append(f"({self.base_where})")
            extra.append(f'"{column}" IS NOT NULL')
            wsql = "WHERE " + " AND ".join(extra)

            sql = f"""
                SELECT bucket, MIN("{column}") AS lo, MAX("{column}") AS hi
                FROM (
                    SELECT "{column}",
                           NTILE(%s) OVER (ORDER BY "{column}") AS bucket
                    FROM "{self.schema}"."{self.table}"
                    {wsql}
                ) t
                GROUP BY bucket
                ORDER BY bucket
            """
            with conn.cursor() as cur:
                cur.execute(sql, (parts,))
                rows = cur.fetchall()
        finally:
            conn.close()

        specs: List[PartitionSpec] = []
        for i, (bucket, lo, hi) in enumerate(rows):
            if lo is None or hi is None:
                continue
            lo, hi = int(lo), int(hi)
            if i < len(rows) - 1:
                range_clause = f'"{column}" >= {lo} AND "{column}" < {hi}'
                label = f"{column}~ntile{parts}#{bucket}:[{lo},{hi})"
            else:
                range_clause = f'"{column}" >= {lo} AND "{column}" <= {hi}'
                label = f"{column}~ntile{parts}#{bucket}:[{lo},{hi}]"
            part_where = f"({self.base_where}) AND ({range_clause})" if self.base_where else range_clause
            specs.append(PartitionSpec(where=part_where, label=label))
        return specs or [PartitionSpec(where="1=0", label="empty")]

# -----------------------------
# Factory & Facade
# -----------------------------
def make_partition_strategy(
    src_conf: Dict[str, Any],
    schema: str,
    table: str,
    part_conf: Dict[str, Any],
    *,
    base_where: Optional[str] = None,
    source_sql: Optional[str] = None
) -> PartitionStrategy:
    
    mode = str(part_conf.get("mode", "auto")).lower()
    logger.info("partition_strategy distinct mode = %s", mode )
    
    if not part_conf or not part_conf.get("enabled"):
        return _FullScanStrategy(base_where)

    if mode == "explicit":
        return ExplicitRangePartition(src_conf, schema, table, part_conf, base_where)
    if mode == "distinct":
        logger.info("partition_strategy distinct base_where = %s", base_where )
        return DistinctValuePartition(src_conf, schema, table, part_conf, base_where, source_sql=source_sql)
        
    if mode in ("hash", "hash_mod", "mod"):
        return HashModuloPartition(src_conf, schema, table, part_conf, base_where)
    if mode in ("percentile", "percentiles", "ntile"):
        return PercentileRangePartition(src_conf, schema, table, part_conf, base_where)
    return AutoNumericPartition(src_conf, schema, table, part_conf, base_where)

class _FullScanStrategy:
    def __init__(self, base_where: Optional[str]) -> None:
        self.base_where = base_where or None
    def plan(self) -> List[PartitionSpec]:
        return [PartitionSpec(where=self.base_where, label="full")]

def plan_partitions(
    source_db_conf: Dict[str, Any],
    schema: Optional[str],
    table: Optional[str],
    partition_conf: Optional[Dict[str, Any]],
    *,
    base_where_template: Optional[str] = None,
    bindings: Optional[Dict[str, Any]] = None,
    target_db_conf: Optional[Dict[str, Any]] = None,
    airflow_vars_get: Optional[Callable[[str], Optional[str]]] = None,
    source_sql: Optional[str] = None
) -> List[PartitionSpec]:
    """
    Facade:
      - (Opsiyonel) dynamic WHERE binding çöz
      - Çözülen WHERE'i partisyon planına push-down et
    """
    resolved_where: Optional[str] = None
    if base_where_template and base_where_template.strip():
        # Bağlantıları geçici aç, binding gerekiyorsa çöz
        src_conn = tgt_conn = None
        try:
            logger.info("partition_strategy base_where_template= %s bindings=%s",base_where_template,bindings)
            src_conn = _connect(source_db_conf) if source_db_conf else None
            tgt_conn = _connect(target_db_conf) if target_db_conf else None
            if bindings:
                resolved_where = resolve_where_with_bindings(
                    base_where_template, bindings,
                    src_conn=src_conn, tgt_conn=tgt_conn,
                    airflow_vars_get=airflow_vars_get
                )
            else:
                resolved_where = base_where_template
        finally:
            try:
                if src_conn: src_conn.close()
            finally:
                if tgt_conn: tgt_conn.close()
    logger.info("partition_strategy 2 base_where_template= %s resolved_where=%s",base_where_template,resolved_where)
    strategy = make_partition_strategy(
        source_db_conf, schema, table, partition_conf or {}, base_where=resolved_where, source_sql=source_sql
    )
    logger.info("partition_strategy 3 base_where_template= %s resolved_where=%s",base_where_template,resolved_where)
    return strategy.plan()
