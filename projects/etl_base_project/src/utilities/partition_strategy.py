# partition strategy
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple, Protocol, Callable
import math
import psycopg2
from decimal import Decimal
from psycopg2.extensions import adapt
import yaml

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

def _sql_literal(val: Any) -> str:
    """Değeri güvenli SQL literal’ına çevir (sayılar tırnaksız, metin/tarih quoted)."""
    if val is None:
        return "NULL"
    if isinstance(val, (int, float, Decimal)):
        return str(val)
    return adapt(val).getquoted().decode()

def _get_min_max(conn, schema: str, table: str, column: str, base_where: Optional[str]) -> Tuple[Optional[Any], Optional[Any]]:
    where = []
    if base_where and base_where.strip():
        where.append(f"({base_where.strip()})")
    where.append(f'"{column}" IS NOT NULL')
    wsql = "WHERE " + " AND ".join(where) if where else ""
    sql = f'SELECT MIN("{column}"), MAX("{column}") FROM "{schema}"."{table}" {wsql}'
    with conn.cursor() as cur:
        cur.execute(sql)
        lo, hi = cur.fetchone()
        return (lo, hi)

def _get_distinct(conn, schema: str, table: str, column: str, limit: Optional[int], base_where: Optional[str]) -> List[Any]:
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
        return [r[0] for r in cur.fetchall() if r[0] is not None]

# ---- Tip çözümleme ve literal üretimi ----
_TEXT_TYPES     = {"character varying", "varchar", "text", "character", "bpchar"}
_NUMERIC_TYPES  = {"smallint", "integer", "bigint", "numeric", "decimal", "real", "double precision", "double"}
_DATE_TYPES     = {"date"}
_TS_TYPES       = {"timestamp without time zone", "timestamp with time zone", "timestamp"}

def _normalize_declared_type(t: Optional[str]) -> Optional[str]:
    if not t:
        return None
    base = str(t).strip().lower().split("(")[0].strip()
    # mapping dosyasında "NUMERIC(8,0)" gibi gelir → "numeric"
    return base

def _get_pg_coltype(conn, schema: str, table: str, column: str) -> Optional[str]:
    if not schema or not table:
        return None
    sql = """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table, column))
        row = cur.fetchone()
    return str(row[0]).lower() if row else None

def _literal_for_declared_type(val: Any, typ: Optional[str]) -> str:
    """
    Kolonu CAST etmiyoruz; literal'i bildirilen TİP'e göre yazıyoruz.
    (mapping dosyası > pg katalog > değerin tipi)
    """
    if val is None:
        return "NULL"
    t = _normalize_declared_type(typ) or ""
    if t in _TEXT_TYPES:
        return _sql_literal(str(val))          # '...'
    if t in _NUMERIC_TYPES:
        return str(val)                        # 12345
    if t in _DATE_TYPES:
        return f"{_sql_literal(str(val))}::date"
    if t in _TS_TYPES:
        return f"{_sql_literal(str(val))}::timestamp"
    if t:  # bilinmiyorsa hint ile cast
        return f"{_sql_literal(str(val))}::{t}"
    # Tip hiç bilinmiyorsa değere göre
    return _sql_literal(val)

def _col_type_from_mapping(mapping_types: Optional[Dict[str, str]], column: str) -> Optional[str]:
    if not mapping_types:
        return None
    # mapping'ler target isme göre tutulur; partition kolonu target adına eşit olmalı
    t = mapping_types.get(column)
    if t:
        return _normalize_declared_type(t)
    # fallback: bazı durumlarda source adı kullanılabilir
    return None

def _typed_between_predicate(
    conn,
    schema: Optional[str],
    table: Optional[str],
    column: str,
    lo: Any,
    hi: Any,
    *,
    mapping_types: Optional[Dict[str, str]] = None,
) -> str:
    declared = _col_type_from_mapping(mapping_types, column) or _get_pg_coltype(conn, schema, table, column)
    lo_lit = _literal_for_declared_type(lo, declared)
    hi_lit = _literal_for_declared_type(hi, declared)
    qcol = '"' + column.replace('"', '""') + '"'
    return f'{qcol} >= {lo_lit} AND {qcol} < {hi_lit}'

def _typed_ge_le_predicate(
    conn,
    schema: Optional[str],
    table: Optional[str],
    column: str,
    lo: Any,
    hi: Any,
    inclusive_hi: bool,
    *,
    mapping_types: Optional[Dict[str, str]] = None,
) -> str:
    declared = _col_type_from_mapping(mapping_types, column) or _get_pg_coltype(conn, schema, table, column)
    lo_lit = _literal_for_declared_type(lo, declared)
    hi_lit = _literal_for_declared_type(hi, declared)
    qcol = '"' + column.replace('"', '""') + '"'
    op_hi = "<=" if inclusive_hi else "<"
    return f'{qcol} >= {lo_lit} AND {qcol} {op_hi} {hi_lit}'

def _typed_eq_predicate(
    conn,
    schema: Optional[str],
    table: Optional[str],
    column: str,
    v: Any,
    *,
    mapping_types: Optional[Dict[str, str]] = None,
) -> str:
    declared = _col_type_from_mapping(mapping_types, column) or _get_pg_coltype(conn, schema, table, column)
    v_lit = _literal_for_declared_type(v, declared)
    qcol = '"' + column.replace('"', '""') + '"'
    return f"{qcol} = {v_lit}"

# -----------------------------
# Mapping loader
# -----------------------------
def _load_mapping_types(mapping_file: Optional[str]) -> Optional[Dict[str, str]]:
    """
    mapping.yaml → {'target_col': 'NUMERIC(8,0)', ...}
    """
    if not mapping_file:
        return None
    try:
        with open(mapping_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        cols = data.get("columns") or []
        types: Dict[str, str] = {}
        for c in cols:
            tgt = (c.get("target") or "").strip()
            typ = (c.get("type") or "").strip()
            if tgt and typ:
                types[tgt] = typ
        return types or None
    except Exception as e:
        logger.warning("Mapping dosyası okunamadı (%s): %s", mapping_file, e)
        return None

# -----------------------------
# Somut stratejiler
# -----------------------------
class AutoNumericPartition:
    """
    min–max aralığını `parts` eşit dilime böler (numeric veya lexicographic).
    base_where → MIN/MAX sorgusuna push-down edilir.
    """
    def __init__(
        self,
        src_conf: Dict[str, Any],
        schema: Optional[str],
        table: Optional[str],
        conf: Dict[str, Any],
        base_where: Optional[str],
        *,
        mapping_types: Optional[Dict[str, str]] = None,
    ) -> None:
        self.src_conf, self.schema, self.table, self.conf = src_conf, schema, table, conf
        self.base_where = (base_where or None)
        self.mapping_types = mapping_types

    def plan(self) -> List[PartitionSpec]:
        column = self.conf["column"]
        parts = int(self.conf.get("parts", 5))
        conn = _connect(self.src_conf)
        try:
            if not self.schema or not self.table:
                # SQL kaynaklı (alt-sorgu) ise MIN/MAX yapamayız; percentil benzeri yaklaşım yoksa full:
                logger.info("AutoNumericPartition: schema/table yok; fallback full.")
                return [PartitionSpec(where=self.base_where, label="full")]
            lo, hi = _get_min_max(conn, self.schema, self.table, column, self.base_where)
            if lo is None or hi is None:
                return [PartitionSpec(where="1=0", label="empty")]
            try:
                width = max(1, math.ceil((float(hi) - float(lo) + 1) / parts))
                numeric_ok = True
            except Exception:
                width = None
                numeric_ok = False

            out: List[PartitionSpec] = []
            if numeric_ok:
                start = lo
                while float(start) <= float(hi):
                    end_excl = min(float(hi) + 1.0, float(start) + width)
                    range_clause = _typed_between_predicate(
                        conn, self.schema, self.table, column, start, end_excl, mapping_types=self.mapping_types
                    )
                    part_where = f"({self.base_where}) AND ({range_clause})" if self.base_where else range_clause
                    out.append(PartitionSpec(where=part_where, label=f"{column}:[{start},{end_excl})"))
                    start = end_excl
            else:
                # numeric değilse kaba NTILE
                sql = f"""
                    SELECT bucket, MIN("{column}") AS lo, MAX("{column}") AS hi
                    FROM (
                        SELECT "{column}",
                               NTILE(%s) OVER (ORDER BY "{column}") AS bucket
                        FROM "{self.schema}"."{self.table}"
                        {"WHERE " + self.base_where if self.base_where else ""}
                        {"AND " if self.base_where else "WHERE "} "{column}" IS NOT NULL
                    ) t
                    GROUP BY bucket
                    ORDER BY bucket
                """
                with conn.cursor() as cur:
                    cur.execute(sql, (parts,))
                    rows = cur.fetchall()
                for i, (bucket, lo_v, hi_v) in enumerate(rows):
                    if lo_v is None or hi_v is None:
                        continue
                    inclusive_hi = (i == len(rows) - 1)
                    clause = _typed_ge_le_predicate(
                        conn, self.schema, self.table, column, lo_v, hi_v, inclusive_hi, mapping_types=self.mapping_types
                    )
                    label = f"{column}~auto_ntile{parts}#{bucket}"
                    part_where = f"({self.base_where}) AND ({clause})" if self.base_where else clause
                    out.append(PartitionSpec(where=part_where, label=label))

            return out or [PartitionSpec(where="1=0", label="empty")]
        finally:
            conn.close()

class ExplicitRangePartition:
    """
    Konfigürasyondan min/max alınır, eşit aralıklara bölünür.
    base_where → her parçaya eklenir.
    """
    def __init__(
        self,
        src_conf: Dict[str, Any],
        schema: Optional[str],
        table: Optional[str],
        conf: Dict[str, Any],
        base_where: Optional[str],
        *,
        mapping_types: Optional[Dict[str, str]] = None,
    ) -> None:
        self.src_conf, self.schema, self.table = src_conf, schema, table
        self.conf, self.base_where = conf, (base_where or None)
        self.mapping_types = mapping_types

    def plan(self) -> List[PartitionSpec]:
        column = self.conf["column"]
        parts = int(self.conf.get("parts", 5))
        lo = self.conf["min"]
        hi = self.conf["max"]
        try:
            width = max(1, math.ceil((float(hi) - float(lo) + 1) / parts))
            numeric_ok = True
        except Exception:
            width = None
            numeric_ok = False

        conn = _connect(self.src_conf)
        try:
            out: List[PartitionSpec] = []
            if numeric_ok:
                start = lo
                while float(start) <= float(hi):
                    end_excl = min(float(hi) + 1.0, float(start) + width)
                    range_clause = _typed_between_predicate(
                        conn, self.schema, self.table, column, start, end_excl, mapping_types=self.mapping_types
                    )
                    part_where = f"({self.base_where}) AND ({range_clause})" if self.base_where else range_clause
                    out.append(PartitionSpec(where=part_where, label=f"{column}:[{start},{end_excl})"))
                    start = end_excl
            else:
                clause = _typed_ge_le_predicate(
                    conn, self.schema, self.table, column, lo, hi, inclusive_hi=True, mapping_types=self.mapping_types
                )
                part_where = f"({self.base_where}) AND ({clause})" if self.base_where else clause
                out.append(PartitionSpec(where=part_where, label=f"{column}:[{lo},{hi}]"))
            return out
        finally:
            conn.close()

class DistinctValuePartition:
    """
    DISTINCT değer başına parça (kolon tipinden bağımsız).
    base_where → DISTINCT sorgusuna push-down edilir; çıkan her parçaya tekrar eklenir.
    """
    def __init__(
        self,
        src_conf: Dict[str, Any],
        schema: Optional[str],
        table: Optional[str],
        conf: Dict[str, Any],
        base_where: Optional[str],
        source_sql: Optional[str] = None,
        *,
        mapping_types: Optional[Dict[str, str]] = None,
    ) -> None:
        self.src_conf, self.schema, self.table, self.conf = src_conf, schema, table, conf
        self.base_where = (base_where or "").strip() or None
        self.source_sql = (source_sql or "").strip() or None
        self.mapping_types = mapping_types

    def plan(self) -> List[PartitionSpec]:
        column = self.conf["column"]
        limit = self.conf.get("distinct_limit")
        conn = _connect(self.src_conf)
        try:
            if self.source_sql:
                sql = f'SELECT DISTINCT "{column}" FROM ({self.source_sql}) AS _src'
                if self.base_where:
                    sql += f" WHERE {self.base_where}"
                if limit:
                    sql += f" LIMIT {int(limit)}"
                with conn.cursor() as cur:
                    cur.execute(sql)
                    values = [r[0] for r in cur.fetchall() if r and r[0] is not None]
            else:
                if not self.schema or not self.table:
                    # tablo yoksa distinct planı yapılamaz
                    return [PartitionSpec(where=self.base_where, label="full")]
                values = _get_distinct(conn, self.schema, self.table, column, limit, self.base_where)
            if not values:
                return [PartitionSpec(where="1=0", label="empty")]

            out: List[PartitionSpec] = []
            # kısa ömürlü ikinci bağlantı: tip okuma gerekirse
            conn2 = _connect(self.src_conf)
            try:
                for v in values:
                    clause = _typed_eq_predicate(
                        conn2, self.schema, self.table, column, v, mapping_types=self.mapping_types
                    )
                    part_where = f"({self.base_where}) AND ({clause})" if self.base_where else clause
                    out.append(PartitionSpec(where=part_where, label=f"{column}={v}"))
            finally:
                conn2.close()
            return out
        finally:
            conn.close()

class HashModuloPartition:
    """
    Numeric sütunda `col % parts = k` ile N kalem parça.
    base_where → her parçaya eklenir (plan sorgusu yok).
    """
    def __init__(self, src_conf: Dict[str, Any], schema: Optional[str], table: Optional[str], conf: Dict[str, Any], base_where: Optional[str]) -> None:
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
    def __init__(
        self,
        src_conf: Dict[str, Any],
        schema: Optional[str],
        table: Optional[str],
        conf: Dict[str, Any],
        base_where: Optional[str],
        *,
        mapping_types: Optional[Dict[str, str]] = None,
    ) -> None:
        self.src_conf, self.schema, self.table, self.conf = src_conf, schema, table, conf
        self.base_where = (base_where or None)
        self.mapping_types = mapping_types

    def plan(self) -> List[PartitionSpec]:
        column = self.conf["column"]
        parts = int(self.conf.get("parts", 5))
        if not self.schema or not self.table:
            # SQL kaynaklı ise NTILE'i alt-sorguda koşturmak gerekir; elimizde yok → full
            return [PartitionSpec(where=self.base_where, label="full")]

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

        conn2 = _connect(self.src_conf)
        try:
            specs: List[PartitionSpec] = []
            for i, (bucket, lo, hi) in enumerate(rows):
                if lo is None or hi is None:
                    continue
                inclusive_hi = (i == len(rows) - 1)
                range_clause = _typed_ge_le_predicate(
                    conn2, self.schema, self.table, column, lo, hi, inclusive_hi, mapping_types=self.mapping_types
                )
                label = f"{column}~ntile{parts}#{bucket}"
                part_where = f"({self.base_where}) AND ({range_clause})" if self.base_where else range_clause
                specs.append(PartitionSpec(where=part_where, label=label))
            return specs or [PartitionSpec(where="1=0", label="empty")]
        finally:
            conn2.close()

# -----------------------------
# Factory & Facade
# -----------------------------
def make_partition_strategy(
    src_conf: Dict[str, Any],
    schema: Optional[str],
    table: Optional[str],
    part_conf: Dict[str, Any],
    *,
    base_where: Optional[str] = None,
    source_sql: Optional[str] = None,
    mapping_types: Optional[Dict[str, str]] = None,
) -> PartitionStrategy:

    mode = str(part_conf.get("mode", "auto")).lower()
    logger.info("**** partition_strategy mode = %s", mode )

    if not part_conf or not part_conf.get("enabled"):
        return _FullScanStrategy(base_where)

    if mode == "explicit":
        return ExplicitRangePartition(src_conf, schema, table, part_conf, base_where, mapping_types=mapping_types)
    if mode == "distinct":
        logger.info("partition_strategy distinct base_where = %s", base_where )
        return DistinctValuePartition(
            src_conf, schema, table, part_conf, base_where, source_sql=source_sql, mapping_types=mapping_types
        )
    if mode in ("hash", "hash_mod", "mod"):
        return HashModuloPartition(src_conf, schema, table, part_conf, base_where)
    if mode in ("percentile", "percentiles", "ntile"):
        return PercentileRangePartition(src_conf, schema, table, part_conf, base_where, mapping_types=mapping_types)
    return AutoNumericPartition(src_conf, schema, table, part_conf, base_where, mapping_types=mapping_types)

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
    source_sql: Optional[str] = None,
    column_mapping_mode: Optional[str] = None,
    mapping_file: Optional[str] = None,
) -> List[PartitionSpec]:
    """
    Facade:
      - (Opsiyonel) dynamic WHERE binding çöz
      - Mapping tiplerini (varsa) yükle
      - Çözülen WHERE'i partisyon planına push-down et
    """
    resolved_where: Optional[str] = None
    if base_where_template and base_where_template.strip():
        src_conn = tgt_conn = None
        try:
            logger.info("partition_strategy base_where_template= %s bindings=%s", base_where_template, bindings)
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

    # Mapping mode ise tipi mapping’den oku
    mapping_types: Optional[Dict[str, str]] = None
    if (column_mapping_mode or "").lower() == "mapping_file":
        mapping_types = _load_mapping_types(mapping_file)

    logger.info(
        "partition_strategy make_partition_strategy öncesi base_where_template=%s resolved_where=%s mapping_types=%s",
        base_where_template, resolved_where, bool(mapping_types)
    )

    strategy = make_partition_strategy(
        source_db_conf,
        schema,
        table,
        partition_conf or {},
        base_where=resolved_where,
        source_sql=source_sql,
        mapping_types=mapping_types,
    )

    logger.info(
        "partition_strategy make_partition_strategy sonrası base_where_template=%s resolved_where=%s mapping_types=%s",
        base_where_template, resolved_where, bool(mapping_types)
    )
    return strategy.plan()
