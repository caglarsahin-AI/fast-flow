from __future__ import annotations
from datetime import datetime, timedelta, date
from typing import Any, Dict, Optional, List
import pandas as pd
from tempfile import SpooledTemporaryFile

from projects.etl_base_project.src.common.logger import logger
from projects.etl_base_project.src.common.database_connection import DatabaseConnection
from projects.etl_base_project.src.etl.extract import Extractor
from projects.etl_base_project.src.etl.transform import Transformer
from projects.etl_base_project.src.etl.load import Loader


class ETLManager:
    """
    Modlar:
      - dates_by_day
      - incremental_by_max_date
      - full_stream (default)
    Opsiyonlar:
      - passthrough_copy (dates_by_day ile)
      - keyset_page_size (unique_key_column ile)
      - where (partition push-down)  ← DAG'de resolved gelir
      - prepare_only
    """
    def __init__(self, source_db_config: Dict[str, Any], target_db_config: Dict[str, Any]) -> None:
        self.source_db_config = source_db_config
        self.target_db_config = target_db_config

    # ---- helpers ----
    @staticmethod
    def _build_select_sql(ex: Extractor, cols: List[str], where: Optional[str], order_by: Optional[str]) -> str:
        qual_src = f'"{ex.source_schema}"."{ex.source_table}"'
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        w = f"WHERE {where}" if (where and where.strip()) else ""
        o = f"ORDER BY {order_by}" if (order_by and order_by.strip()) else ""
        return f"SELECT {cols_sql} FROM {qual_src} {w} {o}"

    def run_etl_task(self, **kwargs) -> None:
        logger.info("run_etl_task başlar")
        method = kwargs.get("load_method")
        if kwargs.get("prepare_only"):
            self._prepare_only(**kwargs); return
        if method == "dates_by_day":
            self._run_dates_by_day(**kwargs); return
        if method == "incremental_by_max_date":
            self._run_incremental_by_max_date(**kwargs); return
        self._run_full_stream(**kwargs)  # default
        logger.info("run_etl_task bitti")

    # ---- prepare only ----
    def _prepare_only(self, **kwargs) -> None:
        src = DatabaseConnection(**self.source_db_config)
        tgt = DatabaseConnection(**self.target_db_config)
        with src, tgt:
            ex = Extractor(src.raw, tgt.raw,
                           kwargs["source_schema"], kwargs["source_table"],
                           kwargs["target_schema"], kwargs["target_table"])
            ld = Loader(tgt.raw, ex.get_column_metadata(), **kwargs)
            cols = ex.get_column_order()
            ld.prepare(pd.DataFrame(columns=cols))
            tgt.commit()

    # ---- dates_by_day ----
    def _run_dates_by_day(self, **kwargs) -> None:
        date_col: str = kwargs["date_column"]
        date_start: str = kwargs["date_start"]
        date_end: str = kwargs["date_end"]
        key_col: Optional[str] = kwargs.get("unique_key_column")
        keyset_ps: Optional[int] = kwargs.get("keyset_page_size")
        passthrough: bool = bool(kwargs.get("passthrough_copy", True))
        spool_mb: int = int(kwargs.get("passthrough_spool_max_mb", 256))

        base_where: Optional[str] = (kwargs.get("where") or "").strip() or None
        order_by: Optional[str]   = (kwargs.get("order_by") or "").strip() or None

        src = DatabaseConnection(**self.source_db_config)
        tgt = DatabaseConnection(**self.target_db_config)
        with src, tgt:
            ex = Extractor(src.raw, tgt.raw,
                           kwargs["source_schema"], kwargs["source_table"],
                           kwargs["target_schema"], kwargs["target_table"])
            tf = Transformer()
            ld = Loader(tgt.raw, ex.get_column_metadata(), **kwargs)

            cols = ex.get_column_order()
            ld.prepare(pd.DataFrame(columns=cols))

            if passthrough:
                for day, _df in ex.iter_by_day(
                    date_col, date_start, date_end, inner_page_size=None,
                    extra_where=base_where, order_by=order_by
                ):
                    with SpooledTemporaryFile(max_size=spool_mb * 1024 * 1024, mode="w+b") as spool:
                        ex.copy_day_to_filelike(
                            date_col, day, cols, spool,
                            key_column=key_col,
                            extra_where=base_where,
                            order_by=order_by
                        )
                        spool.seek(0)
                        ld.copy_from_filelike(spool, cols)
                return

            total = 0
            for _day, df in ex.iter_by_day(
                date_col, date_start, date_end, inner_page_size=None,
                extra_where=base_where, order_by=order_by
            ):
                if df is None or df.empty:
                    continue
                if key_col and keyset_ps:
                    for chunk in ex.iter_day_keyset(date_col, key_col, _day, keyset_ps, extra_where=base_where):
                        out = tf.run(chunk, kwargs["source_table"])
                        ld.run(out); total += len(out)
                else:
                    out = tf.run(df, kwargs["source_table"])
                    ld.run(out); total += len(out)
            logger.info("[dates_by_day] total=%s", total)

    # ---- incremental_by_max_date ----
    def _run_incremental_by_max_date(self, **kwargs) -> None:
        date_col: str = kwargs["date_column"]
        base_where: Optional[str] = (kwargs.get("where") or "").strip() or None
        order_by: Optional[str]   = (kwargs.get("order_by") or "").strip() or None

        src = DatabaseConnection(**self.source_db_config)
        tgt = DatabaseConnection(**self.target_db_config)
        with src, tgt:
            ex = Extractor(src.raw, tgt.raw,
                           kwargs["source_schema"], kwargs["source_table"],
                           kwargs["target_schema"], kwargs["target_table"])
            tf = Transformer()
            ld = Loader(tgt.raw, ex.get_column_metadata(), **kwargs)

            start, end_excl = ex.get_incremental_range(date_col)
            if not start or not end_excl or start >= end_excl:
                logger.info("Incremental boş. start=%s, end=%s", start, end_excl)
                return

            cols = ex.get_column_order()
            ld.prepare(pd.DataFrame(columns=cols))

            total = 0
            for _day, df in ex.iter_by_day(
                date_col, start, end_excl, inner_page_size=None,
                extra_where=base_where, order_by=order_by
            ):
                if df is None or df.empty:
                    continue
                out = tf.run(df, kwargs["source_table"])
                ld.run(out); total += len(out)
            logger.info("[incremental_by_max_date] total=%s", total)

    # ---- full_stream (resolved where push-down) ----
    def _run_full_stream(self, **kwargs) -> None:
        passthrough_full = bool(kwargs.get("passthrough_full", True))
        passthrough_fmt  = (kwargs.get("passthrough_format") or "binary").lower()  # "binary" | "csv"
        spool_mb         = int(kwargs.get("passthrough_spool_max_mb", 512))

        where    = (kwargs.get("where") or "").strip() or None   # RESOLVED (literal)
        order_by = (kwargs.get("order_by") or "").strip() or None

        src = DatabaseConnection(**self.source_db_config)
        tgt = DatabaseConnection(**self.target_db_config)
        with src, tgt:
            ex  = Extractor(src.raw, tgt.raw,
                            kwargs["source_schema"], kwargs["source_table"],
                            kwargs["target_schema"], kwargs["target_table"])
            ld  = Loader(tgt.raw, ex.get_column_metadata(), **kwargs)

            cols = ex.get_column_order()
            ld.prepare(pd.DataFrame(columns=cols))  # mevcut prepare akışı

            if passthrough_full:
                logger.info("[full_stream][passthrough] fmt=%s where=%s order_by=%s",
                            passthrough_fmt, where, order_by)
                mode = "w+b"  # binary/text fark etmeksizin güvenli seçim
                with SpooledTemporaryFile(max_size=spool_mb * 1024 * 1024, mode=mode) as buf:
                    ex.copy_select_to_filelike(
                        columns=cols,
                        file_obj=buf,
                        where=where,
                        order_by=order_by,
                        fmt=passthrough_fmt,
                    )
                    buf.seek(0)
                    ld.copy_from_filelike(
                        file_obj=buf,
                        columns=cols,
                        fmt=passthrough_fmt,
                    )
                return

            # ---- Fallback: stream + transform yolu ----
            rows_loaded = 0
            sql = self._build_select_sql(ex, cols, where, order_by)
            for batch in src.stream(sql=sql, batch_size=int(kwargs.get("batch_size", 100_000))):
                df = pd.DataFrame.from_records(batch, columns=cols)
                if df.empty:
                    continue
                out = Transformer().run(df, kwargs["source_table"])
                ld.run(out); rows_loaded += len(out)
            tgt.commit()
            logger.info("[full_stream][fallback] total=%s", rows_loaded)
