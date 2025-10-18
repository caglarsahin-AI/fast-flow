#etl_manager
from __future__ import annotations
from datetime import datetime, timedelta, date
from typing import Any, Dict, Optional, List, Tuple
import pandas as pd
import os
from tempfile import SpooledTemporaryFile
from airflow.models import Variable
from projects.etl_base_project.src.utilities.dynamic_filters import resolve_where_with_bindings


from projects.etl_base_project.src.common.logger import logger
from projects.etl_base_project.src.common.database_connection import DatabaseConnection
from projects.etl_base_project.src.etl.extract import Extractor
from projects.etl_base_project.src.etl.transform import Transformer
from projects.etl_base_project.src.etl.load import Loader
from projects.etl_base_project.src.utilities.column_mapping import (load_mapping_items, mapping_to_select_and_outcols, mapping_to_ddl_columns)



def wait_for_debug():
    if os.getenv("ENABLE_DEBUG") == "1":
        import debugpy
        if not debugpy.is_client_connected():
            debugpy.listen(("0.0.0.0", 5678))
            print("🔎 Waiting for VS Code debugger on :5678 ...")
            debugpy.wait_for_client()
            debugpy.breakpoint()

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
        

    @staticmethod
    def _qident(name: str) -> str:
        return '"' + name.replace('"','""') + '"'

    @staticmethod
    def _resolve_mapping_path(kwargs: Dict[str, Any]) -> str:
        """
        column_mapping_mode=mapping_file ise mapping dosya yolunu bulur.
        Önce open-config'teki `mapping_file` alanına bakar,
        yoksa task_id tabanlı birkaç standart yerde arar.
        """
        # 1) YAML'da açıkça verilmiş mi?
        given = (kwargs.get("mapping_file") or "").strip()
        logger.info(f"given={given}---os.getcwd()={os.getcwd()}---- os.path.join(os.getcwd(), given)={os.path.join(os.getcwd(), given)} ")
        if given:
            if os.path.exists(given):
                return given
            # göreli yol ise proje köküne göre de bir kez deneyebilirsin
            alt = os.path.join(os.getcwd(), given)
            if os.path.exists(alt):
                return alt
            raise FileNotFoundError(f"Mapping file not found: {given}")

        # 2) Otomatik adaylar
        task_group_id = (kwargs.get("task_group_id") or "").strip()
        if not task_group_id:
            # son çare: target_table
            task_group_id = (kwargs.get("target_table") or "").strip()

        """base_env = os.environ.get("ETL_MAPPING_BASE", "").strip()
        dag_dir  = (kwargs.get("dag_dir") or "").strip()  # istersen DAG tarafında op_kwargs ile geçirilebilir
        """
        CURRENT_DIR = os.path.dirname(__file__)
        CONFIG_PATH = os.path.abspath(
            os.path.join(
                CURRENT_DIR,   # dags/ocean/ocn_iss/level1/src_to_ods
                "../../../../..",  # dags klasöründen çık
                "/projects/ocean/ocn_iss/level1/src_to_stg"
            )
        ) 

        task_dir = os.path.join(CONFIG_PATH, task_group_id)
        mapping_path = os.path.join(task_dir, "mapping.yaml")

        logger.info(f"CURRENT_DIR={CURRENT_DIR}---CONFIG_PATH={CONFIG_PATH}---- task_dir={task_dir} ---- mapping_path={mapping_path}")

        if os.path.exists(mapping_path):
                return mapping_path
        
        """candidates = []
        if base_env:
            candidates.append(os.path.join(base_env, task_id, "mapping.yaml"))
        candidates += [
            os.path.join("mappings", task_id, "mapping.yaml"),
            os.path.join("src_to_stg", task_id, "mapping.yaml"),
        ]"""

        """if task_dir:
            candidates.append(os.path.join(task_dir, "mapping.yaml"))

        for p in candidates:
            if p and os.path.exists(p):
                return p
        """
        
        raise FileNotFoundError(
            f"Mapping file not found for task_id='{task_group_id}'. "
            f"Tried: {mapping_path}. "
            f"Provide `mapping_file` in YAML or set ETL_MAPPING_BASE."
        )
       
    """ 
    def _ensure_table_with_mapping(
        self,
        tgt_conn,
        target_schema: str,
        target_table: str,
        load_method: str,
        ddl_cols: List[Tuple[str, str]],
    ) -> None:
        sch = self._qident(target_schema); tbl = self._qident(target_table)
        fq = f"{sch}.{tbl}"
        create_cols = ", ".join(f'{self._qident(n)} {t}' for n, t in ddl_cols)

        sql_create_if = f"CREATE TABLE IF NOT EXISTS {fq} ({create_cols});"
        sql_create    = f"CREATE TABLE {fq} ({create_cols});"
        sql_drop_if   = f"DROP TABLE IF EXISTS {fq};"
        sql_drop      = f"DROP TABLE {fq};"
        sql_trunc     = f"TRUNCATE TABLE {fq};"
        lm = (load_method or "").lower()

        with tgt_conn.cursor() as cur:
            if lm == "drop_and_create":
                logger.info("drop_and_create sql_drop= %s sql_create=%s",sql_drop,sql_create)
                cur.execute(sql_drop)
                cur.execute(sql_create)
            elif lm == "drop_if_exists_and_create": 
                logger.info("drop_if_exists_and_create sql_drop_if= %s sql_create=%s",sql_drop_if,sql_create)
                cur.execute(sql_drop_if)
                cur.execute(sql_create)
            elif lm == "create_if_not_exists_or_truncate":
                logger.info("create_if_not_exists_or_truncate sql_create_if= %s sql_trunc=%s",sql_create_if,sql_trunc)
                cur.execute(sql_create_if)
                cur.execute(sql_trunc)
            elif lm == "create_if_not_exists":
                logger.info("create_if_not_exists sql_create_if= %s ",sql_create_if)
                cur.execute(sql_create_if)
            else:
                raise ValueError(f"Unsupported load_method: {load_method}")
    """

    # ---- helpers ----
    @staticmethod
    def _build_select_sql(ex: Extractor, cols: List[str], where: Optional[str], order_by: Optional[str]) -> str:
        qual_src = f'"{ex.source_schema}"."{ex.source_table}"'
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        w = f"WHERE {where}" if (where and where.strip()) else ""
        o = f"ORDER BY {order_by}" if (order_by and order_by.strip()) else ""
        return f"SELECT {cols_sql} FROM {qual_src} {w} {o}"

    def run_etl_task(self, **kwargs) -> None:        
        method = kwargs.get("load_method")
        logger.info(f"run_etl_task start: method={method}--prepare_only={kwargs.get('prepare_only')}")
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
        mode = (kwargs.get("column_mapping_mode") or "source").lower()
        source_type    = (kwargs.get("source_type") or "table").lower()
        with DatabaseConnection(**self.source_db_config) as src_conn, \
             DatabaseConnection(**self.target_db_config) as tgt_conn:
            
            if  mode == "mapping_file":
                """ex  = Extractor(src_conn, tgt_conn,
                                kwargs["target_schema"], kwargs["target_table"],
                                source_schema=None, source_table=None,
                                sql_text= kwargs["sql_text"])
                """ 
                mapping_path = self._resolve_mapping_path(kwargs)
                maps = load_mapping_items(mapping_path)
                ddl_cols = mapping_to_ddl_columns(maps)
                """self._ensure_table_with_mapping(
                    tgt_conn, kwargs["target_schema"], kwargs["target_table"],
                    kwargs.get("load_method") or "", ddl_cols
                )"""
                # COPY sırası: mapping’teki target kolonlar
                _, out_cols = mapping_to_select_and_outcols(maps)
                ld = Loader(tgt_conn, {}, **kwargs)  
                ld.prepare(pd.DataFrame(columns=out_cols))
            elif mode == "source":
                ex  = Extractor(src_conn, tgt_conn,
                                kwargs["target_schema"], kwargs["target_table"],
                                source_schema=kwargs["source_schema"], source_table=kwargs["source_table"],
                                sql_text= None)
                """self._ensure_table_with_mapping(
                    tgt_conn, kwargs["target_schema"], kwargs["target_table"],
                    kwargs.get("load_method") or "", ddl_cols
                )"""

                ld = Loader(tgt_conn, ex.get_column_metadata(), **kwargs)
                # Kaynağın kolon sırası + Loader.prepare mevcut davranış
                cols = ex.get_column_order()
                ld.prepare(pd.DataFrame(columns=cols))
                # DDL: mevcut Loader.prepare zaten create_if_not_exists_or_truncate gibi modları yönetiyor
            else:
                    raise NotImplementedError(f"Henüz {source_type} aktarım yöntemi Etl_manager  da ayarlanmadı")

            tgt_conn.commit()

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
        passthrough_fmt  = (kwargs.get("passthrough_format") or "binary").lower()
        spool_mb         = int(kwargs.get("passthrough_spool_max_mb", 512))

        #wait_for_debug() 

        base_where_tpl = (kwargs.get("where") or "").strip() or None
        bindings_conf  = kwargs.get("bindings")
        order_by       = (kwargs.get("order_by") or "").strip() or None
        source_type    = (kwargs.get("source_type") or "table").lower()
        column_mapping_mode = (kwargs.get("column_mapping_mode") or "source").lower()
        sql_text = (kwargs.get("sql_text") or "").strip()
        #logger.info("_run_full_stream sql_text= %s", sql_text.strip())
        

        with DatabaseConnection(**self.source_db_config) as src_conn, \
             DatabaseConnection(**self.target_db_config) as tgt_conn:

            # dynamic bindings (varsa) çöz
            resolved_where = resolve_where_with_bindings(
                base_where_tpl, bindings_conf,
                src_conn=src_conn, tgt_conn=tgt_conn,
                airflow_vars_get=lambda k: Variable.get(k, default_var=None),
            )
            
            if source_type == "table":
                ex  = Extractor(src_conn, tgt_conn,
                                kwargs["target_schema"], kwargs["target_table"],
                                source_schema=kwargs["source_schema"], source_table=kwargs["source_table"],
                                sql_text= None)
            elif source_type == "sql":            # ---------------------------------------------
                ex  = Extractor(src_conn, tgt_conn,
                                kwargs["target_schema"], kwargs["target_table"],
                                source_schema=None, source_table=None,
                                sql_text= kwargs["sql_text"])

            else:
                    raise NotImplementedError(f"Henüz {source_type} aktarım yöntemi Etl_manager  da ayarlanmadı")
            ld  = Loader(tgt_conn, ex.get_column_metadata(), **kwargs)

            if source_type == "sql" and column_mapping_mode=="mapping_file":
                mapping_path = self._resolve_mapping_path(kwargs)   
                maps = load_mapping_items(mapping_path)
                select_items, out_cols = mapping_to_select_and_outcols(maps)
                ddl_cols = mapping_to_ddl_columns(maps)
                """self._ensure_table_with_mapping(
                    tgt_conn, kwargs["target_schema"], kwargs["target_table"],
                    kwargs.get("load_method") or "", ddl_cols
                )"""
                ld.prepare(pd.DataFrame(columns=out_cols))

                if passthrough_full:
                    from tempfile import SpooledTemporaryFile
                    with SpooledTemporaryFile(max_size=spool_mb * 1024 * 1024, mode="w+b") as buf:
                        ex.copy_select_to_filelike(
                            columns=None,
                            select_items=select_items,
                            file_obj=buf,
                            where=resolved_where,
                            order_by=order_by,
                            fmt=passthrough_fmt,
                        )
                        buf.seek(0)
                        ld.copy_from_filelike(file_obj=buf, columns=out_cols, fmt=passthrough_fmt)
                    return

                # fallback
                base_sql = getattr(ex, "sql_text", None) or kwargs.get("sql_text")
                if not base_sql:
                    raise ValueError("source_type=sql için sql_text zorunlu.")
                sql = (
                    "SELECT " + ", ".join(select_items) +
                    f" FROM ({base_sql}) AS _src" +
                    (f" WHERE {resolved_where}" if resolved_where else "") +
                    (f" ORDER BY {order_by}" if order_by else "")
                )
                """
                sql = sql_text + \
                        (" and " if "where" in sql_text.lower() else " where ") + (resolved_where if resolved_where else "") + \
                        (None if "order by" in sql_text.lower() else (f" order by {order_by}" if order_by else ""))
                """
                logger.info("[etl_manager] base_sql= %s ",base_sql)
                
                logger.info("[etl_manager] sql= %s ",sql)
                rows_loaded = 0
                for batch in src_conn.stream(sql=sql, batch_size=int(kwargs.get("batch_size", 50000))):
                    df = pd.DataFrame.from_records(batch, columns=out_cols)
                    if df.empty:
                        continue
                    ld.run(df); rows_loaded += len(df)
                tgt_conn.commit()
                return
            
            elif source_type == "table" and column_mapping_mode=="mapping_file":
                mapping_path = self._resolve_mapping_path(kwargs)   
                maps = load_mapping_items(mapping_path)
                select_items, out_cols = mapping_to_select_and_outcols(maps)
                ddl_cols = mapping_to_ddl_columns(maps)
                """self._ensure_table_with_mapping(
                    tgt_conn, kwargs["target_schema"], kwargs["target_table"],
                    kwargs.get("load_method") or "", ddl_cols
                )"""
                ld.prepare(pd.DataFrame(columns=out_cols))

                if passthrough_full:
                    from tempfile import SpooledTemporaryFile
                    with SpooledTemporaryFile(max_size=spool_mb * 1024 * 1024, mode="w+b") as buf:
                        ex.copy_select_to_filelike(
                            columns=None,
                            select_items=select_items,
                            file_obj=buf,
                            where=resolved_where,
                            order_by=order_by,
                            fmt=passthrough_fmt,
                        )
                        buf.seek(0)
                        ld.copy_from_filelike(file_obj=buf, columns=out_cols, fmt=passthrough_fmt)
                    return

                # fallback
                sql = "SELECT " + ", ".join(select_items) + \
                      f' FROM "{kwargs["source_schema"]}"."{kwargs["source_table"]}"' + \
                      (f" WHERE {resolved_where}" if resolved_where else "") + \
                      (f" ORDER BY {order_by}" if order_by else "")
                rows_loaded = 0
                for batch in src_conn.stream(sql=sql, batch_size=int(kwargs.get("batch_size", 50000))):
                    df = pd.DataFrame.from_records(batch, columns=out_cols)
                    if df.empty:
                        continue
                    ld.run(df); rows_loaded += len(df)
                tgt_conn.commit()
                return

            elif source_type == "table" and column_mapping_mode=="source":
                # --- source yolu (geriye uyum) ---
                cols = ex.get_column_order()
                """self._ensure_table_with_mapping(
                    tgt_conn, kwargs["target_schema"], kwargs["target_table"],
                    kwargs.get("load_method") or "", ddl_cols
                )"""
                ld.prepare(pd.DataFrame(columns=cols))  # mevcut Loader.prepare DDL’i/Truncate’ı yönetir

                if passthrough_full:
                    from tempfile import SpooledTemporaryFile
                    with SpooledTemporaryFile(max_size=spool_mb * 1024 * 1024, mode="w+b") as buf:
                        ex.copy_select_to_filelike(
                            columns=cols,
                            select_items=None,
                            file_obj=buf,
                            where=resolved_where,
                            order_by=order_by,
                            fmt=passthrough_fmt,
                        )
                        buf.seek(0)
                        ld.copy_from_filelike(file_obj=buf, columns=cols, fmt=passthrough_fmt)
                    return

                # fallback (eski stream → df → load)
                rows_loaded = 0
                select_sql = 'SELECT ' + ", ".join(f'"{c}"' for c in cols) + \
                             f' FROM "{kwargs["source_schema"]}"."{kwargs["source_table"]}"' + \
                             (f" WHERE {resolved_where}" if resolved_where else "") + \
                             (f" ORDER BY {order_by}" if order_by else "")
                for batch in src_conn.stream(sql=select_sql, batch_size=int(kwargs.get("batch_size", 50000))):
                    df = pd.DataFrame.from_records(batch, columns=cols)
                    if df.empty:
                        continue
                    ld.run(df); rows_loaded += len(df)
                tgt_conn.commit()
                return

            elif source_type=="csv":
                logger.error("csv will be developed")
                raise NotImplementedError("csv mode will be developed")
            else :
                logger.error("source_type and column_mapping_mode are not complimant")
                raise NotImplementedError("source_type and column_mapping_mode are not complimant")