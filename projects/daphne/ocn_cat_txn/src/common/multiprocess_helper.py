# multiprocess_helper.py
from multiprocessing import Pool
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional
import os
from common.database_connection import DatabaseConnection
from tempfile import SpooledTemporaryFile
from common.logger import logger

class MultiprocessRunner:
    """
    Gün bazlı ETL dispatch ve worker’lar.
    - init: source/target db conf + parse_date_threshold fonksiyonu
    - build_days, midnight: statik yardımcılar
    - process_day_worker: statik, her process kendi conn'unu açar
    - query_target_max: instance; kısa süreli target bağlantı
    - dispatch_days: instance; multiprocess yönetimi
    """

    def __init__(self,
                 source_db_conf: Dict[str, Any],
                 target_db_conf: Dict[str, Any],
                 parse_date_fn):
        self.source_db_conf = source_db_conf
        self.target_db_conf = self._ensure_autocommit(target_db_conf)
        self.parse_date_fn  = parse_date_fn

    # --- Küçük yardımcılar ---
    @staticmethod
    def midnight(v: Any, parse_fn) -> datetime:
        if isinstance(v, datetime):
            return datetime(v.year, v.month, v.day)
        if isinstance(v, date):
            return datetime(v.year, v.month, v.day)
        return parse_fn(str(v))

    @staticmethod
    def build_days(start_incl: Any, end_excl: Any, parse_fn) -> List[datetime]:
        ds = MultiprocessRunner.midnight(start_incl, parse_fn)
        de = MultiprocessRunner.midnight(end_excl, parse_fn)
        out: List[datetime] = []
        cur = ds
        while cur < de:
            out.append(cur)
            cur += timedelta(days=1)
        return out
    
    @staticmethod
    def _ensure_autocommit(conf: Dict[str, Any]) -> Dict[str, Any]:
        return conf
    
    # --- Worker (her process kendi bağlantısını açar) ---
    @staticmethod
    def process_day_worker(day_dt, task_kwargs, source_db_conf, target_db_conf):
        from etl.extract import Extractor
        from etl.load    import Loader
        import os, pandas as pd

        pid = os.getpid()
        with DatabaseConnection(**source_db_conf) as src_conn, \
            DatabaseConnection(**target_db_conf) as tgt_conn:

            extractor = Extractor(src_conn, tgt_conn,
                                task_kwargs['source_schema'], task_kwargs['source_table'],
                                task_kwargs['target_schema'], task_kwargs['target_table'])
            loader = Loader(tgt_conn, extractor.get_column_metadata(), **task_kwargs)
            date_col = task_kwargs['date_column']

            if task_kwargs.get("dedupe_daily", False):
                extractor.delete_target_day(task_kwargs['target_schema'],
                                            task_kwargs['target_table'],
                                            date_col, day_dt)

            if task_kwargs.get("passthrough_copy", False):
                cols = extractor.get_column_order()
                loader.prepare(pd.DataFrame(columns=cols))
                max_mb = int(task_kwargs.get("passthrough_spool_max_mb", 256))
                with SpooledTemporaryFile(max_size=max_mb * 1024 * 1024, mode="w+b") as spool:
                    extractor.copy_day_to_filelike(date_col, day_dt, cols, spool,
                                                key_column=task_kwargs.get("unique_key_column"))
                    spool.seek(0)
                    loader.copy_from_filelike(spool, cols)
                logger.info(f"[pid={pid}] {day_dt.date()} passthrough COPY tamam.")
                return

            key_col   = task_kwargs.get("unique_key_column")
            keyset_ps = int(task_kwargs.get("keyset_page_size", 0)) or None

            if key_col and keyset_ps:
                total = 0
                for chunk in extractor.iter_day_keyset(date_col, key_col, day_dt, keyset_ps):
                    loader.run(chunk); total += len(chunk)
                    logger.info(f"[pid={pid}] {day_dt.date()} keyset page: {len(chunk)}")
                logger.info(f"[pid={pid}] {day_dt.date()} toplam: {total}")
            else:
                df = extractor.get_day_data(date_col, day_dt)
                if df is not None and not df.empty:
                    loader.run(df)
                    logger.info(f"[pid={pid}] {day_dt.date()} yüklendi: {len(df)}")
                else:
                    logger.info(f"[pid={pid}] {day_dt.date()} - 0 satır")
    # --- Target MAX(date) ---
    def query_target_max(self, schema: str, table: str, date_col: str):
        with DatabaseConnection(**self.target_db_conf) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX({date_col}) FROM {schema}.{table}")
                return cur.fetchone()[0]

    # --- Günleri çalıştır ---
    def dispatch_days(self, days: List[datetime], task_kwargs: Dict[str, Any]) -> None:
        if not days:
            logger.info("Çalıştırılacak gün yok.")
            return

        mp = bool(task_kwargs.get("multiprocess", False))
        workers = max(1, int(task_kwargs.get("worker_count", 2)))

        if mp and len(days) > 1:
            with Pool(workers) as pool:
                pool.starmap(
                    MultiprocessRunner.process_day_worker,
                    [(d, task_kwargs, self.source_db_conf, self.target_db_conf) for d in days]
                )
        else:
            for d in days:
                MultiprocessRunner.process_day_worker(
                    d, task_kwargs, self.source_db_conf, self.target_db_conf
                )
