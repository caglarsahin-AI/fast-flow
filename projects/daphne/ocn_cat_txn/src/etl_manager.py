# etl_manager.py
from datetime import datetime, timedelta, date
from typing import Any, Dict
from common.logger import logger
from common.helpers import Helpers
from common.multiprocess_helper import MultiprocessRunner

class ETLManager:
    def __init__(self, source_db_config: Dict[str, Any], target_db_config: Dict[str, Any]) -> None:
        self.source_db_config = source_db_config
        self.target_db_config = target_db_config
        self.runner = MultiprocessRunner(source_db_config, target_db_config, Helpers.parse_date_threshold)

    def run_etl_task(self, **kwargs) -> None:
        method = kwargs.get('load_method')
        if method == 'dates_by_day':
            self._run_dates_by_day(**kwargs)
        elif method == 'incremental_by_max_date':
            self._run_incremental_by_max_date(**kwargs)
        else:
            raise ValueError(f"Unknown load_method: {method}")

    # Backfill: verilen aralık
    def _run_dates_by_day(self, **kwargs) -> None:
        days = self.runner.build_days(kwargs['date_start'], kwargs['date_end'], self.runner.parse_date_fn)
        self.runner.dispatch_days(days, kwargs)

    # Incremental: target MAX → T-1 (exclusive end)
    def _run_incremental_by_max_date(self, **kwargs) -> None:
        raw_max = self.runner.query_target_max(kwargs['target_schema'],
                                               kwargs['target_table'],
                                               kwargs['date_column'])
        if raw_max is None:
            logger.info("Target boş (MAX yok). Incremental atlandı veya önce backfill çalıştırın.")
            return

        start_incl = self.runner.midnight(raw_max, self.runner.parse_date_fn) + timedelta(days=1)

        today = date.today()
        t_minus_1_midnight = datetime(today.year, today.month, today.day) - timedelta(days=1)
        end_excl = t_minus_1_midnight + timedelta(days=1)

        if start_incl >= end_excl:
            logger.info(f"Incremental aralık boş: start={start_incl.date()}, end(excl)={end_excl.date()}")
            return

        days = self.runner.build_days(start_incl, end_excl, self.runner.parse_date_fn)
        self.runner.dispatch_days(days, kwargs)
