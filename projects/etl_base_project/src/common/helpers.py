# common/helpers.py

from datetime import datetime, timedelta
import re
from dateutil.relativedelta import relativedelta

class Helpers:
    @staticmethod
    def parse_date_threshold(threshold_str: str) -> datetime:
        """
        threshold_str:
          - sabit: "now", "today", "yesterday"
          - dinamik: "<n>_days_ago", "<n>_weeks_ago", "<n>_months_ago", "<n>_years_ago"
          - ya da direkt sayısal: "YYYYMMDD" (8), "YYYYMMDDHHMMSS" (14)
        Dönen datetime:
          - "today"  -> şu anki an  (date_end için)
          - "yesterday" -> dünkü günün 00:00:00
          - "<n>_days_ago" vs. -> geri sayılmış günün 00:00:00
        """
        now = datetime.now()
        s = threshold_str.lower().strip()
        # Sabitler
        if s in ("now",):
            return now
        if s == "today":
            return now.replace(hour=0, minute=0, second=0, microsecond=0)
        if s == "yesterday":
            return (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0) 

        # Dinamik gün/hafta/ay/yıl geri sayımı
        m = re.match(r"(\d+)_days?_ago", s)
        if m:
            return (now - timedelta(days=int(m.group(1)))).replace(hour=0, minute=0, second=0, microsecond=0)

        m = re.match(r"(\d+)_weeks?_ago", s)
        if m:
            return (now - timedelta(weeks=int(m.group(1)))).replace(hour=0, minute=0, second=0, microsecond=0)

        m = re.match(r"(\d+)_months?_ago", s)
        if m:
            return (now - relativedelta(months=int(m.group(1)))).replace(hour=0, minute=0, second=0, microsecond=0)

        m = re.match(r"(\d+)_years?_ago", s)
        if m:
            return (now - relativedelta(years=int(m.group(1)))).replace(hour=0, minute=0, second=0, microsecond=0)

        # Direkt numeric string (YYYYMMDD veya YYYYMMDDHHMMSS)
        try:
            num = str(int(s))
        except ValueError:
            raise ValueError(f"Unsupported date_threshold format: {threshold_str}")

        if len(num) == 8:
            return datetime.strptime(num, "%Y%m%d")
        if len(num) == 14:
            return datetime.strptime(num, "%Y%m%d%H%M%S")

        raise ValueError(f"Unsupported date_threshold format: {threshold_str}")

    @staticmethod
    def convert_datetime_to_numeric(dt: datetime, length: int) -> int:
        """
        dt → integer string format:
          - length=8  → YYYYMMDD
          - length=14 → YYYYMMDDHHMMSS
        """
        if length == 8:
            return int(dt.strftime("%Y%m%d"))
        if length == 14:
            return int(dt.strftime("%Y%m%d%H%M%S"))
        raise ValueError(f"Unsupported numeric length: {length}")

    @staticmethod
    def infer_column_length(sample_val: str) -> int:
        """
        Örnek değerin karakter sayısını döner.
        """
        return len(str(sample_val).strip())
