# transform.py
from __future__ import annotations
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from projects.etl_base_project.src.common.logger import logger


class Transformer:
    """
    Batch-safe & vektörize dönüşümler.
    - Mevcut imzayı bozmadan: run(df, source_table) -> df
    - Tablolara özel kurallar: TABLE_RULES
    - Boşluk temizleme, NaN/None normalize, tip dönüştürme
    """

    # Tablo bazlı kurallar (örnek)
    TABLE_RULES: Dict[str, Dict] = {
        "x": {
            "add_etl_date": True,         # etl_date (DATE) eklensin
            "strip_columns": ["name", "surname"],  # trim uygulanacak kolonlar
            "parse_dates": {"txn_date": "%Y-%m-%d"},  # strptime formatı (yoksa infer)
            "to_numeric": ["amount", "fee"],         # sayıya döndür
            "empty_to_null": ["name", "surname", "description"],  # "" -> None
            # (opsiyonel) hedef kolon sırası – Loader ile uyum için
            "target_columns": None,
        }
        # "other_table": {...}
    }

    def __init__(self, *, default_tz: Optional[str] = "Europe/Istanbul"):
        self.default_tz = default_tz

    def run(self, df: pd.DataFrame, source_table: str) -> pd.DataFrame:
        try:
            if df is None or df.empty:
                return df

            rules = self.TABLE_RULES.get(source_table, {})

            # 1) NA normalizasyonu: boş stringleri istenen kolonlarda NULL yap
            empty_to_null_cols = rules.get("empty_to_null") or []
            if empty_to_null_cols:
                for col in empty_to_null_cols:
                    if col in df.columns:
                        # "" veya sadece whitespace → NaN
                        df[col] = df[col].replace(r"^\s*$", np.nan, regex=True)

            # 2) String temizliği: strip
            strip_cols = rules.get("strip_columns") or []
            if strip_cols:
                for col in strip_cols:
                    if col in df.columns and pd.api.types.is_string_dtype(df[col]):
                        # vektörize strip; NaN'ler etkilenmez
                        df[col] = df[col].str.strip()

            # 3) Tarih parse (vektörize)
            parse_dates = rules.get("parse_dates") or {}
            for col, fmt in parse_dates.items():
                if col in df.columns:
                    if fmt:
                        df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce", utc=False)
                    else:
                        df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)
                    # timezone istenirse:
                    if self.default_tz and pd.api.types.is_datetime64_any_dtype(df[col]):
                        # naive ise tz-localize; aware ise tz-convert
                        if df[col].dt.tz is None:
                            df[col] = df[col].dt.tz_localize(self.default_tz, nonexistent="shift_forward", ambiguous="NaT")
                        else:
                            df[col] = df[col].dt.tz_convert(self.default_tz)

            # 4) Sayısal parse
            to_numeric = rules.get("to_numeric") or []
            for col in to_numeric:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # 5) Tabloya özel alanlar
            if rules.get("add_etl_date"):
                # tek seferde alınan tarih; her satırda now() çağrısı yok
                etl_date = pd.Timestamp.now(tz=self.default_tz).date() if self.default_tz else pd.Timestamp.now().date()
                if "etl_date" not in df.columns:
                    df["etl_date"] = etl_date
                else:
                    # varsa üzerine yazma – idempotent davranış için sadece doldur
                    df["etl_date"] = df["etl_date"].fillna(etl_date)

            # 6) Kolon sırası (opsiyonel): Loader target sırasına uydur
            target_cols = rules.get("target_columns")
            if target_cols:
                # eksik olanlar safe şekilde eklenir, fazlalar sonda kalır
                ordered = [c for c in target_cols if c in df.columns]
                leftover = [c for c in df.columns if c not in ordered]
                df = df[ordered + leftover]

            # 7) Genel kalite: sonsuzları ve NaT’leri normalize et
            for col in df.columns:
                if pd.api.types.is_float_dtype(df[col]):
                    df[col].replace([np.inf, -np.inf], np.nan, inplace=True)

            return df

        except Exception as e:
            logger.error(f"[Transformer] {source_table} dönüşüm hatası: {e}")
            raise
