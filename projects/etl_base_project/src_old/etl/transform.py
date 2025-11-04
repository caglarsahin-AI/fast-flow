import pandas as pd
from typing import List, Dict
from common.logger import logger

class Transformer:
    def run(self, df: pd.DataFrame, source_table: str) -> pd.DataFrame:
        try:
            #if not Transformer.__validate_data(df):
                #raise ValueError("Dönüştürülecek geçerli veri bulunamadı.")
        
            if source_table == "x":
                #df["etl_date"] = pd.Timestamp.now()  # **Fake ETL Tarihi Kolonu Ekleyelim**
                df['etl_date'] = pd.Timestamp.now().date()
                logger.info(f"[{source_table}] etl_date kolonu eklendi.")         

            return df
        except Exception as e:
            logger.error(f"Transformation error: {e}")
            raise

    @staticmethod
    def __validate_data(data: List[Dict]) -> bool:
        return bool(data)