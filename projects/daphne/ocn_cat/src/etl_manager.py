from etl.extract import Extractor
from etl.load import Loader
from common.database_connection import DatabaseConnection
from common.logger import logger

class ETLManager:
    def __init__(self, source_db_config: dict, target_db_config: dict) -> None:
        self.source_db_config = source_db_config
        self.target_db_config = target_db_config

    def start_etl(self, **kwargs) -> None:
        try:
            logger.info(f"Starting ETL for {kwargs['source_schema']}.{kwargs['source_table']} → {kwargs['target_schema']}.{kwargs['target_table']} \n Load method: {kwargs['load_method']}")
            src_schema = kwargs['source_schema']
            src_table = kwargs['source_table']
            tgt_schema = kwargs['target_schema']
            tgt_table = kwargs['target_table']
            with DatabaseConnection(**self.source_db_config) as src_conn, DatabaseConnection(**self.target_db_config) as tgt_conn:
                
                extractor  = Extractor(src_conn, tgt_conn, src_schema, src_table, tgt_schema, tgt_table)
                loader = Loader(tgt_conn, extractor.get_column_metadata(), **kwargs)

                df = extractor.run(**kwargs)
                logger.info(f"Extracted {len(df)} rows in one batch.")

                loader.run(df)
                logger.info("Load completed.")
                
        except Exception as e:
            logger.error(f"ETL task for {kwargs['source_schema']}.{kwargs['source_table']} failed: {e}")
            raise