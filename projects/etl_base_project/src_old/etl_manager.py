from etl.extract import Extractor
from etl.transform import Transformer
from etl.load import Loader
from common.database_connection import DatabaseConnection
from common.logger import logger

class ETLManager:
    def __init__(self, source_db_config: dict, target_db_config: dict) -> None:
        self.source_db_config = source_db_config
        self.target_db_config = target_db_config

    def run_etl_task(self, **kwargs) -> None:
        try:
            logger.info(f"Starting ETL for {kwargs['source_schema']}.{kwargs['source_table']} → {kwargs['target_schema']}.{kwargs['target_table']} \n Load method: {kwargs['load_method']}")
            src_schema = kwargs['source_schema']
            src_table = kwargs['source_table']
            tgt_schema = kwargs['target_schema']
            tgt_table = kwargs['target_table']
            load_method = kwargs.get('load_method')
            pagination = bool(kwargs.get('pagination', False))
            page_size = int(kwargs.get('page_size', 20000))
            with DatabaseConnection(**self.source_db_config) as src_conn, DatabaseConnection(**self.target_db_config) as tgt_conn:
                
                extractor  = Extractor(src_conn, tgt_conn, src_schema, src_table, tgt_schema, tgt_table)
                transformer = Transformer()
                loader = Loader(tgt_conn, extractor.get_column_metadata(), **kwargs)

                if pagination or load_method == 'pagination_dates':
                    if load_method == 'pagination_dates':
                        page_iter = extractor.get_paged_data(
                            date_column=kwargs['date_column'],
                            date_start=kwargs['date_start'],
                            date_end=kwargs['date_end'],
                            page_size=page_size
                        )
                    else:
                        page_iter = extractor.get_full_paged_data(
                            page_size=page_size,
                            order_by=kwargs.get('order_by')
                        )
                    for page_df  in page_iter:
                        transformed = transformer.run(page_df, src_table)
                        loader.run(transformed)

                    logger.info("Paginated load completed.")
                else:
                    df = extractor.run(**kwargs)
                    logger.info(f"Extracted {len(df)} rows in one batch.")

                    transformed = transformer.run(df, kwargs['source_table'])
                    logger.info(f"Transformation completed: {transformed.shape}")

                    loader.run(transformed)
                    logger.info("Load completed.")
                
        except Exception as e:
            logger.error(f"ETL task for {kwargs['source_schema']}.{kwargs['source_table']} failed: {e}")
            raise