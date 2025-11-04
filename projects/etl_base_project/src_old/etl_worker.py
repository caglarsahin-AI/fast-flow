import argparse
import json
from etl_manager import ETLManager
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_schema", type=str, required=True, help="Source schema name")
    parser.add_argument("--source_table", type=str, required=True, help="Source table name")
    parser.add_argument("--target_schema", type=str, required=True, help="Target schema name")
    parser.add_argument("--target_table", type=str, required=True, help="Target table name")
    parser.add_argument("--load_method", type=str, required=True, help="Loading method")
    parser.add_argument("--source_db_config", type=str, required=False, default="")
    parser.add_argument("--target_db_config", type=str, required=False, default="")
    parser.add_argument("--unique_key_column", type=str, required=False, help="Unique key column name")
    parser.add_argument("--date_column", type=str, required=False, help="Date column for filtering")
    parser.add_argument("--date_threshold", type=str, required=False, help="Threshold date for filtering")
    parser.add_argument("--date_start", type=str, required=False, help="Start date for filtering")
    parser.add_argument("--date_end", type=str, required=False, help="End date for filtering")
    parser.add_argument("--page_size", type=int, required=False, default=20000, help="Page size for pagination")
    parser.add_argument("--order_by", type=str, required=False, help="Order by column for pagination")
    parser.add_argument("--pagination", type=bool, required=False, default=False, help="Enable pagination")

    args = parser.parse_args()

    source_db_config = json.loads(args.source_db_config) if args.source_db_config else {}
    target_db_config = json.loads(args.target_db_config) if args.target_db_config else {}

    etl_manager = ETLManager(source_db_config, target_db_config)
    etl_manager.run_etl_task(**vars(args))

