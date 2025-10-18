# utilities/mapping_generator.py
from __future__ import annotations


import argparse
import datetime as _dt
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence
from airflow.models import Variable

import os, yaml, json

from projects.etl_base_project.src.common.database_connection import DatabaseConnection
from projects.etl_base_project.src.common.logger import logger


ColumnMeta = Dict[str, Any]


def _infer_sql_type(col: ColumnMeta) -> str:
    """Translate information_schema metadata into a reasonable SQL type string."""
    data_type = (col.get("data_type") or "").lower()
    char_len = col.get("character_maximum_length")
    numeric_precision = col.get("numeric_precision")
    numeric_scale = col.get("numeric_scale")

    if data_type in {"character varying", "varchar"}:
        if char_len and int(char_len) > 0:
            return f"VARCHAR({int(char_len)})"
        return "TEXT"
    if data_type in {"character", "char"}:
        if char_len and int(char_len) > 0:
            return f"CHAR({int(char_len)})"
        return "CHAR"
    if data_type == "text":
        return "TEXT"
    if data_type in {"numeric", "decimal"}:
        if numeric_precision:
            scale = int(numeric_scale or 0)
            return f"NUMERIC({int(numeric_precision)}, {scale})"
        return "NUMERIC"
    if data_type in {"integer", "int4"}:
        return "INTEGER"
    if data_type in {"bigint", "int8"}:
        return "BIGINT"
    if data_type in {"smallint", "int2"}:
        return "SMALLINT"
    if data_type in {"double precision", "float8"}:
        return "DOUBLE PRECISION"
    if data_type in {"real", "float4"}:
        return "REAL"
    if data_type in {"boolean", "bool"}:
        return "BOOLEAN"
    if data_type == "date":
        return "DATE"
    if data_type in {"timestamp", "timestamp without time zone"}:
        return "TIMESTAMP"
    if data_type in {"timestamp with time zone", "timestamptz"}:
        return "TIMESTAMPTZ"
    if data_type in {"time", "time without time zone"}:
        return "TIME"
    if data_type in {"time with time zone", "timetz"}:
        return "TIMETZ"

    return data_type.upper() or "TEXT"


def fetch_columns(conn: DatabaseConnection, schema: str, table: str) -> List[ColumnMeta]:
    sql = """
        SELECT column_name,
               data_type,
               character_maximum_length,
               numeric_precision,
               numeric_scale,
               is_nullable,
               column_default
          FROM information_schema.columns
         WHERE table_schema = %s
           AND table_name   = %s
         ORDER BY ordinal_position
    """
    logger.info("Inspecting %s.%s for mapping template", schema, table)
    rows = conn.query(sql, (schema, table))
    if not rows:
        raise ValueError(f"No columns found for {schema}.{table}")
    return rows

def _lookup_type_names(conn: DatabaseConnection, type_oids: Sequence[int]) -> Dict[int, str]:
    unique = sorted({oid for oid in type_oids if oid is not None})
    if not unique:
        return {}
    placeholders = ",".join(["%s"] * len(unique))
    sql = f"SELECT oid, format_type(oid, NULL) AS type_name FROM pg_type WHERE oid IN ({placeholders})"
    with conn.cursor(dict_cursor=False) as cur:
        cur.execute(sql, tuple(unique))
        return {row[0]: row[1] for row in cur.fetchall()}

def fetch_query_columns(conn: DatabaseConnection, sql: str) -> List[ColumnMeta]:
    wrapped_sql = f"SELECT * FROM ({sql}) AS source_subquery LIMIT 0"
    logger.info("Inspecting SQL query for mapping template")
    with conn.cursor(dict_cursor=False) as cur:
        cur.execute(wrapped_sql)
        description = cur.description or []
    if not description:
        raise ValueError("SQL produced no columns to map")

    type_names = _lookup_type_names(conn, [col.type_code for col in description])
    columns: List[ColumnMeta] = []
    for col in description:
        type_name = type_names.get(col.type_code, "text")
        base_type = type_name.split("(")[0].strip()
        char_len = col.internal_size if col.internal_size and col.internal_size > 0 else None
        precision = col.precision if col.precision and col.precision > 0 else None
        scale = col.scale if col.scale and col.scale > 0 else None
        columns.append(
            {
                "column_name": col.name,
                "data_type": base_type,
                "character_maximum_length": char_len,
                "numeric_precision": precision,
                "numeric_scale": scale,
            }
        )
    return columns

def build_mapping(columns: Iterable[ColumnMeta], *, source: Dict[str, Any]) -> Dict[str, Any]:
    generated = _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    column_entries = []
    for col in columns:
        name = col["column_name"]
        entry = {
            "source": name,
            "target": name,
            "type": _infer_sql_type(col),
        }
        column_entries.append(entry)

    return {
        "version": "1.0",
        "mapping_type": "column",
        "generated_at": generated,
        "source": {
            "schema": source.get("schema"),
            "table": source.get("table"),
            "sql":source.get("sql")
        },
        "columns": column_entries,
    }


def write_mapping(mapping: Dict[str, Any], output_path: Path, *, overwrite: bool = True) -> Path:
    if output_path.exists() and  overwrite:
        raise FileExistsError(f"Mapping file already exists: {output_path}")

    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=True, exist_ok=True)

    header = [
        "# Auto-generated mapping template",
        "# Adjust target names or add transform blocks as needed.",
        "",
    ]
    body = yaml.safe_dump(mapping, sort_keys=False, allow_unicode=False)
    output_path.write_text("\n".join(header) + body, encoding="utf-8")
    logger.info("Mapping template written to %s", output_path)
    return output_path


def generate_mapping_file(
    db_conf: Dict[str, Any],
    *,
    schema: Optional[str] = None,
    table: Optional[str] = None,
    sql_file: Optional[Path] = None,
    output: Path,
    overwrite: bool = True
) -> Path:
    with DatabaseConnection(**db_conf) as conn:
         sql = sql_file.read_text(encoding="utf-8") if sql_file else None
         columns = fetch_columns(conn, schema, table) if not sql  else fetch_query_columns(conn = conn, sql= sql)
    source = {"schema": schema, "table": table, "sql": sql}
    mapping = build_mapping(columns, source=source)
    return write_mapping(mapping, output, overwrite=overwrite)


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a mapping.yaml template for a source table.",
    )
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--database", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--schema", required=False, help="Source schema name")
    parser.add_argument("--table", required=False, help="Source table name")
    parser.add_argument("--sql_file", required=False, help="Source sql pile path")
    parser.add_argument("--output", required=True, help="Path to the mapping.yaml file that will be created.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite the mapping file if it already exists.")
    return parser.parse_args(argv)




def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)
    db_conf = {
        "host": args.host,
        "port": args.port,
        "database": args.database,
        "user": args.user,
        "password": args.password,
        "autocommit": False,
    }

    output = Path(args.output)
    sql_file =  Path(args.sql_file) if args.sql_file else None
    try:
        generate_mapping_file(
            db_conf,
            schema=args.schema,
            table=args.table,
            sql_file=sql_file,
            output=output,
            overwrite=args.overwrite,
        )
    except Exception as exc:
        logger.error("Failed to generate mapping: %s", exc)
        raise SystemExit(1) from exc

"""
def start_main(table_name: str , output_path: str) -> None:

    CURRENT_DIR = os.path.dirname(__file__)
    CONFIG_PATH = os.path.abspath(
    os.path.join(
        CURRENT_DIR,   # etl_base_project/src/utilities
        "../../..",  # dags klasöründen çık
        "ocean/ocn_iss/level1/src_to_stg"
    )
    )
    CONFIG_FILE = os.path.join(CONFIG_PATH, "config_group_1.yaml")


    with open(CONFIG_FILE, "r") as f:
        config = yaml.safe_load(f)
    #source_schema: "ocn_iss"
    #source_table: "crd_account_balance_used_hist"

    etl_tasks     = config["etl_tasks"]
    source_db_var = config["source_db_var"]
    def get_db_config_from_airflow(var_name: str):        
        try:
            return json.loads(Variable.get(var_name))
        except KeyError:
            return None

    source_db_conf = get_db_config_from_airflow(source_db_var)
    print(f"source_db_var {source_db_var}")
    print(f"source_db_conf {source_db_conf}")
    db_conf = {
        "host": source_db_conf.get("host"),
        "port": source_db_conf.get("port"),
        "database": source_db_conf.get("database"),
        "user": source_db_conf.get("user"),
        "password": source_db_conf.get("password"),
        "autocommit": False,
    }

    record = next((t for t in etl_tasks if t.get("source_table") == table_name),
                  None
                )
    print(f"Record={record}")

    #projects/ocean/ocn_iss/level1/src_to_stg/crd_account_balance_used_hist/mapping.yaml

    output = Path(output_path+"/mapping.yaml")
    try:
        generate_mapping_file(
            db_conf=source_db_conf,
            schema=record["source_schema"],
            table=record["source_table"],
            output=output,
            overwrite=True
        )
    except Exception as exc:
        logger.error("Failed to generate mapping: %s", exc)
        raise SystemExit(1) from exc
""" 
        
if __name__ == "__main__":
    main()


    #docker exec -it airflow_webserver python -c "from projects.etl_base_project.src.utilities import mapping_generator as mg; mg.start_main('crd_account_balance_used_hist','/opt/airflow/projects/ocean/ocn_iss/level1/src_to_stg/crd_account_balance_used_hist')"

    #docker exec -it airflow_webserver python -c "from projects.etl_base_project.src.utilities import mapping_generator as mg; mg.start_main('crd_account_balance_used_hist','/opt/airflow/projects/ocean/ocn_iss/level1/src_to_stg/crd_account_balance_used_hist')"