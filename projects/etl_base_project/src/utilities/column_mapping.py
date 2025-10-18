# projects/etl_base_project/src/utilities/column_mapping.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple
import os, yaml

def _qident(name: str) -> str:
    return '"' + str(name).replace('"','""') + '"'

@dataclass(frozen=True)
class MapItem:
    sources: List[str]      # örn: ["first_name","last_name"]
    target:  str            # örn: "full_name"
    sql_type: str | None    # örn: "varchar(200)"
    transform: str | None   # örn: "{source[0]} || ' ' || {source[1]}"

def _load_yaml(path: str) -> Any:
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"Mapping file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _as_list(x) -> List[Any]:
    if x is None: return []
    if isinstance(x, list): return x
    return [x]

def load_mapping_items(path: str) -> List[MapItem]:
    y = _load_yaml(path)
    # ↓↓↓ BURAYI GÜNCELLE
    if isinstance(y, list):
        items = y
    else:
        items = y.get("columns") or y.get("select") or y.get("mappings") or []
    if not isinstance(items, list) or not items:
        raise ValueError("mapping.yaml must have a non-empty 'columns' (or 'select'/'mappings') list.")

    out: List[MapItem] = []
    for i, it in enumerate(items, 1):
        if not isinstance(it, dict):
            raise ValueError(f"mapping item #{i} must be an object")
        raw_src = it.get("source", None)
        srcs = _as_list(raw_src)  # None -> []
        target = it.get("target")
        if not target:
            raise ValueError(f"mapping item #{i} missing 'target'")
        sql_type = it.get("type")
        transform = it.get("transform")

        # source: null için izin; transform şart
        if len(srcs) == 0 and not transform:
            raise ValueError(f"mapping item #{i} has no 'source' and no 'transform'")

        out.append(MapItem(
            sources=[str(s) for s in srcs],
            target=str(target),
            sql_type=(str(sql_type) if sql_type else None),
            transform=(str(transform) if transform else None),
        ))
    return out

def render_sql_expr(item: MapItem) -> str:
    if item.transform:
        expr = item.transform
        # {source} → ilk kaynak (varsa)
        if item.sources:
            expr = expr.replace("{source}", _qident(item.sources[0]))
            # {source[i]} → ilgili kaynak
            for idx, src in enumerate(item.sources):
                expr = expr.replace(f"{{source[{idx}]}}", _qident(src))
        return expr

    # transform yoksa:
    if len(item.sources) == 0:
        # Kaynak yok → NULL döndür (veya istersen sabit '' ya da CURRENT_TIMESTAMP)
        return "NULL"
    if len(item.sources) == 1:
        return _qident(item.sources[0])

    # birden fazla kaynak → boşlukla birleştir
    parts: List[str] = []
    for j, s in enumerate(item.sources):
        if j > 0:
            parts.append(" ' ' ")
        parts.append(_qident(s))
    return " || ".join(parts)

def mapping_to_select_and_outcols(items: List[MapItem]) -> Tuple[List[str], List[str]]:
    select_items: List[str] = []
    out_cols: List[str] = []
    for it in items:
        expr = render_sql_expr(it)
        select_items.append(f"{expr} AS {_qident(it.target)}")
        out_cols.append(it.target)
    return select_items, out_cols

def mapping_to_ddl_columns(items: List[MapItem]) -> List[Tuple[str, str]]:
    cols: List[Tuple[str, str]] = []
    for it in items:
        t = it.sql_type or "text"
        cols.append((it.target, t))
    return cols
