# json2duck.py  (library; no CLI)
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple

import dlt

from utils import sanitize_table_name, get_pk_from_record, stable_pk_from_fields, to_semiflat_row


@dataclass
class JsonCollection:
    name: str
    path: str
    pk_prefer: List[str]
    write_disposition: Optional[str] = None
    enabled: bool = True


@dataclass
class JsonMapping:
    collections: Dict[str, JsonCollection]


def _get_by_dotted_path(obj: Any, dotted: Optional[str]) -> Any:
    if not dotted:
        return obj
    cur = obj
    for part in dotted.split("."):
        if part == "":
            continue
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def _infer_pk_prefer_from_keys(keys: List[str]) -> List[str]:
    base = ["id", "ID", "code", "Code", "key", "Key", "pk", "PK"]
    prefer: List[str] = []
    lower = {k.lower(): k for k in keys}
    for b in base:
        if b in keys:
            prefer.append(b)
        else:
            hit = lower.get(b.lower())
            if hit:
                prefer.append(hit)
    # keep unique, keep stable
    out: List[str] = []
    for k in prefer:
        if k not in out:
            out.append(k)
    return out


def infer_json_mapping(source_name: str, file_path: str, fmt: str, records_path: Optional[str] = None) -> JsonMapping:
    fmt = (fmt or "auto").lower()
    collections: Dict[str, JsonCollection] = {}

    if fmt == "jsonl":
        collections[f"{source_name}_records"] = JsonCollection(
            name=f"{source_name}_records",
            path="$",
            pk_prefer=["id", "ID", "code", "Code", "key", "Key"],
        )
        return JsonMapping(collections=collections)

    # fmt == json
    with open(file_path, "r", encoding="utf-8") as f:
        root = json.load(f)

    if records_path:
        node = _get_by_dotted_path(root, records_path)
        collections[f"{source_name}_records"] = JsonCollection(
            name=f"{source_name}_records",
            path=f"$.{records_path}",
            pk_prefer=["id", "ID", "code", "Code", "key", "Key"],
        )
        return JsonMapping(collections=collections)

    # If root is list -> records
    if isinstance(root, list):
        first = next((x for x in root if isinstance(x, dict)), None)
        pk_pref = _infer_pk_prefer_from_keys(list(first.keys())) if isinstance(first, dict) else ["id", "ID", "code", "Code", "key", "Key"]
        collections[f"{source_name}_records"] = JsonCollection(name=f"{source_name}_records", path="$", pk_prefer=pk_pref)
        return JsonMapping(collections=collections)

    # If root is dict: collections for keys that are lists of dicts
    if isinstance(root, dict):
        for k, v in root.items():
            if isinstance(v, list) and v and all(isinstance(x, dict) for x in v[:50]):
                first = v[0] if isinstance(v[0], dict) else {}
                pk_pref = _infer_pk_prefer_from_keys(list(first.keys()))
                collections[f"{source_name}_{k}"] = JsonCollection(name=f"{source_name}_{k}", path=f"$.{k}", pk_prefer=pk_pref)
        if collections:
            return JsonMapping(collections=collections)

        # fallback single dict
        pk_pref = _infer_pk_prefer_from_keys(list(root.keys()))
        collections[f"{source_name}_root"] = JsonCollection(name=f"{source_name}_root", path="$", pk_prefer=pk_pref)
        return JsonMapping(collections=collections)

    # fallback scalar
    collections[f"{source_name}_value"] = JsonCollection(name=f"{source_name}_value", path="$", pk_prefer=[])
    return JsonMapping(collections=collections)


def iter_json_records(file_path: str, fmt: str, collection: JsonCollection, records_path_override: Optional[str] = None) -> Iterator[Dict[str, Any]]:
    fmt = (fmt or "auto").lower()

    if fmt == "jsonl":
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
                else:
                    yield {"value": obj}
        return

    # json
    with open(file_path, "r", encoding="utf-8") as f:
        root = json.load(f)

    if records_path_override:
        node = _get_by_dotted_path(root, records_path_override)
    else:
        # collection.path is "$" or "$.x.y"
        dotted = collection.path[2:] if collection.path.startswith("$.") else None
        node = _get_by_dotted_path(root, dotted)

    if isinstance(node, list):
        for item in node:
            if isinstance(item, dict):
                yield item
            else:
                yield {"value": item}
        return

    if isinstance(node, dict):
        yield node
        return

    yield {"value": node}


def build_json_resources(
    file_path: str,
    fmt: str,
    mapping: JsonMapping,
    source_name: str,
    raw_table: str,
    write_disposition: str = "append",
    records_path_override: Optional[str] = None,
    pk_fields_override: Optional[List[str]] = None,
):
    raw_table = sanitize_table_name(raw_table, "raw_ingest")

    @dlt.resource(name=raw_table, write_disposition=write_disposition)
    def raw_resource():
        for cname, col in mapping.collections.items():
            if not col.enabled:
                continue
            for obj in iter_json_records(file_path, fmt, col, records_path_override=records_path_override):
                yield {
                    "collection": cname,
                    "path": col.path,
                    "__source_name": source_name,
                    "raw_json": json.dumps(obj, ensure_ascii=False),
                }

    normalized_resources: Dict[str, Any] = {}

    for cname, col in mapping.collections.items():
        if not col.enabled:
            continue
        tname = sanitize_table_name(cname, "json_data")
        col_write_disposition = col.write_disposition or write_disposition

        @dlt.resource(name=tname, write_disposition=col_write_disposition)
        def _res(col=col, cname=cname):
            for obj in iter_json_records(file_path, fmt, col, records_path_override=records_path_override):
                if pk_fields_override:
                    pk = stable_pk_from_fields(obj, pk_fields_override)
                else:
                    pk = get_pk_from_record(obj, col.pk_prefer) or stable_pk_from_fields(obj, None)
                row = to_semiflat_row(obj, pk=str(pk), add_raw_json=True)
                row["__source_name"] = source_name
                if col.path != "$":
                    row["__json_records_path"] = col.path[2:]
                yield row

        normalized_resources[cname] = _res

    return raw_resource, normalized_resources
