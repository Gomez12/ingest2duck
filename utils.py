# utils.py
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import logging
import os
import re
import tempfile
import uuid
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import requests
import yaml
import dlt

logger = logging.getLogger(__name__)


# -----------------------------
# Source checksum & metadata helpers
# -----------------------------
def compute_url_checksum(url: str, timeout_s: int = 180) -> Optional[str]:
    """
    Try HEAD request for E-tag/Content-Length checksum.
    Fallback: download and compute full checksum.
    """
    try:
        logger.debug(f"Checking URL via HEAD request: {url}")
        with requests.head(url, timeout=timeout_s) as r:
            r.raise_for_status()
            etag = r.headers.get("etag")
            content_length = r.headers.get("content-length")
            
            if etag:
                logger.debug(f"Using E-tag for checksum: {etag}")
                sha1 = hashlib.sha1()
                sha1.update(etag.encode("utf-8"))
                return sha1.hexdigest()
            
            if content_length:
                logger.debug(f"Using Content-Length for checksum: {content_length}")
                sha1 = hashlib.sha1()
                sha1.update(url.encode("utf-8"))
                sha1.update(content_length.encode("utf-8"))
                return sha1.hexdigest()
    except Exception as e:
        logger.debug(f"HEAD request failed: {e}")
    
    logger.debug("HEAD request insufficient, falling back to full download")
    return None


def build_sources_resource(sources_metadata: List[Dict[str, Any]], dataset: str):
    """
    Generate dlt resource for sourcesmetadata table.
    Uses append disposition with source_name as primary key.
    """
    @dlt.resource(name="sourcesmetadata", write_disposition="append", primary_key="source_name")
    def sources():
        for sm in sources_metadata:
            yield {
                "source_name": sm["source_name"],
                "source_url": sm["source_url"],
                "source_type": sm["source_type"],
                "first_ingest_timestamp": sm["first_ingest_timestamp"],
                "last_ingest_timestamp": sm["last_ingest_timestamp"],
                "last_source_checksum": sm["last_source_checksum"],
                "last_preliminary_checksum": sm["last_preliminary_checksum"],
                "last_mapping_checksum": sm["last_mapping_checksum"],
                "last_source_size_bytes": sm["last_source_size_bytes"],
                "format": sm["format"],
                "last_run_id": sm["last_run_id"],
                "dataset": dataset,
            }
    return sources


def should_skip_source(
    source_name: str,
    new_checksum: Optional[str],
    new_mapping_checksum: Optional[str],
    pipeline: dlt.Pipeline,
    dataset: str,
    force: bool,
    check_type: str = "exact"
) -> bool:
    """
    Check if source should be skipped.
    - check_type="preliminary": Vergelijk met last_preliminary_checksum
      Skip ALLEEN als preliminary_checksum onveranderd (om download te voorkomen)
    - check_type="exact": Vergelijk met last_source_checksum EN last_mapping_checksum
      Skip als source_checksum onveranderd EN mapping_checksum onveranderd
    - new_mapping_checksum: Huidige mapping checksum (wordt meegegeven)
    """
    if force:
        return False
    if not new_checksum:
        return False

    try:
        with pipeline.sql_client() as client:
            if check_type == "preliminary":
                result = client.execute_sql(
                    """
select (
  SELECT last_preliminary_checksum
  from sourcesmetadata
  WHERE source_name = %s AND dataset = %s and last_preliminary_checksum is not null
  order by last_ingest_timestamp desc
  limit 1
) as last_preliminary_checksum,
(
  SELECT last_mapping_checksum
  from sourcesmetadata
  WHERE source_name = %s AND dataset = %s and last_mapping_checksum is not null
  order by last_ingest_timestamp desc
  limit 1
) as last_mapping_checksum
 """,
                    source_name, dataset, source_name, dataset
                )
                if result is not None:
                    rows = list(result)
                    if rows and rows[0]:
                        stored_prelim = rows[0][0]
                        stored_mapping = rows[0][1]
                        logger.info(f"  Preliminary check: stored={stored_prelim[:16] if stored_prelim else None}..., new={new_checksum[:16] if new_checksum else None}..., stored_mapping={stored_mapping[:16] if stored_mapping else None}..., new_mapping={new_mapping_checksum[:16] if new_mapping_checksum else None}...")
                        if stored_prelim == new_checksum and stored_mapping is not None and stored_mapping == new_mapping_checksum:
                            logger.debug(f"Preliminary checksum and mapping match: {new_checksum[:16]}...")
                            return True
            else:
                result = client.execute_sql(
                    """
select (
  SELECT last_source_checksum
  from sourcesmetadata
  WHERE source_name = %s AND dataset = %s and last_source_checksum is not null
  order by last_ingest_timestamp desc
  limit 1
) as last_source_checksum,
(
  SELECT last_mapping_checksum
  from sourcesmetadata
  WHERE source_name = %s AND dataset = %s and last_mapping_checksum is not null
  order by last_ingest_timestamp desc
  limit 1
) as last_mapping_checksum
 """,
                    source_name, dataset, source_name, dataset
                )
                if result is not None:
                    rows = list(result)
                    if rows and rows[0]:
                        stored_exact = rows[0][0]
                        stored_mapping = rows[0][1]
                        logger.info(f"  Exact check: stored={stored_exact[:16] if stored_exact else None}..., new={new_checksum[:16] if new_checksum else None}..., stored_mapping={stored_mapping[:16] if stored_mapping else None}..., new_mapping={new_mapping_checksum[:16] if new_mapping_checksum else None}...")
                        if stored_exact == new_checksum and stored_mapping is not None and stored_mapping == new_mapping_checksum:
                            logger.debug(f"Exact and mapping checksums match")
                            return True
    except Exception as e:
        logger.warning(f"Could not check sourcesmetadata: {e}")
    
    return False


# -----------------------------
# YAML helpers
# -----------------------------
def ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)


def load_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_yaml(obj: Dict[str, Any], path: str) -> None:
    ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(obj, f, sort_keys=False, allow_unicode=True)


def deep_set(d: Dict[str, Any], path: List[str], value: Any) -> None:
    cur = d
    for p in path[:-1]:
        cur = cur.setdefault(p, {})
    cur[path[-1]] = value


def deep_get(d: Dict[str, Any], path: List[str], default: Any = None) -> Any:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


# -----------------------------
# Common mapping defaults
# -----------------------------
def ensure_common_mapping(y: Dict[str, Any]) -> Dict[str, Any]:
    y.setdefault("version", 1)
    y.setdefault("run", {})
    y["run"].setdefault("source", {})
    y["run"].setdefault("destination", {})
    y["run"].setdefault("options", {})
    y.setdefault("outputs", {})
    y["outputs"].setdefault("raw", {"enabled": True, "table": "raw_ingest"})
    y["outputs"].setdefault("normalized", {"enabled": True})
    y.setdefault("collections", {})  # for json/csv/xlsx; xml keeps own structure too
    # destination defaults
    y["run"]["destination"].setdefault("type", "duckdb")
    y["run"]["destination"].setdefault("ducklake", {})
    return y


def ensure_common_mapping_v2(mapping: Dict[str, Any]) -> Dict[str, Any]:
    mapping.setdefault("version", 1)
    mapping.setdefault("destination", {})
    mapping.setdefault("options", {})
    mapping.setdefault("outputs", {})
    mapping["outputs"].setdefault("raw", {"enabled": True, "table": "raw_ingest"})
    mapping["outputs"].setdefault("normalized", {"enabled": True})
    
    if "sources" not in mapping:
        raise ValueError("Missing 'sources' in mapping. Use migrate_yaml.py to convert old YAML files.")
    
    # Validate each source
    for source in mapping["sources"]:
        if "name" not in source:
            raise ValueError("Each source must have 'name'")
        if not (source.get("url") or source.get("file")):
            raise ValueError(f"Source '{source.get('name')}' must have 'url' or 'file'")
    
    # destination defaults
    mapping["destination"].setdefault("type", "duckdb")
    mapping["destination"].setdefault("ducklake", {})
    
    return mapping


def get_all_sources(mapping: Dict[str, Any]) -> List[Dict[str, Any]]:
    return mapping.get("sources", [])


def build_source_table_name(source_name: str, collection_name: str) -> str:
    return f"{source_name}_{collection_name}"


def generate_run_id() -> str:
    dt = datetime.now().strftime("%Y%m%d_%H%M%S")
    uid = str(uuid.uuid4())[:8]
    return f"ingest_{dt}_{uid}"


def compute_file_checksum(file_path: str) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def compute_mapping_checksum(mapping_path: str) -> str:
    sha256 = hashlib.sha256()
    with open(mapping_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def get_file_size(file_path: str) -> int:
    return os.path.getsize(file_path)


# -----------------------------
# URL Template Formatting
# -----------------------------
def format_url_template(url: str) -> str:
    """
    Format URL template with date/datetime placeholders.
    
    Supported placeholders:
    - {today} - current date
    - {yesterday} - yesterday's date
    - {tomorrow} - tomorrow's date
    - {now} - current datetime
    
    All placeholders support Python date/datetime format strings:
    - {today:%Y%m%d} - 20260103
    - {today:%Y-%m-%d} - 2026-01-03
    - {now:%Y%m%d%H%M%S} - 20260103143025
    
    Args:
        url: URL template with placeholders
        
    Returns:
        Formatted URL string
    """
    today = date.today()
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)
    now = datetime.now()
    
    placeholders = {
        'today': today,
        'yesterday': yesterday,
        'tomorrow': tomorrow,
        'now': now,
    }
    
    return url.format(**placeholders)


# -----------------------------
# IO (url/file) + zip/gz
# -----------------------------
def download_to_file(url: str, out_path: str, timeout_s: int = 180) -> None:
    logger.info(f"Downloading from {url} to {out_path} (timeout: {timeout_s}s)")
    with requests.get(url, stream=True, timeout=timeout_s) as r:
        r.raise_for_status()
        total_size = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0 and downloaded % (10 * 1024 * 1024) == 0:
                        logger.debug(
                            f"Downloaded {downloaded / 1024 / 1024:.1f} MB / {total_size / 1024 / 1024:.1f} MB"
                        )
    logger.info(f"Download completed: {out_path}")


def _guess_format_from_ext(path: str) -> Optional[str]:
    p = path.lower()
    if p.endswith(".xml"):
        return "xml"
    if p.endswith((".jsonl", ".ndjson")):
        return "jsonl"
    if p.endswith(".json"):
        return "json"
    if p.endswith(".csv"):
        return "csv"
    if p.endswith(".xlsx"):
        return "xlsx"
    return None


def _select_zip_member(
    z: zipfile.ZipFile, preferred_member: Optional[str] = None
) -> str:
    names = [n for n in z.namelist() if not n.endswith("/")]

    if not names:
        raise ValueError("ZIP contains no files.")

    if preferred_member:
        # exact match or basename match
        if preferred_member in names:
            return preferred_member
        b = os.path.basename(preferred_member)
        for n in names:
            if os.path.basename(n) == b:
                return n
        raise ValueError(f"ZIP member not found: {preferred_member}")

    # heuristic: choose by extension priority
    priority = ["xml", "jsonl", "json", "csv", "xlsx"]
    best = None
    best_score = -1
    for n in names:
        fmt = _guess_format_from_ext(n)
        if fmt is None:
            score = 0
        else:
            score = 100 - priority.index(fmt) if fmt in priority else 1
        # prefer top-level / smaller path depth
        depth_bonus = max(0, 10 - n.count("/"))
        score += depth_bonus
        if score > best_score:
            best_score = score
            best = n

    return best or names[0]


@dataclass
class PreparedInput:
    path: str
    fmt: str
    tempdirs: List[tempfile.TemporaryDirectory]
    source_name: str  # filename or url basename


def prepare_input(source: Dict[str, Any]) -> PreparedInput:
    """
    - source: run.source dict
      keys:
        url|file, timeout_s, format (xml/json/jsonl/csv/xlsx/auto), member (zip member), sheet, records_path
    - handles .zip and .gz (single file)
    """
    url = source.get("url")
    fpath = source.get("file")
    timeout_s = int(source.get("timeout_s", 180))
    declared_fmt = str(source.get("format") or "auto").lower()
    member = source.get("member")

    tempdirs: List[tempfile.TemporaryDirectory] = []

    if url:
        url = format_url_template(url)
        logger.info(f"Processing URL source: {url}")
        td = tempfile.TemporaryDirectory()
        tempdirs.append(td)
        base = os.path.basename(url.split("?")[0]) or "input"
        dl_path = os.path.join(td.name, base)
        download_to_file(url, dl_path, timeout_s=timeout_s)
        in_path = dl_path
        source_name = base
    else:
        logger.info(f"Processing local file: {fpath}")
        if not fpath:
            raise ValueError("Missing run.source.url or run.source.file")
        if not os.path.exists(fpath):
            raise FileNotFoundError(f"Input file not found: {fpath}")
        in_path = fpath
        source_name = os.path.basename(fpath)

    # unzip
    if in_path.lower().endswith(".zip"):
        logger.info("Detected ZIP archive, extracting...")
        td = tempfile.TemporaryDirectory()
        tempdirs.append(td)
        with zipfile.ZipFile(in_path, "r") as z:
            pick = _select_zip_member(z, preferred_member=member)
            logger.info(f"Selected ZIP member: {pick}")
            out_path = os.path.join(td.name, os.path.basename(pick))
            with z.open(pick) as src, open(out_path, "wb") as dst:
                dst.write(src.read())
        in_path = out_path
        source_name = os.path.basename(out_path)

    # gunzip
    if in_path.lower().endswith(".gz"):
        logger.info("Detected GZIP archive, decompressing...")
        td = tempfile.TemporaryDirectory()
        tempdirs.append(td)
        out_name = os.path.basename(in_path)[:-3] or "input"
        out_path = os.path.join(td.name, out_name)
        with gzip.open(in_path, "rb") as src, open(out_path, "wb") as dst:
            dst.write(src.read())
        in_path = out_path
        source_name = os.path.basename(out_path)

    # infer format
    if declared_fmt and declared_fmt != "auto":
        fmt = declared_fmt
        logger.info(f"Using declared format: {fmt}")
    else:
        fmt = _guess_format_from_ext(in_path) or "unknown"
        logger.info(f"Auto-detected format: {fmt}")

    if fmt not in ("xml", "json", "jsonl", "csv", "xlsx"):
        raise ValueError(
            f"Could not infer format from file name. Set run.source.format explicitly. path={in_path}"
        )

    return PreparedInput(
        path=in_path, fmt=fmt, tempdirs=tempdirs, source_name=source_name
    )


def cleanup_prepared(prep: PreparedInput) -> None:
    logger.debug(f"Cleaning up {len(prep.tempdirs)} temporary directory(s)")
    for td in reversed(prep.tempdirs):
        try:
            td.cleanup()
            logger.debug(f"Cleaned up: {td.name}")
        except Exception:
            pass


# -----------------------------
# Naming + PK + flatten
# -----------------------------
_TABLE_OK_RE = re.compile(r"[^a-zA-Z0-9_]+")


def sanitize_table_name(name: str, fallback: str = "data") -> str:
    s = (name or "").strip()
    if not s:
        return fallback
    s = s.replace("-", "_").replace(" ", "_")
    s = _TABLE_OK_RE.sub("", s)
    s = s.strip("_")
    if not s:
        return fallback
    if s[0].isdigit():
        s = f"t_{s}"
    return s[:120]


def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def stable_pk_from_fields(
    row: Dict[str, Any], pk_fields: Optional[List[str]] = None
) -> str:
    if pk_fields:
        parts: List[str] = []
        for k in pk_fields:
            v = row.get(k)
            if v is None:
                continue
            sv = str(v).strip()
            if sv:
                parts.append(sv)
        if parts:
            return sha1_text("|".join(parts))
    return sha1_text(json.dumps(row, sort_keys=True, ensure_ascii=False))


def get_pk_from_record(record: Dict[str, Any], prefer: List[str]) -> Optional[str]:
    if not prefer:
        return None
    # case-insensitive lookup
    lower_map = {str(k).lower(): k for k in record.keys()}
    for k in prefer:
        kk = str(k).strip()
        if not kk:
            continue
        hit = record.get(kk)
        if hit is None:
            alt = lower_map.get(kk.lower())
            if alt is not None:
                hit = record.get(alt)
        if hit is None:
            continue
        sv = str(hit).strip()
        if sv:
            return sv
    return None


def normalize_scalar(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    try:
        import datetime

        if isinstance(v, (datetime.datetime, datetime.date)):
            return v.isoformat()
    except Exception:
        pass
    return json.dumps(v, ensure_ascii=False)


def to_semiflat_row(
    obj: Dict[str, Any],
    pk: str,
    add_raw_json: bool = True,
) -> Dict[str, Any]:
    """
    Generic flatten:
      - _pk
      - raw_json (optional)
      - scalar keys as columns
      - nested/list -> json__<key> as JSON string
    """
    row: Dict[str, Any] = {"_pk": pk}
    if add_raw_json:
        row["raw_json"] = json.dumps(obj, ensure_ascii=False)

    for k, v in obj.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            row[k] = v
        else:
            row[f"json__{k}"] = json.dumps(v, ensure_ascii=False)

    return row


# -----------------------------
# Destination
# -----------------------------
def build_dlt_destination(dst: Dict[str, Any], dataset: str):
    dest_type = str(dst.get("type") or "duckdb").strip().lower()

    if dest_type == "duckdb":
        duckdb_file = dst.get("duckdb_file")
        if not duckdb_file:
            raise ValueError(
                "Missing run.destination.duckdb_file for destination.type=duckdb"
            )
        logger.info(f"Creating DuckDB destination: {duckdb_file}")
        return (
            dlt.destinations.duckdb(duckdb_file),
            "duckdb",
            {"duckdb_file": duckdb_file},
        )

    if dest_type == "ducklake":
        logger.info("Creating DuckLake destination")
        ducklake_cfg = dst.get("ducklake") or {}
        ducklake_name = ducklake_cfg.get("ducklake_name") or dataset
        catalog = ducklake_cfg.get("catalog")
        storage = ducklake_cfg.get("storage")
        replace_strategy = ducklake_cfg.get("replace_strategy") or "truncate-and-insert"
        logger.debug(
            f"DuckLake config - name: {ducklake_name}, catalog: {catalog}, storage: {storage}, replace_strategy: {replace_strategy}"
        )

        # rely on dlt secrets/config if nothing explicit
        if not ducklake_cfg.get("ducklake_name") and not catalog and not storage:
            try:
                return (
                    dlt.destinations.ducklake(replace_strategy=replace_strategy),
                    "ducklake",
                    {"mode": "config/auto", "replace_strategy": replace_strategy},
                )
            except Exception as e:
                raise ImportError(
                    'DuckLake requires: pip install "dlt[ducklake]"'
                ) from e

        creds = None
        try:
            from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials  # type: ignore

            creds = DuckLakeCredentials(
                ducklake_name=ducklake_name,
                catalog=catalog,
                storage=storage,
            )
        except Exception:
            creds = None

        try:
            if creds is not None:
                return (
                    dlt.destinations.ducklake(credentials=creds, replace_strategy=replace_strategy),
                    "ducklake",
                    {
                        "ducklake_name": ducklake_name,
                        "catalog": catalog,
                        "storage": storage,
                        "replace_strategy": replace_strategy,
                    },
                )
        except TypeError:
            pass

        try:
            return (
                dlt.destinations.ducklake(
                    ducklake_name=ducklake_name,
                    catalog=catalog,
                    storage=storage,
                    replace_strategy=replace_strategy,
                ),
                "ducklake",
                {
                    "ducklake_name": ducklake_name,
                    "catalog": catalog,
                    "storage": storage,
                    "replace_strategy": replace_strategy,
                },
            )
        except Exception as e:
            raise ImportError(
                'Could not initialize DuckLake destination. Ensure: pip install "dlt[ducklake]" '
                "and set run.destination.ducklake.{ducklake_name,catalog,storage}."
            ) from e

    raise ValueError(
        f"Unsupported run.destination.type={dest_type}. Use duckdb|ducklake."
    )
