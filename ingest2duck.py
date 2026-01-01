#!/usr/bin/env python3
# ingest2duck.py  (dispatcher; the only CLI)
from __future__ import annotations

import argparse
import logging
import os
from typing import Any, Dict, List, Optional

import dlt

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

from utils import (
    ensure_common_mapping,
    load_yaml,
    save_yaml,
    deep_set,
    deep_get,
    prepare_input,
    cleanup_prepared,
    build_dlt_destination,
    sanitize_table_name,
)
from xml2duck import infer_xml_mapping, build_xml_resources
from json2duck import infer_json_mapping, build_json_resources
from csv2duck import (
    infer_tabular_mapping_for_csv,
    infer_tabular_mapping_for_xlsx,
    build_tabular_resources,
)


def _default_table_from_source_name(source_name: str) -> str:
    base = os.path.basename(source_name)
    for ext in (".jsonl", ".ndjson", ".json", ".xml", ".csv", ".xlsx", ".gz", ".zip"):
        if base.lower().endswith(ext):
            base = base[: -len(ext)]
            break
    return sanitize_table_name(base, "data")


def main() -> None:
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--mapping",
        required=True,
        help="Mapping YAML for run config + inferred collections",
    )

    # source
    ap.add_argument("--url", default=None, help="Source URL (stored in run.source.url)")
    ap.add_argument(
        "--file", default=None, help="Local file path (stored in run.source.file)"
    )
    ap.add_argument(
        "--format",
        choices=["auto", "xml", "json", "jsonl", "csv", "xlsx"],
        default=None,
        help="Stored in run.source.format",
    )
    ap.add_argument(
        "--timeout-s",
        dest="timeout_s",
        type=int,
        default=None,
        help="Stored in run.source.timeout_s",
    )
    ap.add_argument(
        "--member",
        default=None,
        help="ZIP member to pick (stored in run.source.member)",
    )

    # json options
    ap.add_argument(
        "--records-path",
        default=None,
        help="For JSON: dotted path to list (stored in run.source.records_path)",
    )

    # xlsx options
    ap.add_argument(
        "--sheet",
        default=None,
        help="For XLSX: sheet name (stored in run.source.sheet)",
    )

    # csv options
    ap.add_argument(
        "--delimiter",
        default=None,
        help="CSV delimiter (stored in run.source.delimiter)",
    )
    ap.add_argument(
        "--encoding", default=None, help="CSV encoding (stored in run.source.encoding)"
    )

    # destination
    ap.add_argument("--dataset", default=None, help="Stored in run.destination.dataset")
    ap.add_argument(
        "--duckdb-file",
        dest="duckdb_file",
        default=None,
        help="Stored in run.destination.duckdb_file",
    )
    ap.add_argument(
        "--destination-type",
        choices=["duckdb", "ducklake"],
        default=None,
        help="Stored in run.destination.type",
    )
    ap.add_argument(
        "--ducklake-name",
        default=None,
        help="Stored in run.destination.ducklake.ducklake_name",
    )
    ap.add_argument(
        "--ducklake-catalog",
        default=None,
        help="Stored in run.destination.ducklake.catalog",
    )
    ap.add_argument(
        "--ducklake-storage",
        default=None,
        help="Stored in run.destination.ducklake.storage",
    )

    # run options
    ap.add_argument(
        "--infer-if-missing",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Infer collections if missing (stored in run.options.infer_if_missing)",
    )
    ap.add_argument(
        "--write-disposition",
        choices=["append", "replace"],
        default=None,
        help="Stored in run.options.write_disposition",
    )
    ap.add_argument("--raw-table", default=None, help="Stored in outputs.raw.table")
    ap.add_argument(
        "--table",
        default=None,
        help="For csv/xlsx/json single-stream: default collection/table name (stored in run.options.table)",
    )
    ap.add_argument(
        "--pk-fields",
        default=None,
        help="Comma-separated pk fields override for non-XML (stored in run.options.pk_fields)",
    )

    args = ap.parse_args()

    logger.info(f"Starting ingest2duck with mapping file: {args.mapping}")

    # load + defaults
    y = ensure_common_mapping(load_yaml(args.mapping))
    logger.info("Loaded mapping configuration")

    # CLI -> mapping.run
    if args.url is not None:
        deep_set(y, ["run", "source", "url"], args.url)
    if args.file is not None:
        deep_set(y, ["run", "source", "file"], args.file)
    if args.timeout_s is not None:
        deep_set(y, ["run", "source", "timeout_s"], int(args.timeout_s))
    if args.format is not None:
        deep_set(y, ["run", "source", "format"], args.format)
    if args.member is not None:
        deep_set(y, ["run", "source", "member"], args.member)

    if args.records_path is not None:
        deep_set(y, ["run", "source", "records_path"], args.records_path)
    if args.sheet is not None:
        deep_set(y, ["run", "source", "sheet"], args.sheet)
    if args.delimiter is not None:
        deep_set(y, ["run", "source", "delimiter"], args.delimiter)
    if args.encoding is not None:
        deep_set(y, ["run", "source", "encoding"], args.encoding)

    if args.dataset is not None:
        deep_set(y, ["run", "destination", "dataset"], args.dataset)
    if args.duckdb_file is not None:
        deep_set(y, ["run", "destination", "duckdb_file"], args.duckdb_file)
    if args.destination_type is not None:
        deep_set(y, ["run", "destination", "type"], args.destination_type)
    if args.ducklake_name is not None:
        deep_set(
            y, ["run", "destination", "ducklake", "ducklake_name"], args.ducklake_name
        )
    if args.ducklake_catalog is not None:
        deep_set(
            y, ["run", "destination", "ducklake", "catalog"], args.ducklake_catalog
        )
    if args.ducklake_storage is not None:
        deep_set(
            y, ["run", "destination", "ducklake", "storage"], args.ducklake_storage
        )

    if args.infer_if_missing is not None:
        deep_set(y, ["run", "options", "infer_if_missing"], bool(args.infer_if_missing))
    if args.write_disposition is not None:
        deep_set(y, ["run", "options", "write_disposition"], args.write_disposition)
    if args.raw_table is not None:
        deep_set(y, ["outputs", "raw", "table"], args.raw_table)
    if args.table is not None:
        deep_set(y, ["run", "options", "table"], args.table)
    if args.pk_fields is not None:
        deep_set(
            y,
            ["run", "options", "pk_fields"],
            [s.strip() for s in args.pk_fields.split(",") if s.strip()],
        )

    # persist early (so a "first run" always materializes run config)
    save_yaml(y, args.mapping)
    logger.info("Saved mapping configuration")

    run = y["run"]
    src = run["source"]
    dst = run["destination"]
    opts = run["options"]

    dataset = dst.get("dataset")
    dest_type = str(dst.get("type") or "duckdb").strip().lower()
    infer_if_missing = bool(opts.get("infer_if_missing", False))
    write_disposition = str(opts.get("write_disposition") or "append")
    raw_table = str(deep_get(y, ["outputs", "raw", "table"], "raw_ingest"))
    raw_table = sanitize_table_name(raw_table, "raw_ingest")

    pk_fields_override = opts.get("pk_fields") or None
    if isinstance(pk_fields_override, str):
        pk_fields_override = [
            s.strip() for s in pk_fields_override.split(",") if s.strip()
        ]

    # validation
    missing: List[str] = []
    if not (src.get("url") or src.get("file")):
        missing.append("run.source.url|file")
    if not dataset:
        missing.append("run.destination.dataset")
    if dest_type == "duckdb" and not dst.get("duckdb_file"):
        missing.append("run.destination.duckdb_file (required for type=duckdb)")
    if missing:
        raise ValueError("Missing required settings: " + ", ".join(missing))

    logger.info(
        f"Configuration validated - destination type: {dest_type}, dataset: {dataset}, write disposition: {write_disposition}"
    )

    # prepare input (download + unzip/gunzip + detect fmt)
    logger.info("Preparing input...")
    prep = prepare_input(src)
    try:
        fmt = prep.fmt
        source_name = prep.source_name

        logger.info(f"Detected format: {fmt}, source: {source_name}")

        # default table name
        default_table = sanitize_table_name(
            str(opts.get("table") or _default_table_from_source_name(source_name)),
            "data",
        )
        logger.info(f"Default table name: {default_table}")

        # infer mapping if needed per format
        # - XML uses its own structure under y["xml"] and y["collections_xml"] (we store as y["xml_infer"])
        # - JSON/tabular store into y["collections"] generic
        if fmt == "xml":
            logger.info("Processing XML format")
            # store inferred xml mapping under y["xml_infer"] for readability
            if not y.get("xml_infer") or not (y["xml_infer"].get("collections")):
                if not infer_if_missing:
                    raise ValueError(
                        "XML mapping missing. Set run.options.infer_if_missing=true or run once with --infer-if-missing."
                    )
                logger.info("Inferring XML mapping...")
                xm = infer_xml_mapping(prep.path)
                logger.info(f"Inferred {len(xm.collections)} XML collections")
                y["xml_infer"] = {
                    "root": xm.root,
                    "collections": {
                        cname: {
                            "enabled": True,
                            "path": rule.path,
                            "pk": {"prefer": rule.pk.prefer},
                            "parent": rule.parent,
                            "parent_fk": rule.parent_fk,
                        }
                        for cname, rule in xm.collections.items()
                    },
                }
                # normalized tables default
                y["outputs"]["normalized"]["tables"] = sorted(
                    list(xm.collections.keys())
                )
                save_yaml(y, args.mapping)
                logger.info("Saved XML mapping to configuration")

            # rebuild XmlMapping from yaml
            from xml2duck import XmlMapping, CollectionRule, PKRule

            xm_root = y["xml_infer"]["root"]
            xm_cols: Dict[str, CollectionRule] = {}
            for cname, c in (y["xml_infer"]["collections"] or {}).items():
                if not bool(c.get("enabled", True)):
                    continue
                pk = PKRule(
                    prefer=list((c.get("pk") or {}).get("prefer", ["@id", "id"]))
                )
                xm_cols[cname] = CollectionRule(
                    path=c["path"],
                    pk=pk,
                    parent=c.get("parent"),
                    parent_fk=c.get("parent_fk"),
                    enabled=True,
                )
            xml_mapping = XmlMapping(version=1, root=xm_root, collections=xm_cols)
            logger.info(f"Loaded {len(xm_cols)} enabled XML collections from mapping")

            logger.info("Building XML resources...")
            raw_res, norm_res_map = build_xml_resources(
                xml_path=prep.path,
                xml_mapping=xml_mapping,
                raw_table=raw_table,
                write_disposition=write_disposition,
            )

        elif fmt in ("json", "jsonl"):
            logger.info(f"Processing {fmt.upper()} format")
            # infer collections if missing
            if not y.get("collections"):
                if not infer_if_missing:
                    raise ValueError(
                        "JSON mapping missing. Set run.options.infer_if_missing=true or run once with --infer-if-missing."
                    )
                records_path = src.get("records_path")
                logger.info("Inferring JSON mapping...")
                jm = infer_json_mapping(prep.path, fmt=fmt, records_path=records_path)
                logger.info(f"Inferred {len(jm.collections)} JSON collections")
                y["collections"] = {
                    cname: {
                        "enabled": True,
                        "path": col.path,
                        "pk": {"prefer": col.pk_prefer},
                    }
                    for cname, col in jm.collections.items()
                }
                y["outputs"]["normalized"]["tables"] = sorted(
                    list(jm.collections.keys())
                )
                save_yaml(y, args.mapping)
                logger.info("Saved JSON mapping to configuration")

            # rebuild JsonMapping from yaml
            from json2duck import JsonMapping, JsonCollection

            cols: Dict[str, JsonCollection] = {}
            for cname, c in (y.get("collections") or {}).items():
                if not bool(c.get("enabled", True)):
                    continue
                cols[cname] = JsonCollection(
                    name=cname,
                    path=str(c.get("path") or "$"),
                    pk_prefer=list(
                        (c.get("pk") or {}).get(
                            "prefer", ["id", "ID", "code", "Code", "key", "Key"]
                        )
                    ),
                    enabled=True,
                )
            json_mapping = JsonMapping(collections=cols)
            logger.info(f"Loaded {len(cols)} enabled JSON collections from mapping")

            logger.info("Building JSON resources...")
            raw_res, norm_res_map = build_json_resources(
                file_path=prep.path,
                fmt=fmt,
                mapping=json_mapping,
                raw_table=raw_table,
                write_disposition=write_disposition,
                records_path_override=src.get("records_path"),
                pk_fields_override=pk_fields_override,
            )

        elif fmt in ("csv", "xlsx"):
            logger.info(f"Processing {fmt.upper()} format")
            # infer collections if missing
            if not y.get("collections"):
                if not infer_if_missing:
                    raise ValueError(
                        "Tabular mapping missing. Set run.options.infer_if_missing=true or run once with --infer-if-missing."
                    )
                if fmt == "csv":
                    delim = str(src.get("delimiter") or ",")
                    enc = str(src.get("encoding") or "utf-8")
                    logger.info(
                        f"Inferring CSV mapping with delimiter={delim}, encoding={enc}"
                    )
                    tm = infer_tabular_mapping_for_csv(
                        default_table, prep.path, delimiter=delim, encoding=enc
                    )
                else:
                    sheet = src.get("sheet")
                    logger.info(
                        f"Inferring XLSX mapping with sheet={sheet or 'default'}"
                    )
                    tm = infer_tabular_mapping_for_xlsx(
                        default_table, prep.path, sheet=sheet
                    )
                logger.info(f"Inferred {len(tm.collections)} tabular collections")

                y["collections"] = {
                    cname: {
                        "enabled": True,
                        "sheet": col.sheet,
                        "pk": {"prefer": col.pk_prefer or []},
                    }
                    for cname, col in tm.collections.items()
                }
                y["outputs"]["normalized"]["tables"] = sorted(
                    list(tm.collections.keys())
                )
                save_yaml(y, args.mapping)
                logger.info("Saved tabular mapping to configuration")

            # rebuild TabularMapping from yaml
            from csv2duck import TabularMapping, TabularCollection

            cols: Dict[str, TabularCollection] = {}
            for cname, c in (y.get("collections") or {}).items():
                if not bool(c.get("enabled", True)):
                    continue
                cols[cname] = TabularCollection(
                    name=cname,
                    kind=fmt,
                    sheet=c.get("sheet")
                    or (src.get("sheet") if fmt == "xlsx" else None),
                    pk_prefer=list((c.get("pk") or {}).get("prefer", [])),
                    enabled=True,
                )
            tab_mapping = TabularMapping(collections=cols)
            logger.info(f"Loaded {len(cols)} enabled tabular collections from mapping")

            delim = str(src.get("delimiter") or ",")
            enc = str(src.get("encoding") or "utf-8")

            logger.info("Building tabular resources...")
            raw_res, norm_res_map = build_tabular_resources(
                file_path=prep.path,
                fmt=fmt,
                mapping=tab_mapping,
                raw_table=raw_table,
                write_disposition=write_disposition,
                delimiter=delim,
                encoding=enc,
                pk_fields_override=pk_fields_override,
            )
        else:
            raise ValueError(f"Unsupported format: {fmt}")

        # destination + pipeline
        logger.info(f"Setting up destination: {dest_type}")
        dest_obj, dest_kind, dest_meta = build_dlt_destination(dst, str(dataset))
        pipeline = dlt.pipeline(
            pipeline_name=f"ingest_{dest_kind}_{dataset}",
            destination=dest_obj,
            dataset_name=dataset,
        )
        logger.info(f"Created pipeline: ingest_{dest_kind}_{dataset}")

        # run raw
        if bool(deep_get(y, ["outputs", "raw", "enabled"], True)):
            logger.info(f"Running raw table ingestion to '{raw_table}'...")
            pipeline.run(raw_res)
            logger.info("Raw table ingestion completed")

        # run normalized
        if bool(deep_get(y, ["outputs", "normalized", "enabled"], True)):
            tables = deep_get(y, ["outputs", "normalized", "tables"], []) or list(
                norm_res_map.keys()
            )
            # tables can be names; map by original key; resources are sanitized internally
            logger.info(
                f"Running normalized table ingestion for {len(tables)} tables..."
            )
            for t in tables:
                if t in norm_res_map:
                    logger.info(f"  - Processing table: {t}")
                    pipeline.run(norm_res_map[t])
            logger.info("Normalized table ingestion completed")

        logger.info("Ingestion completed successfully")
        print("Done.")
        print(f"- format      : {fmt}")
        print(f"- destination : {dest_kind}")
        if dest_kind == "duckdb":
            print(f"- duckdb-file : {os.path.abspath(str(dst.get('duckdb_file')))}")
        else:
            print(f"- ducklake    : {dest_meta}")
        print(f"- dataset     : {dataset}")
        print(f"- mapping     : {os.path.abspath(args.mapping)}")

    finally:
        logger.info("Cleaning up temporary files...")
        cleanup_prepared(prep)
        logger.info("Temporary files cleaned up")


if __name__ == "__main__":
    main()
