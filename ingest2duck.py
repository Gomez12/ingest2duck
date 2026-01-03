#!/usr/bin/env python3
# ingest2duck.py  (dispatcher; the only CLI)
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import dlt

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

from utils import (
    ensure_common_mapping_v2,
    load_yaml,
    save_yaml,
    deep_get,
    deep_set,
    prepare_input,
    cleanup_prepared,
    build_dlt_destination,
    sanitize_table_name,
    get_all_sources,
    generate_run_id,
    compute_file_checksum,
    get_file_size,
    compute_url_checksum,
    compute_mapping_checksum,
    build_sources_resource,
    should_skip_source,
)
from xml2duck import infer_xml_mapping, build_xml_resources, XmlMapping, CollectionRule, PKRule
from json2duck import infer_json_mapping, build_json_resources, JsonMapping, JsonCollection
from csv2duck import (
    infer_tabular_mapping_for_csv,
    infer_tabular_mapping_for_xlsx,
    build_tabular_resources,
    TabularMapping,
    TabularCollection,
)


def rebuild_xml_mapping_from_yaml(source_name: str, xml_infer: Dict[str, Any]) -> XmlMapping:
    root = xml_infer["root"]
    cols: Dict[str, CollectionRule] = {}
    for cname, c in (xml_infer.get("collections") or {}).items():
        if not bool(c.get("enabled", True)):
            continue
        pk = PKRule(
            prefer=list((c.get("pk") or {}).get("prefer", ["@id", "id"]))
        )
        cols[cname] = CollectionRule(
            path=c["path"],
            pk=pk,
            parent=c.get("parent"),
            parent_fk=c.get("parent_fk"),
            enabled=True,
        )
    return XmlMapping(version=1, root=root, source_name=source_name, collections=cols)


def generate_new_mapping_template(mapping_path: str) -> None:
    mapping_name = os.path.splitext(os.path.basename(mapping_path))[0]
    duckdb_file = f"./{mapping_name}.duckdb"
    mapping_filename = os.path.basename(mapping_path)

    template = """# ingest2duck Mapping Template
# Generated automatically by --newmapping option
#
# Usage:
#   python ingest2duck.py --mapping """ + mapping_filename + """

# ============================================================================
# VERSION
# ============================================================================
version: 1

# ============================================================================
# SOURCES - Data Bron Definities
# ============================================================================
sources:
  # Voorbeeld 1: XLSX Bestand
  # - name: mijn_dataset
  #   url: https://example.com/data.xlsx
  #   # collections:
  #   #   commoditycodes:
  #   #     sheet: CN2025              # Specifiek tabblad naam
  #   #     use_first_sheet: true     # Of gebruik eerste tabblad

  # Voorbeeld 2: CSV Bestand
  # - name: sales_data
  #   url: https://example.com/sales.csv
  #   format: csv
  #   delimiter: ";"                  # Delimiter: comma (,), semicolon (;), tab (\t)
  #   encoding: utf-8

  # Voorbeeld 3: JSON Bestand
  # - name: api_data
  #   url: https://api.example.com/data.json
  #   records_path: results.items     # Dotted pad naar records (optioneel)

  # Voorbeeld 4: XML Bestand
  # - name: xml_data
  #   url: https://example.com/data.xml

  # Voorbeeld 5: Lokaal Bestand
  # - name: local_file
  #   file: ./data/myfile.xlsx

  # Voorbeeld 6: URL met Datum Placeholder
  # - name: daily_export
  #   url: https://example.com/export_{today:%Y%m%d}.xlsx
  #   # Datum placeholders: {today}, {yesterday}, {tomorrow}, {now}
  #   # Format strings: %Y (jaar), %m (maand), %d (dag), %H:%M:%S (tijd)

# ============================================================================
# DESTINATION - Waar wordt de data opgeslagen
# ============================================================================
destination:
  # Type: 'duckdb' (lokaal) of 'ducklake' (productie)
  # Start met duckdb voor testen, schakel later over naar ducklake
  type: duckdb

  # Dataset naam (wordt gebruikt als schema naam)
  dataset: """ + mapping_name + """

  # DuckDB specifieke instellingen
  duckdb_file: """ + duckdb_file + """

  # DuckLake specifieke instellingen (alleen als type: ducklake)
  # ducklake:
  #   ducklake_name: ducklake
  #   catalog: sqlite:///ducklake.sqlitedb
  #   storage: ./ducklake/
  #   replace_strategy: truncate-and-insert  # Opties: truncate-and-insert, insert-from-staging, staging-optimized

# ============================================================================
# OPTIONS - Globale Opties
# ============================================================================
options:
  # Automatisch mappings infereren als ze ontbreken (aanbevolen voor eerste run)
  infer_if_missing: true

  # Globale write disposition (kan per collectie worden overschreven)
  # append: voeg nieuwe data toe (default)
  # replace: vervang de volledige tabel
  # merge: update bestaande en voeg nieuwe toe (vereist PK)
  # skip: sla deze tabel over
  write_disposition: append

# ============================================================================
# OUTPUTS - Output Configuratie
# ============================================================================
outputs:
  raw:
    # Raw tabel: alle data in één tabel met metadata (handig voor debug)
    enabled: true
    table: raw_ingest

  normalized:
    # Genormaliseerd: per collectie een aparte tabel met platte kolommen
    enabled: true
    tables: []  # Wordt automatisch bijgewerkt na eerste run

# ============================================================================
# COLLECTIONS - Tabulaire Mappings (CSV/XLSX/JSON)
# ============================================================================
# collections:
#   # Naam moet beginnen met source_name (bijv. mijn_dataset_data)
#   mijn_dataset_data:
#     enabled: true                      # Collectie verwerken
#     pk:
#       prefer:                          # Voorkeursvolgorde voor primaire sleutel
#         - id
#         - ID
#         - code
#         - Code
#         - uuid
#     write_disposition: append          # Overschrijft globale write_disposition
#     # sheet: Sheet1                     # XLSX: specifiek tabblad
#     # use_first_sheet: false            # XLSX: gebruik eerste tabblad
#     # path: $.data.items                # JSON: dotted pad naar records

# ============================================================================
# XML_INFER - XML Specifieke Mappings
# ============================================================================
# xml_infer:
#   bronnaam:
#     root: IXF                           # Root element naam
#     collections:
#       tabelnaam:
#         enabled: true
#         path: /IXF/Units/Unit           # XPath naar entities
#         pk:
#           prefer:
#             - Code
#             - '@id'
#             - id
#         parent: null                    # Parent collectie naam (voor hiërarchie)
#         parent_fk: null                 # Foreign key kolom naam (meestal _parent_pk)
"""
    
    with open(mapping_path, "w", encoding="utf-8") as f:
        f.write(template)
    
    logger.info("✓ Created new mapping template: " + mapping_path)
    logger.info("✓ DuckDB destination: " + duckdb_file)
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. Edit " + os.path.basename(mapping_path) + " to add your source(s)")
    logger.info("  2. Run: python ingest2duck.py --mapping " + os.path.basename(mapping_path))
    logger.info("  3. Adjust mapping based on inferred results")
    logger.info("  4. Change destination.type to 'ducklake' for production")


def main() -> None:
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--newmapping",
        dest="newmapping",
        default=None,
        help="Create a new mapping template file",
    )

    ap.add_argument(
        "--mapping",
        default=None,
        help="Mapping YAML for run config + inferred collections",
    )

    ap.add_argument("--dataset", default=None, help="Override destination.dataset")
    ap.add_argument(
        "--duckdb-file",
        dest="duckdb_file",
        default=None,
        help="Override destination.duckdb_file",
    )
    ap.add_argument(
        "--destination-type",
        choices=["duckdb", "ducklake"],
        default=None,
        help="Override destination.type",
    )
    ap.add_argument(
        "--ducklake-name",
        default=None,
        help="Override destination.ducklake.ducklake_name",
    )
    ap.add_argument(
        "--ducklake-catalog",
        default=None,
        help="Override destination.ducklake.catalog",
    )
    ap.add_argument(
        "--ducklake-storage",
        default=None,
        help="Override destination.ducklake.storage",
    )
    ap.add_argument(
        "--ducklake-replace-strategy",
        choices=["truncate-and-insert", "insert-from-staging", "staging-optimized"],
        default="truncate-and-insert",
        help="Override destination.ducklake.replace_strategy",
    )

    ap.add_argument(
        "--infer-if-missing",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Infer collections if missing",
    )
    ap.add_argument(
        "--write-disposition",
        choices=["append", "replace", "merge", "skip"],
        default=None,
        help="Global write disposition",
    )
    ap.add_argument("--raw-table", default=None, help="Override outputs.raw.table")

    ap.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Forceer herverwerking van alle sources, ongeacht checksum",
    )

    args = ap.parse_args()

    if args.newmapping:
        if os.path.exists(args.newmapping):
            logger.error("Mapping file already exists: " + args.newmapping)
            logger.info("Delete the existing file first to create a new template")
            return
        
        generate_new_mapping_template(args.newmapping)
        return
    
    if not args.mapping:
        ap.error("--mapping is required (use --newmapping <file> to create a new template)")

    logger.info(f"Starting ingest2duck with mapping file: {args.mapping}")

    # Laad nieuwe multi-source structuur
    y = ensure_common_mapping_v2(load_yaml(args.mapping))
    logger.info("Loaded mapping configuration")
    mapping_checksum = compute_mapping_checksum(args.mapping)
    logger.debug(f"Mapping checksum: {mapping_checksum[:16]}...")

    # Generate run ID
    run_id = generate_run_id()
    logger.info(f"Run ID: {run_id}")

    # Haal alle sources op
    sources = get_all_sources(y)
    if not sources:
        raise ValueError("No sources defined")
    logger.info(f"Found {len(sources)} source(s) to process")

    # Apply CLI overrides
    dst = y["destination"]
    opts = y["options"]
    
    if args.dataset is not None:
        dst["dataset"] = args.dataset
    if args.duckdb_file is not None:
        dst["duckdb_file"] = args.duckdb_file
    if args.destination_type is not None:
        dst["type"] = args.destination_type
    if args.ducklake_name is not None:
        dst.setdefault("ducklake", {})["ducklake_name"] = args.ducklake_name
    if args.ducklake_catalog is not None:
        dst.setdefault("ducklake", {})["catalog"] = args.ducklake_catalog
    if args.ducklake_storage is not None:
        dst.setdefault("ducklake", {})["storage"] = args.ducklake_storage
    if args.ducklake_replace_strategy is not None:
        dst.setdefault("ducklake", {})["replace_strategy"] = args.ducklake_replace_strategy
    
    if args.infer_if_missing is not None:
        opts["infer_if_missing"] = bool(args.infer_if_missing)
    if args.write_disposition is not None:
        opts["write_disposition"] = args.write_disposition
    if args.raw_table is not None:
        deep_set(y, ["outputs", "raw", "table"], args.raw_table)

    # Destination & options
    dest_type = str(dst.get("type") or "duckdb").strip().lower()
    dataset = dst.get("dataset")
    infer_if_missing = bool(opts.get("infer_if_missing", False))
    global_write_disposition = str(opts.get("write_disposition") or "append")
    raw_table = str(deep_get(y, ["outputs", "raw", "table"], "raw_ingest"))
    raw_table = sanitize_table_name(raw_table, "raw_ingest")

    dest_obj, dest_kind, dest_meta = build_dlt_destination(dst, dataset)
    pipeline = dlt.pipeline(
        pipeline_name=f"ingest_{dest_kind}_{dataset}",
        destination=dest_obj,
        dataset_name=dataset,
    )
    logger.info(f"Created pipeline: ingest_{dest_kind}_{dataset}")

    # Process elke source
    all_raw_resources = []
    all_normalized_resources = {}
    all_prepared = []
    sources_metadata_updates: List[Dict[str, Any]] = []

    for source_config in sources:
        source_name = source_config["name"]
        logger.info(f"Processing source: {source_name}")

        source_url = source_config.get("url") or source_config.get("file", "")
        source_type = "url" if source_config.get("url") else "file"

        # Stap A: Checksum berekenen (VOOR prepare_input) - voorlopige check via HEAD request
        preliminary_checksum: Optional[str] = None
        source_size: Optional[int] = None
        timeout_s = int(source_config.get("timeout_s", 180))

        if source_config.get("url"):
            logger.info(f"  Checking URL checksum: {source_url}")
            try:
                preliminary_checksum = compute_url_checksum(source_url, timeout_s)
                if preliminary_checksum:
                    logger.info(f"  URL checksum (preliminary): {preliminary_checksum[:16]}...")
                else:
                    logger.info(f"  URL checksum not available via HEAD request, will compute after download")
            except Exception as e:
                logger.warning(f"  Could not compute URL checksum: {e}")
        else:
            fpath = source_config.get("file")
            if fpath and os.path.exists(fpath):
                logger.info(f"  Computing file checksum: {fpath}")
                try:
                    preliminary_checksum = compute_file_checksum(fpath)
                    source_size = get_file_size(fpath)
                    logger.info(f"  File checksum: {preliminary_checksum[:16]}...")
                except Exception as e:
                    logger.warning(f"  Could not compute file checksum: {e}")

        # Stap B: Check of skip met voorlopige checksum (VOOR download/prepare)
        if should_skip_source(source_name, preliminary_checksum, mapping_checksum, pipeline, dataset, args.force, check_type="preliminary"):
            logger.info(f"  Skipping '{source_name}' - preliminary checksum unchanged (use --force to override)")
            continue

        # Prepare input (download + detect fmt)
        prep = prepare_input(source_config)
        all_prepared.append(prep)

        try:
            fmt = prep.fmt

            # Stap D: Bereken exacte checksum en grootte van gedownloade file
            exact_checksum: Optional[str] = None
            try:
                exact_checksum = compute_file_checksum(prep.path)
                source_size = get_file_size(prep.path)
                logger.info(f"  Exact checksum: {exact_checksum[:16]}...")
            except Exception as e:
                logger.warning(f"  Could not compute file checksum: {e}")

            # Stap E: Check NOGMAALS met exacte checksum (NA download)
            if should_skip_source(source_name, exact_checksum, mapping_checksum, pipeline, dataset, args.force, check_type="exact"):
                logger.info(f"  Skipping '{source_name}' - exact and mapping checksums unchanged (use --force to override)")
                cleanup_prepared(prep)
                continue

            # Format-specific processing
            if fmt == "xml":
                # Check/Infer XML mapping
                xml_infer = y.get("xml_infer", {}).get(source_name)
                if not xml_infer or not xml_infer.get("collections"):
                    if not infer_if_missing:
                        raise ValueError(f"XML mapping for source '{source_name}' missing")
                    logger.info("  Inferring XML mapping...")
                    xm = infer_xml_mapping(source_name, prep.path)
                    if "xml_infer" not in y:
                        y["xml_infer"] = {}
                    y["xml_infer"][source_name] = {
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
                        }
                    }
                    save_yaml(y, args.mapping)
                    logger.info("  Saved XML mapping to configuration")
                else:
                    # Rebuild from yaml
                    xm = rebuild_xml_mapping_from_yaml(source_name, xml_infer)
                    logger.info(f"  Loaded {len(xm.collections)} XML collections from mapping")

                raw_res, norm_res = build_xml_resources(
                    xml_path=prep.path,
                    xml_mapping=xm,
                    source_name=source_name,
                    raw_table=raw_table,
                    write_disposition=global_write_disposition,
                )

            elif fmt in ("json", "jsonl"):
                # JSON processing
                collections_config = source_config.get("collections", {})

                # Infer if needed
                if not y.get("collections") or not any(c.startswith(f"{source_name}_") for c in y.get("collections", {})):
                    if not infer_if_missing:
                        raise ValueError(f"JSON mapping for source '{source_name}' missing")
                    logger.info("  Inferring JSON mapping...")
                    records_path = source_config.get("records_path")
                    jm = infer_json_mapping(source_name, prep.path, fmt=fmt, records_path=records_path)
                    logger.info(f"  Inferred {len(jm.collections)} JSON collections")

                    # Merge with existing collections
                    if "collections" not in y:
                        y["collections"] = {}
                    for cname, col in jm.collections.items():
                        y["collections"][cname] = {
                            "enabled": True,
                            "path": col.path,
                            "pk": {"prefer": col.pk_prefer},
                        }
                    save_yaml(y, args.mapping)
                    logger.info("  Saved JSON mapping to configuration")

                # Rebuild JsonMapping
                json_collections: Dict[str, JsonCollection] = {}
                for cname, c in (y.get("collections") or {}).items():
                    if not cname.startswith(f"{source_name}_"):
                        continue
                    if not bool(c.get("enabled", True)):
                        continue

                    # Override with source-specific config
                    col_config = collections_config.get(cname.replace(f"{source_name}_", ""), {})

                    json_collections[cname] = JsonCollection(
                        name=cname,
                        path=str(col_config.get("path") or c.get("path") or "$"),
                        pk_prefer=list((c.get("pk") or {}).get("prefer", ["id", "ID", "code", "Code", "key", "Key"])),
                        write_disposition=col_config.get("write_disposition"),
                        enabled=True,
                    )

                json_mapping = JsonMapping(collections=json_collections)
                logger.info(f"  Loaded {len(json_collections)} enabled JSON collections from mapping")

                raw_res, norm_res = build_json_resources(
                    file_path=prep.path,
                    fmt=fmt,
                    mapping=json_mapping,
                    source_name=source_name,
                    raw_table=raw_table,
                    write_disposition=global_write_disposition,
                    records_path_override=source_config.get("records_path"),
                )

            elif fmt in ("csv", "xlsx"):
                # Tabular processing
                tabular_collections_config = source_config.get("collections", {})
                default_table = "data"

                # Infer if needed
                if not y.get("collections") or not any(c.startswith(f"{source_name}_") for c in y.get("collections", {})):
                    if not infer_if_missing:
                        raise ValueError(f"Tabular mapping for source '{source_name}' missing")
                    logger.info("  Inferring tabular mapping...")

                    if fmt == "csv":
                        delim = str(source_config.get("delimiter") or ",")
                        enc = str(source_config.get("encoding") or "utf-8")
                        tm = infer_tabular_mapping_for_csv(source_name, default_table, prep.path, delim, enc)
                    else:
                        # XLSX: support multiple collections or default single
                        if tabular_collections_config:
                            # Multiple collections configured
                            tab_collections: Dict[str, TabularCollection] = {}
                            for col_name, col_cfg in tabular_collections_config.items():
                                sheet = col_cfg.get("sheet")
                                use_first_sheet = col_cfg.get("use_first_sheet", False)
                                tm = infer_tabular_mapping_for_xlsx(
                                    source_name, col_name, prep.path,
                                    sheet=sheet, use_first_sheet=use_first_sheet
                                )
                                tab_collections.update(tm.collections)
                            tm = TabularMapping(collections=tab_collections)
                        else:
                            # Default: single collection
                            sheet = source_config.get("sheet")
                            use_first_sheet = source_config.get("use_first_sheet", False)
                            tm = infer_tabular_mapping_for_xlsx(
                                source_name, default_table, prep.path,
                                sheet=sheet, use_first_sheet=use_first_sheet
                            )

                    logger.info(f"  Inferred {len(tm.collections)} tabular collections")

                    # Merge with existing collections
                    if "collections" not in y:
                        y["collections"] = {}
                    for cname, col in tm.collections.items():
                        y["collections"][cname] = {
                            "enabled": True,
                            "sheet": col.sheet,
                            "use_first_sheet": col.use_first_sheet,
                            "pk": {"prefer": col.pk_prefer or []},
                        }
                    save_yaml(y, args.mapping)
                    logger.info("  Saved tabular mapping to configuration")

                # Rebuild TabularMapping
                tabular_collections: Dict[str, TabularCollection] = {}
                for cname, c in (y.get("collections") or {}).items():
                    if not cname.startswith(f"{source_name}_"):
                        continue
                    if not bool(c.get("enabled", True)):
                        continue

                    # Override with source-specific config
                    col_name = cname.replace(f"{source_name}_", "")
                    col_config = tabular_collections_config.get(col_name, {})

                    tabular_collections[cname] = TabularCollection(
                        name=cname,
                        kind=fmt,
                        sheet=col_config.get("sheet") or c.get("sheet") or (source_config.get("sheet") if fmt == "xlsx" else None),
                        use_first_sheet=col_config.get("use_first_sheet", c.get("use_first_sheet", False)),
                        pk_prefer=list((c.get("pk") or {}).get("prefer", [])),
                        write_disposition=col_config.get("write_disposition"),
                        enabled=True,
                    )

                tab_mapping = TabularMapping(collections=tabular_collections)
                logger.info(f"  Loaded {len(tabular_collections)} enabled tabular collections from mapping")

                csv_delimiter = str(source_config.get("delimiter") or ",") if fmt == "csv" else ","
                csv_encoding = str(source_config.get("encoding") or "utf-8")
                
                raw_res, norm_res = build_tabular_resources(
                    file_path=prep.path,
                    fmt=fmt,
                    mapping=tab_mapping,
                    source_name=source_name,
                    raw_table=raw_table,
                    write_disposition=global_write_disposition,
                    delimiter=csv_delimiter,
                    encoding=csv_encoding,
                )

            else:
                raise ValueError(f"Unsupported format: {fmt}")

            # Stap F: Collect metadata voor sourcesmetadata tabel
            current_timestamp = datetime.now().isoformat() + "Z"

            # Check of dit een nieuwe source is
            existing_source = None
            try:
                with pipeline.sql_client() as client:
                    result = client.execute_sql(
                        "SELECT first_ingest_timestamp, last_source_checksum "
                        "FROM sourcesmetadata "
                        "WHERE source_name = %s AND dataset = %s",
                        source_name, dataset
                    )
                    if result is not None:
                        rows = list(result)
                        if rows and rows[0]:
                            existing_source = {
                                "first_ingest_timestamp": rows[0][0],
                                "last_source_checksum": rows[0][1],
                            }
            except Exception as e:
                logger.debug(f"Could not query sourcesmetadata table: {e}")

            # Update of new source record
            if existing_source:
                # Check of checksum gewijzigd
                update_run_id = (existing_source["last_source_checksum"] != exact_checksum)

                sources_metadata_updates.append({
                    "source_name": source_name,
                    "source_url": source_url,
                    "source_type": source_type,
                    "first_ingest_timestamp": existing_source["first_ingest_timestamp"],
                    "last_ingest_timestamp": current_timestamp,
                    "last_source_checksum": exact_checksum,
                    "last_preliminary_checksum": preliminary_checksum,
                    "last_mapping_checksum": mapping_checksum,
                    "last_source_size_bytes": source_size,
                    "format": fmt,
                    "last_run_id": run_id if update_run_id else None,
                })
            else:
                # Nieuwe source
                sources_metadata_updates.append({
                    "source_name": source_name,
                    "source_url": source_url,
                    "source_type": source_type,
                    "first_ingest_timestamp": current_timestamp,
                    "last_ingest_timestamp": current_timestamp,
                    "last_source_checksum": exact_checksum,
                    "last_preliminary_checksum": preliminary_checksum,
                    "last_mapping_checksum": mapping_checksum,
                    "last_source_size_bytes": source_size,
                    "format": fmt,
                    "last_run_id": run_id,
                })

            # Voeg resources toe aan lists
            all_raw_resources.append(raw_res)
            all_normalized_resources.update(norm_res)

        except Exception:
            # Cleanup only on error
            cleanup_prepared(prep)
            raise

    # Update sourcesmetadata table
    if sources_metadata_updates:
        logger.info(f"Updating sourcesmetadata table with {len(sources_metadata_updates)} records")
        pipeline.run(build_sources_resource(sources_metadata_updates, dataset))

    # Run raw
    if bool(deep_get(y, ["outputs", "raw", "enabled"], True)):
        logger.info(f"Running raw table ingestion to '{raw_table}'...")
        for raw_res in all_raw_resources:
            pipeline.run(raw_res)
        logger.info("Raw table ingestion completed")

    # Run normalized
    if bool(deep_get(y, ["outputs", "normalized", "enabled"], True)):
        # Auto-detect tables from normalized_resources
        tables = list(all_normalized_resources.keys())
        logger.info(f"Running normalized table ingestion for {len(tables)} tables...")
        for t in tables:
            logger.info(f"  - Processing table: {t}")
            pipeline.run(all_normalized_resources[t])
        logger.info("Normalized table ingestion completed")
    
    # Cleanup temp files after all ingestion is complete
    for prep in all_prepared:
        cleanup_prepared(prep)

    logger.info("Ingestion completed successfully")
    print("Done.")
    print(f"- run_id      : {run_id}")
    print(f"- destination : {dest_kind}")
    if dest_kind == "duckdb":
        print(f"- duckdb-file : {os.path.abspath(str(dst.get('duckdb_file')))}")
    else:
        print(f"- ducklake    : {dest_meta}")
    print(f"- dataset     : {dataset}")
    print(f"- mapping     : {os.path.abspath(args.mapping)}")
    print(f"- sources     : {', '.join(s['name'] for s in sources)}")


if __name__ == "__main__":
    main()
