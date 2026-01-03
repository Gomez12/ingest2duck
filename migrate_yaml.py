#!/usr/bin/env python3
# migrate_yaml.py - Convert old v1 YAML to new v2 multi-source structure
from __future__ import annotations

import sys
from typing import Any, Dict, Optional

from utils import load_yaml, save_yaml, deep_get


def migrate_yaml_v1_to_v2(yaml_path: str, dry_run: bool = False) -> Dict[str, Any]:
    """Convert v1 (single source) to v2 (multi-source) structure"""
    y = load_yaml(yaml_path)
    
    # Check if already v2
    if "sources" in y:
        print(f"File is already v2 format: {yaml_path}")
        return y
    
    # Check if v1 format
    if "run" not in y or "source" not in y.get("run", {}):
        print(f"File is not v1 format: {yaml_path}")
        return y
    
    print(f"Migrating {yaml_path} from v1 to v2...")
    
    # Extract data from v1 format
    run = y["run"]
    src = run["source"]
    dst = run.get("destination", {})
    opts = run.get("options", {})
    outputs = y.get("outputs", {})
    
    # Generate source name from dataset
    dataset_name = dst.get("dataset", "data")
    source_name = dataset_name.lower().replace(" ", "_").replace("-", "_")
    
    # Build new sources array
    new_sources = [{
        "name": source_name,
        "url": src.get("url"),
        "file": src.get("file"),
        "format": src.get("format"),
        "records_path": src.get("records_path"),
        "sheet": src.get("sheet"),
        "delimiter": src.get("delimiter"),
        "encoding": src.get("encoding"),
        "timeout_s": src.get("timeout_s"),
        "member": src.get("member"),
        "collections": {}
    }]
    
    # Convert collections
    old_collections = y.get("collections", {})
    if old_collections:
        new_collections = {}
        for col_name, col_config in old_collections.items():
            # Remove existing source name prefix if any
            clean_name = col_name
            if clean_name.startswith(f"{source_name}_"):
                clean_name = clean_name[len(source_name)+1:]
            
            new_collections[clean_name] = {
                "sheet": col_config.get("sheet"),
                "path": col_config.get("path"),
                "pk": col_config.get("pk"),
                "parent": col_config.get("parent"),
                "parent_fk": col_config.get("parent_fk"),
                "enabled": col_config.get("enabled", True),
            }
        
        new_sources[0]["collections"] = new_collections
    
    # Convert xml_infer if exists
    xml_infer = y.get("xml_infer")
    new_xml_infer = {}
    if xml_infer:
        new_xml_infer[source_name] = xml_infer
    
    # Build new structure
    new_y = {
        "version": y.get("version", 1),
        "sources": new_sources,
        "destination": dst,
        "options": opts,
        "outputs": outputs,
    }
    
    if new_xml_infer:
        new_y["xml_infer"] = new_xml_infer
    
    # Save
    if not dry_run:
        save_yaml(new_y, yaml_path)
        print(f"Saved migrated file: {yaml_path}")
        print(f"Source name: {source_name}")
        print(f"Collections migrated: {len(old_collections) if old_collections else 0}")
    else:
        print("Dry run - not saving")
        print(f"Source name: {source_name}")
        print(f"Collections would be migrated: {len(old_collections) if old_collections else 0}")
    
    return new_y


def main() -> None:
    import argparse
    
    ap = argparse.ArgumentParser()
    ap.add_argument("yaml_file", help="YAML file to migrate")
    ap.add_argument("--dry-run", action="store_true", help="Show what would change without saving")
    args = ap.parse_args()
    
    migrate_yaml_v1_to_v2(args.yaml_file, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
