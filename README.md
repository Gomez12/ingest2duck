# ingest2duck

Data ingestion tool for loading XML, JSON, CSV, and XLSX files into DuckDB or DuckLake.

## Features

- Multi-source ingestion with automatic format detection
- XML, JSON, JSONL, CSV, and XLSX support
- Raw + normalized table output
- Automatic primary key inference
- Source checksum tracking with smart skipping
- DuckDB and DuckLake destinations

## Usage

```bash
python ingest2duck.py --mapping <config.yml> [options]
```

## CLI Options

### Core Options

- `--mapping PATH` (required): Mapping YAML for run config + inferred collections
- `--dataset NAME`: Override destination.dataset
- `--force`: Force reprocessing of all sources, ignoring checksums

### Destination Options

**DuckDB:**
- `--duckdb-file PATH`: Override destination.duckdb_file

**DuckLake:**
- `--ducklake-name NAME`: Override destination.ducklake.ducklake_name
- `--ducklake-catalog URL`: Override destination.ducklake.catalog
- `--ducklake-storage PATH`: Override destination.ducklake.storage
- `--ducklake-replace-strategy {truncate-and-insert,insert-from-staging,staging-optimized}`: Override replace strategy

### Processing Options

- `--infer-if-missing / --no-infer-if-missing`: Infer collections if missing
- `--write-disposition {append,replace,merge,skip}`: Global write disposition
- `--raw-table NAME`: Override outputs.raw.table

## Write Dispositions

### append
- Adds new data to existing tables
- No staging tables created (unless merge is used)
- Default for most resources
- Recommended for incremental data loading

### replace
- Truncates tables before inserting new data
- No staging tables created
- Useful for full refresh scenarios

### merge
- Updates existing rows and adds new ones
- **Always creates staging tables in DuckLake** (regardless of replace_strategy)
- Requires primary key or merge key to be defined
- Staging tables are created as `{dataset}_staging.{table_name}`
- Staging tables are automatically cleaned up after successful merge operations

**When Merge Creates Staging:**
- This is normal behavior in DuckLake
- Merge requires staging for atomic updates and conflict resolution
- Staging is independent of the destination `replace_strategy` setting
- To avoid staging, use `append` or `replace` dispositions instead

## DuckLake Replace Strategies

### truncate-and-insert (Default, Recommended)

- Direct inserts to destination tables
- No staging tables created
- Faster for small datasets
- No rollback capability on errors
- No validation before commit

**Recommended for:** ingest2duck workflows with smaller datasets

### insert-from-staging

- Load data to staging tables first
- Insert from staging to destination
- Atomic writes with rollback capability
- Slower for small datasets, but safer

**Recommended for:** Production workloads with strict consistency requirements

### staging-optimized

- Optimized staging workflow
- Best for large datasets
- Maximum safety and performance

**Recommended for:** Large batch jobs (>1GB per run)

## Source Checksum Tracking

The system implements two-step checksum checking for source files:

1. **URL Sources:**
   - HEAD request → E-tag/Content-Length checksum (if available)
   - Fast skip for URLs with E-tags
   - Fallback to download + exact checksum

2. **File Sources:**
   - Direct checksum computation
   - Instant skip if checksum unchanged

3. **Exact Checksum (After Download):**
    - SHA256 checksum of actual file
    - Accurate skip for all sources
    - Updates `sourcesmetadata` table

### sourcesmetadata Table

Stores metadata for each source:
- `source_name`: Primary key
- `source_url`: URL or file path
- `source_type`: "url" or "file"
- `first_ingest_timestamp`: Timestamp of first ingest
- `last_ingest_timestamp`: Timestamp of last ingest
- `last_source_checksum`: SHA256 checksum
- `last_source_size_bytes`: Size in bytes
- `format`: Data format (xml, json, csv, xlsx)
- `last_run_id`: Run ID when checksum last changed
- `dataset`: Dataset name

**Note:** The `sourcesmetadata` table uses `write_disposition="append"` to avoid creating staging tables. Using merge disposition would trigger staging table creation in DuckLake.

### CLI Force Option

Use `--force` to skip checksum checking and reprocess all sources:
```bash
python ingest2duck.py --mapping config.yml --force
```

## Example Configuration

```yaml
version: 1
sources:
  - name: cbs_data
    url: https://example.com/data.xlsx
    collections:
      commoditycodes:
        use_first_sheet: true

destination:
  type: ducklake
  ducklake:
    ducklake_name: my_lake
    catalog: sqlite:///catalog.db
    storage: ./storage/
    replace_strategy: truncate-and-insert
  dataset: cbs_all

options:
  infer_if_missing: true

outputs:
  raw:
    enabled: true
    table: raw_ingest
  normalized:
    enabled: true
    tables: []
```

## Tables

### Raw Table
Contains all ingested data with:
- `collection`: Collection name
- `path`: Source path/sheet
- `__source_name`: Reference to `sourcesmetadata` table
- `raw_json`: Full JSON representation

### Normalized Tables
Flattened tables per collection with:
- `_pk`: Primary key
- `raw_json`: Full JSON representation
- `__source_name`: Reference to `sourcesmetadata` table
- Collection-specific columns

### sourcesmetadata Table
Source metadata and checksum tracking (see above)
