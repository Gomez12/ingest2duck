# AGENTS.md

This file provides guidance for agentic coding assistants working on the ingest2duck repository.

## Build / Lint / Test Commands

**Run the main CLI:**
```bash
# Create a new mapping template
python ingest2duck.py --newmapping <path>

# Run with existing mapping
python ingest2duck.py --mapping <path> [options]
```

**Format validation:** No formal linting setup. Run `python -m py_compile <file>` to verify syntax.

**Testing:** No test framework exists. Manual testing via CLI execution.

## Code Style Guidelines

### Imports

Always start files with `from __future__ import annotations`.

Import order:
1. Standard library imports
2. Third-party imports (dlt, lxml, yaml, requests, openpyxl)
3. Local imports

Group imports alphabetically within each section:
```python
from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

import dlt
import yaml
from lxml import etree

from utils import sanitize_table_name, get_pk_from_record
```

### Type Annotations

- **Always** add type hints to function parameters and return types
- Use common types: `Any`, `Dict[str, Any]`, `List[str]`, `Optional[str]`, `Iterator[Dict[str, Any]]`, `Tuple`, `Union`
- Annotate all function parameters with types
- Use `-> None` for functions that don't return values
- No formal type checking (no mypy), but write annotations as if enabled
- Use `# type: ignore` sparingly, prefer `Dict[str, Any]` over `dict` for clarity

### Naming Conventions

- **Functions:** `snake_case` (e.g., `infer_json_mapping`, `sanitize_table_name`)
- **Variables:** `snake_case` (e.g., `file_path`, `raw_table`, `write_disposition`)
- **Classes:** `PascalCase` (e.g., `JsonMapping`, `CollectionRule`, `TabularCollection`)
- **Constants:** `UPPER_SNAKE_CASE` (e.g., `_TABLE_OK_RE`, `PK_BAD_RE`)
- **Private functions:** Prefix with underscore (e.g., `_infer_pk_prefer_from_header`)
- **Private class attributes:** Prefix with underscore

Compile regex patterns as module-level constants for performance:
```python
_REFISH_RE = re.compile(r"(ref|refs|_id|id|code)$", re.IGNORECASE)
```

### Dataclasses

Use `@dataclass` for configuration and data model objects:
```python
@dataclass
class JsonCollection:
    name: str
    path: str
    pk_prefer: List[str]
    enabled: bool = True
```

Set sensible defaults for optional boolean fields.

### File Structure & Comments

- Add a header comment indicating purpose:
  ```python
  # csv2duck.py (library; no CLI) -> supports CSV + XLSX with raw + normalized
  ```
- Initialize logger at module level: `logger = logging.getLogger(__name__)`
- Use `logger.info()` for important messages, `logger.debug()` for diagnostics, `logger.warning()` for non-critical issues
- Keep comments minimal, focused on "why" not "what"
- Use `#` for comments, docstrings only for complex functions or public APIs

### Functions

- Keep functions focused on a single responsibility
- Use generators/yield for streaming large datasets (e.g., `iter_json_records`, `iter_csv_rows`)
- Return iterators when processing large files to avoid loading everything into memory
- Validate inputs early and raise `ValueError` with descriptive messages

### Error Handling

- Raise `ValueError` for invalid user inputs, `FileNotFoundError` for missing files
- Use descriptive error messages that tell the user what's wrong and how to fix it
- Use try-except for expected errors (file operations, network requests)
- Use finally blocks for cleanup (temporary directories, file handles)

### Data Structures

- Use `Dict[str, Any]` for dynamic/nested data (JSON, YAML)
- Use `List[str]` for ordered collections of strings
- Use `Iterator[T]` for generators to signal streaming behavior
- Use `Optional[T]` for nullable values
- Prefer dataclasses over plain dicts for structured data

### String Handling & Resource Management

- Always use encoding="utf-8" when opening files
- Use `.strip()` on user inputs and file paths
- Use `json.dumps(obj, ensure_ascii=False)` for JSON serialization
- Use context managers (`with`) for file operations
- Clean up temporary directories in finally blocks
- Close workbooks and file handles explicitly

### Function Naming

- Use verbs for actions: `infer`, `build`, `parse`, `sanitize`, `load`, `save`
- Use descriptive names that indicate what the function returns:
  - `iter_*`: returns an iterator
  - `infer_*`: analyzes and returns mapping/configuration
  - `build_*`: constructs resources/objects
  - `get_*`: retrieves a value (may return None)

### DuckLake Configuration

DuckLake supports three replace strategies:
- `truncate-and-insert` (default, recommended): Direct inserts, no staging, faster for small datasets
- `insert-from-staging`: Load to staging first, atomic writes with rollback, safer but slower
- `staging-optimized`: Best for large datasets, maximum safety and performance

Configuration:
- YAML: `destination.ducklake.replace_strategy`
- CLI: `--ducklake-replace-strategy {truncate-and-insert,insert-from-staging,staging-optimized}`

**Important:** Use `truncate-and-insert` for ingest2duck (smaller datasets, no rollback needed). When using `write_disposition: merge`, DuckLake automatically creates staging tables regardless of replace_strategy. Staging tables: `{dataset}_staging.{table_name}`, cleaned up after successful merge.

### Source Checksum Tracking

Two-step checksum checking for source files:

**Flow:**
1. URL sources: HEAD request → E-tag/Content-Length checksum (preliminary check)
2. If not skipped: download file, compute exact SHA256 checksum
3. If checksum unchanged + mapping unchanged: skip processing
4. Otherwise: process data and update `sourcesmetadata` table

**CLI:** `--force` to skip all checksum checks

**sourcesmetadata Table** stores: `source_name` (PK), `source_url`, `source_type`, timestamps, checksums, size, format, `last_run_id`, `dataset`.

**Foreign Key:** Raw and normalized tables reference `sourcesmetadata.source_name` instead of duplicating metadata.
