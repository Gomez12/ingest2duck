# AGENTS.md

This file provides guidance for agentic coding assistants working on the ingest2duck repository.

## Build / Lint / Test Commands

This repository uses a simple Python script structure without a formal build system.

**Run the main CLI:**
```bash
python ingest2duck.py --mapping <path> [options]
```

**Format validation:**
- No formal linting setup is configured
- To check code manually: run `python -m py_compile <file>` to verify syntax

**Testing:**
- No test framework or test directory exists in this repository
- Manual testing via CLI execution is the current approach

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
- Use common types: `Any`, `Dict[str, Any]`, `List[str]`, `Optional[str]`, `Iterator[Dict[str, Any]]`
- Annotate all function parameters with types
- Use `-> None` for functions that don't return values
- For complex nested types, use `Tuple` and `Union` as needed

### Naming Conventions

- **Functions:** `snake_case` (e.g., `infer_json_mapping`, `sanitize_table_name`)
- **Variables:** `snake_case` (e.g., `file_path`, `raw_table`, `write_disposition`)
- **Classes:** `PascalCase` (e.g., `JsonMapping`, `CollectionRule`, `TabularCollection`)
- **Constants:** `UPPER_SNAKE_CASE` (e.g., `_TABLE_OK_RE`, `PK_BAD_RE`)
- **Private functions:** Prefix with underscore (e.g., `_infer_pk_prefer_from_header`)
- **Private class attributes:** Prefix with underscore

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

### File Structure

- Add a header comment indicating purpose:
  ```python
  # csv2duck.py (library; no CLI) -> supports CSV + XLSX with raw + normalized
  ```

### Functions

- Keep functions focused on a single responsibility
- Use generators/yield for streaming large datasets (e.g., `iter_json_records`, `iter_csv_rows`)
- Return iterators when processing large files to avoid loading everything into memory
- Validate inputs early and raise `ValueError` with descriptive messages
- Use try-finally blocks for cleanup (temporary files, resources)

### Error Handling

- Raise `ValueError` for invalid user inputs with clear error messages
- Raise `FileNotFoundError` for missing files
- Use descriptive error messages that tell the user what's wrong and how to fix it
- Use try-except for expected errors (file operations, network requests)
- Use finally blocks for cleanup (temporary directories, file handles)

### Comments

- Keep comments minimal and focused on explaining "why", not "what"
- Use `#` for comments, not docstrings for single-line explanations
- Add docstrings only for complex functions or public APIs
- File headers should clearly indicate if the file is a CLI or library

### Data Structures

- Use `Dict[str, Any]` for dynamic/nested data (JSON, YAML)
- Use `List[str]` for ordered collections of strings
- Use `Iterator[T]` for generators to signal streaming behavior
- Use `Optional[T]` for nullable values
- Prefer dataclasses over plain dicts for structured data

### String Handling

- Always use encoding="utf-8" when opening files
- Use `.strip()` on user inputs and file paths
- Use `json.dumps(obj, ensure_ascii=False)` for JSON serialization to preserve Unicode

### Resource Management

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

### Type Checking

The codebase doesn't have formal type checking (no mypy config), but:
- Write type annotations as if type checking were enabled
- Use `# type: ignore` sparingly and only when necessary
- Prefer `Dict[str, Any]` over `dict` for type clarity
- Use `from __future__ import annotations` to enable forward references
