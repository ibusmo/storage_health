from __future__ import annotations

from pathlib import Path
from typing import Any

from sd_health.db import insert_run
from sd_health.log import append_jsonl


def persist_run(
    record: dict[str, Any],
    *,
    db_path: Path,
    jsonl_path: Path | None = None,
) -> None:
    """Write to SQLite; optionally mirror to JSONL for export compatibility."""
    insert_run(db_path, record)
    if jsonl_path is not None:
        append_jsonl(jsonl_path, record)
