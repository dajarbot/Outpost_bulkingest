"""Configuration defaults for Outpost Bulk Ingest."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

# Default locations; can be overridden via CLI args.
DEFAULT_DB_PATH = Path("bulkingest.db")
DEFAULT_DATA_ROOT = Path("data")


@dataclass
class Config:
    """Simple config container."""

    db_path: Path = DEFAULT_DB_PATH
    data_root: Path = DEFAULT_DATA_ROOT

