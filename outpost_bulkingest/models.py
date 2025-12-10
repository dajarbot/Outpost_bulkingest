"""Dataclasses used throughout the ingest pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class Project:
    id: int
    name: str
    input_path: Path
    root_path: Path
    created_at: str


@dataclass
class Job:
    id: int
    project_id: int
    original_relpath: str
    original_fullpath: str
    status: str
    error_message: Optional[str]
    created_at: str
    updated_at: str
    hash: str
    num_pages: Optional[int]
    file_extension: str

