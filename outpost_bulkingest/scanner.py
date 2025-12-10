"""Folder scanning and job creation logic."""

from __future__ import annotations

import hashlib
import logging
import shutil
import zipfile
from collections import deque
from pathlib import Path
from typing import Dict, Set

from . import db
from .models import Project

logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS: Set[str] = {".pdf", ".tif", ".tiff", ".jpg", ".jpeg", ".png"}


def _is_hidden(path: Path) -> bool:
    return path.name.startswith(".")


def compute_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def extract_zip_if_needed(conn, project: Project, zip_path: Path) -> bool:
    """Extract the zip if not already processed. Returns True if extracted now."""
    zip_hash = compute_sha256(zip_path)
    if db.zip_already_extracted(conn, project.id, zip_path, zip_hash):
        return False

    try:
        with zipfile.ZipFile(zip_path, "r") as archive:
            archive.extractall(zip_path.parent)
        db.record_zip_extraction(conn, project.id, zip_path, zip_hash)
        logger.info("Extracted zip %s for project %s", zip_path, project.id)
        return True
    except zipfile.BadZipFile as exc:
        logger.error("Failed to extract zip %s: %s", zip_path, exc)
        return False


def scan_project(conn, project_id: int) -> Dict[str, int]:
    """Scan a project input path, create jobs, and expand zips."""
    project = db.get_project(conn, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    input_path = project.input_path
    if not input_path.exists() or not input_path.is_dir():
        raise FileNotFoundError(f"Input path does not exist or is not a directory: {input_path}")

    originals_dir = project.root_path / "originals"
    originals_dir.mkdir(parents=True, exist_ok=True)

    summary = {"new_jobs": 0, "skipped_existing": 0, "zip_extracted": 0}

    queue = deque([input_path])
    while queue:
        current_dir = queue.popleft()
        if not current_dir.is_dir():
            continue

        for entry in current_dir.iterdir():
            if _is_hidden(entry):
                continue

            if entry.is_dir():
                queue.append(entry)
                continue

            suffix = entry.suffix.lower()
            if suffix == ".zip":
                extracted_now = extract_zip_if_needed(conn, project, entry)
                if extracted_now:
                    summary["zip_extracted"] += 1
                    queue.append(entry.parent)
                continue

            if suffix in SUPPORTED_EXTENSIONS:
                file_hash = compute_sha256(entry)
                if db.job_exists(conn, project.id, file_hash):
                    summary["skipped_existing"] += 1
                    continue

                dest_name = f"{file_hash}_{entry.name}"
                dest_path = originals_dir / dest_name
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(entry, dest_path)

                relpath = dest_path.relative_to(originals_dir)
                db.insert_job(
                    conn,
                    project.id,
                    str(relpath),
                    str(dest_path),
                    file_hash,
                    suffix,
                )
                summary["new_jobs"] += 1
                logger.info("Queued job for %s", entry)
            else:
                continue

    return summary

