"""Worker that processes OCR jobs."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Optional

from . import db, ocr
from .models import Job, Project

logger = logging.getLogger(__name__)


def _get_project_cache(conn, jobs) -> Dict[int, Project]:
    cache: Dict[int, Project] = {}
    for job in jobs:
        if job.project_id not in cache:
            project = db.get_project(conn, job.project_id)
            if project:
                cache[job.project_id] = project
    return cache


def process_jobs(
    conn, project_id: Optional[int] = None, limit: int = 10
) -> Dict[str, int]:
    jobs = db.fetch_pending_jobs(conn, limit=limit, project_id=project_id)
    summary = {"processed": 0, "succeeded": 0, "failed": 0}

    if not jobs:
        return summary

    project_cache = _get_project_cache(conn, jobs)

    for job in jobs:
        project = project_cache.get(job.project_id)
        if not project:
            logger.error("Skipping job %s; project %s not found", job.id, job.project_id)
            continue

        try:
            db.set_job_status(conn, job.id, "in_progress")
            result = ocr.perform_ocr(Path(job.original_fullpath))

            ocr_dir = Path(project.root_path) / "ocr"
            ocr_dir.mkdir(parents=True, exist_ok=True)
            output_path = ocr_dir / f"{job.id}.json"
            with output_path.open("w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)

            db.set_job_status(conn, job.id, "done")
            summary["succeeded"] += 1
        except Exception as exc:
            logger.exception("Job %s failed", job.id)
            db.set_job_status(conn, job.id, "failed", str(exc))
            summary["failed"] += 1
        finally:
            summary["processed"] += 1

    return summary

