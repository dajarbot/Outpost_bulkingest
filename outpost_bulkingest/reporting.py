"""Reporting utilities for summaries and folder explanations."""

from __future__ import annotations

import itertools
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from . import db
from .models import Project


def folder_structure_table(data_root: Path) -> List[Tuple[str, str]]:
    """Return a list of (path, description) rows describing the layout."""
    return [
        (f"{data_root}/projects/", "Root for all projects"),
        (f"{data_root}/projects/<project_id>/", "Root for a specific project"),
        ("<project_root>/originals/", "Canonical copies of original input files"),
        ("<project_root>/ocr/", "OCR output files (JSON/TXT)"),
        ("<project_root>/logs/", "Logs related to ingest / processing"),
        ("<project_root>/reports/", "Generated summary/report files"),
    ]


def render_folder_structure_table(data_root: Path) -> str:
    rows = folder_structure_table(data_root)
    path_col_width = max(len(path) for path, _ in rows)
    header = f"{'Path pattern'.ljust(path_col_width)} | Description"
    divider = f"{'-' * path_col_width}-|-------------------------------------------"
    lines = [header, divider]
    for path, desc in rows:
        lines.append(f"{path.ljust(path_col_width)} | {desc}")
    return "\n".join(lines)


def generate_report(
    conn, project_id: Optional[int] = None
) -> Dict[str, object]:
    """Build a summary data structure for projects and jobs."""
    projects: List[Project]
    if project_id is not None:
        project = db.get_project(conn, project_id)
        projects = [project] if project else []
    else:
        projects = db.list_projects(conn)

    status_counter: Counter[str] = Counter()
    overall_jobs = 0
    project_entries: List[Dict[str, object]] = []

    for project in projects:
        status_counts = db.count_jobs_by_status(conn, project.id)
        ext_counts = db.count_jobs_by_extension(conn, project.id)
        total_jobs = sum(status_counts.values())
        overall_jobs += total_jobs
        status_counter.update(status_counts)
        project_entries.append(
            {
                "id": project.id,
                "name": project.name,
                "total_jobs": total_jobs,
                "status_counts": status_counts,
                "extension_counts": ext_counts,
            }
        )

    return {
        "total_projects": len(projects),
        "projects": project_entries,
        "overall_status": dict(status_counter),
        "overall_jobs": overall_jobs,
    }


def format_report(report: Dict[str, object]) -> str:
    lines: List[str] = []
    lines.append("Outpost Bulk Ingest Report")
    lines.append("--------------------------")
    lines.append(f"Total projects: {report['total_projects']}")
    lines.append(f"Total jobs: {report['overall_jobs']}")
    lines.append("")

    for project in report["projects"]:
        lines.append(f"Project {project['id']} - {project['name']}")
        lines.append(f"  Total jobs: {project['total_jobs']}")
        lines.append("  Statuses:")
        for status, count in sorted(project["status_counts"].items()):
            lines.append(f"    {status}: {count}")
        lines.append("  File extensions:")
        for ext, count in sorted(project["extension_counts"].items()):
            lines.append(f"    {ext}: {count}")
        lines.append("")

    if report["projects"]:
        lines.append("Overall status counts:")
        for status, count in sorted(report["overall_status"].items()):
            lines.append(f"  {status}: {count}")

    return "\n".join(lines)

