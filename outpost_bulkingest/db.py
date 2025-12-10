"""SQLite helpers and schema management."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

from . import models

logger = logging.getLogger(__name__)


def get_connection(db_path: Path) -> sqlite3.Connection:
    """Return a connection with a Row factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path) -> sqlite3.Connection:
    """Initialize the database schema."""
    if db_path.parent and not db_path.parent.exists():
        db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            input_path TEXT NOT NULL,
            root_path TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            original_relpath TEXT NOT NULL,
            original_fullpath TEXT NOT NULL,
            status TEXT NOT NULL,
            error_message TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            hash TEXT NOT NULL,
            num_pages INTEGER,
            file_extension TEXT NOT NULL,
            FOREIGN KEY(project_id) REFERENCES projects(id),
            UNIQUE(project_id, hash)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS zip_extractions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            zip_path TEXT NOT NULL,
            zip_hash TEXT NOT NULL,
            extracted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(project_id) REFERENCES projects(id),
            UNIQUE(project_id, zip_path, zip_hash)
        )
        """
    )

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_project_status ON jobs(project_id, status)"
    )

    conn.commit()
    logger.info("Database initialized at %s", db_path)
    return conn


def _row_to_project(row: sqlite3.Row) -> models.Project:
    return models.Project(
        id=row["id"],
        name=row["name"],
        input_path=Path(row["input_path"]),
        root_path=Path(row["root_path"]),
        created_at=row["created_at"],
    )


def _row_to_job(row: sqlite3.Row) -> models.Job:
    return models.Job(
        id=row["id"],
        project_id=row["project_id"],
        original_relpath=row["original_relpath"],
        original_fullpath=row["original_fullpath"],
        status=row["status"],
        error_message=row["error_message"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        hash=row["hash"],
        num_pages=row["num_pages"],
        file_extension=row["file_extension"],
    )


def create_project(
    conn: sqlite3.Connection, name: str, input_path: Path, data_root: Path
) -> models.Project:
    """Insert a project record and build the canonical folder layout."""
    input_path = input_path.resolve()
    data_root = data_root.resolve()
    cur = conn.execute(
        "INSERT INTO projects (name, input_path, root_path) VALUES (?, ?, ?)",
        (name, str(input_path), ""),
    )
    project_id = cur.lastrowid
    project_root = data_root / "projects" / str(project_id)
    conn.execute(
        "UPDATE projects SET root_path = ? WHERE id = ?",
        (str(project_root), project_id),
    )
    conn.commit()

    for sub in ("originals", "ocr", "logs", "reports"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)

    project = get_project(conn, project_id)
    logger.info("Created project %s (%s)", project_id, name)
    return project


def get_project(conn: sqlite3.Connection, project_id: int) -> Optional[models.Project]:
    cur = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
    row = cur.fetchone()
    return _row_to_project(row) if row else None


def list_projects(conn: sqlite3.Connection) -> List[models.Project]:
    cur = conn.execute("SELECT * FROM projects ORDER BY id ASC")
    return [_row_to_project(row) for row in cur.fetchall()]


def job_exists(conn: sqlite3.Connection, project_id: int, file_hash: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM jobs WHERE project_id = ? AND hash = ? LIMIT 1",
        (project_id, file_hash),
    )
    return cur.fetchone() is not None


def insert_job(
    conn: sqlite3.Connection,
    project_id: int,
    original_relpath: str,
    original_fullpath: str,
    file_hash: str,
    file_extension: str,
    num_pages: Optional[int] = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO jobs (
            project_id, original_relpath, original_fullpath, status,
            error_message, hash, num_pages, file_extension
        ) VALUES (?, ?, ?, 'pending', NULL, ?, ?, ?)
        """,
        (
            project_id,
            original_relpath,
            original_fullpath,
            file_hash,
            num_pages,
            file_extension,
        ),
    )
    conn.commit()
    job_id = cur.lastrowid
    logger.info("Created job %s for project %s", job_id, project_id)
    return job_id


def fetch_pending_jobs(
    conn: sqlite3.Connection, limit: int = 10, project_id: Optional[int] = None
) -> List[models.Job]:
    params = []
    query = "SELECT * FROM jobs WHERE status = 'pending'"
    if project_id is not None:
        query += " AND project_id = ?"
        params.append(project_id)
    query += " ORDER BY created_at ASC, id ASC LIMIT ?"
    params.append(limit)
    cur = conn.execute(query, tuple(params))
    return [_row_to_job(row) for row in cur.fetchall()]


def set_job_status(
    conn: sqlite3.Connection,
    job_id: int,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    conn.execute(
        """
        UPDATE jobs
        SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (status, error_message, job_id),
    )
    conn.commit()


def record_zip_extraction(
    conn: sqlite3.Connection, project_id: int, zip_path: Path, zip_hash: str
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO zip_extractions (project_id, zip_path, zip_hash)
        VALUES (?, ?, ?)
        """,
        (project_id, str(zip_path), zip_hash),
    )
    conn.commit()


def zip_already_extracted(
    conn: sqlite3.Connection, project_id: int, zip_path: Path, zip_hash: str
) -> bool:
    cur = conn.execute(
        """
        SELECT 1 FROM zip_extractions
        WHERE project_id = ? AND zip_path = ? AND zip_hash = ?
        LIMIT 1
        """,
        (project_id, str(zip_path), zip_hash),
    )
    return cur.fetchone() is not None


def count_jobs_by_status(
    conn: sqlite3.Connection, project_id: Optional[int] = None
) -> Dict[str, int]:
    params = []
    query = "SELECT status, COUNT(*) as count FROM jobs"
    if project_id is not None:
        query += " WHERE project_id = ?"
        params.append(project_id)
    query += " GROUP BY status"
    cur = conn.execute(query, tuple(params))
    return {row["status"]: row["count"] for row in cur.fetchall()}


def count_jobs_by_extension(
    conn: sqlite3.Connection, project_id: Optional[int] = None
) -> Dict[str, int]:
    params = []
    query = "SELECT file_extension, COUNT(*) as count FROM jobs"
    if project_id is not None:
        query += " WHERE project_id = ?"
        params.append(project_id)
    query += " GROUP BY file_extension"
    cur = conn.execute(query, tuple(params))
    return {row["file_extension"]: row["count"] for row in cur.fetchall()}


def total_jobs(conn: sqlite3.Connection, project_id: Optional[int] = None) -> int:
    params = []
    query = "SELECT COUNT(*) as count FROM jobs"
    if project_id is not None:
        query += " WHERE project_id = ?"
        params.append(project_id)
    cur = conn.execute(query, tuple(params))
    row = cur.fetchone()
    return int(row["count"]) if row else 0


def get_job(conn: sqlite3.Connection, job_id: int) -> Optional[models.Job]:
    cur = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    row = cur.fetchone()
    return _row_to_job(row) if row else None
