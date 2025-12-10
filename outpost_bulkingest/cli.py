"""Command-line entrypoints for Outpost Bulk Ingest."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

from . import config, db, reporting, scanner, worker


def _configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")


def _get_connection(db_path: Path):
    return db.init_db(db_path)


def cmd_init_db(args) -> None:
    conn = _get_connection(args.db_path)
    conn.close()
    print(f"Database initialized at {args.db_path}")


def cmd_create_project(args) -> None:
    input_path = Path(args.input_path)
    if not input_path.exists() or not input_path.is_dir():
        raise SystemExit(f"Input path does not exist or is not a directory: {input_path}")

    conn = _get_connection(args.db_path)
    project = db.create_project(conn, args.name, input_path, Path(args.data_root))
    print(f"Created project {project.id} at {project.root_path}")
    conn.close()


def cmd_scan(args) -> None:
    conn = _get_connection(args.db_path)
    summary = scanner.scan_project(conn, args.project_id)
    conn.close()
    print(
        f"Created {summary['new_jobs']} new jobs. "
        f"Skipped {summary['skipped_existing']} existing files. "
        f"Processed {summary['zip_extracted']} zip archives."
    )


def cmd_work(args) -> None:
    conn = _get_connection(args.db_path)
    summary = worker.process_jobs(conn, project_id=args.project_id, limit=args.limit)
    conn.close()
    print(
        f"Processed {summary['processed']} jobs. "
        f"Succeeded: {summary['succeeded']}, Failed: {summary['failed']}."
    )


def cmd_status(args) -> None:
    conn = _get_connection(args.db_path)
    if args.project_id:
        project = db.get_project(conn, args.project_id)
        if not project:
            raise SystemExit(f"Project {args.project_id} not found")
        counts = db.count_jobs_by_status(conn, project.id)
        print(f"Status for project {project.id} ({project.name}):")
    else:
        counts = db.count_jobs_by_status(conn)
        print("Global job status:")

    for status, count in sorted(counts.items()):
        print(f"  {status}: {count}")
    conn.close()


def cmd_report(args) -> None:
    conn = _get_connection(args.db_path)
    report_data = reporting.generate_report(conn, project_id=args.project_id)
    print(reporting.format_report(report_data))
    conn.close()


def cmd_explain_structure(args) -> None:
    table = reporting.render_folder_structure_table(Path(args.data_root))
    print(table)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Outpost bulk ingest CLI")
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--db-path",
        type=Path,
        default=config.DEFAULT_DB_PATH,
        help="Path to SQLite database file",
    )

    init_db_parser = subparsers.add_parser("init-db", parents=[common], help="Initialize the database schema")
    init_db_parser.set_defaults(func=cmd_init_db)

    create_proj = subparsers.add_parser("create-project", parents=[common], help="Create a new project")
    create_proj.add_argument("--name", required=True, help="Project name")
    create_proj.add_argument("--input-path", required=True, help="Folder to ingest")
    create_proj.add_argument(
        "--data-root",
        default=config.DEFAULT_DATA_ROOT,
        help="Root folder to store project data (default: ./data)",
    )
    create_proj.set_defaults(func=cmd_create_project)

    scan_parser = subparsers.add_parser("scan", parents=[common], help="Scan input folder and create jobs")
    scan_parser.add_argument("--project-id", required=True, type=int, help="Project ID")
    scan_parser.set_defaults(func=cmd_scan)

    work_parser = subparsers.add_parser("work", parents=[common], help="Process pending jobs")
    work_parser.add_argument("--project-id", type=int, help="Limit to a specific project")
    work_parser.add_argument("--limit", type=int, default=10, help="Number of jobs to process (default 10)")
    work_parser.set_defaults(func=cmd_work)

    status_parser = subparsers.add_parser("status", parents=[common], help="Show job status summary")
    status_parser.add_argument("--project-id", type=int, help="Project ID to filter")
    status_parser.set_defaults(func=cmd_status)

    report_parser = subparsers.add_parser("report", parents=[common], help="Generate a high-level report")
    report_parser.add_argument("--project-id", type=int, help="Project ID to report on")
    report_parser.set_defaults(func=cmd_report)

    explain_parser = subparsers.add_parser("explain-structure", help="Describe the folder layout")
    explain_parser.add_argument(
        "--data-root",
        default=config.DEFAULT_DATA_ROOT,
        help="Root folder to store project data (default: ./data)",
    )
    explain_parser.set_defaults(func=cmd_explain_structure)

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(verbose=args.verbose)
    args.func(args)


if __name__ == "__main__":
    main()

