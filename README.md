# Outpost Bulk Ingest

Outpost Bulk Ingest scans folders of documents, expands zip archives, queues OCR jobs in SQLite, and writes OCR outputs into a canonical project structure. It targets on-prem deployments (Jetson Orin, macOS) with only standard-library dependencies plus pytest for tests.

## Quickstart

```bash
python -m outpost_bulkingest.cli init-db
python -m outpost_bulkingest.cli create-project --name WEM_Unit12 --input-path /path/to/folder
python -m outpost_bulkingest.cli scan --project-id 1
python -m outpost_bulkingest.cli work --project-id 1 --limit 20
python -m outpost_bulkingest.cli status --project-id 1
python -m outpost_bulkingest.cli report --project-id 1
python -m outpost_bulkingest.cli explain-structure
```

## Folder Structure

| Path pattern | Description |
|--------------|-------------|
| `data/projects/` | Root for all projects |
| `data/projects/<project_id>/` | Root for a specific project |
| `<project_root>/originals/` | Canonical copies of original input files |
| `<project_root>/ocr/` | OCR output files (JSON/TXT) |
| `<project_root>/logs/` | Logs related to ingest / processing |
| `<project_root>/reports/` | Generated summary/report files |

## Local Run Notes

From the repo root with an active virtualenv:

```bash
python -m outpost_bulkingest.cli init-db
python -m outpost_bulkingest.cli create-project --name Example --input-path /path/to/input
python -m outpost_bulkingest.cli scan --project-id 1
python -m outpost_bulkingest.cli work --project-id 1
python -m outpost_bulkingest.cli report --project-id 1
```
