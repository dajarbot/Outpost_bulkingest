from pathlib import Path

from outpost_bulkingest import db, worker


def test_worker_processes_job(tmp_path):
    db_path = tmp_path / "bulkingest.db"
    conn = db.init_db(db_path)

    input_dir = tmp_path / "input"
    input_dir.mkdir()

    project = db.create_project(conn, "proj", input_dir, tmp_path / "data")
    originals_dir = Path(project.root_path) / "originals"
    source_file = originals_dir / "doc.pdf"
    source_file.write_text("dummy")

    job_id = db.insert_job(
        conn,
        project.id,
        source_file.name,
        str(source_file),
        "hash123",
        ".pdf",
    )

    summary = worker.process_jobs(conn, project_id=project.id, limit=1)
    assert summary["succeeded"] == 1

    job = db.get_job(conn, job_id)
    assert job is not None
    assert job.status == "done"

    output_path = Path(project.root_path) / "ocr" / f"{job_id}.json"
    assert output_path.exists()
    conn.close()

