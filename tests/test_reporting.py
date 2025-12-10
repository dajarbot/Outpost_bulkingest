from pathlib import Path

from outpost_bulkingest import db, reporting


def test_generate_report_returns_counts(tmp_path):
    db_path = tmp_path / "bulkingest.db"
    conn = db.init_db(db_path)

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    project = db.create_project(conn, "proj", input_dir, tmp_path / "data")

    originals = Path(project.root_path) / "originals"
    file_one = originals / "one.pdf"
    file_one.write_text("one")
    file_two = originals / "two.tif"
    file_two.write_text("two")

    job1 = db.insert_job(conn, project.id, file_one.name, str(file_one), "hash1", ".pdf")
    db.set_job_status(conn, job1, "done")
    db.insert_job(conn, project.id, file_two.name, str(file_two), "hash2", ".tif")

    report = reporting.generate_report(conn)
    assert report["total_projects"] == 1
    assert report["overall_jobs"] == 2

    project_entry = report["projects"][0]
    assert project_entry["status_counts"]["done"] == 1
    assert project_entry["status_counts"]["pending"] == 1
    assert project_entry["extension_counts"][".pdf"] == 1
    assert project_entry["extension_counts"][".tif"] == 1
    conn.close()

