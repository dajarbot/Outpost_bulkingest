from pathlib import Path
import zipfile

from outpost_bulkingest import db, scanner


def test_scan_handles_zip_and_supported_files(tmp_path):
    db_path = tmp_path / "bulkingest.db"
    conn = db.init_db(db_path)

    input_dir = tmp_path / "input"
    input_dir.mkdir()

    file_a = input_dir / "a.pdf"
    file_a.write_text("document a")
    file_b = input_dir / "b.png"
    file_b.write_text("image b")

    zip_path = input_dir / "archive.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("nested/c.pdf", "zip content")

    project = db.create_project(conn, "proj", input_dir, tmp_path / "data")

    summary = scanner.scan_project(conn, project.id)
    assert summary["zip_extracted"] == 1
    assert (input_dir / "nested" / "c.pdf").exists()
    assert db.total_jobs(conn, project.id) == 3

    originals_dir = Path(project.root_path) / "originals"
    copied_files = list(originals_dir.iterdir())
    assert len(copied_files) == 3

    # Second scan should be idempotent
    summary_again = scanner.scan_project(conn, project.id)
    assert summary_again["new_jobs"] == 0
    assert summary_again["zip_extracted"] == 0
    conn.close()

