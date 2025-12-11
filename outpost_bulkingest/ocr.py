"""OCR abstraction layer using Tesseract and pdftoppm."""

from __future__ import annotations

import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List

from PIL import Image
import pytesseract

logger = logging.getLogger(__name__)

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tif", ".tiff"}


def run_ocr(job_id: int, input_path: Path) -> Dict[str, Any]:
    """Run OCR for a single file and return structured output."""
    suffix = input_path.suffix.lower()
    if suffix in IMAGE_EXTENSIONS:
        text = _ocr_image(input_path)
        pages = [{"page_index": 1, "text": text}]
    elif suffix == ".pdf":
        page_texts = _ocr_pdf(input_path)
        pages = [{"page_index": idx + 1, "text": page_texts[idx]} for idx in range(len(page_texts))]
        text = "\n\n".join(page_texts)
    else:
        raise ValueError(f"Unsupported file extension for OCR: {suffix}")

    return {"job_id": job_id, "text": text, "pages": pages}


def _ocr_image(path: Path) -> str:
    """Run OCR on an image file."""
    with Image.open(path) as img:
        return pytesseract.image_to_string(img)


def _ocr_pdf(path: Path) -> List[str]:
    """Rasterize PDF to PNGs and run OCR per page."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_prefix = Path(tmpdir) / "page"
        cmd = ["pdftoppm", "-png", str(path), str(output_prefix)]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f"pdftoppm failed: {proc.stderr.strip()}")

        images = sorted(Path(tmpdir).glob(f"{output_prefix.name}-*.png"), key=_page_sort_key)
        if not images:
            raise RuntimeError("No images produced from PDF rasterization")

        texts: List[str] = []
        for image_path in images:
            texts.append(_ocr_image(image_path))
        return texts


def _page_sort_key(path: Path) -> int:
    """Extract a numeric sort key from pdftoppm output filenames."""
    try:
        return int(path.stem.split("-")[-1])
    except ValueError:
        return 0

