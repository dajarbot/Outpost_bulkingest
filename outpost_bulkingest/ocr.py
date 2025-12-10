"""OCR abstraction layer.

Currently uses a stub implementation that can be swapped out later.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any


def perform_ocr(file_path: Path) -> Dict[str, Any]:
    """Mock OCR function."""
    return {
        "file": str(file_path),
        "text": f"Fake OCR output for {file_path.name}",
        "pages": [],
    }

