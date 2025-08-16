from __future__ import annotations
from pathlib import Path
import json
from typing import Any, Dict, Optional

_DATA_DIR = Path(".smartsql")
_DATA_DIR.mkdir(exist_ok=True)
_CATALOG_FILE = _DATA_DIR / "catalog.json"

def set_local_catalog(catalog: Dict[str, Any]) -> None:
    if not isinstance(catalog, dict):
        raise ValueError("catalog must be a dict")
    with _CATALOG_FILE.open("w", encoding="utf-8") as f:
        json.dump({"catalog": catalog}, f, indent=2)

def get_local_catalog() -> Optional[Dict[str, Any]]:
    if not _CATALOG_FILE.exists():
        return None
    try:
        obj = json.loads(_CATALOG_FILE.read_text(encoding="utf-8"))
        return obj.get("catalog")
    except Exception:
        return None
