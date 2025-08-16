from __future__ import annotations
from pathlib import Path
import json
from typing import Any, Dict, Optional

_DATA_DIR = Path(".smartsql")
_DATA_DIR.mkdir(exist_ok=True)
_REGISTRY_FILE = _DATA_DIR / "contract.json"

def set_active_contract(contract: Dict[str, Any]) -> None:
    """Persist the active contract locally."""
    if not isinstance(contract, dict):
        raise ValueError("contract must be a dict")
    with _REGISTRY_FILE.open("w", encoding="utf-8") as f:
        json.dump({"active": contract}, f, indent=2)

def get_active_contract() -> Optional[Dict[str, Any]]:
    """Return the active contract if present, else None."""
    if not _REGISTRY_FILE.exists():
        return None
    try:
        obj = json.loads(_REGISTRY_FILE.read_text(encoding="utf-8"))
        return obj.get("active")
    except Exception:
        return None

def get_active_version() -> Optional[str]:
    c = get_active_contract()
    if not c:
        return None
    return c.get("version") or c.get("contract_version")
