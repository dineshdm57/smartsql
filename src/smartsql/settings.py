from __future__ import annotations
from pathlib import Path
import json
from typing import Any, Dict

_DATA_DIR = Path(".smartsql")
_DATA_DIR.mkdir(exist_ok=True)
_SETTINGS_FILE = _DATA_DIR / "settings.json"

_DEFAULTS: Dict[str, Any] = {
    # BigQuery (all optional; leave None while offline)
    "data_project": None,       # where tables live
    "dataset": None,            # e.g., "prod"
    "billing_project": None,    # optional: where jobs bill
    "location": None,           # e.g., "US" or "EU"
    "auth_mode": "adc",         # "adc" | "service_account" | "oauth"

    # LLM providers toggles (keys in .env or secret manager; not stored here)
    "providers": {
        "gemini": True,
        "openai": False,
        "anthropic": False,
        "azure_openai": False,
        "vertex_ai": False
    }
}

def _read() -> Dict[str, Any]:
    if not _SETTINGS_FILE.exists():
        return dict(_DEFAULTS)
    try:
        return json.loads(_SETTINGS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return dict(_DEFAULTS)

def get_settings_store() -> Dict[str, Any]:
    """Return current settings (defaults if none saved)."""
    # always merge over defaults to avoid missing keys
    cur = _read()
    merged = dict(_DEFAULTS)
    # shallow merge for providers
    prov = dict(_DEFAULTS["providers"])
    prov.update(cur.get("providers", {}))
    merged.update(cur)
    merged["providers"] = prov
    return merged

def set_settings_store(new: Dict[str, Any]) -> None:
    """Persist provided settings (partial allowed)."""
    cur = get_settings_store()
    cur.update({k: v for k, v in (new or {}).items() if k in _DEFAULTS})
    if "providers" in (new or {}):
        prov = dict(_DEFAULTS["providers"])
        prov.update(new["providers"] or {})
        cur["providers"] = prov
    _SETTINGS_FILE.write_text(json.dumps(cur, indent=2), encoding="utf-8")
