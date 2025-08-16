from typing import Dict, Any

class StewardAgent:
    name = "A"

    def ingest(self, file_bytes: bytes, filename: str) -> Dict[str, Any]:
        """
        Stub: accepts uploaded schema file bytes and returns a placeholder Contract summary.
        Real parsing comes later.
        """
        size = len(file_bytes or b"")
        return {
            "contract_id": "telemetry",
            "version": "v0.0.1",
            "source_filename": filename,
            "bytes_received": size,
            "notes": "Stub ingest OK — parsing to be implemented."
        }

    def handle(self, text: str) -> str:
        return "A(steward): stub ok"
