import re
from dataclasses import dataclass

INTENTS = [
    "upload_schema","verify_tables","kpi_query",
    "schema_qna","table_fields_qna","vendor_research","unknown"
]

@dataclass
class Route:
    intent: str
    target: str  # "A" (Steward), "B" (Verifier), "C" (Analyst)

def detect_intent(text: str) -> Route:
    t = (text or "").lower().strip()

    if "upload" in t or "add schema" in t:
        return Route("upload_schema","A")
    if any(k in t for k in ["verify","check fields","columns","field coverage"]):
        return Route("verify_tables","B")
    if any(k in t for k in ["otel","openinference","vendor","new field","spec update"]):
        return Route("vendor_research","A")
    if any(k in t for k in ["what does","meaning of","schema","contract"]) and "verify" not in t:
        return Route("schema_qna","A")
    if any(k in t for k in ["which table","which field","column path"]):
        return Route("table_fields_qna","B")
    if any(k in t for k in ["top","by","percent","avg","p95","cost","spend","latency","rate","trend"]):
        # crude time-window heuristic
        if re.search(r"\b(last|past)\s+\d+\s*(d|day|days|w|week|weeks|m|month|months)\b", t):
            return Route("kpi_query","C")
        return Route("kpi_query","C")
    return Route("unknown","A")
