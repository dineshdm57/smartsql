from typing import TypedDict, Optional, Dict, Any, List
from langgraph.graph import StateGraph, END
from smartsql.router import detect_intent
from smartsql.agents.verifier import VerifierAgent
from smartsql.agents.analyst import AnalystAgent
from smartsql.registry import get_active_contract
from smartsql.sql_policy import lint_sql

class SmartState(TypedDict, total=False):
    text: str
    dataset: str
    table: Optional[str]
    intent: str
    result: Dict[str, Any]
    error: str

def route_node(state: SmartState) -> SmartState:
    r = detect_intent(state.get("text",""))
    state["intent"] = r.intent
    return state

def verify_node(state: SmartState) -> SmartState:
    dataset = state.get("dataset") or "prod"
    table = state.get("table")
    if not table:
        state["error"] = "verify needs a table (e.g., 'verify prod spans')."
        return state
    v = VerifierAgent()
    state["result"] = v.compare_to_contract(project=None, dataset=dataset, table=table)
    return state

def ask_node(state: SmartState) -> SmartState:
    dataset = state.get("dataset") or "prod"
    text = state.get("text","")
    a = AnalystAgent()
    draft = a.draft_sql(nl_query=text, dataset=dataset)

    # Lint policy (same logic as API)
    active = get_active_contract() or {}
    policy = (active.get("policy") or {})
    require_time_window = bool(policy.get("require_time_window", True))
    time_fields: List[str] = []
    for tbl, ent in (active.get("entities") or {}).items():
        for fname, fmeta in (ent.get("fields") or {}).items():
            t = (fmeta.get("type") or "").upper()
            if t in {"TIMESTAMP","DATETIME","DATE"} or fname.lower() in {"ts","timestamp","event_ts","created_at","time"}:
                time_fields.extend([f"{dataset}.{tbl}.{fname}", f"{tbl}.{fname}", fname])
    violations = lint_sql(draft.get("sql",""), dataset=dataset, require_time_window=require_time_window, time_fields=time_fields)
    draft["policy_ok"] = all(v.get("severity") != "error" for v in violations)
    draft["violations"] = violations

    state["result"] = draft
    return state

def _branch(state: SmartState) -> str:
    intent = state.get("intent") or "unknown"
    if intent == "verify_tables":
        return "verify"
    if intent == "kpi_query":
        return "ask"
    return "end"

def build_graph():
    g = StateGraph(SmartState)
    g.add_node("route", route_node)
    g.add_node("verify", verify_node)
    g.add_node("ask", ask_node)

    g.set_entry_point("route")
    g.add_conditional_edges("route", _branch, {
        "verify": "verify",
        "ask": "ask",
        "end": END
    })
    g.add_edge("verify", END)
    g.add_edge("ask", END)
    return g.compile()

def invoke_graph(text: str, dataset: str = "prod", table: Optional[str] = None) -> SmartState:
    app = build_graph()
    return app.invoke({"text": text, "dataset": dataset, "table": table})
