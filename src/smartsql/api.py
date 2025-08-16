from typing import Optional, Dict, Any, List
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Body
from fastapi.responses import FileResponse
from smartsql.config import get_settings
from smartsql.agents.steward import StewardAgent
from smartsql.agents.verifier import VerifierAgent
from smartsql.agents.analyst import AnalystAgent
from smartsql.registry import set_active_contract, get_active_contract, get_active_version
from smartsql.catalog import set_local_catalog, get_local_catalog
from smartsql.sql_policy import lint_sql
from smartsql.router import detect_intent
from smartsql.settings import get_settings_store, set_settings_store
from smartsql.graph import invoke_graph
from smartsql.bq_exec import dry_run as bq_dry_run, execute as bq_execute

app = FastAPI(title="SmartSQL API", version="0.1.0")

# ---- UI routes ----
@app.get("/")
def ui_index():
    return FileResponse("src/web/index.html")

@app.get("/app.js")
def ui_app_js():
    return FileResponse("src/web/app.js")

# ---- Health ----
@app.get("/health")
def health():
    s = get_settings()
    return {"status": "ok", "provider": s.provider, "offline": s.offline}

# ---- Settings ----
@app.get("/settings")
def settings_get():
    s = get_settings()
    store = get_settings_store()
    store_out = dict(store)
    store_out["runtime"] = {"offline": s.offline, "provider": s.provider}
    return {"ok": True, "settings": store_out}

@app.post("/settings")
def settings_set(payload: Dict[str, Any] = Body(...)):
    try:
        set_settings_store(payload or {})
        return {"ok": True, "settings": get_settings_store()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"settings save failed: {e}")

# ---- Upload schema ----
@app.post("/upload")
async def upload_schema(file: UploadFile = File(...)):
    try:
        data = await file.read()
        steward = StewardAgent()
        result = steward.ingest(data, file.filename or "unknown")
        return {"ok": True, "contract": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"upload failed: {e}")

# ---- Contract ----
@app.post("/contract/activate")
def contract_activate(contract: Dict[str, Any] = Body(...)):
    try:
        set_active_contract(contract)
        return {"ok": True, "active_version": get_active_version()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"activate failed: {e}")

@app.get("/contract/active")
def contract_active():
    c = get_active_contract()
    if not c:
        return {"ok": False, "active": None}
    return {"ok": True, "active": c, "version": get_active_version()}

# ---- Verify (cloud ping) ----
@app.get("/verify")
def verify(project: Optional[str] = Query(None), dataset: Optional[str] = Query(None)):
    verifier = VerifierAgent()
    return verifier.verify(project=project, dataset=dataset)

# ---- Verify compare (contract vs table) ----
@app.get("/verify/compare")
def verify_compare(
    project: Optional[str] = Query(None),
    dataset: str = Query(...),
    table: str = Query(...)
):
    verifier = VerifierAgent()
    return verifier.compare_to_contract(project=project, dataset=dataset, table=table)

# ---- Local Catalog (offline) ----
@app.post("/catalog")
def catalog_set(catalog: Dict[str, Any] = Body(...)):
    try:
        set_local_catalog(catalog)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"catalog set failed: {e}")

@app.get("/catalog")
def catalog_get():
    cat = get_local_catalog()
    return {"ok": bool(cat), "catalog": cat}

# ---- Ask (NL → SQL draft; offline-only for now) ----
@app.post("/ask/draft")
def ask_draft(payload: Dict[str, Any] = Body(...)):
    nl_query = (payload or {}).get("nl_query")
    dataset = (payload or {}).get("dataset") or "prod"
    if not nl_query or not isinstance(nl_query, str):
        raise HTTPException(status_code=400, detail="nl_query (string) is required.")

    analyst = AnalystAgent()
    draft = analyst.draft_sql(nl_query=nl_query, dataset=dataset)

    active = get_active_contract() or {}
    policy = (active.get("policy") or {})
    require_time_window = bool(policy.get("require_time_window", True))

    time_fields: List[str] = []
    for tbl, ent in (active.get("entities") or {}).items():
        for fname, fmeta in (ent.get("fields") or {}).items():
            t = (fmeta.get("type") or "").upper()
            if t in {"TIMESTAMP", "DATETIME", "DATE"} or fname.lower() in {"ts","timestamp","event_ts","created_at","time"}:
                time_fields.extend([f"{dataset}.{tbl}.{fname}", f"{tbl}.{fname}", fname])

    violations = lint_sql(draft.get("sql",""), dataset=dataset, require_time_window=require_time_window, time_fields=time_fields)
    policy_ok = all(v.get("severity") != "error" for v in violations)

    draft["policy_ok"] = policy_ok
    draft["violations"] = violations
    return draft

# ---- Ask/Execute (confirmation gate; offline blocks; online supports dry-run + execute) ----
@app.post("/ask/execute")
def ask_execute(payload: Dict[str, Any] = Body(...)):
    """
    Body: { "sql": "...", "dataset": "prod", "confirm": true|false }
    Behavior:
      - Offline mode: always blocked (no cloud calls).
      - Online:
          * if confirm=false or missing -> DRY RUN estimate, return status="estimate"
          * if confirm=true -> execute with row cap, return rows + schema
    """
    s = get_settings()
    store = get_settings_store()

    sql = (payload or {}).get("sql") or ""
    dataset = (payload or {}).get("dataset") or store.get("dataset") or "prod"
    confirm = bool((payload or {}).get("confirm"))

    if not sql.strip():
        raise HTTPException(status_code=400, detail="sql is required.")

    # Lint against current policy before any cloud call
    active = get_active_contract() or {}
    policy = (active.get("policy") or {})
    require_time_window = bool(policy.get("require_time_window", True))
    time_fields: List[str] = []
    for tbl, ent in (active.get("entities") or {}).items():
        for fname, fmeta in (ent.get("fields") or {}).items():
            t = (fmeta.get("type") or "").upper()
            if t in {"TIMESTAMP","DATETIME","DATE"} or fname.lower() in {"ts","timestamp","event_ts","created_at","time"}:
                time_fields.extend([f"{dataset}.{tbl}.{fname}", f"{tbl}.{fname}", fname])
    violations = lint_sql(sql, dataset=dataset, require_time_window=require_time_window, time_fields=time_fields)
    if any(v.get("severity") == "error" for v in violations):
        return {"status":"policy_block","message":"SQL violates policy; fix and retry.","violations":violations}

    if s.offline:
        return {
            "status":"blocked",
            "message":"Offline mode: execution disabled. Connect BigQuery and flip offline off.",
            "details":{"dataset":dataset}
        }

    # Online: need data project (tables) and optional billing project (jobs)
    data_project = store.get("data_project")
    billing_project = store.get("billing_project") or data_project
    location = store.get("location")  # e.g., "US" or "EU"
    if not data_project:
        return {"status":"blocked","message":"Missing data_project in settings. Set it via /settings and retry."}

    # Dry-run or execute
    try:
        if not confirm:
            est = bq_dry_run(sql=sql, data_project=data_project, dataset=dataset, billing_project=billing_project, location=location)
            return {"status":"estimate","message":"Dry-run cost estimate. Reply yes to execute.","estimate":est}
        # Confirmed -> execute
        res = bq_execute(sql=sql, data_project=data_project, dataset=dataset, billing_project=billing_project, location=location, max_rows=200)
        return {"status":"ok","message":"Query executed.","result":res}
    except Exception as e:
        return {"status":"blocked","message":f"BigQuery job failed: {e}"}

# ---- Chat (router via LangGraph orchestrator) ----
@app.post("/chat")
def chat_router(payload: Dict[str, Any] = Body(...)):
    """
    Body: { "text": "...", "dataset": "prod", "table": "spans" (optional) }
    Delegates to the LangGraph orchestrator (router → agents).
    """
    text = (payload or {}).get("text") or ""
    dataset = (payload or {}).get("dataset") or "prod"
    table = (payload or {}).get("table")
    out = invoke_graph(text=text, dataset=dataset, table=table)
    res = out.get("result")
    if res:
        return {"ok": True, "intent": out.get("intent"), "result": res}
    intent = out.get("intent","unknown")
    if intent == "verify_tables":
        return {"ok": False, "intent": intent, "message": "Please provide dataset and table, e.g., 'verify prod spans'."}
    return {"ok": False, "intent": intent, "message": "I didn't fully understand. Try 'verify <dataset> <table>' or ask a KPI question."}
