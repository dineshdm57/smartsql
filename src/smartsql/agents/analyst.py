from typing import Dict, Any, List
from smartsql.registry import get_active_contract
from smartsql.llm import get_llm

class AnalystAgent:
    name = "C"

    def handle(self, text: str) -> str:
        return "C(analyst): stub ok"

    def draft_sql(self, nl_query: str, dataset: str = "prod") -> Dict[str, Any]:
        contract = get_active_contract()
        if not contract:
            return {"status": "blocked", "message": "No active contract. Upload/activate a contract first."}

        entities = (contract or {}).get("entities") or {}
        if not entities:
            return {"status": "blocked", "message": "Active contract has no entities defined."}

        parts: List[str] = []
        time_field_candidates: List[str] = []
        allowed_tables = list(entities.keys())
        allowed_fq = [f"`{dataset}.{tbl}`" for tbl in allowed_tables]
        for tbl, ent in entities.items():
            fields = ent.get("fields") or {}
            fdescs = []
            for k, v in fields.items():
                t = (v.get("type") or "").upper()
                fdescs.append(f"{k}:{t}")
                if t in {"TIMESTAMP", "DATETIME", "DATE"} or k.lower() in {"ts","timestamp","event_ts","created_at","time"}:
                    time_field_candidates.append(f"{tbl}.{k}")
            parts.append(f"- {tbl}(" + ", ".join(fdescs) + ")")
        contract_summary = "\n".join(parts)
        time_hint = ", ".join(time_field_candidates) if time_field_candidates else "none available"

        table_rule_extra = ""
        if len(allowed_fq) == 1:
            table_rule_extra = f"You MUST use the only allowed table {allowed_fq[0]}."
        else:
            table_rule_extra = f"Choose only from these tables: {', '.join(allowed_fq)}."

        rules = f"""
You are SmartSQL Analyst. Convert the user's natural language request into ONE BigQuery StandardSQL SELECT query.

HARD RULES:
- SELECT-only. No DDL/DML/temporary tables. One statement only.
- Use ONLY tables/fields from this contract:
{contract_summary}
- Fully-qualify tables with BACKTICKS as one of: {', '.join(allowed_fq)}.
  Never write placeholders like `{dataset}.table` or unqualified names.
  {table_rule_extra}
- Do NOT add WHERE filters or constants unless explicitly requested by the user.
- If the user mentions a time window and a valid time field exists, apply it using one of: {time_hint}.
  If none exist, omit the time filter and do NOT invent a field.
- If asked for "success rate", compute as AVG(CAST(success AS INT64)).
- Prefer simple, readable SQL with explicit JOINs and clear aliases (t1, t2, ...).
- Always end with:  LIMIT 5000
- Output ONLY raw SQL. No explanations. No markdown or code fences.
""".strip()

        prompt = rules + "\n\nUser request:\n" + nl_query.strip()

        llm = get_llm()
        sql = llm.chat([{"role": "user", "content": prompt}]).strip()

        if sql.startswith("```"):
            sql = sql.strip("`").replace("sql", "", 1).strip()
        return {
            "status": "draft",
            "message": "Draft SQL generated (offline mode; execution disabled).",
            "contract_version": contract.get("version"),
            "dataset": dataset,
            "sql": sql,
            "can_execute": False,
            "reason": "offline mode (no BigQuery execution); dry-run not attempted."
        }
