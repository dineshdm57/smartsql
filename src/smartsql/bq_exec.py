from __future__ import annotations
from typing import Dict, Any, Optional
import re

def _has_limit(sql: str) -> bool:
    return bool(re.search(r"\bLIMIT\s+\d+\b", sql, re.I))

def dry_run(sql: str, data_project: str, dataset: str, billing_project: Optional[str], location: Optional[str]) -> Dict[str, Any]:
    from google.cloud import bigquery
    client = bigquery.Client(project=billing_project or data_project, location=location)
    job_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
        default_dataset=f"{data_project}.{dataset}",
    )
    job = client.query(sql, job_config=job_config)
    bytes_proc = job.total_bytes_processed or 0
    return {
        "status": "estimate",
        "bytes_processed": int(bytes_proc),
        "dataset": dataset,
        "billing_project": billing_project or data_project,
        "location": location or "auto",
        "has_limit": _has_limit(sql),
    }

def execute(sql: str, data_project: str, dataset: str, billing_project: Optional[str], location: Optional[str], max_rows: int = 200) -> Dict[str, Any]:
    from google.cloud import bigquery
    client = bigquery.Client(project=billing_project or data_project, location=location)
    job_config = bigquery.QueryJobConfig(
        default_dataset=f"{data_project}.{dataset}",
        use_query_cache=True,
    )
    job = client.query(sql, job_config=job_config)
    result = job.result(max_results=max_rows)
    schema = [{"name": f.name, "type": f.field_type, "mode": f.mode} for f in result.schema]
    rows = [dict(row.items()) for row in result]
    return {
        "status": "ok",
        "rowcount": len(rows),
        "schema": schema,
        "rows": rows,
        "bytes_processed": int(job.total_bytes_processed or 0),
        "slot_ms": int(job.slot_millis or 0),
        "dataset": dataset,
        "billing_project": billing_project or data_project,
        "location": location or "auto",
    }
