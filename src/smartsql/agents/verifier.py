from typing import Dict, Any, Optional, List, Tuple
from smartsql.registry import get_active_contract
from smartsql.config import get_settings
from smartsql.catalog import get_local_catalog

class VerifierAgent:
    name = "B"

    def verify(self, project: Optional[str] = None, dataset: Optional[str] = None) -> Dict[str, Any]:
        """Connectivity check (cloud only). Skipped in offline mode."""
        s = get_settings()
        if s.offline:
            return {"status": "ok", "message": "Offline mode: cloud checks skipped.", "details": {"offline": True}}

        info: Dict[str, Any] = {"project": project, "dataset": dataset}
        try:
            from google.cloud import bigquery
            client = bigquery.Client(project=project)
            info["adc_found"] = True

            if dataset:
                ds_ref = client.dataset(dataset)
                ds = client.get_dataset(ds_ref)
                info.update({"dataset_exists": True, "location": ds.location})
                return {"status": "ok", "message": f"Connected to BigQuery; dataset '{dataset}' exists.", "details": info}
            else:
                sample = [d.dataset_id for d in client.list_datasets(page_size=3)]
                info["datasets_sample"] = sample
                return {"status": "ok", "message": "Connected to BigQuery; no dataset specified.", "details": info}

        except Exception as e:
            hint = (
                "Set GOOGLE_APPLICATION_CREDENTIALS to a service account JSON, "
                "or run: gcloud auth application-default login"
            )
            return {"status": "blocked", "message": f"BigQuery access failed: {e}", "details": {"hint": hint, **info}}

    def compare_to_contract(self, project: Optional[str], dataset: str, table: str) -> Dict[str, Any]:
        """
        Compare active Contract vs a table schema.
        - Offline mode: use Local Catalog (no cloud).
        - Online: fetch BigQuery table schema (metadata-only).
        Checks field existence + TYPE + MODE. No row scans.
        """
        active = get_active_contract()
        if not active:
            return {"status": "blocked", "message": "No active contract. Upload/activate a contract first.", "details": {}}

        contract_version = active.get("version")
        entities = (active or {}).get("entities") or {}
        entity = entities.get(table)
        if not entity:
            return {
                "status": "blocked",
                "message": f"Table '{table}' not defined in active contract.",
                "details": {"contract_version": contract_version, "entities_available": list(entities.keys())}
            }

        expected_fields: Dict[str, Dict[str, str]] = (entity.get("fields") or {})
        exp: Dict[str, Tuple[str, str]] = {
            name.lower(): (fld.get("type","").upper(), (fld.get("mode") or "NULLABLE").upper())
            for name, fld in expected_fields.items()
        }

        s = get_settings()
        if s.offline:
            # --- OFFLINE: compare against Local Catalog ---
            catalog = get_local_catalog()
            if not catalog:
                return {
                    "status": "blocked",
                    "message": "Offline mode: no Local Catalog found. Upload/set a catalog first.",
                    "details": {"hint": "POST /catalog with a JSON metadata map"}
                }

            # Expect structure: {"datasets": {"prod": {"spans": {"fields": {"field":{"type":..,"mode":..}}}}}}
            ds_map = (catalog.get("datasets") or {})
            tentry = ((ds_map.get(dataset) or {}).get(table) or {})
            afields: Dict[str, Dict[str, str]] = (tentry.get("fields") or {})
            act: Dict[str, Tuple[str, str]] = {
                name.lower(): (fld.get("type","").upper(), (fld.get("mode") or "NULLABLE").upper())
                for name, fld in afields.items()
            }
            return self._diff(exp, act, contract_version, project="offline", dataset=dataset, table=table)

        # --- ONLINE: BigQuery metadata compare ---
        try:
            from google.cloud import bigquery
            client = bigquery.Client(project=project)
            proj = client.project if project is None else project
            table_ref = f"{proj}.{dataset}.{table}"
            tbl = client.get_table(table_ref)
            act: Dict[str, Tuple[str, str]] = {c.name.lower(): (c.field_type.upper(), c.mode.upper()) for c in tbl.schema}
            return self._diff(exp, act, contract_version, project=proj, dataset=dataset, table=table)
        except Exception as e:
            return {
                "status": "blocked",
                "message": f"Failed to fetch table metadata or compare: {e}",
                "details": {"dataset": dataset, "table": table, "contract_version": contract_version}
            }

    def _diff(
        self,
        expected: Dict[str, Tuple[str, str]],
        actual: Dict[str, Tuple[str, str]],
        contract_version: Optional[str],
        project: str,
        dataset: str,
        table: str
    ) -> Dict[str, Any]:
        missing = [name for name in expected.keys() if name not in actual]
        extra = [name for name in actual.keys() if name not in expected]
        type_mismatches: List[Dict[str, Any]] = []
        mode_mismatches: List[Dict[str, Any]] = []

        for name, (t_exp, m_exp) in expected.items():
            if name in actual:
                t_act, m_act = actual[name]
                if t_exp != t_act:
                    type_mismatches.append({"field": name, "expected": t_exp, "actual": t_act})
                if m_exp != m_act:
                    mode_mismatches.append({"field": name, "expected": m_exp, "actual": m_act})

        if missing or type_mismatches:
            status = "blocker"
        elif mode_mismatches or extra:
            status = "warn"
        else:
            status = "pass"

        summary = f"{len(expected)-len(missing)}/{len(expected)} fields match; " \
                  f"{len(missing)} missing, {len(type_mismatches)} type diffs, {len(mode_mismatches)} mode diffs, {len(extra)} extra."

        return {
            "status": status,
            "message": summary,
            "details": {
                "contract_version": contract_version,
                "project": project,
                "dataset": dataset,
                "table": table,
                "expected_count": len(expected),
                "missing_fields": missing,
                "type_mismatches": type_mismatches,
                "mode_mismatches": mode_mismatches,
                "extra_fields": extra
            }
        }
