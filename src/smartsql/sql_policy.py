import re
from typing import List, Dict

FORBIDDEN = re.compile(r'\b(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|MERGE|TRUNCATE|BEGIN|COMMIT)\b', re.I)
CROSS_OR_NATURAL = re.compile(r'\b(CROSS\s+JOIN|NATURAL\s+JOIN)\b', re.I)
JOIN_NO_ON = re.compile(r'\bJOIN\b(?![^;]*\bON\b)', re.I)  # naive: JOIN without ON in same statement

def lint_sql(sql: str, dataset: str, require_time_window: bool = True, time_fields: List[str] | None = None) -> List[Dict]:
    """
    Returns a list of violations: {code, severity, message}
    Policy:
      - SELECT-only
      - Fully-qualified tables with the configured dataset
      - Time window (if required)
      - Row limit present
      - No CROSS/NATURAL JOIN; warn on JOIN without ON (naive)
    """
    violations: List[Dict] = []
    s = (sql or "").strip()

    # SELECT-only
    if not re.search(r'^\s*SELECT\b', s, re.I):
        violations.append({"code":"NOT_SELECT", "severity":"error", "message":"Query must start with SELECT."})
    if FORBIDDEN.search(s):
        violations.append({"code":"FORBIDDEN_STATEMENT", "severity":"error", "message":"DDL/DML keywords detected."})

    # Fully-qualified tables
    tables = re.findall(r'`([^`]+)`', s)
    if tables:
        bad = [t for t in tables if not t.startswith(f"{dataset}.")]
        if bad:
            violations.append({"code":"BAD_DATASET", "severity":"error", "message":f"Tables must be qualified with `{dataset}.` Found: {bad}."})
    else:
        # If no backticked tables are found, warn the user to fully-qualify
        violations.append({"code":"UNQUALIFIED_TABLES", "severity":"warn", "message":"No fully-qualified tables found (use backticks and dataset.table)."})

    # Time window requirement
    if require_time_window:
        time_ok = False
        tfields = time_fields or []
        # crude check: any time field name and a comparator near it
        for f in tfields:
            if re.search(rf'\b{re.escape(f)}\b\s*(>=|>|BETWEEN)', s, re.I):
                time_ok = True
                break
        if not time_ok:
            violations.append({"code":"MISSING_TIME_WINDOW", "severity":"error", "message":"Missing required time window filter on a declared time field."})

    # LIMIT requirement
    if not re.search(r'\bLIMIT\s+\d+\b', s, re.I):
        violations.append({"code":"MISSING_LIMIT", "severity":"warn", "message":"LIMIT not found; add LIMIT to control scan size."})

    # Cross/Natural join disallowed
    if CROSS_OR_NATURAL.search(s):
        violations.append({"code":"BAD_JOIN", "severity":"error", "message":"CROSS/NATURAL JOIN not allowed."})

    # JOIN without ON (naive check)
    if JOIN_NO_ON.search(s):
        violations.append({"code":"JOIN_WITHOUT_ON", "severity":"warn", "message":"JOIN without ON detected (naive check). Ensure explicit join conditions."})

    # Semicolons (multi-statement) are discouraged
    if ";" in s.strip()[:-1]:
        violations.append({"code":"MULTI_STATEMENT", "severity":"warn", "message":"Multiple statements detected; only one SELECT is allowed."})

    return violations
