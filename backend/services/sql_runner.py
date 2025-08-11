from __future__ import annotations

import re
from typing import Any, Dict, Tuple

import pandas as pd

from backend.services.data_manager import DataSession


FORBIDDEN_TOKENS = {
    "drop",
    "insert",
    "update",
    "delete",
    "create",
    "alter",
    "copy",
    "attach",
    "detach",
    "pragma",
    "call",
    "system",
}


def is_safe_select(sql: str) -> bool:
    text = re.sub(r"\s+", " ", sql).strip().strip(";")
    lowered = text.lower()
    if not (lowered.startswith("select ") or lowered.startswith("with ")):
        return False
    if any(tok in lowered for tok in FORBIDDEN_TOKENS):
        return False
    return True


def run_sql(session: DataSession, sql: str) -> Tuple[pd.DataFrame | None, Dict[str, Any]]:
    """Execute a SELECT/CTE query against the session's DuckDB table.

    Returns (df, meta) where meta may contain an 'error' message.
    """
    sql_clean = sql.strip().strip(";")
    if not is_safe_select(sql_clean):
        return None, {"error": "Only SELECT/WITH queries are allowed."}
    try:
        df = session.duckdb_conn.execute(sql_clean).df()
        return df, {
            "row_count": int(len(df)),
            "columns": list(df.columns),
        }
    except Exception as exc:  # noqa: BLE001
        return None, {"error": f"SQL execution failed: {exc}"}


