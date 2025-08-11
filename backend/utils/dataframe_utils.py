from __future__ import annotations

import io
from typing import Any, Dict, List, Tuple

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype


def try_read_csv(file_bytes: bytes) -> pd.DataFrame:
    """Try reading CSV with common encodings. Raise last exception if all fail."""
    last_exc: Exception | None = None
    for encoding in ("utf-8", "utf-8-sig", "cp1251", "latin1"):
        try:
            return pd.read_csv(io.BytesIO(file_bytes), encoding=encoding)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
    if last_exc:
        raise last_exc
    raise ValueError("Unable to read CSV with common encodings")


def read_dataframe_from_bytes(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Load a DataFrame from CSV/XLS/XLSX bytes.

    - For CSV tries multiple encodings
    - For Excel reads the first sheet
    """
    lower = filename.lower()
    if lower.endswith(".csv"):
        df = try_read_csv(file_bytes)
    elif lower.endswith(".xlsx") or lower.endswith(".xls"):
        df = pd.read_excel(io.BytesIO(file_bytes))
    else:
        # Fallback: try CSV, then Excel
        try:
            df = try_read_csv(file_bytes)
        except Exception:  # noqa: BLE001
            df = pd.read_excel(io.BytesIO(file_bytes))

    # Normalize column names by stripping whitespace
    df.columns = [str(c).strip() for c in df.columns]
    return df


def infer_column_kind(series: pd.Series) -> str:
    if is_datetime64_any_dtype(series):
        return "datetime"
    if is_numeric_dtype(series):
        return "numeric"
    # Treat low-cardinality strings as categorical
    unique_count = series.dropna().nunique()
    if unique_count <= max(10, int(0.02 * len(series))):
        return "categorical"
    return "text"


def infer_schema(df: pd.DataFrame) -> List[Dict[str, Any]]:
    schema: List[Dict[str, Any]] = []
    for name in df.columns:
        kind = infer_column_kind(df[name])
        schema.append(
            {
                "name": name,
                "pandas_dtype": str(df[name].dtype),
                "kind": kind,
            }
        )
    return schema


def preview_dataframe(df: pd.DataFrame, rows: int) -> Dict[str, Any]:
    sample = df.head(rows)
    return {
        "columns": list(sample.columns),
        "rows": sample.to_dict(orient="records"),
        "row_count": int(len(df)),
        "column_count": int(df.shape[1]),
    }


