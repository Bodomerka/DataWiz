from __future__ import annotations

from typing import Any, Dict

from backend.config import settings
from backend.services.data_manager import DataSession, data_manager
from backend.services.llm_client import llm_client
from backend.services.sql_runner import run_sql
from backend.utils.dataframe_utils import preview_dataframe


class QueryEngine:
    def answer(self, session_id: str, question: str) -> Dict[str, Any]:
        session: DataSession = data_manager.get_session(session_id)
        data_manager.maybe_cleanup()

        schema = [
            {"name": c, "pandas_dtype": str(session.dataframe[c].dtype), "kind": ""}
            for c in session.dataframe.columns
        ]
        sample = session.dataframe.head(settings.sample_rows_for_llm).to_dict(
            orient="records"
        )

        if not llm_client.is_available():
            return {
                "answer": "LLM is not configured. Set OPENAI_API_KEY to enable natural language querying.",
                "sql": None,
                "result_preview": None,
                "explanation": None,
            }

        proposal = llm_client.propose_sql(question=question, schema=schema, sample_rows=sample)
        sql = proposal.get("sql") or ""
        explanation = proposal.get("explanation") or None

        if not sql:
            return {
                "answer": proposal.get("answer_hint") or "I could not derive a valid SQL for this question.",
                "sql": None,
                "result_preview": None,
                "explanation": explanation,
            }

        df, meta = run_sql(session, sql)
        if df is None:
            return {
                "answer": meta.get("error", "SQL execution failed"),
                "sql": sql if settings.enable_sql_output else None,
                "result_preview": None,
                "explanation": explanation,
            }

        # Build a small textual answer if the result is a single value
        answer_text: str
        if df.shape == (1, 1):
            answer_text = f"{df.columns[0]} = {df.iloc[0, 0]}"
        else:
            answer_text = (
                f"Query returned {meta.get('row_count')} rows and {len(meta.get('columns', []))} columns."
            )

        return {
            "answer": answer_text,
            "sql": sql if settings.enable_sql_output else None,
            "result_preview": preview_dataframe(df, rows=min(20, len(df))),
            "explanation": explanation,
        }


query_engine = QueryEngine()


