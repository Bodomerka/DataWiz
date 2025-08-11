from __future__ import annotations

import json
import re
from typing import Any, Dict, List

from backend.config import settings
from backend.utils.logger import logger


class LLMClient:
    """Minimal OpenAI Chat Completions wrapper used for text-to-SQL planning."""

    def __init__(self) -> None:
        if not settings.openai_api_key:
            self._client = None
            logger.warning("OPENAI_API_KEY not set. LLM features will be disabled.")
        else:
            try:
                from openai import OpenAI  # type: ignore

                self._client = OpenAI(api_key=settings.openai_api_key)
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to initialize OpenAI client: %s", exc)
                self._client = None

    def is_available(self) -> bool:
        return self._client is not None

    def _extract_json(self, text: str) -> Dict[str, Any]:
        """Extract the first JSON object from a string; fallback to empty dict."""
        # Remove code fences if present
        cleaned = text.strip()
        cleaned = re.sub(r"^```(json)?", "", cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()

        # Try direct parse
        try:
            return json.loads(cleaned)
        except Exception:  # noqa: BLE001
            pass

        # Fallback: find the first {...} block
        match = re.search(r"\{[\s\S]*\}", cleaned)
        if match:
            try:
                return json.loads(match.group(0))
            except Exception:  # noqa: BLE001
                return {}
        return {}

    def propose_sql(self, question: str, schema: List[Dict[str, Any]], sample_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not self._client:
            raise RuntimeError("LLM is not available. Set OPENAI_API_KEY in environment.")

        schema_text = "\n".join(
            [f"- \"{col['name']}\" ({col['pandas_dtype']}, {col['kind']})" for col in schema]
        )

        sample_text = json.dumps(sample_rows[: settings.sample_rows_for_llm], ensure_ascii=False)

        system_prompt = (
            "You are a senior data analyst helping translate natural language questions into DuckDB SQL. "
            "You work with a single table named `data`. "
            "Only use the provided columns. Quote identifiers with double quotes. Use DuckDB syntax. "
            "Return a strict JSON object with keys: sql (string, SELECT/CTE only), explanation (short string), answer_hint (optional short string). "
            "If the question cannot be answered with the columns, set sql to an empty string and explain why."
        )

        user_prompt = (
            f"Table schema (name, pandas dtype, kind):\n{schema_text}\n\n"
            f"Small data sample (JSON rows):\n{sample_text}\n\n"
            f"Question: {question}\n\n"
            "Respond with JSON only, no markdown fences. Example: {\"sql\": \"SELECT ...\", \"explanation\": \"...\"}."
        )

        completion = self._client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.1,
        )

        content = completion.choices[0].message.content or "{}"
        parsed = self._extract_json(content)
        if not isinstance(parsed, dict):
            parsed = {}
        return {
            "sql": (parsed.get("sql") or "").strip(),
            "explanation": (parsed.get("explanation") or "").strip(),
            "answer_hint": (parsed.get("answer_hint") or "").strip(),
            "raw": content,
        }


llm_client = LLMClient()


