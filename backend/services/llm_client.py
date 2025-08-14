from __future__ import annotations

import json
import re
from typing import Any, Dict, List

from backend.config import settings
from backend.utils.logger import logger


class LLMClient:
    """LLM wrapper for text-to-SQL planning. Uses LangChain when available."""

    def __init__(self) -> None:
        # Base OpenAI client (fallback path)
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

        # Try to prepare a LangChain pipeline
        self._lc_chain = None
        if self._client is not None:
            try:
                from langchain_openai import ChatOpenAI  # type: ignore
                from langchain.prompts import ChatPromptTemplate  # type: ignore
                from langchain_core.output_parsers import (  # type: ignore
                    JsonOutputParser,
                )

                self._lc_llm = ChatOpenAI(
                    model=settings.openai_model,
                    temperature=0.1,
                    api_key=settings.openai_api_key,
                )

                # Few high-signal instructions with strict JSON output
                system_instructions = (
                    "You are a senior data analyst translating questions to DuckDB SQL over a single table named \"data\". "
                    "Rules: Use only provided columns; quote identifiers with double quotes; use DuckDB syntax; "
                    "return STRICT JSON with keys: sql (string, only SELECT or WITH), explanation (short), answer_hint (short, optional). "
                    "If not answerable, set sql to empty string and explain why."
                )

                human_template = (
                    "Table schema (name, pandas dtype, kind):\n{schema_text}\n\n"
                    "Small data sample (JSON rows):\n{sample_text}\n\n"
                    "Question: {question}\n\n"
                    "Respond with JSON only, no markdown fences."
                )

                prompt = ChatPromptTemplate.from_messages(
                    [
                        ("system", system_instructions),
                        ("human", human_template),
                    ]
                )

                parser = JsonOutputParser()
                # LCEL chain: prompt -> model -> JSON parser
                self._lc_chain = prompt | self._lc_llm | parser
            except Exception as exc:  # noqa: BLE001
                # LangChain is optional; fallback to raw OpenAI client
                logger.info("LangChain unavailable, falling back to raw OpenAI: %s", exc)
                self._lc_chain = None

    def is_available(self) -> bool:
        return self._client is not None

    def _extract_json(self, text: str) -> Dict[str, Any]:
        """Extract the first JSON object from a string; fallback to empty dict."""
        cleaned = text.strip()
        cleaned = re.sub(r"^```(json)?", "", cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()
        try:
            return json.loads(cleaned)
        except Exception:  # noqa: BLE001
            pass
        match = re.search(r"\{[\s\S]*\}", cleaned)
        if match:
            try:
                return json.loads(match.group(0))
            except Exception:  # noqa: BLE001
                return {}
        return {}

    def _format_schema(self, schema: List[Dict[str, Any]]) -> str:
        return "\n".join(
            [f"- \"{col['name']}\" ({col['pandas_dtype']}, {col['kind']})" for col in schema]
        )

    def propose_sql(self, question: str, schema: List[Dict[str, Any]], sample_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not self._client:
            raise RuntimeError("LLM is not available. Set OPENAI_API_KEY in environment.")

        schema_text = self._format_schema(schema)
        sample_text = json.dumps(sample_rows[: settings.sample_rows_for_llm], ensure_ascii=False)

        # Preferred path: LangChain pipeline with strict JSON parsing
        if self._lc_chain is not None:
            try:
                parsed = self._lc_chain.invoke(
                    {
                        "schema_text": schema_text,
                        "sample_text": sample_text,
                        "question": question,
                    }
                )
                if not isinstance(parsed, dict):
                    parsed = {}
                return {
                    "sql": (parsed.get("sql") or "").strip(),
                    "explanation": (parsed.get("explanation") or "").strip(),
                    "answer_hint": (parsed.get("answer_hint") or "").strip(),
                    "raw": json.dumps(parsed, ensure_ascii=False),
                }
            except Exception as exc:  # noqa: BLE001
                logger.info("LangChain parsing failed, retrying with raw OpenAI: %s", exc)

        # Fallback: raw OpenAI Chat Completions
        try:
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
        except Exception as exc:  # noqa: BLE001
            logger.error("LLM call failed: %s", exc)
            return {
                "sql": "",
                "explanation": "LLM call failed",
                "answer_hint": "I could not derive a valid SQL for this question.",
                "raw": "{}",
            }

    def repair_sql(
        self,
        question: str,
        schema: List[Dict[str, Any]],
        sample_rows: List[Dict[str, Any]],
        previous_sql: str,
        error_message: str,
    ) -> Dict[str, Any]:
        """Attempt to fix a failing DuckDB SQL using the error message as guidance."""
        if not self._client:
            raise RuntimeError("LLM is not available. Set OPENAI_API_KEY in environment.")

        schema_text = self._format_schema(schema)
        sample_text = json.dumps(sample_rows[: settings.sample_rows_for_llm], ensure_ascii=False)

        # Try LangChain structured repair
        if self._lc_chain is not None:
            try:
                from langchain_openai import ChatOpenAI  # type: ignore
                from langchain.prompts import ChatPromptTemplate  # type: ignore
                from langchain_core.output_parsers import JsonOutputParser  # type: ignore

                llm = getattr(self, "_lc_llm", None) or ChatOpenAI(
                    model=settings.openai_model,
                    temperature=0.1,
                    api_key=settings.openai_api_key,
                )
                system_instructions = (
                    "You are a DuckDB SQL expert. Given a question, schema, sample rows, a previous SQL and an error, "
                    "produce a corrected SQL query if possible. Use only the provided columns, quote identifiers with double quotes, "
                    "and return STRICT JSON with keys: sql, explanation, answer_hint (optional). If not fixable, set sql to empty string."
                )
                human_template = (
                    "Table schema (name, pandas dtype, kind):\n{schema_text}\n\n"
                    "Sample rows (JSON):\n{sample_text}\n\n"
                    "Question: {question}\n\n"
                    "Previous SQL:\n{previous_sql}\n\n"
                    "Execution error: {error_message}\n\n"
                    "Respond with JSON only."
                )
                prompt = ChatPromptTemplate.from_messages(
                    [("system", system_instructions), ("human", human_template)]
                )
                parser = JsonOutputParser()
                parsed = (prompt | llm | parser).invoke(
                    {
                        "schema_text": schema_text,
                        "sample_text": sample_text,
                        "question": question,
                        "previous_sql": previous_sql,
                        "error_message": error_message,
                    }
                )
                if not isinstance(parsed, dict):
                    parsed = {}
                return {
                    "sql": (parsed.get("sql") or "").strip(),
                    "explanation": (parsed.get("explanation") or "").strip(),
                    "answer_hint": (parsed.get("answer_hint") or "").strip(),
                    "raw": json.dumps(parsed, ensure_ascii=False),
                }
            except Exception as exc:  # noqa: BLE001
                logger.info("LangChain repair failed, retrying with raw OpenAI: %s", exc)

        # Fallback to raw OpenAI
        try:
            system_prompt = (
                "You are a DuckDB SQL expert. Given a question, schema, sample rows, a previous SQL and an error, "
                "produce a corrected SQL query if possible. Use only the provided columns, quote identifiers with double quotes. "
                "Return JSON: {sql, explanation, answer_hint?}. If not fixable, set sql to empty string."
            )
            user_prompt = (
                f"Table schema (name, pandas dtype, kind):\n{schema_text}\n\n"
                f"Sample rows (JSON):\n{sample_text}\n\n"
                f"Question: {question}\n\n"
                f"Previous SQL:\n{previous_sql}\n\n"
                f"Execution error: {error_message}\n\n"
                "Respond with JSON only."
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
        except Exception as exc:  # noqa: BLE001
            logger.error("LLM repair call failed: %s", exc)
            return {
                "sql": "",
                "explanation": "LLM repair failed",
                "answer_hint": "I could not fix the SQL for this question.",
                "raw": "{}",
            }


llm_client = LLMClient()


