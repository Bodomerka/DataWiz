from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from threading import RLock
from typing import Dict, Optional

import duckdb
import pandas as pd
from fastapi import UploadFile

from backend.config import settings
from backend.utils.dataframe_utils import infer_schema, preview_dataframe, read_dataframe_from_bytes
from backend.utils.logger import logger


@dataclass
class DataSession:
    session_id: str
    duckdb_conn: duckdb.DuckDBPyConnection
    table_name: str
    dataframe: pd.DataFrame
    created_at: float = field(default_factory=lambda: time.time())
    last_used_at: float = field(default_factory=lambda: time.time())

    def touch(self) -> None:
        self.last_used_at = time.time()


class DataManager:
    """In-memory session store for uploaded data and attached DuckDB connections."""

    def __init__(self) -> None:
        self._sessions: Dict[str, DataSession] = {}
        self._lock = RLock()

    def _make_session_id(self) -> str:
        return uuid.uuid4().hex

    async def create_session_from_upload(self, file: UploadFile) -> Dict:
        file_bytes = await file.read()
        size_mb = len(file_bytes) / (1024 * 1024)
        if size_mb > settings.max_file_size_mb:
            raise ValueError(
                f"File too large: {size_mb:.1f}MB > {settings.max_file_size_mb}MB"
            )

        # Heavy IO/CPU in thread
        df: pd.DataFrame = await asyncio.to_thread(
            read_dataframe_from_bytes, file_bytes, file.filename
        )

        if df.empty:
            raise ValueError("Uploaded table has no rows")

        if len(df) > settings.max_rows:
            logger.info(
                "Truncating rows from %s to MAX_ROWS=%s for performance",
                len(df),
                settings.max_rows,
            )
            df = df.head(settings.max_rows).copy()

        # Create session + attach DataFrame to DuckDB
        session_id = self._make_session_id()
        conn = duckdb.connect()
        table_name = "data"
        conn.register("df_view", df)
        conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_view"
        )

        session = DataSession(
            session_id=session_id,
            duckdb_conn=conn,
            table_name=table_name,
            dataframe=df,
        )

        with self._lock:
            self._sessions[session_id] = session

        schema = infer_schema(df)
        preview = preview_dataframe(df, settings.preview_rows)

        return {
            "session_id": session_id,
            "schema": schema,
            "preview": preview,
        }

    def get_session(self, session_id: str) -> DataSession:
        with self._lock:
            session = self._sessions.get(session_id)
        if not session:
            raise KeyError("Session not found or expired")
        session.touch()
        return session

    def maybe_cleanup(self) -> None:
        ttl_seconds = settings.session_ttl_minutes * 60
        now = time.time()
        to_delete: list[str] = []
        with self._lock:
            for sid, sess in self._sessions.items():
                if now - sess.last_used_at > ttl_seconds:
                    to_delete.append(sid)
            for sid in to_delete:
                try:
                    self._sessions[sid].duckdb_conn.close()
                except Exception:  # noqa: BLE001
                    pass
                del self._sessions[sid]
        if to_delete:
            logger.info("Cleaned up %d expired sessions", len(to_delete))


data_manager = DataManager()


