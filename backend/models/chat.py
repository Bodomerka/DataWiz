from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    session_id: str = Field(..., description="ID of the data session returned by /api/upload")
    message: str = Field(..., description="User question in natural language")


class ChatResponse(BaseModel):
    answer: str
    sql: Optional[str] = None
    result_preview: Optional[Dict[str, Any]] = None
    explanation: Optional[str] = None


