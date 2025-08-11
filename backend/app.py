from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import orjson
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from backend.config import settings
from backend.models.chat import ChatRequest, ChatResponse
from backend.services.data_manager import data_manager
from backend.services.query_engine import query_engine
from backend.utils.logger import logger


def orjson_dumps(v: Any, *, default: Any | None = None) -> str:
    return orjson.dumps(v, default=default).decode()


try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


app = FastAPI(default_response_class=JSONResponse)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/healthz")
async def healthz() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/api/upload")
async def upload_table(file: UploadFile = File(...)) -> Dict[str, Any]:
    try:
        result = await data_manager.create_session_from_upload(file)
        return result
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve)) from ve
    except Exception as exc:  # noqa: BLE001
        logger.exception("Upload failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/chat", response_model=ChatResponse)
async def chat(req: ChatRequest) -> ChatResponse:
    try:
        result = await _answer_chat(req)
        return ChatResponse(**result)
    except KeyError as ke:
        raise HTTPException(status_code=404, detail=str(ke)) from ke
    except RuntimeError as re:
        raise HTTPException(status_code=503, detail=str(re)) from re
    except Exception as exc:  # noqa: BLE001
        logger.exception("Chat failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


async def _answer_chat(req: ChatRequest) -> Dict[str, Any]:
    # Offload heavy steps if needed; current operations are lightweight
    return await _to_thread(query_engine.answer, req.session_id, req.message)


async def _to_thread(func, *args, **kwargs):  # type: ignore[no-untyped-def]
    import asyncio

    return await asyncio.to_thread(func, *args, **kwargs)


# Serve frontend
root_dir = Path(__file__).resolve().parents[1]
frontend_dir = root_dir / "frontend"
if frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(frontend_dir), html=True), name="frontend")


