import os
from dataclasses import dataclass, field
from typing import List

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    # .env is optional
    pass


def _default_cors_origins() -> List[str]:
    value = os.getenv("CORS_ORIGINS")
    return value.split(",") if value else ["*"]


@dataclass
class Settings:
    """Runtime configuration loaded from environment variables."""

    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    cors_origins: List[str] = field(default_factory=_default_cors_origins)
    max_file_size_mb: int = int(os.getenv("MAX_FILE_SIZE_MB", "25"))
    max_rows: int = int(os.getenv("MAX_ROWS", "100000"))
    preview_rows: int = int(os.getenv("PREVIEW_ROWS", "20"))
    sample_rows_for_llm: int = int(os.getenv("SAMPLE_ROWS_FOR_LLM", "10"))
    enable_sql_output: bool = bool(int(os.getenv("ENABLE_SQL_OUTPUT", "1")))
    session_ttl_minutes: int = int(os.getenv("SESSION_TTL_MINUTES", "60"))


settings = Settings()


