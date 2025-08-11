## DataWiz — AI-агент для питань до таблиць (CSV/Excel)

FastAPI застосунок з простим веб-чатом, який дозволяє:
- завантажити CSV/Excel;
- ставити питання природною мовою;
- отримувати відповідь і (за можливості) SQL-запит, який використовувався для обчислень.

### Архітектура (коротко)
- Backend: `FastAPI` + `pandas` + `DuckDB` + `OpenAI` (LLM)
- Завантаження файлу створює сесію з власним in-memory DuckDB підключенням і таблицею `data`
- Чат-запит перетворюється LLM у DuckDB SQL, який виконується над `data`
- Повертається відповідь, превʼю результату, SQL та пояснення

### Запуск локально (Windows)
1. Python 3.9+
2. У корені проєкту:
   ```powershell
   python -m venv .venv
   .venv\Scripts\Activate.ps1
   pip install -r backend/requirements.txt
   $env:OPENAI_API_KEY = "<your_key_here>"   # Обовʼязково для LLM
   uvicorn backend.app:app --reload --port 8000
   ```
3. Відкрийте браузер: `http://localhost:8000`

Якщо `OPENAI_API_KEY` не задано — LLM відключено, чат відповідатиме повідомленням про відсутність конфігурації.

### Edge cases
- великі файли: обмеження за розміром (MAX_FILE_SIZE_MB) і кількістю рядків (MAX_ROWS)
- порожні таблиці: валідація при завантаженні
- некоректні запитання: LLM пояснює, чому не може побудувати SQL
- SQL-безпека: дозволені лише SELECT/WITH

### Конфігурація через змінні середовища
- `OPENAI_API_KEY` — ключ для OpenAI
- `OPENAI_MODEL` — модель (за замовчуванням `gpt-4o-mini`)
- `MAX_FILE_SIZE_MB` (25), `MAX_ROWS` (100000)
- `PREVIEW_ROWS` (20), `SAMPLE_ROWS_FOR_LLM` (10)
- `SESSION_TTL_MINUTES` (60)
- `CORS_ORIGINS` ("*")

### Структура
- `backend/app.py` — FastAPI, ендпоїнти `/api/upload`, `/api/chat`
- `backend/services/*` — менеджер сесій, LLM, виконання SQL
- `backend/utils/*` — утиліти DataFrame, логування
- `frontend/*` — простий UI (HTML/CSS/JS)