# Lead Enrichment Local

Local FastAPI + Jinja2 + SQLite lead enrichment with local Ollama only (no paid APIs/cloud).

## Debug + Observability Features

- `DEBUG_MODE` toggle in `.env` for richer diagnostics.
- Structured backend logging (startup, upload, parsing, enrichment, per-lead stages, Ollama calls, exports).
- CSV Parse Inspector before enrichment starts.
- Persisted CSV parse diagnostics (`csv_parse_diagnostics`).
- Per-lead processing trace (`lead_debug_events`).
- Raw Ollama diagnostics persisted on classifications.
- Local LLM test UI page and health page.

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
cp .env.example .env
```

Start Ollama and model:

```bash
ollama serve
ollama pull qwen3-coder:30b
```

Run app:

```bash
uvicorn app.main:app --reload
```

## Enable DEBUG_MODE

In `.env`:

```env
DEBUG_MODE=true
```

When true, UI shows richer traces and DB stores extra payloads (`payload_json`, Ollama request/response diagnostics).

## CSV Preview / Parse Inspector

1. Upload CSV on `/`.
2. You are redirected to `/runs/{id}/preview`.
3. Inspect:
   - original filename
   - detected/normalized headers
   - internal mapping (`company_name`, `website`, `city`, `state`, `phone`, `email`)
   - first 10 parsed rows and cleaned rows
   - found/missing columns and warnings
4. Click **Start Enrichment**.

## Local LLM Test Page

Open `/debug/llm`:

- View configured Ollama URL/model and debug flag.
- **Test Connection** checks reachability + model availability.
- **Send Prompt** sends manual prompt/system/options and shows:
  - raw response
  - parsed response
  - parse errors
  - request duration

## Health Page

Open `/debug/health` to check:

- app status
- DB connectivity
- Ollama connectivity + model
- uploads/exports directories
- run count
- `DEBUG_MODE`

## Stored Debug Data

### `csv_parse_diagnostics`
Stores original/normalized headers, header mapping, row count, preview rows, cleaned preview rows, warnings.

### `lead_debug_events`
Chronological per-lead stage events (`normalize`, `crawl`, `extract`, `classify`, `score`, `persist`) with concise messages and optional payloads.

### `lead_classifications` (new debug fields)
- `ollama_request_payload_json`
- `ollama_raw_response`
- `ollama_parse_error`

## Main Routes

- `/` upload + runs
- `/runs/{id}/preview` CSV inspector
- `/runs/{id}` run debug view
- `/leads/{id}` lead detail + trace
- `/debug/llm` local LLM testing
- `/debug/health` sanity checks

