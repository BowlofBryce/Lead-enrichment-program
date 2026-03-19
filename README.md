# Lead Enrichment Local

A local-first, free lead enrichment web app for personal research and outreach prep.

Upload a CSV, crawl public business websites, extract structured signals using deterministic parsing first, enrich fuzzy fields with local Ollama classification, review results in a simple UI, and export enriched CSVs.

## Stack

- Python 3.11+
- FastAPI + Jinja2 templates
- SQLite + SQLAlchemy
- Pandas
- Requests + BeautifulSoup4
- Playwright (fallback for JS-heavy pages)
- Ollama (local), default model: `qwen3-coder`

## What It Does

1. Upload lead CSV
2. Normalize/clean and dedupe rows
3. Crawl homepage + likely subpages (`contact`, `about`, `team`, `services`)
4. Extract emails, phones, social links, address, and website signals
5. Classify/summarize with local Ollama (when available)
6. Persist everything in SQLite
7. Show run + lead details in web UI
8. Export enriched results as CSV

## Project Structure

```text
app/
  main.py
  db.py
  models.py
  schemas.py
  routes/
  services/
  templates/
  static/
data/
  uploads/
  exports/
  sample_leads.csv
requirements.txt
.env.example
README.md
```

## Setup (Mac Local)

1. Create a virtualenv:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Install Playwright browser runtime (required for fallback crawling):

```bash
playwright install chromium
```

4. Create `.env` from example:

```bash
cp .env.example .env
```

5. (Optional but recommended) Start Ollama:

```bash
ollama serve
ollama pull qwen3-coder
```

## Run the App

```bash
uvicorn app.main:app --reload
```

Open: [http://127.0.0.1:8000](http://127.0.0.1:8000)

## Usage Flow

1. Go to home page.
2. Upload `data/sample_leads.csv` (or your own CSV).
3. Run starts automatically (or click start if disabled).
4. Open run details to inspect per-lead status and enrichment preview.
5. Open a lead to inspect raw input, crawled pages, extraction, AI output, and warnings/errors.
6. Click export to download enriched CSV.

## CSV Input Columns

Expected columns (missing columns are safely defaulted blank):

- `company_name`
- `website`
- `city`
- `state`
- `phone`
- `email`

## Error Handling Notes

Handled gracefully with row-level status/error fields:

- Missing website / invalid URL
- Request timeout / SSL/network failures
- Empty or non-extractable pages
- Ollama unavailable / timeout / malformed model JSON
- CSV parse errors
- Export failures

When Ollama is unavailable, enrichment still completes with deterministic extraction results and marks the row with `llm_fallback` details.

## Known Limitations

- Address extraction uses heuristics/regex and may miss complex formats.
- Social link extraction is best-effort from HTML and may miss JS-injected links.
- No async queue; processing runs in local FastAPI background tasks.
- Deduping is in-run only by normalized domain + cleaned company name.

## Future Improvements

- Better address parsing and geocoding (local/offline-friendly where possible)
- Smarter page discovery and crawl depth controls
- Add pagination/filtering for large runs
- Add configurable scoring profiles
- Improve JSON repair and model response validation
