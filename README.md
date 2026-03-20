# Lead Enrichment Local (Apollo-style row enricher)

Local-first FastAPI + Jinja2 + SQLite lead row enrichment for Apollo-like CSV exports. No paid APIs, no cloud dependencies.

## New local model controls (no day-to-day `.env` edits)

You can now control model behavior from the UI per enrichment run:
- choose any installed local Ollama model on the CSV preview page,
- add run-level custom instructions (operator guidance),
- manage local models from a dedicated `/models` page.

`.env`/settings still provide the **default fallback model**, but daily switching should happen in-app.

## What changed

The product is now **row-first** instead of website-first. For each CSV row, it now:
1. maps flexible Apollo-style headers into a canonical schema,
2. normalizes row values,
3. analyzes missing/suspicious fields,
4. runs a local-first company resolution stage when website/domain is missing,
5. resolves the best anchor (original vs resolved),
6. crawls a company site only when useful,
6. enriches company/site fields,
7. runs person-aware validation checks,
8. exports original + canonical + enriched + score/provenance columns.

The crawler is still used as a reusable company enrichment module.

## Supported input columns (aliases)

Common aliases are mapped to canonical fields:
- first_name: `first_name`, `firstname`, `first`
- last_name: `last_name`, `lastname`, `last`
- full_name: `name`, `full_name`, `full name`
- company_name: `company`, `company_name`, `organization`, `account_name`
- title: `title`, `job_title`, `position`
- email: `email`, `work_email`
- phone: `phone`, `mobile`, `work_phone`
- website: `website`, `company_website`, `url`
- company_domain: `domain`, `company_domain`
- linkedin_url: `linkedin`, `linkedin_url`, `linkedin profile`
- city/state/location
- address/street/street_address

## Canonical lead-row schema

Core canonical fields:
- first_name, last_name, full_name, normalized_full_name
- title, normalized_title
- company_name, normalized_company_name
- email, normalized_email, email_domain
- phone, normalized_phone
- company_domain, website, linkedin_url
- city, state, location_text

Row analysis + result fields:
- anchor_type, anchor_value, anchor_reason
- anchor_source
- resolved_website, resolved_domain
- resolution_method, resolution_confidence, resolution_status, resolution_notes
- fields_present_json, fields_missing_json, fields_suspicious_json
- enrichment_confidence, person_match_confidence, company_match_confidence
- lead_quality_score, validation_notes, outreach_angle
- enrichment_status, enrichment_error

Company/site enrichment fields:
- public_company_email, public_company_phone, company_address
- business_type, services_json, short_summary
- contact/about/team URLs + social URLs
- has_contact_form, has_online_booking, has_chat_widget, mentions_financing

## Company resolution stage (local-first)

Before crawl/enrichment, rows missing website/domain go through deterministic resolution:
1. Short-circuit: skip if strong existing anchor already exists.
2. Email derivation: use non-generic email domain when available.
3. Public web search (DuckDuckGo HTML): query company + city/state (+address if present).
4. Candidate validation: fetch candidate pages with `requests` and Playwright fallback.
5. Deterministic scoring and conservative selection.

Evidence used during scoring:
- company name similarity (domain/title/H1/body)
- city/state/address mentions
- phone match on page (strong signal)
- local-business hints (`tattoo`, `studio`, `ink`, etc.)
- reject social/listing domains as final canonical websites when possible

Outcomes are explicit:
- `skipped_existing_anchor`
- `resolved`
- `ambiguous`
- `unresolved`
- `failed`

If confidence is weak, rows stay unresolved with candidate evidence and notes.

## Anchor resolution order

Priority order per row:
1. linkedin_url
2. email domain
3. website
4. company_domain
5. company_name + city/state
6. company_name

Rows with no usable anchor are marked `unresolved` (not crashed).

## Person-level behavior (honest constraints)

This app does **person-aware validation**, not full people enrichment:
- Splits full name where possible.
- Normalizes person/company/title/contact fields.
- Checks team/about text for person name matches.
- Adds notes like “person name found on team/about page” or “company site found but person not found”.

It does **not** invent personal emails, direct dials, or job titles not present in input/crawled content.

## Field provenance

`provenance_json` stores per-field source labels, e.g.:
- `original_csv`
- `normalized_from_original`
- `derived_from_website_or_email`
- `derived_from_email_domain`
- `website_extraction`
- `llm_classification`

## Scoring semantics

Deterministic first:
- `company_match_confidence`: domain/website/company consistency + crawl success
- `company_match_confidence` also incorporates resolution quality signals when resolution ran
- `person_match_confidence`: person fields + linkedin + team/about name evidence
- `enrichment_confidence`: weighted rollup
- `lead_quality_score`: 0–100 scaled with penalties for missing critical fields

## Debug workflow

- `/runs/{id}/preview`: header mapping + canonical parse preview + mapping warnings
- `/runs/{id}`: per-row anchors, analysis, enrichment outputs, scores
- `/runs/{id}` auto-refreshes during processing for live progress + per-row updates
- `/api/runs/{id}/progress`: JSON feed used by live run view polling
- `/leads/{id}`: original row, canonical row, analysis, provenance, debug trace
- `/debug/health`: DB + Ollama + directory checks
- `/debug/llm`: local manual Ollama test UI
- `/models`: list/pull/create local Ollama models and presets

## Per-run model selection and custom instructions

On `/runs/{id}/preview` (and also from a pending run page `/runs/{id}`), before starting enrichment:
1. Pick **Model for this run** from currently installed Ollama models.
2. Optionally add **Run-level custom instructions**.
3. Start the run.

Behavior:
- If a model is selected, that model is used for the run’s LLM classification calls.
- If no model is selected, the app falls back to `OLLAMA_MODEL` from settings/.env.
- Run instructions are appended as run context/hints (not full prompt replacement).
- Core safety rules stay intact: conservative outputs, no hallucinated fields, uncertainty is explicit.

If a selected model is missing at run start, the run fails gracefully with a clear error on the run detail page.

## Models page (`/models`)

The local models page includes:
- live installed model list from Ollama (`name`, `size`, `modified`),
- Ollama connectivity status,
- pull form for new models,
- local preset model creation form.

### Pulling models

- Use the **Pull Model** form with a model name (e.g. `qwen3:14b`, `qwen3:8b`, `mistral-small3.2`).
- Pulling an already-installed model is safe; Ollama handles this idempotently.
- After pull/create actions, the page can be refreshed to see the latest installed list.

### Preset model creation

Use **Create Local Preset Model** to derive a local model from a base model with custom system instructions.
- Example: base `qwen3:14b` + preset name `qwen3:14b-tattoo`.
- Presets are local-only via Ollama’s create API.
- If create fails (invalid name/instructions/Ollama error), the UI shows a clear error message.

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
cp .env.example .env
ollama serve
ollama pull qwen3-coder:30b
uvicorn app.main:app --reload
```

> `.env` model config is fallback default only. Use `/models` + run preview selection for day-to-day model switching.

## End-to-end run

```bash
# start app
uvicorn app.main:app --reload

# upload CSV at /
# review parse at /runs/{id}/preview
# start enrichment
# inspect resolution details at /runs/{id} and /leads/{lead_id}
# export from /runs/{id}/export
```

## Practical local-business test flow

1. Upload `data/sample_leads.csv` (includes website-present rows, name+location rows, ambiguous rows).
2. Confirm preview mapping includes `company_name`, `city`, `state`, and `address` where present.
3. Start run and inspect run detail for:
   - `resolution_status`
   - `resolution_confidence`
   - whether anchor source is `original_or_derived` or `resolution`.
4. Open a lead detail page to review candidate list and debug events:
   - `resolution.search`
   - `resolution.candidate_found`
   - `resolution.candidate_scored`
   - `resolution.selected` / `resolution.unresolved`.
5. Export CSV and verify resolution columns are included.

## Limitations

- Public search HTML parsing can break if search engines change markup.
- Some local businesses only have social/listing pages; these are treated cautiously and may remain unresolved.
- This tool does not claim resolution success unless evidence passes threshold.

Backward compatibility is preserved: CSVs with only company/site/location still run.


## Dynamic schema-aware enrichment flow (new)

Each run now uses three local model roles:
- **Schema inference model (strong model, once per run):** infers semantic roles from arbitrary CSV headers + sample rows and emits a strict JSON parsing/transformation/search plan.
- **Query generation model (light model):** used for unresolved rows to propose conservative query candidates.
- **Main enrichment model:** used for enrichment/classification after resolution.

The resolver now rejects location-only queries (e.g. `Edgewood MD`) and requires entity/business identity in company-resolution searches.

### New run/lead observability
- Run-level: schema inference JSON + search strategy JSON.
- Lead-level: semantic row JSON, generated queries JSON, query-generation notes, candidate website scoring traces.

### Model defaults in `.env`
- `DEFAULT_ENRICHMENT_MODEL`
- `DEFAULT_SCHEMA_INFERENCE_MODEL`
- `DEFAULT_QUERY_GENERATION_MODEL`

If your machine cannot handle 30B, choose smaller defaults (for example 8B/14B).

## macOS full terminal setup
See `INSTALL.md` for a beginner-friendly, from-scratch setup guide.
