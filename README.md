# Lead Enrichment Local (Apollo-style row enricher)

Local-first FastAPI + Jinja2 + SQLite lead row enrichment for Apollo-like CSV exports. No paid APIs, no cloud dependencies.

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
