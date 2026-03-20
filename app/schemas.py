from __future__ import annotations

from pydantic import BaseModel


class LeadUploadRow(BaseModel):
    first_name: str | None = None
    last_name: str | None = None
    full_name: str | None = None
    company_name: str | None = None
    title: str | None = None
    email: str | None = None
    phone: str | None = None
    website: str | None = None
    company_domain: str | None = None
    linkedin_url: str | None = None
    city: str | None = None
    state: str | None = None
    location: str | None = None
