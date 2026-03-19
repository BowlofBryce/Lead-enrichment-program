from __future__ import annotations

from pydantic import BaseModel


class LeadUploadRow(BaseModel):
    company_name: str | None = None
    website: str | None = None
    city: str | None = None
    state: str | None = None
    phone: str | None = None
    email: str | None = None
