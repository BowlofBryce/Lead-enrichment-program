from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Lead Enrichment Local"
    debug_mode: bool = True
    database_url: str = "sqlite:///./lead_enrichment.db"
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "qwen3:14b"
    default_enrichment_model: str = "qwen3:14b"
    default_schema_inference_model: str = "qwen3:14b"
    default_query_generation_model: str = "qwen3:8b"
    ollama_timeout_seconds: int = Field(default=120, gt=0)
    request_timeout_seconds: int = 20
    google_places_api_key: str = ""
    yelp_api_key: str = ""

    # Lead discovery provider configuration
    discovery_provider: str = "brave"
    brave_search_api_key: str = ""
    brave_search_base_url: str = "https://api.search.brave.com/res/v1"
    brave_search_timeout_seconds: int = Field(default=15, gt=0)
    brave_search_max_results_per_query: int = Field(default=12, ge=1, le=20)
    brave_search_country: str = "us"
    brave_search_search_lang: str = "en"
    brave_search_freshness: str = ""
    brave_search_max_retries: int = Field(default=2, ge=0, le=5)

    max_queries_per_location: int = 2
    discovery_enable_explicit_medspa_synonyms: bool = False
    discovery_http_max_retries: int = 3
    discovery_retry_backoff_seconds: float = 1.5
    discovery_parallel_workers: int = 3
    discovery_batch_commit_size: int = 150

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
