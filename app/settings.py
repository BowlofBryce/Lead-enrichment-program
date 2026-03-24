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
    # Lead discovery provider (canonical): DuckDuckGo HTML search
    duckduckgo_enabled: bool = True
    max_queries_per_location: int = 2
    discovery_enable_explicit_medspa_synonyms: bool = False
    discovery_enable_duckduckgo_html: bool = True
    discovery_duckduckgo_html_url: str = "https://html.duckduckgo.com/html/"
    discovery_duckduckgo_user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    duckduckgo_min_delay_seconds: float = 5.0
    duckduckgo_max_delay_seconds: float = 12.0
    duckduckgo_consecutive_403_threshold: int = 2
    discovery_duckduckgo_min_interval_seconds: float = 0.35
    discovery_duckduckgo_max_results_per_query: int = 12
    discovery_http_max_retries: int = 3
    discovery_retry_backoff_seconds: float = 1.5
    discovery_parallel_workers: int = 3
    discovery_batch_commit_size: int = 150

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
