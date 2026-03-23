from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Lead Enrichment Local"
    debug_mode: bool = True
    database_url: str = "sqlite:///./lead_enrichment.db"
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "qwen3-coder:30b"
    default_enrichment_model: str = "qwen3:14b"
    default_schema_inference_model: str = "qwen3-coder:30b"
    default_query_generation_model: str = "qwen3:8b"
    ollama_timeout_seconds: int = 45
    request_timeout_seconds: int = 20
    google_places_api_key: str = ""
    yelp_api_key: str = ""
    # Lead discovery provider (canonical): DuckDuckGo HTML search
    discovery_enable_duckduckgo_html: bool = True
    discovery_duckduckgo_html_url: str = "https://html.duckduckgo.com/html/"
    discovery_duckduckgo_user_agent: str = "lead-enrichment-local/1.0 (+duckduckgo-discovery)"
    discovery_duckduckgo_min_interval_seconds: float = 0.35
    discovery_duckduckgo_max_results_per_query: int = 12
    discovery_http_max_retries: int = 3
    discovery_retry_backoff_seconds: float = 1.5
    discovery_parallel_workers: int = 3
    discovery_batch_commit_size: int = 150

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
