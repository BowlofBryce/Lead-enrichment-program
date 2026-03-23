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
    # Directory scrapers (primary) — no search-engine scraping
    discovery_enable_yelp_directory: bool = True
    discovery_enable_yellowpages_directory: bool = True
    discovery_enable_osm_fallback: bool = True
    discovery_enable_google_places: bool = False
    discovery_enable_yelp_fusion_api: bool = False
    discovery_osm_user_agent: str = "lead-enrichment-local/1.0"
    discovery_google_min_interval_seconds: float = 0.25
    discovery_yelp_min_interval_seconds: float = 0.3
    discovery_osm_min_interval_seconds: float = 1.1
    discovery_yellowpages_min_interval_seconds: float = 1.0
    discovery_yelp_max_pages: int = 5
    discovery_yellowpages_max_pages: int = 5
    discovery_yelp_fetch_detail: bool = True
    discovery_yellowpages_fetch_detail: bool = True
    discovery_yelp_detail_max_per_query: int = 25
    discovery_yellowpages_detail_max_per_query: int = 25
    discovery_yelp_use_playwright_fallback: bool = True
    discovery_yellowpages_use_playwright_fallback: bool = True
    discovery_http_max_retries: int = 3
    discovery_retry_backoff_seconds: float = 1.5
    discovery_parallel_workers: int = 3
    discovery_batch_commit_size: int = 150

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
