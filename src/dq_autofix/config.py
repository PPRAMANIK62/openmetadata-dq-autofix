"""Configuration management using Pydantic Settings."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # OpenMetadata connection
    openmetadata_host: str = Field(
        default="http://localhost:8585",
        description="OpenMetadata server URL",
    )
    openmetadata_token: str | None = Field(
        default=None,
        description="JWT token for OpenMetadata authentication",
    )

    # Application settings
    log_level: str = Field(
        default="INFO",
        description="Logging level",
    )

    # Optional LLM configuration (for future phases)
    llm_provider: str | None = Field(
        default=None,
        description="LLM provider: ollama, groq, or openai",
    )
    llm_model: str | None = Field(
        default=None,
        description="LLM model name",
    )
    ollama_host: str = Field(
        default="http://localhost:11434",
        description="Ollama server URL",
    )
    groq_api_key: str | None = Field(
        default=None,
        description="Groq API key",
    )


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
