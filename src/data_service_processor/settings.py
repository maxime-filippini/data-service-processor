from pydantic import AnyHttpUrl, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class EnvironmentSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
    )


class ProcessorSettings(EnvironmentSettings):
    """Required for both CLI and HTTP job execution."""

    processing_api_url: AnyHttpUrl
    processing_api_token: SecretStr = Field(min_length=1)
    r2_endpoint_url: AnyHttpUrl
    r2_access_key_id: SecretStr = Field(min_length=1)
    r2_secret_access_key: SecretStr = Field(min_length=1)


class HttpSettings(EnvironmentSettings):
    """Required only when using the container's authenticated HTTP endpoint."""

    processor_api_token: SecretStr = Field(min_length=1)
