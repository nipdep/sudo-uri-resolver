from __future__ import annotations

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Environment-backed settings for the Fuseki URI resolver."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    fuseki_server_url: str
    fuseki_dataset: str = "idea_kg"
    persistent_uri_base: str = "https://purl.org/twc/sudo/kg/"
    public_base_path: str = Field(
        default="",
        validation_alias=AliasChoices("PUBLIC_BASE_PATH", "RESOLVER_ROOT_PATH", "ROOT_PATH"),
    )

    @field_validator("public_base_path", mode="before")
    @classmethod
    def _normalize_public_base_path(cls, value: str | None) -> str:
        if value is None:
            return ""

        normalized = str(value).strip()
        if normalized in {"", "/"}:
            return ""
        if not normalized.startswith("/"):
            normalized = f"/{normalized}"
        return normalized.rstrip("/")

    @model_validator(mode="after")
    def _validate(self) -> "AppSettings":
        if not self.fuseki_server_url.strip():
            raise ValueError("FUSEKI_SERVER_URL must be a non-empty URL")
        if not self.fuseki_dataset.strip().strip("/"):
            raise ValueError("FUSEKI_DATASET must be a non-empty dataset path or name")
        if not self.persistent_uri_base.strip():
            raise ValueError("PERSISTENT_URI_BASE must be a non-empty URI base")
        return self
