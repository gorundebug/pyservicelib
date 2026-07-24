"""User-owned service configuration. The generator never overwrites this file."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .config_generated import GeneratedConfig


class CustomConfig(BaseModel):
    """Add service-owned root YAML fields here."""

    model_config = ConfigDict(extra="ignore")


class Config(GeneratedConfig):
    custom: CustomConfig = Field(default_factory=CustomConfig)

    @classmethod
    def load_config(cls, obj: dict[str, Any]) -> dict[str, Any]:
        return {"custom": CustomConfig.model_validate(obj)}
