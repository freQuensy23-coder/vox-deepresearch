"""Configuration for the deep stage trigger of Telegram research."""

from pydantic import BaseModel, Field


class DeepStageConfig(BaseModel):
    """Sends the deep stage request to the thread once the first research report is ready."""

    enabled: bool = Field(default=False, description="Whether a finished Telegram research report starts the deep stage")
