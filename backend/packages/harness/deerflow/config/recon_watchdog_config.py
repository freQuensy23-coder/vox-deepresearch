"""Configuration for the Vox recon watchdog middleware."""

from pydantic import BaseModel, Field


class ReconWatchdogConfig(BaseModel):
    """Nudges the lead agent when it stops sending Vox requests during recon."""

    enabled: bool = Field(default=True, description="Whether the lead agent is reminded to keep sending Vox requests")
    idle_seconds: int = Field(default=300, ge=1, description="Seconds without a new Vox request before the reminder is injected")
