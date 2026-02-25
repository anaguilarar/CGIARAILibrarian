"""
Librarian Layer — schema.py

Pydantic models that define the shape of synthesis_report.json.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class GlobalStats(BaseModel):
    """Top-level aggregate statistics across the entire dataset."""

    total_count: int = Field(..., description="Total number of research records")
    ontology_breakdown: dict[str, int] = Field(
        default_factory=dict,
        description="Record count per ontology category (Water, Adaptation, Mitigation, …)",
    )


class TopPaper(BaseModel):
    doi: str
    title: str
    score: float
    citations: int
    downloads: int
    views: int
    repository: str


class Profile(BaseModel):
    """Reusable profile for a country or production system cluster."""

    count: int = Field(..., description="Number of records in this cluster")
    narrative: str = Field(
        ...,
        description="3-sentence LLM-generated qualitative summary",
    )
    adaptation: str = Field(
        ...,
        description="3-sentence LLM-generated qualitative summary",
    )
    mitigation: str = Field(
        ...,
        description="3-sentence LLM-generated qualitative summary",
    )
    water: str = Field(
        ...,
        description="3-sentence LLM-generated qualitative summary",
    )
    top_dois: list[TopPaper] = Field(
        default_factory=list,
        description="Highest-ranked papers in this cluster with metrics",
    )


class IdentifiedGap(BaseModel):
    """An evidence gap flagged for a priority area."""

    area: str = Field(..., description="Country or production system name")
    type: str = Field(..., description="'country' or 'production_system'")
    count: int = Field(..., description="Number of records currently retrieved")
    note: str = Field(..., description="Human-readable gap description")


class SynthesisReport(BaseModel):
    """
    Root model for the full synthesis report — the 'Digital Brain'.

    This is the JSON contract that downstream consumers (Agent 3,
    Dashboard) depend on.
    """

    generated_at: str = Field(
        ..., description="ISO-8601 timestamp of report generation"
    )
    global_stats: GlobalStats
    country_profiles: dict[str, Profile] = Field(default_factory=dict)
    system_profiles: dict[str, Profile] = Field(default_factory=dict)
    heatmap: dict[str, dict[str, int]] = Field(default_factory=dict)
    identified_gaps: list[IdentifiedGap] = Field(default_factory=list)
