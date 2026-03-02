"""
Librarian Layer — report_builder.py

Assembles outputs from the Actuary and Curator layers into a
SynthesisReport and writes it to disk as JSON.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from cgiar_mas_agent2.agent2.output.schema import (
    GlobalStats,
    IdentifiedGap,
    Profile,
    SynthesisReport,
    TopPaper,
)

logger = logging.getLogger(__name__)


def replace_doi(text):
    def _replace_doi(value: str) -> str:
        if value is None:
            return ""
        value = value.replace("doi:", "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:")
        value = value.replace("10568/", "https://hdl.handle.net/10568/")
        value = value.replace("10947/", "https://hdl.handle.net/10947/")
        return value

    if isinstance(text, dict):
        for key, value in text.items():
            text[key] = _replace_doi(value)
        return text

    return _replace_doi(text)
    

def build_report(
    *,
    total_count: int,
    ontology_counts: dict[str, int],
    country_counts: dict[str, int],
    system_counts: dict[str, int],
    heatmap: dict[str, dict[str, int]],
    country_narratives: dict[str, str],
    system_narratives: dict[str, str],
    country_top_papers: dict[str, list[dict]],
    system_top_papers: dict[str, list[dict]],
    gaps: list[dict],
) -> SynthesisReport:
    """Assemble all layer outputs into a single ``SynthesisReport``."""

    global_stats = GlobalStats(
        total_count=total_count,
        ontology_breakdown=ontology_counts,
    )

    # --- Country profiles ---------------------------------------------------
    country_profiles: dict[str, Profile] = {}
    for country, count in country_counts.items():
        narrative = country_narratives.get(country, "")
        top_dois = country_top_papers.get(country, [])
        try:
            #"https://dataverse.harvard.edu/dataset.xhtml?persistentId="
            narrative = replace_doi(narrative)

            country_profiles[country] = Profile(
                count=count,
                narrative=narrative["narrative"],
                adaptation=narrative["adaptation"],
                mitigation=narrative["mitigation"],
                water=narrative["water"],
            top_dois=top_dois,
        )
        except:
            country_profiles[country] = Profile(
                count=count,
                narrative=narrative,
                adaptation="",
                mitigation="",
                water="",
            top_dois=top_dois,
        )

    # --- System profiles ----------------------------------------------------
    system_profiles: dict[str, Profile] = {}
    for system, count in system_counts.items():
        narrative = system_narratives.get(system, "")
        narrative = replace_doi(narrative)
        top_dois = system_top_papers.get(system, [])
        system_profiles[system] = Profile(
            count=count,
            narrative=narrative["narrative"],
            adaptation=narrative["adaptation"],
            mitigation=narrative["mitigation"],
            water=narrative["water"],
            top_dois=top_dois,
        )

    # --- Gaps ---------------------------------------------------------------
    identified_gaps = [IdentifiedGap(**g) for g in gaps]

    return SynthesisReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        global_stats=global_stats,
        country_profiles=country_profiles,
        system_profiles=system_profiles,
        heatmap=heatmap,
        identified_gaps=identified_gaps,
    )


def save_report(report: SynthesisReport, path: str | Path) -> None:
    """Serialize *report* to a pretty-printed JSON file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(report.model_dump_json(indent=2))
    logger.info("Synthesis report saved → %s", path)
