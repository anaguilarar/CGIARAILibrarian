"""
Curator Layer — gaps.py

Detect evidence gaps: clusters where the paper count falls below
the configured threshold AND the cluster is in the PRIORITY_TARGETS list.
"""

from __future__ import annotations

from cgiar_mas_agent2.config import settings


def detect_gaps(
    counts: dict[str, int],
    area_type: str,
    threshold: int | None = None,
) -> list[dict]:
    """
    Scan *counts* and flag entries below *threshold* that are in the
    priority targets list.

    Parameters
    ----------
    counts : dict[str, int]
        e.g. {"Vietnam": 12, "Myanmar": 2, ...}
    area_type : str
        "country" or "production_system" — included in each gap record.
    threshold : int, optional
        Override ``settings.GAP_THRESHOLD`` if needed.

    Returns
    -------
    list[dict]
        Each entry: {area, type, count, note}.
    """
    if threshold is None:
        threshold = settings.GAP_THRESHOLD

    gaps: list[dict] = []

    # Check priority targets that appear in the counts with low volume
    for area, count in counts.items():
        if count < threshold and area.lower() in settings.PRIORITY_TARGETS:
            gaps.append(
                {
                    "area": area,
                    "type": area_type,
                    "count": count,
                    "note": (
                        f"Current retrieval indicates limited digital records "
                        f"for {area} ({count} record{'s' if count != 1 else ''}). "
                        f"This may represent an evidence gap or a retrieval limitation."
                    ),
                }
            )

    # Also check priority targets that are *completely absent*
    if area_type == "country":
        priority_list = settings.PRIORITY_COUNTRIES
    else:
        priority_list = settings.PRIORITY_SYSTEMS

    present = {k.lower() for k in counts}
    for target in priority_list:
        if target.lower() not in present:
            gaps.append(
                {
                    "area": target,
                    "type": area_type,
                    "count": 0,
                    "note": (
                        f"Current retrieval indicates limited digital records "
                        f"for {target} (0 records). "
                        f"This may represent an evidence gap or a retrieval limitation."
                    ),
                }
            )

    # Sort by count ascending (most severe gaps first)
    gaps.sort(key=lambda g: g["count"])
    return gaps
