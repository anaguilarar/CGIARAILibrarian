"""
Actuary Layer — counter.py

Deterministic volume counts by country, production_system, or ontology_tags.
Handles multi-valued columns (semicolon- or comma-delimited).
"""

from __future__ import annotations

import pandas as pd


def _explode_column(df: pd.DataFrame, column: str) -> pd.Series:
    """Split multi-valued cells and explode into individual rows."""
    sep = ";" if column == "ontology_tags" else ","
    series = (
        df[column]
        .fillna("Unknown")
        .astype(str)
        .str.split(sep)
        .explode()
        .str.strip()
    )
    # Normalise empty strings that might result from splitting
    series = series.replace("", "Unknown")
    return series


def count_by_column(df: pd.DataFrame, column: str) -> dict[str, int]:
    """
    Return value-counts for *column* as a plain dict.

    Multi-valued cells are exploded so each value is counted independently.
    """
    exploded = _explode_column(df, column)
    counts = exploded.value_counts().to_dict()
    # Ensure keys are native Python types for JSON serialisation
    return {str(k): int(v) for k, v in counts.items()}


def get_ontology_breakdown_by_group(
    df: pd.DataFrame,
    group_col: str,
    ontology_tags: list[str] | None = None,
) -> dict[str, dict[str, int]]:
    """
    For each unique value in *group_col*, count how many records carry each
    ontology tag.

    Returns::

        {
            "Kenya": {"Adaptation": 12, "Mitigation": 4, "Water": 7},
            ...
        }
    """
    if ontology_tags is None:
        ontology_tags = ["Adaptation", "Mitigation", "Water"]

    if group_col not in df.columns or "ontology_tags" not in df.columns:
        return {}

    working = df[[group_col, "ontology_tags"]].copy()

    # Explode the group column (countries = comma-sep, ontology = semicolon-sep)
    group_sep = ";" if group_col == "ontology_tags" else ","
    working[group_col] = (
        working[group_col].fillna("Unknown").astype(str).str.split(group_sep)
    )
    working = working.explode(group_col)
    working[group_col] = working[group_col].str.strip().replace("", "Unknown")

    if group_col == "production_system":
        working[group_col] = working[group_col].str.lower()

    # Explode ontology_tags
    working["ontology_tags"] = (
        working["ontology_tags"].fillna("Unknown").astype(str).str.split(";")
    )
    working = working.explode("ontology_tags")
    working["ontology_tags"] = working["ontology_tags"].str.strip().replace("", "Unknown")

    result: dict[str, dict[str, int]] = {}
    for group_val, grp in working.groupby(group_col):
        tag_counts = grp["ontology_tags"].value_counts().to_dict()
        result[str(group_val)] = {
            tag: int(tag_counts.get(tag, 0)) for tag in ontology_tags
        }
    return result
