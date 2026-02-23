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
