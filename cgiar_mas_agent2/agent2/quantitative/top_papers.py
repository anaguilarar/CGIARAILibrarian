"""
Actuary Layer — top_papers.py

Top-N papers by ranking_score for each unique group value.
"""

from __future__ import annotations

import pandas as pd


def get_top_papers(
    df: pd.DataFrame,
    group_col: str,
    n: int = 5,
) -> dict[str, list[dict]]:
    """
    For each unique value in *group_col*, return the top *n* papers
    sorted by ``ranking_score`` descending.

    Multi-valued cells (comma-separated for country, semicolon for
    ontology_tags) are exploded before grouping.

    Returns::

        {
            "Vietnam": [
                {"doi": "10568/111168", "title": "...", "score": 71.76},
                ...
            ],
            ...
        }
    """
    working = df[["title", "doi_pid", "ranking_score", group_col]].copy()
    working["ranking_score"] = pd.to_numeric(working["ranking_score"], errors="coerce").fillna(0)

    # Explode multi-valued group column
    sep = ";" if group_col == "ontology_tags" else ","
    working[group_col] = working[group_col].fillna("Unknown").astype(str).str.split(sep)
    working = working.explode(group_col)
    working[group_col] = working[group_col].str.strip()
    working[group_col] = working[group_col].replace("", "Unknown")

    # Normalise production_system to lowercase
    if group_col == "production_system":
        working[group_col] = working[group_col].str.lower()

    result: dict[str, list[dict]] = {}
    for group_val, grp in working.groupby(group_col):
        top = grp.nlargest(n, "ranking_score")
        result[str(group_val)] = [
            {
                "doi": str(row["doi_pid"]),
                "title": str(row["title"]),
                "score": round(float(row["ranking_score"]), 2),
            }
            for _, row in top.iterrows()
        ]
    return result
