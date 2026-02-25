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
    # Ensure columns exist to avoid KeyError if the CSV is missing them
    cols_to_extract = ["title", "doi_pid", "ranking_score", group_col, "citation_count", "downloads_count", "total_views", "repository_source"]
    available_cols = [c for c in cols_to_extract if c in df.columns]
    working = df[available_cols].copy()
    if "ranking_score" in working.columns:
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
                "title": str(row.get("title", "")),
                "score": round(float(row.get("ranking_score", 0)), 2),
                "citations": int(row.get("citation_count", 0)) if pd.notna(row.get("citation_count", 0)) else 0,
                "downloads": int(row.get("downloads_count", 0)) if pd.notna(row.get("downloads_count", 0)) else 0,
                "views": int(row.get("total_views", 0)) if pd.notna(row.get("total_views", 0)) else 0,
                "repository": str(row.get("repository_source", "Unknown")),
            }
            for _, row in top.iterrows()
        ]
    return result
