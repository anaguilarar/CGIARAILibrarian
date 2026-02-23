"""
Curator Layer — sampler.py

Select the top-N highest-ranked abstracts per cluster for LLM synthesis.
"""

from __future__ import annotations

import pandas as pd


def sample_top_abstracts(
    df: pd.DataFrame,
    group_col: str,
    n: int = 10,
) -> dict[str, list[dict]]:
    """
    For each unique value in *group_col*, return the top *n* records
    (by ranking_score) together with their abstract and doi_pid.

    Only records that have a non-empty abstract are considered.

    Returns::

        {
            "Vietnam": [
                {"doi": "...", "title": "...", "abstract": "...", "score": 71.76},
                ...
            ],
            ...
        }
    """
    cols = ["title", "abstract", "doi_pid", "ranking_score", "ontology_tags", "classification_explanation", group_col]
    working = df[cols].copy()
    working["ranking_score"] = pd.to_numeric(
        working["ranking_score"], errors="coerce"
    ).fillna(0)

    # Drop rows without an abstract (nothing to synthesise)
    working = working[working["abstract"].notna() & (working["abstract"].str.strip() != "")]

    # Explode multi-valued group column
    sep = ";" if group_col == "ontology_tags" else ","
    working[group_col] = working[group_col].fillna("Unknown").astype(str).str.split(sep)
    working = working.explode(group_col)
    working[group_col] = working[group_col].str.strip()
    working[group_col] = working[group_col].replace("", "Unknown")

    if group_col == "production_system":
        working[group_col] = working[group_col].str.lower()

    result: dict[str, list[dict]] = {}
    for group_val, grp in working.groupby(group_col):
        top = grp.nlargest(n, "ranking_score")
        result[str(group_val)] = [
            {
                "doi": str(row["doi_pid"]),
                "title": str(row["title"]),
                "abstract": str(row["abstract"]),
                "ontolgy_tags": str(row["ontology_tags"]),
                "classification_explanation": str(row["classification_explanation"]),
                "score": round(float(row["ranking_score"]), 2),
            }
            for _, row in top.iterrows()
        ]
    return result
