"""
Actuary Layer — global_datasets.py

Produces global top-N dataset rankings (not per-country/system) grouped
by three engagement metrics: downloads, citations, views.
"""

from __future__ import annotations

import pandas as pd


def _parse_tags(val) -> list[str]:
    if isinstance(val, list):
        return [t.strip() for t in val if str(t).strip()]
    if isinstance(val, str):
        return [t.strip() for t in val.replace(";", ",").split(",") if t.strip()]
    return []


def get_global_top_datasets(df: pd.DataFrame, n: int = 100) -> dict[str, list[dict]]:
    """
    Return the top-N datasets globally ranked by each engagement metric.

    Returns::

        {
            "most_downloaded": [...],
            "most_cited":      [...],
            "most_viewed":     [...],
        }

    Each item matches the TopPaper schema plus ``ontology_tags``.
    """
    cols = [
        "title", "doi_pid", "ranking_score",
        "citation_count", "downloads_count", "total_views",
        "repository_source", "dataset_type", "ontology_tags",
    ]
    available = [c for c in cols if c in df.columns]
    working = df[available].copy()

    for col in ["ranking_score", "citation_count", "downloads_count", "total_views"]:
        if col in working.columns:
            working[col] = pd.to_numeric(working[col], errors="coerce").fillna(0)

    def to_list(sorted_df: pd.DataFrame) -> list[dict]:
        result = []
        seen: set[str] = set()
        for _, row in sorted_df.iterrows():
            doi = str(row["doi_pid"])
            if doi in seen:
                continue
            seen.add(doi)
            result.append({
                "doi":          doi,
                "title":        str(row.get("title", "")),
                "score":        round(float(row.get("ranking_score", 0)), 2),
                "citations":    int(row.get("citation_count", 0)),
                "downloads":    int(row.get("downloads_count", 0)),
                "views":        int(row.get("total_views", 0)),
                "repository":   str(row.get("repository_source", "Unknown")),
                "dataset_type": str(row.get("dataset_type", "unknown")),
                "ontology_tags": _parse_tags(row.get("ontology_tags", [])),
            })
        return result

    return {
        "most_downloaded": to_list(working.nlargest(n, "downloads_count")),
        "most_cited":      to_list(working.nlargest(n, "citation_count")),
        "most_viewed":     to_list(working.nlargest(n, "total_views")),
    }
