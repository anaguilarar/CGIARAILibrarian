"""
Actuary Layer — heatmap.py

Cross-tabulation of country × production_system.
"""

from __future__ import annotations
from ...config import settings
import pandas as pd


def normalize_production_system(raw_term: str) -> str:
    """
    Maps a raw, messy production system string to a standardized CGIAR category.
    """
    if not raw_term or not isinstance(raw_term, str):
        return "General"

    # 1. Clean the input (lowercase, remove newlines, extra spaces)
    clean_term = raw_term.lower().replace("\n", " ").strip()
    
    # 2. Iterate through groups to find a match
    for category, keywords in settings.PRODUCTION_SYSTEM_GROUPS.items():
        # Check against exact list first
        # We lowercase the list items for comparison
        clean_keywords = [k.lower().replace("\n", " ") for k in keywords]
        
        if clean_term in clean_keywords:
            return category
            
    # 3. Fuzzy Fallback (if no direct match found)
    # If the raw term contains key words, map it anyway.
    if "rice" in clean_term: return "rice"
    if "maize" in clean_term: return "maize"
    if "wheat" in clean_term: return "wheat"
    if "fish" in clean_term or "aquaculture" in clean_term: return "fisheries and aquaculture"
    if "cattle" in clean_term or "livestock" in clean_term: return "livestock (general/small ruminants)"
    if "coffee" in clean_term or "cacao" in clean_term: return "coffee and cocoa"
    if "bean" in clean_term or "legume" in clean_term: return "legumes"
    
    # 4. Default return if nothing matches
    return "general / cross-cutting"

def build_heatmap(df: pd.DataFrame) -> dict[str, dict[str, int]]:
    """
    Return a nested dict: {country: {production_system: count, ...}, ...}.

    Multi-valued country fields (comma-separated) are exploded so a single
    record can appear under several countries.
    """
    working = df[["country", "production_system"]].copy()
    working["country"] = working["country"].fillna("unknown").astype(str).str.strip().str.lower()
    working["country"] = working["country"].str.split(",")
    working = working.explode("country")
    working["country"] = working["country"].str.strip()
    working["country"] = working["country"].replace("", "unknown")

    working["country"] = working["country"].replace('the democratic republic of the', 'the democratic republic of the congo')
    working["country"] = working["country"].apply(lambda x: 'unknown' if x == 'republic of' else x)

    working["production_system"] = (
        working["production_system"].fillna("unknown").astype(str).str.strip().str.lower()
    )

    working["production_system"] = working["production_system"].str.split(",")
    working = working.explode("production_system")
    working["production_system"] = working["production_system"].str.strip()

    # Reset index to avoid ValueError with duplicate indices from explode
    working = working.reset_index(drop=True)

    ct = pd.crosstab(working["country"], working["production_system"])

    heatmap: dict[str, dict[str, int]] = {}
    for country in ct.index:
        row = ct.loc[country]
        heatmap[str(country)] = {
            str(sys): int(cnt) for sys, cnt in row.items() if cnt > 0
        }
    return heatmap

