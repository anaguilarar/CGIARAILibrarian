"""
Builds a lightweight datasets.json from agent1_results.csv.

Filters to dataset records only and keeps the columns needed by the frontend.
Output: cgiar_mas_agent2/output/datasets.json

Usage:
    python build_datasets_json.py
    python build_datasets_json.py --csv path/to/agent1_results.csv
"""

import argparse
import json
import pandas as pd
from pathlib import Path


COLS = [
    "doi_pid",
    "title",
    "citation_count",
    "downloads_count",
    "total_views",
    "ontology_tags",
    "dataset_type",
    "repository_source",
    "ranking_score",
]


def build(csv_path: Path, out_path: Path):
    print(f"Reading {csv_path} ...")
    df = pd.read_csv(csv_path, encoding="utf-8", low_memory=False, usecols=lambda c: c in COLS + ["is_dataset"])

    # Filter to dataset records
    flag_mask = df["is_dataset"].astype(str).str.lower().isin(["true", "1"]) if "is_dataset" in df.columns else pd.Series(False, index=df.index)
    dv_mask   = df["repository_source"].str.strip().str.lower().str.contains("dataverse", na=False) if "repository_source" in df.columns else pd.Series(False, index=df.index)
    df = df[flag_mask | dv_mask].copy()
    print(f"  {len(df)} dataset records found.")

    # Keep only frontend columns
    for col in COLS:
        if col not in df.columns:
            df[col] = ""

    df = df[COLS]

    # Normalise numeric columns
    for col in ["citation_count", "downloads_count", "total_views", "ranking_score"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Parse ontology_tags into a list
    def parse_tags(val):
        if not val or str(val).strip() in ("", "nan"):
            return []
        return [t.strip() for t in str(val).replace(";", ",").split(",") if t.strip()]

    df["ontology_tags"] = df["ontology_tags"].apply(parse_tags)

    # Clean up string fields
    for col in ["doi_pid", "title", "dataset_type", "repository_source"]:
        df[col] = df[col].astype(str).str.strip().replace("nan", "")

    records = df.to_dict(orient="records")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = out_path.stat().st_size / 1024
    print(f"  Saved {len(records)} records -> {out_path}  ({size_kb:.0f} KB)")


if __name__ == "__main__":
    root = Path(__file__).parent
    default_csv = root / "cgiar_mas_agent1" / "output" / "agent1_results.csv"
    default_out = root / "cgiar_mas_agent2" / "output" / "datasets.json"

    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path, default=default_csv)
    parser.add_argument("--out", type=Path, default=default_out)
    args = parser.parse_args()

    build(args.csv, args.out)
