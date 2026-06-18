"""
Builds a lightweight datasets.json from agent1_results.csv.

Filters to dataset records only and keeps the columns needed by the frontend.
Also resolves country names from keywords/affiliation/abstract/title using the
same logic as Agent 2 main pipeline.
Output: cgiar_mas_agent2/output/datasets.json

Usage:
    python build_datasets_json.py
    python build_datasets_json.py --csv path/to/agent1_results.csv
"""

import argparse
import json
import re
import sys
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

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
    "country",
    "production_system",
]

DEMONYMS: dict = {
    "ethiopian": "Ethiopia", "kenyan": "Kenya", "nigerian": "Nigeria",
    "vietnamese": "Vietnam", "tanzanian": "Tanzania", "ugandan": "Uganda",
    "bangladeshi": "Bangladesh", "nepali": "Nepal", "nepalese": "Nepal",
    "ghanaian": "Ghana", "senegalese": "Senegal", "malian": "Mali",
    "nigerien": "Niger", "rwandan": "Rwanda", "mozambican": "Mozambique",
    "zambian": "Zambia", "zimbabwean": "Zimbabwe", "malawian": "Malawi",
    "colombian": "Colombia", "honduran": "Honduras", "guatemalan": "Guatemala",
    "salvadoran": "El Salvador", "nicaraguan": "Nicaragua", "chilean": "Chile",
    "peruvian": "Peru", "bolivian": "Bolivia", "cambodian": "Cambodia",
    "philippine": "Philippines", "filipino": "Philippines",
    "indian": "India", "indonesian": "Indonesia", "pakistani": "Pakistan",
    "sri lankan": "Sri Lanka", "sudanese": "Sudan",
    "south sudanese": "South Sudan", "congolese": "Congo",
    "ivorian": "Côte D'ivoire", "cameroonian": "Cameroon",
    "burkinabe": "Burkina Faso", "togolese": "Togo", "beninese": "Benin",
    "somali": "Somalia", "namibian": "Namibia",
    "botswanan": "Botswana", "lesotho": "Lesotho", "swazi": "Eswatini",
    "myanmar": "Myanmar", "burmese": "Myanmar", "laotian": "Laos",
    "thai": "Thailand", "chinese": "China", "afghan": "Afghanistan",
}

COUNTRY_ALIASES: dict = {
    "viet nam": "Vietnam", "lao pdr": "Laos",
    "lao people's democratic republic": "Laos",
    "dr congo": "Congo", "drc": "Congo",
    "democratic republic of congo": "Congo",
    "ivory coast": "Côte D'ivoire", "cote d'ivoire": "Côte D'ivoire",
    "burkina": "Burkina Faso", "swaziland": "Eswatini",
    "cabo verde": "Cape Verde", "timor-leste": "Timor-Leste",
    "east timor": "Timor-Leste",
    "tanzania, united republic of": "Tanzania",
    "bolivia, plurinational state of": "Bolivia",
    "iran, islamic republic of": "Iran",
    "korea, republic of": "South Korea",
    "syrian arab republic": "Syria",
    "united republic of tanzania": "Tanzania",
}


def extract_countries_from_text(text, country_set):
    if not isinstance(text, str):
        if isinstance(text, list):
            text = " ".join(text)
        else:
            return set()
    text_lower = text.lower()
    found = set()
    for country in country_set:
        if re.search(r"\b" + re.escape(country.lower()) + r"\b", text_lower):
            found.add(country.title())
    for demonym, country_name in DEMONYMS.items():
        if re.search(r"\b" + re.escape(demonym) + r"\b", text_lower):
            found.add(country_name)
    for alias, country_name in COUNTRY_ALIASES.items():
        if re.search(r"\b" + re.escape(alias) + r"\b", text_lower):
            found.add(country_name)
    return found


def process_row(row, country_names_set):
    current_country = row["country"]
    if pd.notna(current_country) and str(current_country).strip() != "":
        parts = [c.strip().lower() for c in str(current_country).split(",")]
        valid = [c.title() for c in parts if c in country_names_set]
        if valid:
            seen, unique = set(), []
            for c in valid:
                if c not in seen:
                    seen.add(c)
                    unique.append(c)
            return ", ".join(unique)

    for field in ["keywords", "affiliation", "abstract", "title"]:
        found = extract_countries_from_text(row.get(field), country_names_set)
        if found:
            return ", ".join(sorted(found))
    return ""


def build(csv_path: Path, out_path: Path):
    print(f"Reading {csv_path} ...")

    usecols = lambda c: c in COLS + ["is_dataset", "keywords", "affiliation", "abstract"]
    df = pd.read_csv(csv_path, encoding="utf-8", low_memory=False, usecols=usecols)

    # Filter to dataset records
    flag_mask = df["is_dataset"].astype(str).str.lower().isin(["true", "1"]) if "is_dataset" in df.columns else pd.Series(False, index=df.index)
    dv_mask   = df["repository_source"].str.strip().str.lower().str.contains("dataverse", na=False) if "repository_source" in df.columns else pd.Series(False, index=df.index)
    df = df[flag_mask | dv_mask].copy()
    print(f"  {len(df)} dataset records found.")

    # Resolve country names from text fields
    try:
        from cgiar_mas_agent2.config import settings
        country_names_set = set(name.lower() for name in settings.COUNTRIES)
        print(f"  Resolving countries from text ({len(country_names_set)} known countries)...")
        df["country"] = df.apply(lambda row: process_row(row, country_names_set), axis=1)
        resolved = df["country"].ne("").sum()
        print(f"  Country resolved for {resolved} / {len(df)} records.")
    except Exception as e:
        print(f"  Warning: could not resolve countries ({e}). Using raw column.")

    # Normalize production_system to match Agent 2 profile keys
    try:
        from cgiar_mas_agent2.agent2.quantitative.heatmap import normalize_production_system
        def _norm_ps(val):
            if pd.isna(val):
                return "general / cross-cutting"
            parts = str(val).split(",")
            norms = list(dict.fromkeys(normalize_production_system(p.strip()) for p in parts if p.strip()))
            return ",".join(norms) if norms else "general / cross-cutting"
        df["production_system"] = df["production_system"].apply(_norm_ps) if "production_system" in df.columns else "general / cross-cutting"
        print("  Production systems normalized.")
    except Exception as e:
        print(f"  Warning: could not normalize production systems ({e}).")

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
    for col in ["doi_pid", "title", "dataset_type", "repository_source", "country", "production_system"]:
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
