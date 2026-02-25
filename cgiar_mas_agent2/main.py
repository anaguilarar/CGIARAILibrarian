"""
Agent 2 — Pipeline Orchestrator (main.py)

Reads Agent 1's CSV, runs the three synthesis layers, and writes
synthesis_report.json.

Usage::

    cd d:\\OneDrive - CGIAR\\scripts\\CGIARLibrarian
    python -m cgiar_mas_agent2.main
"""

from __future__ import annotations

import logging
import sys
import time
import json
import os
import re
from typing import List

# Ensure we can import modules from strict structure
sys.path.append(".")


import pandas as pd
import pycountry
import numpy as np

from cgiar_mas_agent2.config import settings

# Layer 1 — Actuary (Quantitative)
from cgiar_mas_agent2.agent2.quantitative.counter import count_by_column
from cgiar_mas_agent2.agent2.quantitative.heatmap import build_heatmap, normalize_production_system
from cgiar_mas_agent2.agent2.quantitative.top_papers import get_top_papers


from cgiar_mas_agent2.agent2.qualitative.sampler import sample_top_abstracts
from cgiar_mas_agent2.agent2.qualitative.synthesizer import LLMSynthesizer
from cgiar_mas_agent2.agent2.qualitative.gaps import detect_gaps

# Layer 3 — Librarian (Output)
from cgiar_mas_agent2.agent2.output.report_builder import build_report, save_report

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("agent2")

def extract_countries_from_text(text, country_set):
    """
    Finds countries in text using word boundaries to ensure accuracy.
    """
    if not isinstance(text, str):
        if isinstance(text, list):
            text = ' '.join(text) # Join lists (like keywords)
        else:
            return set()
    
    found = set()
    for country in country_set:
        # \b ensures we match "Niger" but not "Nigeria", "Oman" but not "Woman"
        if re.search(r'\b' + re.escape(country.lower()) + r'\b', text.lower()):
            found.add(country.title())
            
    return found

def process_row(row, country_names_set):
    """
    Logic to clean existing country or find it in other columns.
    """
    current_country = row['country']
    
    
    if pd.notna(current_country) and str(current_country).strip() != '':
    
        parts = [c.strip().lower() for c in str(current_country).split(',')]
        valid_countries = [c.title() for c in parts if c in country_names_set]

        if valid_countries:
            uniquecountries = []
            for cu in valid_countries:
                if cu not in uniquecountries:
                    uniquecountries.append(cu)
                    
            return ', '.join(uniquecountries)

    found = extract_countries_from_text(row.get('keywords'), country_names_set)
    if found: return ', '.join(found)
    
    found = extract_countries_from_text(row.get('abstract'), country_names_set)
    if found: return ', '.join(found)
    
    found = extract_countries_from_text(row.get('title'), country_names_set)
    if found: return ', '.join(found)
    
    return '' # Return empty string if nothing found

def organize_country_names_column(df, country_names):
    # Convert list to set for O(1) lookup speed
    country_names_set = set(name.lower() for name in country_names)
    
    # Use apply with axis=1 (Row-wise operation)
    # This is much faster than iterating rows and rebuilding DataFrames
    df['country'] = df.apply(lambda row: process_row(row, country_names_set), axis=1)
    
    return df

def organize_llm_outputs(llm_outputs):
    if isinstance(llm_outputs, dict):
        narrat_resp = llm_outputs.get("narrative", str(llm_outputs))
        adaptation_resp = "" if llm_outputs.get("Adaptation", "") == "No evidence found in this sample." else llm_outputs.get("Adaptation", "")
        mitigation_resp = "" if llm_outputs.get("Mitigation", "") == "No evidence found in this sample." else llm_outputs.get("Mitigation", "")
        water_resp = "" if llm_outputs.get("Water", "") == "No evidence found in this sample." else llm_outputs.get("Water", "")
    else:
        narrat_resp = str(llm_outputs)
        adaptation_resp = ""
        mitigation_resp = ""
        water_resp = ""

    return {
        "narrative": narrat_resp,
        "adaptation": adaptation_resp,
        "mitigation": mitigation_resp,
        "water": water_resp
    }

class Agent2Pipeline:
    def __init__(self, input_csv_path: str, output_json_path: str):
        self.input_csv_path = input_csv_path
        self.output_json_path = output_json_path
        self.synthesizer = LLMSynthesizer()

    def run(self) -> None:
        t0 = time.perf_counter()

        # ── 1. Read CSV ──────────────────────────────────────────────────────────
        logger.info("Reading input CSV: %s", self.input_csv_path)
        df = pd.read_csv(self.input_csv_path, low_memory=False)
        
        total_count = len(df)
        logger.info("Loaded %d records.", total_count)

        df = organize_country_names_column(df, settings.COUNTRIES)
    
                    
        # Normalize production_system early
        def _normalize_ps_column(val):
            if pd.isna(val):
                return "Unknown"
            parts = str(val).split(",")
            norms = set()
            for p in parts:
                if p.strip():
                    norms.add(normalize_production_system(p.strip()))
            return ",".join(norms) if norms else "Unknown"

        if "production_system" in df.columns:
            df["production_system"] = df["production_system"].apply(_normalize_ps_column)

        # ── 2. Actuary layer ─────────────────────────────────────────────────────
        logger.info("── Layer 1: Actuary (Quantitative) ──")

        country_counts = count_by_column(df, "country")
        system_counts = count_by_column(df, "production_system")
        ontology_counts = count_by_column(df, "ontology_tags")
        logger.info(
            "  Counts → %d countries, %d systems, %d ontology categories",
            len(country_counts),
            len(system_counts),
            len(ontology_counts),
        )

        heatmap = build_heatmap(df)
        logger.info("  Heatmap → %d country rows", len(heatmap))

        country_top = get_top_papers(df, "country", n=settings.TOP_N_PAPERS)
        system_top = get_top_papers(df, "production_system", n=settings.TOP_N_PAPERS)
        logger.info("  Top papers extracted.")

        # ── 3. Curator layer ─────────────────────────────────────────────────────
        logger.info("── Layer 2: Curator (Qualitative) ──")

        # 3a. Sample abstracts
        country_samples = sample_top_abstracts(df, "country", n=settings.SAMPLE_SIZE)
        system_samples = sample_top_abstracts(df, "production_system", n=settings.SAMPLE_SIZE)

        # 3b. Generate narratives (LLM calls)
        checkpoint_path = self.output_json_path.replace(".json", "_narratives_ckpt.json")
        checkpoint_data = {"country": {}, "system": {}}
        if os.path.exists(checkpoint_path):
            try:
                with open(checkpoint_path, "r", encoding="utf-8") as f:
                    checkpoint_data = json.load(f)
                logger.info("  Loaded narrative checkpoint: %d country, %d system.", 
                            len(checkpoint_data.get("country", {})), len(checkpoint_data.get("system", {})))
            except Exception as e:
                logger.warning("  Could not load narrative checkpoint: %s", e)

        logger.info("  Generating country narratives (%d clusters)…", len(country_samples))
        country_narratives: dict[str, str] = {}
        for country, abstracts in country_samples.items():
            if country in checkpoint_data.get("country", {}):
                logger.info("    → %s (Loaded from checkpoint)", country)
                chk_val = checkpoint_data["country"][country]
                country_narratives[country] = organize_llm_outputs(chk_val)
                continue

            count = country_counts.get(country, 0)
            logger.info("    → %s (%d papers, sample=%d)", country, count, len(abstracts))
            narrative_resp = self.synthesizer.synthesize(abstracts, country, count)
            country_narratives[country] = organize_llm_outputs(narrative_resp)
            
            # Save checkpoint
            checkpoint_data.setdefault("country", {})[country] = narrative_resp
            with open(checkpoint_path, "w", encoding="utf-8") as f:
                json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)

        logger.info("  Generating system narratives (%d clusters)…", len(system_samples))
        system_narratives: dict[str, str] = {}
        for system, abstracts in system_samples.items():
            if system in checkpoint_data.get("system", {}):
                logger.info("    → %s (Loaded from checkpoint)", system)
                chk_val = checkpoint_data["system"][system]
                system_narratives[system] = organize_llm_outputs(chk_val)
                continue

            count = system_counts.get(system, 0)
            logger.info("    → %s (%d papers, sample=%d)", system, count, len(abstracts))
            narrative_resp = self.synthesizer.synthesize(abstracts, system, count)
            system_narratives[system] = organize_llm_outputs(narrative_resp)
            
            # Save checkpoint
            checkpoint_data.setdefault("system", {})[system] = narrative_resp
            with open(checkpoint_path, "w", encoding="utf-8") as f:
                json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)

        # 3c. Gap detection
        country_gaps = detect_gaps(country_counts, area_type="country")
        system_gaps = detect_gaps(system_counts, area_type="production_system")
        all_gaps = country_gaps + system_gaps
        logger.info("  Identified %d gaps.", len(all_gaps))

        # ── 4. Librarian layer ───────────────────────────────────────────────────
        logger.info("── Layer 3: Librarian (Output) ──")

        report = build_report(
            total_count=total_count,
            ontology_counts=ontology_counts,
            country_counts=country_counts,
            system_counts=system_counts,
            heatmap=heatmap,
            country_narratives=country_narratives,
            system_narratives=system_narratives,
            country_top_papers=country_top,
            system_top_papers=system_top,
            gaps=all_gaps,
        )

        save_report(report, self.output_json_path)

        elapsed = time.perf_counter() - t0
        logger.info("Pipeline complete in %.1f s", elapsed)
        logger.info("  Output → %s", self.output_json_path)
        logger.info(
            "  Summary: %d records, %d country profiles, %d system profiles, %d gaps",
            report.global_stats.total_count,
            len(report.country_profiles),
            len(report.system_profiles),
            len(report.identified_gaps),
        )


if __name__ == "__main__":


    agent2_pipeline = Agent2Pipeline(settings.INPUT_CSV_PATH, settings.OUTPUT_JSON_PATH)
    agent2_pipeline.run()

