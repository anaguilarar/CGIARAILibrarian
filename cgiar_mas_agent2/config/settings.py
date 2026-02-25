"""
Agent 2 Configuration — paths, thresholds, priority lists, and LLM settings.

Inherits LLM config from Agent 1's conventions (Ollama backend).
"""

import os
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent  # CGIARLibrarian/

INPUT_CSV_PATH = os.getenv(
    "AGENT2_INPUT_CSV",
    str(_PROJECT_ROOT / "cgiar_mas_agent1" / "output" / "agent1_results.csv"),
)

OUTPUT_DIR = _PROJECT_ROOT / "cgiar_mas_agent2" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_JSON_PATH = os.getenv(
    "AGENT2_OUTPUT_JSON",
    str(OUTPUT_DIR / "synthesis_report.json"),
)

# ── Thresholds ───────────────────────────────────────────────────────────────
GAP_THRESHOLD: int = 5          # clusters below this count are flagged
TOP_N_PAPERS: int = 5           # high-impact papers per cluster
SAMPLE_SIZE: int = 10           # max abstracts sent to LLM per cluster

# ── Priority targets ─────────────────────────────────────────────────────────
# Only clusters matching these are eligible for gap-flagging.
# Extend this list as the mandate evolves.
PRIORITY_COUNTRIES: list[str] = [
    "Kenya", "Vietnam", "Ethiopia", "Honduras", "Colombia",
    "Rwanda", "India", "Bangladesh", "Nepal", "Myanmar",
    "Senegal", "Mali", "Niger", "Tanzania", "Uganda",
    "Ghana", "Nigeria", "Mozambique", "Philippines", "Cambodia",
    "Laos", "Guatemala", "El Salvador", "Nicaragua", "Chile",
]

PRODUCTION_SYSTEM_GROUPS = {
    "rice": [
        "rice", "rice (rainfed)", "rice (oryza sativa)"
    ],
    "maize": [
        "maize"
    ],
    "wheat": [
        "wheat", "durum wheat", "triticum durum, bread wheat", "triticum aestivum"
    ],
    "roots and tubers": ["potato", "potatoes", "cassava", "sweet potato", "sweet potatoes", 
        "yam", "tubers", "arrowroots"],
    
    "bananas": [
        "banana", 
    ],
    "legumes": [
        "beans", "legume", "drybeans", "dry beans", "soybeans", "soybean", 
        "groundnuts", "beans (bush beans)", "bean", "common bean", 
        "chickpea", "pigeonpea", "cowpeas", "cowpea", "lablab", 
        "lablab purpureus", "phaseolus vulgaris", 
        "cash crops (legumes forthcoming)", "legume (maize, beans, pigeon pea)"
    ],
    "livestock (dairy)": [
        "dairy", "cattle milk", "livestock (dairy)", "livestock (cattle, dairy)"
    ],
    "livestock (general/beef)": [
        "beef", "beef cattle", "livestock (beef)", "plant-based meats",
        "livestock", "livestock (cattle)", "cattle"
    ],
    "livestock (small)": [
        "livestock (sheep)", 
        "livestock (bees, aquaculture not applicable; **beehive**)"
    ],
    "fisheries and aquaculture": [
        "fish", "fisheries", "coastal fisheries", "aquaculture", "mollusc", 
        "tilapia (aquaculture)", "aquaculture (tilapia)", "fish (aquaculture)", 
        "aquaculture (farmer ponds)", "aquaculture (tilapia, catfish)"
    ],
    "coffee and cocoa": [
        "coffee", "arabica coffee", "coffea arabica", "cocoa", 
        "theobroma cacao", "theobroma cacao (cocoa)", 
        "agroforestry (coffea arabica, theobroma cacao)"
    ],
    "dryland cereals (sorghum/millets)": [
        "sorghum", "millet", "finger millet", "pearl millet", 
        "bajra (pearl millet)", "barley"
    ],
    "general cereals": [
        "grains", "cereals", "céréales (grains)"
    ],
    "fruits and vegetables": [
        "vegetables", "vegetables (tomato, cucumbers, okra)", "fruits", 
        "mango", "poplar", "macadamia" # Poplar/Macadamia are trees but fit horticulture/forestry context
    ],
    "forages and pastures": [
        "forages", "pastures", "forage grasses (panicum maximum, brachiaria humidicola)", 
        "alfalfa (medicago sativa)", "brachiaria (forages)", 
        "forages (grasses, legumes)", "desmodium ovalifolium (forages)", 
        "general (crops, pastures)", "gliricidia", "gliricidia sepium"
    ],
    "agroforestry": [
        "agroforestry", "general (food trees)"
    ],
    "general / cross-cutting": [
        "general", "general (agricultural households including crops, livestock)",
        "general (13 staple food crops: unspecified, e.g., drought-prone staples)",
        "neglected and underutilized species (nus)", 
        "glyphosate herbicide", "inorganic fertilizer", "cotton"
    ]
}

PRIORITY_SYSTEMS: list[str] = [
    "rice", "maize", "wheat", "livestock (dairy)","livestock (general/beef)", "legumes", "agroforestry", "forages and pastures",
    "dryland cereals (sorghum/millets)",
    "roots and tubers", "general / cross-cutting",
]



PRIORITY_TARGETS: list[str] = [
    t.lower() for t in (PRIORITY_COUNTRIES + PRIORITY_SYSTEMS)
]

# ── LLM settings (mirrors Agent 1) ──────────────────────────────────────────
LLM_BACKEND = os.getenv("LLM_BACKEND", "ollama")
LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME", "ministral-3:14b")
LLM_API_BASE = os.getenv("LLM_API_BASE", "http://localhost:11434")


#--- countries
import re

COUNTRIES = []

with open('cgiar_mas_agent2/config/countries.txt', 'r') as fn:
    text = fn.readlines()
    for te in text:
        clean_text = re.sub(r"[\[\]]", "", te)
        clean_text = re.sub(r'<[^>]+>', '', clean_text).replace(',', '').replace('"','').strip().lower()
        if clean_text: COUNTRIES.append(clean_text)