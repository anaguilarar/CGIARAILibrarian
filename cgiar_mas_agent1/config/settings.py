"""
Agent 1 Configuration — paths, api urls, ranking weights, cgiar centers, LLM settings.
"""

import os
from typing import List

# Sources Configuration
CGSPACE_API_URL = os.getenv("CGSPACE_API_URL", "https://cgspace.cgiar.org/server/api")
CGSPACE_API_URL_METRICS = 'https://cgspace.cgiar.org/server/api/statistics/usagereports/{uuid}_{metric}'

GARDIAN_API_URL = os.getenv("GARDIAN_API_URL", "https://api.qvantum.scio.services/api/node/agris/gardian_index")
GARDIAN_API_KEY = os.getenv("GARDIAN_API_KEY", "")
#DATAVERSE_API_URL = os.getenv("DATAVERSE_API_URL", "https://dataverse.harvard.edu/api/search")
DATAVERSE_API_URL = os.getenv("DATAVERSE_API_URL", "")
DATAVERSE_API_URL_METRICS = os.getenv("DATAVERSE_API_URL_METRICS", "https://dataverse.harvard.edu")
# LLM Configuration
# Options: "ollama", "llama-cpp-python", "mock"
LLM_BACKEND = os.getenv("LLM_BACKEND", "ollama") 
#LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME", "ministral-3:latest")
LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME", "ministral-3:14b")
#LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME", "gemma3:4b")
#LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME", "qwen3:8b")
LLM_API_BASE = os.getenv("LLM_API_BASE", "http://localhost:11434")

# CGIAR Centers Data
CGIAR_CENTERS: List[str] = [
    "CGIAR", "AfricaRice", "Bioversity", "CIAT", "CIFOR", "CIMMYT", "CIP", 
    "ICARDA", "ICRAF", "ICRISAT", "IFPRI", "IITA", "ILRI", "IRRI", "IWMI",
    "AICCRA", "WorldFish", "World Agroforestry", 
    "International Institute of Tropical Agriculture"
    "Alliance of Bioversity International and CIAT",
    "International Center for Tropical Agriculture - CIAT",
    "International Maize and Wheat Improvement Center - (CIMMYT)"
    "International Food Policy Research Institute"
]

# Ranking Weights
WEIGHT_CITATIONS = 0.35
WEIGHT_RECENCY = 0.15
WEIGHT_IMPACT = 0.15
WEIGHT_USAGE = 0.25
WEIGHT_LLMCLASS = 0.1

# Output Schema
REQUIRED_COLUMNS = [
    "title", "abstract", "authors", "year", "affiliation", "country", 
    "production_system", "doi_pid", "citation_count", "ontology_tag", 
    "classification_confidence", "classification_explanation", 
    "ranking_score", "repository_source"
]
