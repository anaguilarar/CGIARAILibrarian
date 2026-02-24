import re
import json
import logging
import requests
from typing import Dict, Any, List
from ...config.settings import LLM_API_BASE, LLM_MODEL_NAME

logger = logging.getLogger(__name__)

class LLMClassifier:
    """Interface for Local LLM (Ollama)."""

    def __init__(self):
        self.api_url = f"{LLM_API_BASE}/api/generate"
        self.model = LLM_MODEL_NAME
    
    def _optimize_text(self, text: str, max_chars: int = 2000) -> str:
        """
        Cleans and truncates text to fit LLM context without losing critical info.
        Strategy: Keep the Start (Introduction) and the End (Conclusion).
        """
        if not text:
            return ""

        clean_text = re.sub(r'<[^>]+>', '', text)

        clean_text = " ".join(clean_text.split())

        # Smart Truncation
        if len(clean_text) <= max_chars:
            return clean_text
        
        head_length = int(max_chars * 0.7)
        tail_length = int(max_chars * 0.3)
        
        return f"{clean_text[:head_length]} ... [TRUNCATED] ... {clean_text[-tail_length:]}"
    
    def _build_prompt(self, title: str, abstract: str, keywords: str) -> str:
        return f"""You are a strict scientific classifier for the CGIAR Climate Hub.
    
                Task 1: Analyze the following research text and classify it using the Climate Ontology.
                        A paper may belong to ONE, MULTIPLE, or NONE of the categories.
                Task 2: Extract the primary Production System (Crop/Commodity).
                You must NOT infer, interpret beyond the text, or use external knowledge.

                Ontology Definitions:
                1. Water: irrigation, water management, hydrological cycles, water productivity.
                2. Adaptation: resilience, climate vulnerability, heat tolerance, drought tolerance, risk management, adaptation measures.
                3. Mitigation: carbon sequestration, greenhouse gas reduction, GHG mitigation, low-carbon agriculture.

                Production System Extraction:
                    - Identify the specific crop, animal, or system discussed.
                    - Common CGIAR Systems: Rice, Maize, Wheat, Cassava, Potato, Beans, Livestock (Cattle/Dairy), Aquaculture, Agroforestry.
                    - If multiple are mentioned (e.g., "Rice-Wheat rotation"), combine them (e.g., "Rice/Wheat").
                    - If no specific commodity is mentioned, use "General".
                    - be sure that the mentioned crop system is not part of a center name e.g. Secretariat of Agriculture and Livestock (SAG) the crop system is not Livestock
                
                Input Data:
                - Title: {title}
                - keywords: {keywords}
                - Abstract: {abstract}

                Instructions:
                - If the text discusses multiple topics (e.g., solar irrigation is Water + Mitigation), include ALL relevant tags.
                - If it fits none, return an empty list [].
                - Provide a confidence score (0.0 to 1.0) based on how clearly the abstract matches the definitions.
                - Explanation: Citation of specific keywords found in the text. MAX 15 WORDS. **MUST BE IN ENGLISH.**
                - NEGATIVE CONSTRAINT: Do not mention categories that are NOT present.
                - Confidence: 0.9 (Explicit match), 0.7 (Thematic match), <0.5 (Weak).

                Output Schema (JSON ONLY):
                {{
                    "ontology_tags": ["Category1", "Category2"], 
                    "production_system": "Name of System (e.g. Rice)",
                    "classification_confidence": 0.95,
                    "explanation": "Quote exact phrases from the text that justify each tag."
                }}
                """

    def classify(self, title: str, abstract: str, keywords: str) -> Dict[str, Any]:
        """
        Returns classification dictionary: {tags, confidence, classification_explanation}.
        """
        if keywords is None:
            keywords = ''
        
        clean_abstract = self._optimize_text(abstract)
        clean_title = title.replace("{", "(").replace("}", ")")
        prompt = self._build_prompt(clean_title, clean_abstract, keywords)
        
        payload = {
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "format": "json",
                    "options": {
                        "temperature": 0.0,
                        "num_ctx": 4096,      # Ensure context window fits abstract
                        "seed": 42
                    }}
        try:
            
            response = requests.post(
                self.api_url, 
                json=payload,
                timeout=120
            )
            response.raise_for_status()
            result = response.json()
            
            content = result.get("response", "")
            data = json.loads(content)
            
            # 5. Parse Tags
            tags = data.get("ontology_tags", [])
            if isinstance(tags, str): 
                tags = [tags]
            
            # 6. Sanitize Tags (Remove hallucinations)
            valid_ontology = {"Water", "Adaptation", "Mitigation"}
            clean_tags = [t for t in tags if t in valid_ontology]

            prod_system = data.get("production_system", "General")
            if not prod_system or prod_system.lower() in ["none", "non", "unknown"]:
                prod_system = "General"

            explanation = data.get("explanation", "No explanation provided.")
            if isinstance(explanation, list):
                explanation = "; ".join([str(i) for i in explanation])

            if isinstance(explanation, dict):
                explanation = "; ".join([f'{k}: {v}' for k, v in explanation.items()])

            return {
                "ontology_tags": clean_tags,
                "production_system": prod_system.lower(),
                "classification_confidence": float(data.get("classification_confidence", 0.0)),
                "classification_explanation": explanation,
                "models_name": self.model
            }
            
        except json.JSONDecodeError:
            logger.error(f"LLM produced invalid JSON: {content}")
            return self._fallback_response()
        except Exception as e:
            logger.error(f"LLM Classification failed: {e}")
            return self._fallback_response()
            
    def _fallback_response(self):
        return {
            "ontology_tags": [],
            "production_system": "Unclassified",
            "classification_confidence": 0.0,
            "classification_explanation": "Classification failed.",
            "models_name": self.model
        }