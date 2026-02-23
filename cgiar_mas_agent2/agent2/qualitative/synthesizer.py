import re
import json
import logging
import requests
from typing import Dict, Any, List
from ...config.settings import LLM_API_BASE, LLM_MODEL_NAME

logger = logging.getLogger(__name__)

class LLMSynthesizer:
    """Interface for Local LLM (Ollama) to generate analytical narratives for Agent 2."""

    def __init__(self):
        self.api_url = f"{LLM_API_BASE}/api/generate"
        self.model = LLM_MODEL_NAME
    
    def _optimize_text(self, text: str, max_chars: int = 2000) -> str:
        """
        Cleans and truncates text to fit LLM context.
        Used for individual abstracts before combining them into a cluster context.
        """
        if not text:
            return ""
        # Remove HTML tags
        clean_text = re.sub(r'<[^>]+>', '', text)
        # Standardize whitespace
        clean_text = " ".join(clean_text.split())

        if len(clean_text) <= max_chars:
            return clean_text
        
        head_length = int(max_chars * 0.7)
        tail_length = int(max_chars * 0.3)
        return f"{clean_text[:head_length]} ... [TRUNCATED] ... {clean_text[-tail_length:]}"

    def _build_prompt(self, paper_context: str, cluster_name: str, total_count: int, sample_size: int, source_ids: List[str]) -> str:
        
        # Formatting source IDs for the prompt
        source_ids_str = ', '.join(source_ids)

        return f"""You are the Research Cataloguer for CGIAR.

            Objective: Summarize in one sentece or two the independent research contributions in the area of: '{cluster_name}'.
            Analyze CGIAR's contribution in '{cluster_name}' separated by Climate Ontology.
            
            Context:
            - This cluster represents {total_count} research outputs.
            - You are analyzing a sample of the top {sample_size} highest-impact records.
            - Do NOT assume this sample represents the total volume.
            - Do NOT use external knowledge. Only use the provided data.
            - **CRITICAL:** Group findings strictly by their assigned Ontology (Water, Adaptation, Mitigation).
            - **CRITICAL:** These are independent studies. Do NOT connect them unless the text explicitly says they are linked.

            Available Source IDs to Cite:
            {source_ids_str}
            
            Input Evidence  (Note the ONTOLOGY tags):
            {paper_context}

            STRICT Rules for Generation:
            1. **Categorization:** Answer "What has CGIAR done in {cluster_name} for [Category]?" for each of the three categories (Water, Adaptation, Mitigation).
            2. **Independence:** Treat each source as a standalone finding. Do NOT infer chronological links unless explicitly stated.
            3. **Institutional Voice:** Frame all findings as CGIAR's active work. Use subjects like "CGIAR," "The Center," "Researchers," or specific center names (e.g., "CIP," "CIAT").
            4. **No Meta-Commentary:** **EXTREMELY IMPORTANT:** DO NOT start with "Research in this cluster...", "This dataset...", "The papers...", or "This sample...". Start directly with the actor (e.g., "CGIAR implemented...", "Bioversity developed...").
            5. **No False Connections:** Do NOT use phrases like "building on this," "consequently," "this led to," or "subsequently" between different papers.
            6. **Exclusivity:** If the provided text does not contain evidence for a specific category (e.g., no 'Water' papers), explicitly state "No evidence found in this sample." for that category.
            7. **Format:** Use bullet points for distinct studies within each category.
            8. **Mandatory Citations:** Every single summary point MUST end with its specific Source ID in parentheses.

            Output Schema (JSON ONLY):
            {{
                "narrative": "CGIAR [Center Name] prioritized [Theme] in [Location] through [Innovation]. Specifically: \\n- [Center Name] developed [Innovation 1] (Source A). \\n- [Center Name] assessed [Impact 2] (Source B).",
                "Adaptation": "• CGIAR [Center] developed [Action/Innovation] (Source_1).\\n• [Center] implemented [Policy] (Source_2).",
                "Mitigation": "• CGIAR researched [Carbon/GHG Topic] (Source_3).",
                "Water": "No evidence found in this sample.",
                "synthesis_confidence": 0.95,
                "explanation": "Summarized distinct outputs without inferring causality."
            }}
            """

    def synthesize(self, abstracts: List[Dict[str, Any]], cluster_name: str, total_count: int) -> Dict[str, Any]:
        """
        
        """
        if not abstracts:
            return self._fallback_response("No abstracts provided for this cluster.")

        # 1. Build the consolidated evidence block
        paper_entries = []
        source_ids = []
        for i, a in enumerate(abstracts):
            doi = a.get('doi', 'N/A')
            if doi not in source_ids:
                source_ids.append(doi)
            tags_clean = a.get('ontolgy_tags', [])
            if isinstance(tags_clean, str):
                # Basic string cleanup if it comes as string representation of list
                tags_clean = tags_clean.replace("[", "").replace("]", "").replace("'", "").replace(';',',')
            elif isinstance(tags_clean, list):
                tags_clean = ", ".join(tags_clean)
            else:
                tags_clean = "Unclassified"
            # Optimize each abstract to ensure the combined block isn't massive
            clean_abs = self._optimize_text(a.get('abstract', ''), max_chars=1900)
            clean_agent1_explanation = self._optimize_text(a.get('classification_explanation', ''),max_chars = 500)
            entry = f"""SOURCE_ID: {doi}\n
                        ONTOLOGY: [{tags_clean}], {clean_agent1_explanation}\n
                        TEXT: {a.get('title', '')}. {clean_abs}\n"""

            paper_entries.append(entry)
        
        combined_context = "\n\n".join(paper_entries)
        
        # 2. Construct Prompt
        prompt = self._build_prompt(
            paper_context=combined_context, 
            cluster_name=cluster_name, 
            total_count=total_count, 
            sample_size=len(abstracts),
            source_ids=source_ids
        )
        
        # 3. Request Settings
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0.0,  # Low temperature for factual synthesis
                "num_ctx": 9492,     # Large context for multiple abstracts
                "seed": 42
            }
        }

        try:
            response = requests.post(self.api_url, json=payload, timeout=240) # Longer timeout for synthesis
            response.raise_for_status()
            result = response.json()
            
            content = result.get("response", "")
            data = json.loads(content)

            # Return structured result
            return {
                "narrative": data.get("narrative", "Synthesis failed to generate text."),
                "Adaptation": data.get("Adaptation", "No evidence processed."),
                "Mitigation": data.get("Mitigation", "No evidence processed."),
                "Water": data.get("Water", "No evidence processed."),
                "synthesis_confidence": float(data.get("synthesis_confidence", 0.0)),
                "model_name": self.model
            }
            
        except json.JSONDecodeError:
            logger.error(f"Agent 2 LLM produced invalid JSON for {cluster_name}")
            return self._fallback_response("Invalid JSON generated by LLM.")
        except Exception as e:
            logger.error(f"Agent 2 Synthesis failed for {cluster_name}: {e}")
            return self._fallback_response(str(e))
            
    def _fallback_response(self, error_msg: str) -> Dict[str, Any]:
        return {
            "narrative": f"Narrative generation unavailable. {error_msg}",
            "Adaptation": "Error",
            "Mitigation": "Error",
            "Water": "Error",
            "synthesis_confidence": 0.0,
            "model_name": self.model
        }