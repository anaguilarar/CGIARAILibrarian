import math
from datetime import datetime
from ...config.settings import WEIGHT_CITATIONS, WEIGHT_RECENCY, WEIGHT_IMPACT, WEIGHT_USAGE, WEIGHT_LLMCLASS


class Ranker:
    """
    Calculates impact score based on metadata signals: Citations, Recency, Metadata Quality (DOI), and Usage (Views/Downloads).
    """

    def __init__(self):
        self.current_year = datetime.now().year
        self.MAX_EXPECTED_CITATIONS = 1000
        self.MAX_EXPECTED_INTERACTIONS = 6000
        
        
    def _normalize_log(self, count: int, max_expected: int) -> float:
        """Log-scale normalization: log(1 + count) / log(1 + max_expected)."""
        # Assumption: 1000 citations is a 'max' reliable signal for normalization score of 1.0
        return min(math.log(1 + count) / math.log(max_expected+1), 1.0) * 100

    def _recency_decay(self, year: int) -> float:
        """Inverse decay: 1 / (age + 1). Returns 0-100 scale."""
        age = max(0, self.current_year - year)
        return (1 / (age + 1)) * 100

    def _calculate_usage_score(self, views:int, downloads:int):
        views = views or 0
        downloads = downloads or 0

        weighted_interaction = views + downloads * 3
        
        return self._normalize_log(weighted_interaction, self.MAX_EXPECTED_INTERACTIONS)
        
    def calculate_score(self, citation_count: int, year: int, has_doi: bool, views: int, downloads:int, llm_confidence:float) -> float:
        """
        Returns a scalar score 0-100.
        Adaptive: If citation_count is 0 (common in repository data), reliable weights are shifted.
        """
        
        score_citations = self._normalize_log(citation_count, self.MAX_EXPECTED_CITATIONS)
        score_recency = self._recency_decay(year)
        score_impact = 100.0 if has_doi else 50.0
        score_usage = self._calculate_usage_score(views, downloads)
        score_relevance = llm_confidence * 100
        
        # Adaptive Weighting
        if citation_count > 0:
            
            w_c = WEIGHT_CITATIONS
            w_u = WEIGHT_USAGE
            w_r = WEIGHT_RECENCY
            w_m = WEIGHT_IMPACT
            w_rel = WEIGHT_LLMCLASS
            
        else:
            # Standard weights
            w_c = 0
            w_u = 0.4  # High importance on views/downloads
            w_r = 0.25  # High importance on newness
            w_m = 0.2
            w_rel = 0.15

        final_score = (
            (w_c * score_citations) +
            (w_u * score_usage) +
            (w_r * score_recency) +
            (w_m * score_impact)+
            (w_rel * score_relevance)
        )
        
        return round(final_score, 2)
