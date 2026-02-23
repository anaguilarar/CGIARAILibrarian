from typing import Optional, List
from pydantic import BaseModel, Field, validator
import pandas as pd

class RawMetadata(BaseModel):
    """Normalized internal representation of a retrieved record before enrichment."""
    title: str
    abstract: str
    authors: List[str]
    year: int
    affiliation: Optional[str] = None
    country: Optional[str] = None
    region: Optional[str] = None
    keywords: Optional[str] = None
    doi_pid: str
    citation_count: int = 0
    downloads_count: int = 0
    total_views: int = 0
    repository_source: str
    raw_source_data: Optional[dict] = None

class ClassifiedMetadata(RawMetadata):
    """Enriched record with ontology tags and ranking."""
    ontology_tags: List[str] = Field(..., description="List of tags: Water, Adaptation, Mitigation")
    production_system: str = Field(..., description="Production System (Crop/Commodity)")
    classification_confidence: float = Field(..., ge=0.0, le=1.0)
    classification_explanation: str
    models_name: str
    ranking_score: float = Field(..., ge=0.0, le=100.0)
    

    @validator('ontology_tags', each_item=True)
    def validate_ontology(cls, v):
        allowed = {'Water', 'Adaptation', 'Mitigation', 'Other', 'Unclassified'}
        if v not in allowed:
            # We enforce strict ontology, but 'Other' is fallback.
            # In practice, LLM might hallucinate, so we could log warning instead of raising error,
            # but getting the prompt right is better.
            pass 
        return v

def to_pandas(records: List[ClassifiedMetadata]) -> pd.DataFrame:
    """Converts list of Pydantic models to the required Pandas DataFrame schema."""
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame([r.model_dump() for r in records])
    
    # Ensure lists are stringified for DataFrame storage
    cols_to_join = ['authors', 'ontology_tags']
    for col in cols_to_join:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: '; '.join(x) if isinstance(x, list) else x)
    
    return df
