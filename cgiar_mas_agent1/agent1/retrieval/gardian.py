import requests
import json
from typing import Generator, Any, Dict, List
from .base import BaseConnector
from ..core.domain import RawMetadata
from ...config.settings import GARDIAN_API_URL, GARDIAN_API_KEY

class GardianConnector(BaseConnector):
    def __init__(self):
        super().__init__("GARDIAN")
        self.api_url = GARDIAN_API_URL
        self.api_key = GARDIAN_API_KEY

    def search(self, query: str, limit: int = 100) -> Generator[RawMetadata, None, None]:
        """
        Searches GARDIAN for items matching the query.
        Assumes standard ElasticSearch-like structure or similar based on the URL provided.
        """
        # Note: API parameters based on standard patterns. 
        # Adjust 'q', 'size', 'from' keys if the specific API differs.
        
        page = 0
        page_size = 20
        items_yielded = 0
        
        while items_yielded < limit:
            params = {
                "q": query,       # Common search parameter
                "size": page_size,
                "from": page * page_size
            }
            
            # If the user provided URL is an exact endpoint that needs a POST body or different params
            # We might need to adjust, but starting with GET param `q` is standard for search APIs.

            try:
                # Prepare headers
                headers = {}
                if self.api_key:
                    # Assumption: Bearer token. Use "x-api-key" if that's what the provider expects.
                    headers["Authorization"] = f"Bearer {self.api_key}"

                # Based on the URL provided, this looks like a direct index search
                # Assuming simple GET request works
                response = requests.get(self.api_url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()
                
                # Check for ElasticSearch style: { "hits": { "hits": [ ... ] } }
                # OR: { "results": [ ... ] }
                results = []
                
                hits_container = data.get("hits", {})
                if isinstance(hits_container, dict) and "hits" in hits_container:
                     results = hits_container.get("hits", [])
                elif "results" in data:
                     results = data.get("results", [])
                elif "data" in data:
                     results = data.get("data", [])
                
                if not results:
                    break

                for item in results:
                    if items_yielded >= limit:
                        break
                    
                    # ElasticSearch results often wrap the actual data in "_source"
                    source_data = item.get("_source", item)
                    
                    metadata = self._map_to_domain(source_data)
                    yield metadata
                    items_yielded += 1

                # Pagination Check
                # If we got fewer results than requested, implies end of data
                if len(results) < page_size:
                    break
                    
                page += 1

            except requests.exceptions.RequestException as e:
                # Log error but try to continue or break gracefully
                print(f"Error fetching data from GARDIAN: {e}")
                break
            except json.JSONDecodeError:
                print("Error: GARDIAN API returned non-JSON response.")
                break

    def _map_to_domain(self, source: Dict[str, Any]) -> RawMetadata:
        """
        Maps GARDIAN JSON to RawMetadata.
        Tries multiple common field names since we don't have the exact schema.
        """
        def get_first(keys: List[str], default=None):
            for k in keys:
                val = source.get(k)
                if val:
                    return val
            return default

        # Title
        title = get_first(["title", "name", "label"], "Untitled")
        
        # Abstract
        abstract = get_first(["abstract", "description", "summary", "snippet"], "")
        
        # Authors
        # Could be a list of strings, list of dicts, or single string
        authors_raw = get_first(["authors", "creators", "contributors"], [])
        authors = []
        if isinstance(authors_raw, str):
            authors = [authors_raw]
        elif isinstance(authors_raw, list):
            for a in authors_raw:
                if isinstance(a, str):
                    authors.append(a)
                elif isinstance(a, dict):
                    authors.append(a.get("name", a.get("value", str(a))))

        # Year
        year_val = get_first(["year", "publication_year", "date", "issued"], 0)
        try:
             # Extract year if it's a full date string like "2023-01-01"
             year_str = str(year_val)
             if len(year_str) >= 4:
                 year = int(year_str[:4])
             else:
                 year = 0
        except (ValueError, TypeError):
             year = 0

        # DOI / PID
        doi = get_first(["doi", "handle", "url", "uri", "id"], "")

        return RawMetadata(
            title=title,
            abstract=abstract or "",
            authors=authors,
            year=year,
            doi_pid=str(doi),
            repository_source="GARDIAN",
            raw_source_data=source
        )
