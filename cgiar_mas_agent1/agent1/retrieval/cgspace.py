import requests
import re
from tqdm import tqdm
from typing import Generator, List, Dict, Any, Optional
from .base import BaseConnector
from ..core.domain import RawMetadata
from ...config.settings import CGSPACE_API_URL, CGSPACE_API_URL_METRICS


def cgspace_metrics(uuid:str, metric: str) -> int:
    """_summary_

    Args:
        uuid (_type_): _description_
        metric (_type_): can be either TotalVisits or TotalDownloads

    Returns:
        _type_: _description_
    """
    cgspace_url = CGSPACE_API_URL_METRICS.format(uuid = uuid, metric = metric)
    response = requests.get(cgspace_url)
    response.raise_for_status()
    data = response.json()
    
    pointsdata = data.get('points',[])
    if len(pointsdata)>0:
        return pointsdata[0].get('values',{}).get('views', None)
    else:
        return 0


class CGSpaceConnector(BaseConnector):
    def __init__(self):
        super().__init__("CGSpace")
        self.api_url = CGSPACE_API_URL

    def search(self, query: str, limit: int = 100, start_offset: int = 0) -> Generator[RawMetadata, None, None]:
        """
        Searches CGSpace (DSpace 7) for items matching the query.
        """
        endpoint = f"{self.api_url}/discover/search/objects"
        
        # DSpace 7 Pagination: pages are 0-indexed
        page_size = 20 # Fetch in chunks
        page = start_offset ## // page_size
        
        self.last_position = page
        
        items_yielded = 0
        with tqdm(total=limit, desc="Fetching Data", unit="item") as pbar:
            while items_yielded < limit:
                params = {
                    "query": query,
                    "dsoType": "ITEM",  # Restrict results to Items (ignore Communities/Collections)
                    "page": page,
                    "size": page_size
                }

                try:
                    response = requests.get(endpoint, params=params)
                    response.raise_for_status()
                    data = response.json()

                    # DSpace 7 HAL-JSON structure parsing
                    # Structure: _embedded -> searchResult -> _embedded -> objects
                    search_result = data.get("_embedded", {}).get("searchResult", {})
                    objects = search_result.get("_embedded", {}).get("objects", [])

                    if not objects:
                        break  # No more results

                    for obj in objects:
                        if items_yielded >= limit:
                            break
                        
                        # The actual item metadata is inside "_embedded.indexableObject"
                        item_data = obj.get("_embedded", {}).get("indexableObject", {})
                        metadata_dict = item_data.get("metadata", {})

                        yield self._map_to_domain(item_data, metadata_dict)
                        items_yielded += 1
                        pbar.update(1)

                    # Check if we have reached the total available pages
                    total_pages = search_result.get("page", {}).get("totalPages", 0)
                    if page >= total_pages - 1:
                        break
                    
                    page += 1
                    self.last_position = page
                    
                except requests.exceptions.RequestException as e:
                    pbar.write(f"Error fetching data from Dataverse: {e}")
                    items_yielded += 1
                

    def _map_to_domain(self, item_data: dict, metadata: dict) -> RawMetadata:
        """
        Helper to map DSpace 7 JSON to your internal RawMetadata format.
        """
        
        # Helper to safely get the first value of a metadata field
        def get_meta(field, limit:int = None):
            
            
            metadafield = metadata.get(field, [{}])
            if len(metadafield) == 1:
                return metadafield[0].get("value", None)
            
            else:
                if limit is None:
                    limit = len(metadafield)
                datavals =  [val.get("value", None) for i, val in enumerate(metadafield) if i <= limit]
                return ', '.join(datavals) or ""

        
        # Fallback for abstract if description.abstract is missing, try description
        abstract = get_meta("dc.description.abstract") or get_meta("dc.description") or get_meta("dcterms.abstract") or ""
        
        # country
        
        country = get_meta('cg.coverage.country') or  ""
        
        # region
        
        region = get_meta('cg.coverage.region') or  ""
        
        keywords = get_meta('dcterms.subject', limit = 10) or ""
        
        downloads_count = cgspace_metrics(item_data['uuid'], "TotalDownloads" )
        total_views = cgspace_metrics(item_data['uuid'], "TotalVisits" ) 
        # Handle Authors
        authors_raw = metadata.get("dc.contributor.author", [])
        authors = [a.get("value") for a in authors_raw if "value" in a]

        # Handle Date (Try multiple fields)
        date_issued = get_meta("dc.date.issued") or get_meta("dc.date.accessioned")
        year = 0
        if date_issued:
            
            
            match = re.search(r'\d{4}', date_issued)
            if match:
                year = int(match.group(0))

        # Check for DOI or Handle
        doi_pid = get_meta("cg.identifier.doi") or get_meta("dc.identifier.doi") or item_data.get("handle") or ""

        
        citation_count = 0
        return RawMetadata(
            title=get_meta("dc.title") or "Untitled",
            abstract=abstract,
            authors=authors,
            year=year,
            country=country,
            region=region,
            keywords=keywords,
            doi_pid=doi_pid,
            citation_count=citation_count,
            total_views = total_views,
            downloads_count = downloads_count,
            repository_source="CGSpace",
            raw_source_data=item_data 
        )
