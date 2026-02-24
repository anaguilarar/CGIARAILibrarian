import time
import requests
from typing import Generator, Any, Dict, List
from tqdm import tqdm
from .base import BaseConnector
from ..core.domain import RawMetadata
from ...config.settings import DATAVERSE_API_URL, DATAVERSE_API_URL_METRICS
from ..processing.filters import CGIARFilter
from ..analysis.utils import get_crossref_citation_count
import copy

def dataverse_metrics(uuid, metric):
    """_summary_

    Args:
        uuid (_type_): _description_
        metric (_type_): can be either viewsTotal or downloadsTotal

    Returns:
        _type_: _description_
    """
    dataverse_url = f"{DATAVERSE_API_URL_METRICS}/api/datasets/:persistentId/makeDataCount/{metric}"
    params = {"persistentId": uuid}
    response = requests.get(dataverse_url, params=params)
    response.raise_for_status()
    data = response.json().get('data', {})
    return data.get(metric, None)
    

class DataverseConnector(BaseConnector):
    def __init__(self):
        super().__init__("Dataverse")
        self.api_url = DATAVERSE_API_URL
        self.cgiarfilterer = CGIARFilter()
        self.last_position = []

    def search(self, query: str, limit: int = 100, uuid_list = None, start_offset: int = 0) -> Generator[RawMetadata, None, None]:
        """
        Searches Dataverse for items matching the query.
        """
        # Dataverse Search API: /api/search?q={query}&start={start}&type=dataset
        
        if uuid_list is None: uuid_listc = set()
        else: uuid_listc = copy.deepcopy(uuid_list)
        start = start_offset
        self.last_position = [start]
        per_page = 10 # Dataverse default is often 10, max is usually 1000 but safest to paginate small
        items_yielded = 0
        consecutive_errors = 0
        max_consecutive_errors = 4
        with tqdm(total=limit, desc="Fetching Data", unit="item") as pbar:
            while items_yielded < limit:
                if consecutive_errors > max_consecutive_errors:
                    pbar.write("Too many consecutive 403/Errors. Stopping search to avoid ban.")
                    break
                params = {
                    "q": query,
                    "type": "dataset", # Usually we want datasets, but could also be "file"
                    "start": start,
                    "per_page": per_page
                }

                try:
                    response = requests.get(self.api_url, params=params)
                    if response.status_code == 403:
                        print(response.status_code)
                        pbar.write(f"403 Forbidden at start index {start}. Skipping batch.")
                        time.sleep(2)
                        # Optimization: Skip the whole page, not just 1 item
                        start += (per_page)
                        consecutive_errors += 1
                        continue
                    
                    consecutive_errors = 0
                    response.raise_for_status()
                    data = response.json()
                    
                    # Response structure: { "status": "OK", "data": { "total_count": X, "items": [...] } }
                    
                    results = data.get("data", {}).get("items", [])
                    
                    if not results:
                        break
                    
                    ## only the first one is gonna take into account
                    for item in results:
                        if items_yielded >= limit:
                            break
                            
                        rawdata = self._map_to_domain(item)
                        
                        if self.cgiarfilterer.is_cgiar_affiliated(rawdata):
                            if rawdata.doi_pid not in uuid_listc:
                                
                                self._enrich_metrics(rawdata, item.get('global_id'))
                                uuid_listc.add(rawdata.doi_pid)
                                items_yielded += 1
                                pbar.update(1)

                                yield rawdata
                                

                    # Pagination Check
                    total_count = data.get("data", {}).get("total_count", 0)
                    if start + len(results) >= total_count:
                        break
                        
                    start += len(results)
                    self.last_position.append(start)
                    
                except requests.exceptions.RequestException as e:
                    pbar.write(f"Error fetching data from Dataverse: {e}")
                    items_yielded += 1
                    #print(f"Error fetching data from Dataverse: {e}")
                    start += per_page
                    self.last_position.append(start)
                    consecutive_errors += 1
                    time.sleep(1)
                    
                    continue
                
    def _enrich_metrics(self, rawdata: RawMetadata, global_id: str):
        """
        Helper method to fetch metrics only when necessary.
        """
        try:
            
            d_count = dataverse_metrics(global_id, "downloadsTotal")
            rawdata.downloads_count = d_count if d_count is not None else 0
            
            v_count = dataverse_metrics(global_id, "viewsTotal")
            rawdata.total_views = v_count if v_count is not None else 0
            
            #if "doi" in global_id:
            #    countv = get_crossref_citation_count(global_id)
            #    rawdata.citation_count = countv if countv is not None else 0
            
        except Exception as e:
            print(e)
            # Fail silently on metrics so we don't lose the paper
            pass
        
    def _map_to_domain(self, item: Dict[str, Any]) -> RawMetadata:
        """
        Maps Dataverse JSON to RawMetadata.
        """
        """
        Maps Dataverse JSON to RawMetadata.
        REMOVED: Heavy API calls and Print statements
        """
        authors = item.get("authors", [])
        
        affiliation = item.get('contacts', None)
        if affiliation:
            affiliation = affiliation[0].get('affiliation', None)
            
        description = item.get("description", "")
        keywords = item.get('keywords', [''])
        if len(keywords) > 0:
            keywords = ', '.join(keywords[:10])
            
        published_at = item.get("published_at", "")
        year = 0
        if published_at and len(published_at) >= 4:
            try:
                year = int(published_at[:4])
            except ValueError:
                pass
        
        # We initialize metrics at 0, they will be filled later ONLY if needed
        return RawMetadata(
            title=item.get("name", "Untitled"),
            abstract=description,
            authors=authors,
            affiliation=affiliation,
            keywords=keywords,
            year=year,
            citation_count=0,
            downloads_count=0, # Placeholder
            total_views=0,     # Placeholder
            doi_pid=item.get("global_id", ""),
            repository_source="Dataverse",
            raw_source_data=item
        )
        