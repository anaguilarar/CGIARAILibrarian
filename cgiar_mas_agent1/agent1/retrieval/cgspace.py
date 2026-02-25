import requests
import re
import copy
from tqdm import tqdm
from typing import Generator, List, Dict, Any, Optional
from .base import BaseConnector
from ..core.domain import RawMetadata
from ...config.settings import CGSPACE_API_URL, CGSPACE_API_URL_METRICS, QUERIES
from ..analysis.utils import get_crossref_citation_count

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

    def search(self, query: str, limit: int = 100, uuid_list = None, start_offset: int = 0) -> Generator[RawMetadata, None, None]:
        """
        Searches CGSpace for items matching the query.
        """
        if uuid_list is None: uuid_listc = set()
        else: uuid_listc = copy.deepcopy(uuid_list)
        endpoint = f"{self.api_url}/discover/search/objects"
        
        # DSpace 7 Pagination: pages are 0-indexed
        page_size = 20 # Fetch in chunks
        page = start_offset ## // page_size
        self.query = query
        self.last_position = [page]
        
        items_yielded = 0
        with tqdm(total=limit, desc="Fetching Data", unit="item") as pbar:
            while items_yielded < limit:
                params = {
                    "query": self.query,
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

                        rawdata = self._map_to_domain(item_data, metadata_dict)
                        if rawdata.doi_pid not in uuid_listc:
                            self._enrich_metrics(rawdata, item_data['uuid'])
                            uuid_listc.add(rawdata.doi_pid)
                            items_yielded += 1
                            pbar.update(1)
                            
                            yield rawdata

                    # Check if we have reached the total available pages
                    total_pages = search_result.get("page", {}).get("totalPages", 0)
                    if page >= total_pages - 1:
                        pbar.write(f'Search exhausted. Scanned {total_pages} records')
                        newqueryindex = QUERIES.index(self.query)+1
                        if newqueryindex>=len(QUERIES): 
                            pbar.write('all queries were used')
                            break
                        self.query = QUERIES[newqueryindex]
                        pbar.write(f'Search query changed to :{self.query}')
                        page = 0
                        self.last_position = [page]
                        continue
                    
                    page += 1
                    self.last_position.append(page)
                    
                    
                except requests.exceptions.RequestException as e:
                    pbar.write(f"Error fetching data from cgspace: {e}")
                    items_yielded += 1
                    page += 1
                    self.last_position.append(page)

    def _enrich_metrics(self, rawdata: RawMetadata, global_id: str):
        """
        Helper method to fetch metrics only when necessary.
        """
        try:
            d_count = cgspace_metrics(global_id, "TotalDownloads" )
            rawdata.downloads_count = d_count if d_count is not None else 0
            
            v_count = cgspace_metrics(global_id, "TotalVisits" )
            rawdata.total_views = v_count if v_count is not None else 0

            #if "doi" in rawdata.doi_pid:
            #    countv = get_crossref_citation_count(rawdata.doi_pid)
            #    rawdata.citation_count = countv if countv is not None else 0
            
        except Exception as e:
            print(e)
            # Fail silently on metrics so we don't lose the paper
            pass            

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

        return RawMetadata(
            title=get_meta("dc.title") or "Untitled",
            abstract=abstract,
            authors=authors,
            year=year,
            country=country,
            region=region,
            keywords=keywords,
            doi_pid=doi_pid,
            citation_count=0,
            total_views = 0,
            downloads_count = 0,
            repository_source="CGSpace",
            raw_source_data=item_data 
        )
