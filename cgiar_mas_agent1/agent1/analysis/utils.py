import requests

def get_crossref_citation_count(doi):
    """
    Retrieves the citation count for a given DOI using the CrossRef API.
    """
    url = f"https://api.crossref.org/works/{doi}"
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an exception for bad status codes
        data = response.json()
        count = data.get("message", {}).get("is-referenced-by-count")
        if count is not None:
            return count
        else:
            return "Citation count not available or no matches found in CrossRef."
    except requests.exceptions.RequestException as e:
        return None

def get_unified_citation_count(doi, verbose = False):
    """
    Retrieves citation count by checking DataCite (for datasets) first,
    then Crossref (for papers).
    """
    # 1. Clean the DOI string (remove 'doi:' prefix)
    clean_doi = doi.replace("doi:", "").strip()
    
    # --- STRATEGY A: Check DataCite (Best for Dataverse/Zenodo) ---
    datacite_url = f"https://api.datacite.org/dois/{clean_doi}"
    try:
        response = requests.get(datacite_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            # DataCite stores citation counts in 'attributes' -> 'citationCount'
            # Note: This relies on the repository reporting citations to DataCite
            count = data.get("data", {}).get("attributes", {}).get("citationCount")
            if count is not None:
                if verbose: print(f"Found in DataCite: {count}")
                return count
    except Exception as e:
        pass # Fail silently and try next source

    # --- STRATEGY B: Check Crossref (Best for Journal Articles) ---
    crossref_url = f"https://api.crossref.org/works/{clean_doi}"
    try:
        response = requests.get(crossref_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            count = data.get("message", {}).get("is-referenced-by-count")
            if count is not None:
                if verbose: print(f"Found in Crossref: {count}")
                return count
    except Exception as e:
        pass

    # --- STRATEGY C: OpenCitations (Alternative fallback) ---
    # Sometimes DataCite has the DOI but not the count, whereas OpenCitations might track it
    try:
        oc_url = f"https://opencitations.net/index/coci/api/v1/citation-count/{clean_doi}"
        response = requests.get(oc_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                count = int(data[0].get("count", 0))
                if verbose: print(f"Found in OpenCitations: {count}")
                return count
    except Exception:
        pass

    return 0