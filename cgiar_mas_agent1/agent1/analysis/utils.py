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
