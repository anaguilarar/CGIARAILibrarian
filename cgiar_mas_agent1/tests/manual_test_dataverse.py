import sys
import os

# Add project root to path so we can import internal modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from cgiar_mas_agent1.agent1.retrieval.dataverse import DataverseConnector

def test_dataverse_connection():
    print("Initializing Dataverse Connector...")
    connector = DataverseConnector()
    print(f"API URL: {connector.api_url}")
    
    query = "wheat"
    print(f"Searching for '{query}' (limit=5)...")
    
    try:
        results = list(connector.search(query, limit=5))
        
        if not results:
            print("No results found. Check API URL or Query.")
            return

        print(f"\nSuccessfully retrieved {len(results)} records.")
        print("-" * 50)
        
        for i, record in enumerate(results, 1):
            print(f"Record {i}:")
            print(f"  Title: {record.title}")
            print(f"  Year: {record.year}")
            print(f"  Authors: {record.authors}")
            print(f"  DOI/Handle: {record.doi_pid}")
            print(f"  Abstract (truncated): {record.abstract[:100]}...")
            print("-" * 50)
            
    except Exception as e:
        print(f"Test Failed with Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_dataverse_connection()
