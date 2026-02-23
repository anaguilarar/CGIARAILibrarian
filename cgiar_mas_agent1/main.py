import logging
import sys
import json
from typing import List

# Ensure we can import modules from strict structure
sys.path.append(".")

from cgiar_mas_agent1.agent1.core.domain import RawMetadata, ClassifiedMetadata, to_pandas
from cgiar_mas_agent1.agent1.retrieval.base import BaseConnector
from cgiar_mas_agent1.agent1.retrieval.cgspace import CGSpaceConnector
from cgiar_mas_agent1.agent1.retrieval.dataverse import DataverseConnector
from cgiar_mas_agent1.agent1.retrieval.gardian import GardianConnector
from cgiar_mas_agent1.agent1.processing.filters import CGIARFilter
from cgiar_mas_agent1.agent1.intelligence.llm import LLMClassifier
from cgiar_mas_agent1.agent1.analysis.ranking import Ranker
from cgiar_mas_agent1.config.settings import GARDIAN_API_KEY

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Agent1_Pipeline")

class Agent1Pipeline:
    def __init__(self):
        self.filter = CGIARFilter()
        self.classifier = LLMClassifier()
        self.ranker = Ranker()
        self.connectors: List[BaseConnector] = [] 
        
        # Register Connectors
        self.connectors.append(CGSpaceConnector())
        self.connectors.append(DataverseConnector())
        
        # Only add Gardian if Key is present
        if GARDIAN_API_KEY:
            self.connectors.append(GardianConnector())
        else:
            logger.warning("Skipping GARDIAN Connector: No API Key provided.")

    def run(self, query: str = "climate change", total_target: int = 50, batch_size: int = 10):
        logger.info(f"Starting Agent 1 Pipeline with query: '{query}' | Target: {total_target} records.")
        
        all_raw_records = []
        
        # 1. Retrieval (Fetch enough to likely result in total_target valid CGIAR papers)
        # We fetch more than target because filtering will drop many.
        fetch_limit = total_target * 2 
        logger.info(f"Phase 1: Retrieval (Aiming for {fetch_limit} raw candidates to ensure {total_target} valid output)...")
        
        final_results = []
        
        # Checkpoint Setup
        CHECKPOINT_FILE = "cgiar_mas_agent1/output/agent1_checkpoint.jsonl"
        STATE_FILE = "cgiar_mas_agent1/output/agent1_state.json"
        
        processed_ids = set()
        offsets = {"CGSpace": 0, "Dataverse": 0}
        
        # Check State for Query Persistence
        import os
        last_query = ""
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    state = json.load(f)
                    last_query = state.get("last_query", "")
                    saved_offsets = state.get("offsets", {})
            except Exception:
                pass
        
        resume_mode = (last_query == query)
        if resume_mode:
            offsets = saved_offsets if saved_offsets else {"CGSpace": 0, "Dataverse": 0}
            logger.info(f"Resuming query '{query}' with SAVED offsets: {offsets}")
            
        else:
            logger.warning(f"Query changed or no state. Starting fresh.")
            offsets = {"CGSpace": 0, "Dataverse": 0}
            
        # Load existing checkpoint if available
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        record_data = json.loads(line)
                        # Track by DOI/PID if available, else Title
                        pid = record_data.get("doi_pid") or record_data.get("title")
                        if pid:
                            processed_ids.add(pid)
                            # Rehydrate object to keep in final memory
                            final_results.append(ClassifiedMetadata(**record_data))
                            
                    except json.JSONDecodeError:
                        continue
            logger.info(f"Loaded {len(processed_ids)} records from checkpoint.")
            if resume_mode:
                logger.info(f"Resuming with offsets: {offsets}")
        except FileNotFoundError:
            logger.info("No checkpoint found. Starting fresh.")


        for connector in self.connectors:
            try:
                # Calculate share per connector
                limit_per_source = int(fetch_limit / len(self.connectors))
                logger.info(f"Querying {connector.source_name} for {limit_per_source} records...")
                
                # Fetching
                current_offset = offsets.get(connector.source_name, 0)
                logger.info(f"Querying {connector.source_name} starting at offset {current_offset} for {limit_per_source} records...")
                results = list(connector.search(query, limit=limit_per_source, start_offset=current_offset))
                logger.info(f"Retrieved {len(results)} records from {connector.source_name}")
                if hasattr(connector, 'last_position'):
                    offsets[connector.source_name] = connector.last_position
                else:
                    # Fallback for CGSpace if it doesn't have this logic yet
                    offsets[connector.source_name] += len(results) 
                    
                all_raw_records.extend(results)
            except Exception as e:
                logger.error(f"Error in {connector.source_name}: {e}")
        
        if not all_raw_records:
            logger.warning("No records found from any source.")
            return

        # Save current state
        with open(STATE_FILE, "w") as f:
            json.dump({
                "last_query": query,
                "offsets": offsets
            }, f)
            
        # 2. Filtering
        #logger.info("Phase 2: Filtering...")
        #valid_records = self.filter.filter_batch(all_raw_records)
        logger.info(f"Retained {len(all_raw_records)} records after CGIAR filtering.")

        if not all_raw_records:
            logger.warning("No records passed the CGIAR filter.")
            return

        # 3. Classification & Ranking (Batched)
        logger.info(f"Phase 2: Classification & Ranking (Processing in batches of {batch_size})...")


        # Generator for batches
        def get_batches(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        total_processed_new = 0
        
        for batch_idx, batch in enumerate(get_batches(all_raw_records, batch_size)):
            logger.info(f"Processing Batch {batch_idx + 1} ({len(batch)} records)...")
            
            for record in batch:
                # Checkpoint Check
                pid = record.doi_pid or record.title
                if pid in processed_ids:
                    # logger.info(f"Skipping processed record: {pid}")
                    continue

                # Classify
                try:
                    cls_result = self.classifier.classify(record.title, record.abstract, record.keywords)
                except Exception:
                    # Fallback
                    cls_result = {
                        "ontology_tags": ["Unclassified"], 
                        "classification_confidence": 0.0, 
                        "classification_explanation": "Error"
                    }
                
                # Rank
                score = self.ranker.calculate_score(
                    record.citation_count, 
                    record.year, 
                    bool(record.doi_pid),
                    views=record.total_views,
                    downloads=record.downloads_count,
                    llm_confidence=cls_result['classification_confidence']
                )

                # Merge
                final_record = ClassifiedMetadata(
                    **record.model_dump(),
                    **cls_result,
                    ranking_score=score
                )
                final_results.append(final_record)
                
                # Incremental Save (Append to JSONL)
                with open(CHECKPOINT_FILE, "a", encoding="utf-8") as f:
                    f.write(final_record.model_dump_json() + "\n")
                
                # Add to memory set to prevent dupes in same run
                processed_ids.add(pid)
                total_processed_new += 1
            
            # Optional: Intermediate save or sleep here if API limits concern
        
        # 4. Output
        # Convert all results (loaded + new) to DataFrame
        if final_results:
            df = to_pandas(final_results)
            output_path = "cgiar_mas_agent1/output/agent1_results.csv"
            df.to_csv(output_path, index=False)
            logger.info(f"Pipeline Complete. Processed {total_processed_new} new records (Total: {len(final_results)}).")
            logger.info(f"Data saved to {output_path}")
            logger.info(f"\n{df.head()}")
        else:
            logger.warning("No results to save.")

if __name__ == "__main__":
    agent = Agent1Pipeline()
    agent.run(total_target=100)
    
