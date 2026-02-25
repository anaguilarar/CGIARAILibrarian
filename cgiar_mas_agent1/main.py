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
from cgiar_mas_agent1.config.settings import GARDIAN_API_KEY, DATAVERSE_API_URL

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
        
        if DATAVERSE_API_URL:
            self.connectors.append(DataverseConnector())
        else:
            logger.warning("Skipping DATAVERSE Connector: No API Key provided.")
        # Only add Gardian if Key is present
        if GARDIAN_API_KEY:
            self.connectors.append(GardianConnector())
        else:
            logger.warning("Skipping GARDIAN Connector: No API Key provided.")
        
        self.state_file = "cgiar_mas_agent1/output/agent1_state.json"
        self.checkpoint_file = "cgiar_mas_agent1/output/agent1_checkpoint.jsonl"
    
    def _save_state(self,query, offsets):
        
        with open(self.state_file, "w") as f:
            json.dump({
                "last_query": query,
                "offsets": offsets
            }, f)

    def run(self, query: str = "climate change", total_target: int = 50, batch_size: int = 10):

        

        logger.info(f"Starting Agent 1 Pipeline with query: '{query}' | Target: {total_target} records.")
        
        all_raw_records = []
        
        # 1. Retrieval (Fetch enough to likely result in total_target valid CGIAR papers)
        # We fetch more than target because filtering will drop many.
        fetch_limit = total_target * 2 
        logger.info(f"Phase 1: Retrieval (Aiming for {fetch_limit} raw candidates to ensure {total_target} valid output)...")
        
        final_results = []
        
                
        processed_ids = set()
        saved_offsets = {connector.source_name:0 for connector in self.connectors}
        saved_queries = {connector.source_name:query for connector in self.connectors}
        
        # Check State for Query Persistence
        import os
        last_query = ""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r") as f:
                    state = json.load(f)
                    if "offsets" in state:
                        saved_offsets.update(state["offsets"])
                    if "last_query" in state and isinstance(state["last_query"], dict):
                        saved_queries.update(state["last_query"])
                        
            except Exception:
                pass
        
        offsets = saved_offsets if saved_offsets else {"CGSpace": 0, "Dataverse": 0}
        logger.info(f"Resuming query '{query}' with SAVED offsets: {offsets}")
            
        # Load existing checkpoint if available
        try:
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        record_data = json.loads(line)
                        # Track by DOI/PID if available, else Title
                        pid = record_data.get("doi_pid") or record_data.get("title")
                        if pid:
                            processed_ids.add(pid)
                            final_results.append(ClassifiedMetadata(**record_data))
                            
                    except json.JSONDecodeError:
                        continue
            logger.info(f"Loaded {len(processed_ids)} records from checkpoint.")

        except FileNotFoundError:
            logger.info("No checkpoint found. Starting fresh.")

        offsets_positions = {connector.source_name:[] for connector in self.connectors}

        current_offsets = saved_offsets.copy()
        current_queries = saved_queries.copy()

        for connector in self.connectors:
            source = connector.source_name
            try:
                # Calculate share per connector
                limit_per_source = int(fetch_limit / len(self.connectors))
                
                active_query = current_queries.get(source, query)
                active_offset = current_offsets.get(source, 0)
                logger.info(f"Querying {source} for {limit_per_source} records... | Query: '{active_query}' | Offset: {active_offset}")
                
                # Fetching
                
                results = list(connector.search(active_query, 
                        limit=limit_per_source, start_offset=active_offset, uuid_list = processed_ids))
                logger.info(f"Retrieved {len(results)} records from {source}")
                if hasattr(connector, 'last_position'):
                    #offsets[connector.source_name] = connector.last_position
                    offsets_positions[source] = connector.last_position
                else:
                    # Fallback for CGSpace if it doesn't have this logic yet
                    offsets_positions[source].append(len(results))
                    #offsets[connector.source_name] += len(results)
                if hasattr(connector, 'query'):
                    current_queries[source] = connector.query
                    
                all_raw_records.extend(results)
            except Exception as e:
                logger.error(f"Error in {source}: {e}")
        
        if not all_raw_records:
            logger.warning("No records found from any source.")
            return
        # Save current state
            
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
        position_datasets_count = {connector.source_name:0 for connector in self.connectors}
        for batch_idx, batch in enumerate(get_batches(all_raw_records, batch_size)):
            logger.info(f"Processing Batch {batch_idx + 1} ({len(batch)} records)...")
            
            for record in batch:
                # Checkpoint Check
                pid = record.doi_pid or record.title
                if pid in processed_ids:
                    offsets[record.repository_source]= offsets_positions[record.repository_source][position_datasets_count[record.repository_source]] if position_datasets_count[record.repository_source] < len(
                        offsets_positions[record.repository_source]) else offsets_positions[record.repository_source][-1]
                    position_datasets_count[record.repository_source] +=1
                    # logger.info(f"Skipping processed record: {pid}")
                    continue

                # Classify
                try:
                    cls_result = self.classifier.classify(record.title, record.abstract, record.keywords)
                except Exception:
                    # Fallback
                    cls_result = {
                        "ontology_tags": [],
                        "production_system": "Unclassified",
                        "classification_confidence": 0.0,
                        "classification_explanation": "Classification failed.",
                        "models_name": self.classifier.model
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
                with open(self.checkpoint_file, "a", encoding="utf-8") as f:
                    f.write(final_record.model_dump_json() + "\n")
                
                # Add to memory set to prevent dupes in same run
                processed_ids.add(pid)
                total_processed_new += 1
            
            # Optional: Intermediate save or sleep here if API limits concern
                offsets[record.repository_source]= offsets_positions[record.repository_source][position_datasets_count[record.repository_source]] if position_datasets_count[record.repository_source] < len(
                    offsets_positions[record.repository_source]) else offsets_positions[record.repository_source][-1]
                position_datasets_count[record.repository_source] +=1
                
        
        self._save_state(current_queries, offsets)
            
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
    
