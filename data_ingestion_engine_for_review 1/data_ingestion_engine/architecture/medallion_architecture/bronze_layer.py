"""
architecture/medallion_architecture/bronze_layer.py
────────────────────────────────────────────────────
Bronze Layer: Add Metadata to Raw Records

Responsibility:
- Add ingestion metadata to raw records
- 1:1 mapping (no transformations)
- Track ingestion time and source

Data Flow:
    raw_records → Bronze Layer (add metadata) → bronze_records

Key Points:
- Pure 1:1 mapping with metadata only
- No transformations or cleaning
- Preserves all original data
"""

import logging
from typing import List, Dict, Any
from datetime import datetime

log = logging.getLogger(__name__)


class BronzeLayer:
    """
    Bronze Layer: Add metadata to raw records.
    
    Process:
    - Add ingestion timestamp
    - Add source information
    - Add layer identifier
    - 1:1 mapping (no transformations)
    """
    
    def __init__(self, config: dict):
        """
        Initialize the Bronze layer.
        
        Args:
            config: Full configuration from config.yaml
        """
        self.config = config
        log.info("BronzeLayer: initialized")
    
    def process(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process records through bronze layer.
        
        Args:
            raw_records: Raw records from reader
        
        Returns:
            Bronze records with metadata
        """
        log.info("BronzeLayer: processing %d records", len(raw_records))
        
        bronze_records = []
        ingestion_time = datetime.utcnow().isoformat()
        
        for idx, record in enumerate(raw_records):
            # Use _api_name (batch_api reader) or _source_table (batch_database
            # reader) if available, else "reader". This becomes the Silver
            # layer's dedup namespace, so records from different APIs/tables
            # that happen to share an id value aren't treated as duplicates.
            source_name = record.pop("_api_name", None) or record.pop("_source_table", None) or "reader"
            bronze_record = {
                "_bronze_ingestion_time": ingestion_time,
                "_bronze_source": source_name,
                "_bronze_layer": "bronze",
                "_bronze_record_index": idx,
                **record  # 1:1 mapping - raw data as-is
            }
            bronze_records.append(bronze_record)
        
        log.info("BronzeLayer: added metadata to %d records", len(bronze_records))
        return bronze_records
