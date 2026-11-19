"""
structured_readers/streaming_cloud_read.py
──────────────────────────────────
Reads from managed cloud messaging: GCP Pub/Sub, AWS Kinesis, Azure Event Hub.

Config: config.yaml under structured.streaming.cloud —
  provider (pubsub|kinesis|eventhub),
  project / stream_name / namespace,
  subscription / shard_id / consumer_group,
  max_messages (default 500)

Registered in core/registry.py as:
  "reader": { "streaming_cloud": "structured_readers.streaming_cloud_read.StreamingCloudReader" }
"""

from structured_readers.base_reader import BaseReader
from typing import Generator
import logging
import json

log = logging.getLogger(__name__)


class StreamingCloudReader(BaseReader):
    """
    Multi-cloud streaming reader supporting GCP Pub/Sub, AWS Kinesis, and Azure Event Hub.
    """

    def connect(self):
        """Connect to cloud messaging service."""
        self._cfg = self.config.get("structured", {}).get("streaming", {}).get("cloud", {})
        if not self._cfg.get("enabled", True):
            raise ValueError("Cloud streaming configuration is disabled (structured.streaming.cloud.enabled: false)")

        provider = self._cfg.get("provider", "pubsub").lower()

        if provider == "pubsub":
            self._connect_pubsub()
        elif provider == "kinesis":
            self._connect_kinesis()
        elif provider == "eventhub":
            self._connect_eventhub()
        else:
            raise ValueError(
                f"Unsupported provider: {provider}. "
                f"Supported: pubsub, kinesis, eventhub"
            )
        
        log.info("StreamingCloudReader: connected → provider=%s", provider)
        self._connected = True
    
    def _connect_pubsub(self):
        """Connect to GCP Pub/Sub."""
        try:
            from google.cloud import pubsub_v1
        except ImportError:
            raise ImportError(
                "google-cloud-pubsub is required for GCP Pub/Sub. "
                "Install with: pip install google-cloud-pubsub"
            )
        
        project_id = self._cfg.get("project")
        subscription_id = self._cfg.get("subscription")
        
        if not project_id:
            raise ValueError("project is required for Pub/Sub")
        if not subscription_id:
            raise ValueError("subscription is required for Pub/Sub")
        
        self._client = pubsub_v1.SubscriberClient()
        self._subscription_path = self._client.subscription_path(project_id, subscription_id)
        self._provider = "pubsub"
        
        log.info("StreamingCloudReader: Pub/Sub subscription ready — %s", self._subscription_path)
    
    def _connect_kinesis(self):
        """Connect to AWS Kinesis."""
        try:
            import boto3
        except ImportError:
            raise ImportError(
                "boto3 is required for AWS Kinesis. "
                "Install with: pip install boto3"
            )
        
        stream_name = self._cfg.get("stream_name")
        region = self._cfg.get("region", "us-east-1")
        
        if not stream_name:
            raise ValueError("stream_name is required for Kinesis")
        
        self._client = boto3.client("kinesis", region_name=region)
        self._stream_name = stream_name
        self._provider = "kinesis"
        
        # Get shard IDs
        try:
            response = self._client.describe_stream(StreamName=stream_name)
            self._shard_ids = [shard["ShardId"] for shard in response["StreamDescription"]["Shards"]]
            log.info("StreamingCloudReader: Kinesis stream ready — %s (shards: %s)",
                     stream_name, self._shard_ids)
        except Exception as exc:
            log.error("StreamingCloudReader: failed to describe Kinesis stream — %s", exc)
            raise
    
    def _connect_eventhub(self):
        """Connect to Azure Event Hub."""
        try:
            from azure.eventhub import EventHubConsumerClient
            from azure.identity import DefaultAzureCredential
        except ImportError:
            raise ImportError(
                "azure-eventhub and azure-identity are required for Event Hub. "
                "Install with: pip install azure-eventhub azure-identity"
            )
        
        namespace = self._cfg.get("namespace")
        eventhub_name = self._cfg.get("eventhub_name")
        consumer_group = self._cfg.get("consumer_group", "$Default")
        
        if not namespace:
            raise ValueError("namespace is required for Event Hub")
        if not eventhub_name:
            raise ValueError("eventhub_name is required for Event Hub")
        
        fully_qualified_namespace = f"{namespace}.servicebus.windows.net"
        
        self._client = EventHubConsumerClient(
            fully_qualified_namespace=fully_qualified_namespace,
            eventhub_name=eventhub_name,
            consumer_group=consumer_group,
            credential=DefaultAzureCredential()
        )
        self._provider = "eventhub"
        
        log.info("StreamingCloudReader: Event Hub ready — %s/%s",
                 namespace, eventhub_name)

    def read(self) -> Generator:
        """
        Read messages from cloud messaging service and yield micro-batches.
        """
        if not self._connected:
            raise RuntimeError("Reader not connected. Call connect() first.")
        
        max_messages = self._cfg.get("max_messages", 500)
        
        if self._provider == "pubsub":
            yield from self._read_pubsub(max_messages)
        elif self._provider == "kinesis":
            yield from self._read_kinesis(max_messages)
        elif self._provider == "eventhub":
            yield from self._read_eventhub(max_messages)
    
    def _read_pubsub(self, max_messages: int) -> Generator:
        """Read from GCP Pub/Sub."""
        from google.cloud.pubsub_v1 import types
        
        log.info("StreamingCloudReader: starting Pub/Sub consumer (max_messages=%d)", max_messages)
        
        while True:
            try:
                # Pull messages from subscription
                response = self._client.pull(
                    request={"subscription": self._subscription_path, "max_messages": max_messages}
                )
                
                if not response.received_messages:
                    log.debug("StreamingCloudReader: no messages from Pub/Sub")
                    yield []
                    continue
                
                batch = []
                ack_ids = []
                
                for msg in response.received_messages:
                    try:
                        # Parse message data
                        try:
                            record = json.loads(msg.message.data.decode("utf-8"))
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            record = {"raw_data": msg.message.data.decode("utf-8", errors="replace")}
                        
                        # Add Pub/Sub metadata
                        record["_pubsub_message_id"] = msg.message.message_id
                        record["_pubsub_publish_time"] = msg.message.publish_time.isoformat()
                        
                        batch.append(record)
                        ack_ids.append(msg.ack_id)
                    
                    except Exception as exc:
                        log.error("StreamingCloudReader: failed to parse Pub/Sub message — %s", exc)
                        continue
                
                # Acknowledge messages
                if ack_ids:
                    try:
                        self._client.acknowledge(
                            request={"subscription": self._subscription_path, "ack_ids": ack_ids}
                        )
                        log.debug("StreamingCloudReader: acknowledged %d Pub/Sub messages", len(ack_ids))
                    except Exception as exc:
                        log.error("StreamingCloudReader: failed to acknowledge messages — %s", exc)
                
                if batch:
                    log.info("StreamingCloudReader: yielding batch of %d Pub/Sub messages", len(batch))
                    yield batch
            
            except Exception as exc:
                log.error("StreamingCloudReader: Pub/Sub read error — %s", exc)
                yield []
    
    def _read_kinesis(self, max_messages: int) -> Generator:
        """Read from AWS Kinesis."""
        log.info("StreamingCloudReader: starting Kinesis consumer (max_messages=%d)", max_messages)
        
        # Initialize shard iterators
        shard_iterators = {}
        for shard_id in self._shard_ids:
            try:
                response = self._client.get_shard_iterator(
                    StreamName=self._stream_name,
                    ShardId=shard_id,
                    ShardIteratorType="LATEST"
                )
                shard_iterators[shard_id] = response["ShardIterator"]
            except Exception as exc:
                log.error("StreamingCloudReader: failed to get shard iterator for %s — %s",
                         shard_id, exc)
        
        while True:
            batch = []
            
            # Read from all shards
            for shard_id, shard_iterator in list(shard_iterators.items()):
                if not shard_iterator:
                    continue
                
                try:
                    response = self._client.get_records(
                        ShardIterator=shard_iterator,
                        Limit=max_messages
                    )
                    
                    # Update shard iterator
                    shard_iterators[shard_id] = response.get("NextShardIterator")
                    
                    # Process records
                    for record in response.get("Records", []):
                        try:
                            try:
                                data = json.loads(record["Data"].decode("utf-8"))
                            except (json.JSONDecodeError, UnicodeDecodeError):
                                data = {"raw_data": record["Data"].decode("utf-8", errors="replace")}
                            
                            # Add Kinesis metadata
                            data["_kinesis_shard_id"] = shard_id
                            data["_kinesis_sequence_number"] = record["SequenceNumber"]
                            data["_kinesis_partition_key"] = record["PartitionKey"]
                            
                            batch.append(data)
                        
                        except Exception as exc:
                            log.error("StreamingCloudReader: failed to parse Kinesis record — %s", exc)
                            continue
                
                except Exception as exc:
                    log.error("StreamingCloudReader: failed to read from shard %s — %s", shard_id, exc)
                    continue
            
            if batch:
                log.info("StreamingCloudReader: yielding batch of %d Kinesis records", len(batch))
                yield batch
            else:
                log.debug("StreamingCloudReader: no records from Kinesis")
                yield []
    
    def _read_eventhub(self, max_messages: int) -> Generator:
        """Read from Azure Event Hub."""
        log.info("StreamingCloudReader: starting Event Hub consumer (max_messages=%d)", max_messages)
        
        def on_event(partition_context, event):
            """Callback for each event received."""
            if not hasattr(self, "_event_batch"):
                self._event_batch = []
            
            try:
                try:
                    data = json.loads(event.body_as_str())
                except (json.JSONDecodeError, ValueError):
                    data = {"raw_data": event.body_as_str()}
                
                # Add Event Hub metadata
                data["_eventhub_partition_key"] = event.partition_key
                data["_eventhub_sequence_number"] = event.sequence_number
                data["_eventhub_offset"] = event.offset
                
                self._event_batch.append(data)
            
            except Exception as exc:
                log.error("StreamingCloudReader: failed to parse Event Hub event — %s", exc)
        
        def on_error(partition_context, error):
            """Callback for errors."""
            log.error("StreamingCloudReader: Event Hub error — %s", error)
        
        # Start receiving events
        self._event_batch = []
        
        with self._client:
            self._client.receive(on_event=on_event, on_error=on_error, starting_position="-1")
            
            # Yield batches
            while True:
                if self._event_batch:
                    batch = self._event_batch[:max_messages]
                    self._event_batch = self._event_batch[max_messages:]
                    log.info("StreamingCloudReader: yielding batch of %d Event Hub events", len(batch))
                    yield batch
                else:
                    yield []

    def close(self):
        """Close cloud messaging connection."""
        if self._connected:
            try:
                if self._provider == "pubsub":
                    self._client.close()
                elif self._provider == "kinesis":
                    # boto3 client doesn't need explicit close
                    pass
                elif self._provider == "eventhub":
                    self._client.close()
                
                log.info("StreamingCloudReader: %s client closed", self._provider)
            except Exception as exc:
                log.error("StreamingCloudReader: error closing client — %s", exc)
        
        self._connected = False
