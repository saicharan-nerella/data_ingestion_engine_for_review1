"""
structured_readers/streaming_kafka_read.py
──────────────────────────────────
Kafka consumer with manual offset commit (at-least-once semantics).

Config: config.yaml under structured.streaming.kafka —
  bootstrap_servers, topics (list), consumer_group,
  auto_offset_reset (earliest|latest),
  batch_size (default 500), poll_timeout_ms (default 1000)

Registered in core/registry.py as:
  "reader": { "streaming_kafka": "structured_readers.streaming_kafka_read.StreamingKafkaReader" }
"""
from structured_readers.base_reader import BaseReader
from typing import Generator
import logging
import json

log = logging.getLogger(__name__)


class StreamingKafkaReader(BaseReader):
    """
    Kafka consumer that yields micro-batches of messages.
    Uses manual offset commit for at-least-once semantics.
    """

    def connect(self):
        """Connect to Kafka cluster and subscribe to topics."""
        try:
            from confluent_kafka import Consumer
        except ImportError:
            raise ImportError(
                "confluent-kafka is required for Kafka streaming. "
                "Install with: pip install confluent-kafka"
            )

        self._cfg = self.config.get("structured", {}).get("streaming", {}).get("kafka", {})
        if not self._cfg.get("enabled", True):
            raise ValueError("Kafka streaming configuration is disabled (structured.streaming.kafka.enabled: false)")

        bootstrap_servers = self._cfg.get("bootstrap_servers")
        topics = self._cfg.get("topics", [])
        consumer_group = self._cfg.get("consumer_group")
        auto_offset_reset = self._cfg.get("auto_offset_reset", "earliest")
        
        if not bootstrap_servers:
            raise ValueError("bootstrap_servers is required")
        if not topics:
            raise ValueError("topics list is required")
        if not consumer_group:
            raise ValueError("consumer_group is required")
        
        # Create Kafka consumer with manual offset commit
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": consumer_group,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,  # Manual commit after confirmed write
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        })
        
        # Subscribe to topics
        self._consumer.subscribe(topics)
        
        log.info("StreamingKafkaReader: connected → bootstrap_servers=%s, topics=%s, group=%s",
                 bootstrap_servers, topics, consumer_group)
        self._connected = True

    def read(self) -> Generator:
        """
        Continuously poll Kafka; yield micro-batches.
        Caller commits offsets after every successful downstream write.
        """
        if not self._connected:
            raise RuntimeError("Reader not connected. Call connect() first.")
        
        batch_size = self._cfg.get("batch_size", 500)
        timeout_ms = self._cfg.get("poll_timeout_ms", 1000)
        timeout_s = timeout_ms / 1000.0
        
        log.info("StreamingKafkaReader: starting to poll (batch_size=%d, timeout=%dms)",
                 batch_size, timeout_ms)
        
        while True:
            batch = []
            
            # Poll for messages up to batch_size
            for _ in range(batch_size):
                msg = self._consumer.poll(timeout_s)
                
                if msg is None:
                    # Timeout reached, no more messages
                    break
                
                if msg.error():
                    # Handle Kafka errors
                    error_code = msg.error().code()
                    error_str = msg.error().str()
                    
                    # Ignore partition EOF errors (normal end of partition)
                    if error_code == -191:  # _PARTITION_EOF
                        log.debug("StreamingKafkaReader: reached end of partition")
                        break
                    
                    # Log other errors but continue
                    log.error("StreamingKafkaReader: Kafka error — %s (%s)", error_str, error_code)
                    continue
                
                # Parse message value
                try:
                    if msg.value() is None:
                        log.warning("StreamingKafkaReader: received null message value")
                        continue
                    
                    # Try to parse as JSON, fall back to string
                    try:
                        record = json.loads(msg.value().decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        record = {"raw_value": msg.value().decode("utf-8", errors="replace")}
                    
                    # Add Kafka metadata
                    record["_kafka_topic"] = msg.topic()
                    record["_kafka_partition"] = msg.partition()
                    record["_kafka_offset"] = msg.offset()
                    record["_kafka_timestamp"] = msg.timestamp()[1]
                    
                    batch.append(record)
                
                except Exception as exc:
                    log.error("StreamingKafkaReader: failed to parse message — %s", exc)
                    continue
            
            # Yield batch if not empty
            if batch:
                log.info("StreamingKafkaReader: yielding batch of %d messages", len(batch))
                yield batch
                
                # Commit offsets after successful write (caller should handle this)
                try:
                    self._consumer.commit(asynchronous=False)
                    log.debug("StreamingKafkaReader: offsets committed")
                except Exception as exc:
                    log.error("StreamingKafkaReader: failed to commit offsets — %s", exc)
            else:
                # No messages in this poll cycle, yield empty to allow caller to check for exit
                log.debug("StreamingKafkaReader: no messages in poll cycle")
                yield []

    def close(self):
        """Close Kafka consumer connection."""
        if self._connected and hasattr(self, "_consumer"):
            try:
                self._consumer.close()
                log.info("StreamingKafkaReader: consumer closed")
            except Exception as exc:
                log.error("StreamingKafkaReader: error closing consumer — %s", exc)
        
        self._connected = False
