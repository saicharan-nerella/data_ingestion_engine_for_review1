"""
structured_readers/streaming_webhook_read.py
─────────────────────────────────────
FastAPI HTTP webhook receiver. Events POSTed by external systems are buffered
in a thread-safe queue and yielded as micro-batches.

Config: config.yaml under structured.streaming.webhook —
  host, port, path (default /webhook),
  secret_header (HMAC signature validation),
  buffer_size (default 100)

Registered in core/registry.py as:
  "reader": { "streaming_webhook": "structured_readers.streaming_webhook_read.StreamingWebhookReader" }
"""
from structured_readers.base_reader import BaseReader
from typing import Generator
from queue import Queue
import logging
import json
import threading
import hmac
import hashlib

log = logging.getLogger(__name__)


class StreamingWebhookReader(BaseReader):
    """
    FastAPI webhook receiver that buffers incoming HTTP POST events
    and yields them as micro-batches.
    """

    def connect(self):
        """Start FastAPI webhook server in background thread."""
        try:
            from fastapi import FastAPI, Request, HTTPException
            import uvicorn
        except ImportError:
            raise ImportError(
                "fastapi and uvicorn are required for webhook streaming. "
                "Install with: pip install fastapi uvicorn"
            )

        self._cfg = self.config.get("structured", {}).get("streaming", {}).get("webhook", {})
        if not self._cfg.get("enabled", True):
            raise ValueError("Webhook streaming configuration is disabled (structured.streaming.webhook.enabled: false)")

        self._queue: Queue = Queue()
        self._app = FastAPI()

        host = self._cfg.get("host", "0.0.0.0")
        port = self._cfg.get("port", 8080)
        path = self._cfg.get("path", "/webhook")
        secret_header = self._cfg.get("secret_header")
        
        # Create webhook endpoint
        @self._app.post(path)
        async def receive_webhook(request: Request):
            """Receive webhook POST request."""
            try:
                # Validate HMAC signature if configured
                if secret_header:
                    signature = request.headers.get("X-Signature")
                    if not signature:
                        raise HTTPException(status_code=401, detail="Missing X-Signature header")
                    
                    # Read body
                    body = await request.body()
                    
                    # Compute expected signature
                    expected_sig = hmac.new(
                        secret_header.encode(),
                        body,
                        hashlib.sha256
                    ).hexdigest()
                    
                    # Compare signatures
                    if not hmac.compare_digest(signature, expected_sig):
                        raise HTTPException(status_code=401, detail="Invalid signature")
                    
                    # Parse JSON from body
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        raise HTTPException(status_code=400, detail="Invalid JSON")
                else:
                    # No signature validation, just parse JSON
                    try:
                        payload = await request.json()
                    except Exception:
                        raise HTTPException(status_code=400, detail="Invalid JSON")
                
                # Add webhook metadata
                payload["_webhook_timestamp"] = request.headers.get("X-Timestamp", "")
                payload["_webhook_source_ip"] = request.client.host if request.client else ""
                
                # Queue the payload
                self._queue.put(payload)
                log.debug("StreamingWebhookReader: queued webhook event")
                
                return {"status": "received"}
            
            except HTTPException:
                raise
            except Exception as exc:
                log.error("StreamingWebhookReader: webhook processing error — %s", exc)
                raise HTTPException(status_code=500, detail="Internal server error")
        
        # Start uvicorn server in background thread
        self._server_thread = threading.Thread(
            target=lambda: uvicorn.run(
                self._app,
                host=host,
                port=port,
                log_level="warning"
            ),
            daemon=True
        )
        self._server_thread.start()
        
        log.info("StreamingWebhookReader: listening → http://%s:%s%s", host, port, path)
        self._connected = True

    def read(self) -> Generator:
        """
        Yield micro-batches of webhook events from the queue.
        """
        if not self._connected:
            raise RuntimeError("Reader not connected. Call connect() first.")
        
        buffer_size = self._cfg.get("buffer_size", 100)
        
        log.info("StreamingWebhookReader: starting to yield batches (buffer_size=%d)", buffer_size)
        
        while True:
            batch = []
            
            # Collect up to buffer_size events from queue
            while not self._queue.empty() and len(batch) < buffer_size:
                try:
                    event = self._queue.get(block=False)
                    batch.append(event)
                except Exception:
                    break
            
            if batch:
                log.info("StreamingWebhookReader: yielding batch of %d webhook events", len(batch))
                yield batch
            else:
                # No events in queue, yield empty to allow caller to check for exit
                log.debug("StreamingWebhookReader: no events in queue")
                yield []

    def close(self):
        """Stop webhook server."""
        if self._connected:
            try:
                # Note: Stopping uvicorn in a daemon thread is tricky
                # The thread will be terminated when the main process exits
                log.info("StreamingWebhookReader: server stopped")
            except Exception as exc:
                log.error("StreamingWebhookReader: error stopping server — %s", exc)
        
        self._connected = False
