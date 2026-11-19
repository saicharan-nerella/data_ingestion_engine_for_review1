"""
structured_readers/streaming_socket_read.py
────────────────────────────────────
Raw TCP or UDP socket reader. Useful for IoT, log shippers, custom telemetry.

Config: config.yaml under structured.streaming.socket —
  host, port, protocol (tcp|udp), buffer_bytes (default 4096)

Registered in core/registry.py as:
  "reader": { "streaming_socket": "structured_readers.streaming_socket_read.StreamingSocketReader" }
"""
from structured_readers.base_reader import BaseReader
from typing import Generator
import logging
import socket
import json
import threading

log = logging.getLogger(__name__)


class StreamingSocketReader(BaseReader):
    """
    Raw TCP/UDP socket reader that accepts incoming connections
    and yields data as micro-batches.
    """

    def connect(self):
        """Create and bind socket."""
        self._cfg = self.config.get("structured", {}).get("streaming", {}).get("socket", {})
        if not self._cfg.get("enabled", True):
            raise ValueError("Socket streaming configuration is disabled (structured.streaming.socket.enabled: false)")

        host = self._cfg.get("host", "0.0.0.0")
        port = self._cfg.get("port")
        protocol = self._cfg.get("protocol", "tcp").lower()

        if not port:
            raise ValueError("port is required")
        
        if protocol not in ("tcp", "udp"):
            raise ValueError(f"Unsupported protocol: {protocol}. Supported: tcp, udp")
        
        # Create socket
        if protocol == "tcp":
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._sock.bind((host, port))
            self._sock.listen(5)
            self._protocol = "tcp"
            log.info("StreamingSocketReader: TCP socket listening → %s:%d", host, port)
        
        else:  # UDP
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._sock.bind((host, port))
            self._protocol = "udp"
            log.info("StreamingSocketReader: UDP socket listening → %s:%d", host, port)
        
        self._host = host
        self._port = port
        self._buffer_bytes = self._cfg.get("buffer_bytes", 4096)
        self._connected = True

    def read(self) -> Generator:
        """
        Read from socket and yield micro-batches.
        For TCP: accepts connections and reads from each client.
        For UDP: reads datagrams.
        """
        if not self._connected:
            raise RuntimeError("Reader not connected. Call connect() first.")
        
        log.info("StreamingSocketReader: starting to read from socket")
        
        if self._protocol == "tcp":
            yield from self._read_tcp()
        else:  # UDP
            yield from self._read_udp()
    
    def _read_tcp(self) -> Generator:
        """Read from TCP socket."""
        batch_size = self._cfg.get("batch_size", 100)
        
        while True:
            batch = []
            
            try:
                # Accept incoming connection
                conn, addr = self._sock.accept()
                log.info("StreamingSocketReader: TCP connection from %s:%d", addr[0], addr[1])
                
                # Read data from connection
                while len(batch) < batch_size:
                    try:
                        # Set timeout to avoid blocking forever
                        conn.settimeout(1.0)
                        data = conn.recv(self._buffer_bytes)
                        
                        if not data:
                            # Connection closed
                            log.debug("StreamingSocketReader: TCP connection closed by client")
                            break
                        
                        # Parse data
                        try:
                            # Try to parse as JSON
                            try:
                                record = json.loads(data.decode("utf-8"))
                            except (json.JSONDecodeError, UnicodeDecodeError):
                                # Fall back to raw string
                                record = {"raw_data": data.decode("utf-8", errors="replace")}
                            
                            # Add socket metadata
                            record["_socket_protocol"] = "tcp"
                            record["_socket_source_ip"] = addr[0]
                            record["_socket_source_port"] = addr[1]
                            
                            batch.append(record)
                        
                        except Exception as exc:
                            log.error("StreamingSocketReader: failed to parse TCP data — %s", exc)
                            continue
                    
                    except socket.timeout:
                        # Timeout waiting for data, close connection
                        log.debug("StreamingSocketReader: TCP read timeout")
                        break
                    
                    except Exception as exc:
                        log.error("StreamingSocketReader: TCP read error — %s", exc)
                        break
                
                # Close connection
                try:
                    conn.close()
                except Exception:
                    pass
                
                if batch:
                    log.info("StreamingSocketReader: yielding batch of %d TCP records", len(batch))
                    yield batch
                else:
                    yield []
            
            except Exception as exc:
                log.error("StreamingSocketReader: TCP accept error — %s", exc)
                yield []
    
    def _read_udp(self) -> Generator:
        """Read from UDP socket."""
        batch_size = self._cfg.get("batch_size", 100)
        
        log.info("StreamingSocketReader: UDP socket ready (batch_size=%d)", batch_size)
        
        while True:
            batch = []
            
            try:
                # Receive datagrams
                while len(batch) < batch_size:
                    try:
                        # Set timeout to avoid blocking forever
                        self._sock.settimeout(1.0)
                        data, addr = self._sock.recvfrom(self._buffer_bytes)
                        
                        # Parse data
                        try:
                            # Try to parse as JSON
                            try:
                                record = json.loads(data.decode("utf-8"))
                            except (json.JSONDecodeError, UnicodeDecodeError):
                                # Fall back to raw string
                                record = {"raw_data": data.decode("utf-8", errors="replace")}
                            
                            # Add socket metadata
                            record["_socket_protocol"] = "udp"
                            record["_socket_source_ip"] = addr[0]
                            record["_socket_source_port"] = addr[1]
                            
                            batch.append(record)
                        
                        except Exception as exc:
                            log.error("StreamingSocketReader: failed to parse UDP data — %s", exc)
                            continue
                    
                    except socket.timeout:
                        # Timeout waiting for data
                        log.debug("StreamingSocketReader: UDP receive timeout")
                        break
                    
                    except Exception as exc:
                        log.error("StreamingSocketReader: UDP receive error — %s", exc)
                        break
                
                if batch:
                    log.info("StreamingSocketReader: yielding batch of %d UDP records", len(batch))
                    yield batch
                else:
                    yield []
            
            except Exception as exc:
                log.error("StreamingSocketReader: UDP error — %s", exc)
                yield []

    def close(self):
        """Close socket."""
        if self._connected and hasattr(self, "_sock"):
            try:
                self._sock.close()
                log.info("StreamingSocketReader: socket closed")
            except Exception as exc:
                log.error("StreamingSocketReader: error closing socket — %s", exc)
        
        self._connected = False
