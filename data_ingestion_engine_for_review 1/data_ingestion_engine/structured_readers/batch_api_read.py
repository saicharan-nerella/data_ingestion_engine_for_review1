"""
structured_readers/batch_api_read.py
──────────────────────────────────────
Batch reader for REST API sources.

Reads one or more API endpoints defined under config["structured"]["batch"]["apis"],
handles pagination, authentication, incremental loads, and checkpointing.
Returns raw data records (no transformation).

Transformation happens at the architecture layer.
Writing is handled by the user-selected writer component.

Registered in core/registry.py as:
  "reader": { "batch_api": "structured_readers.batch_api_read.BatchAPIReader" }
"""

import time
import logging
import os
from typing import Generator, List, Dict, Any

import requests
from structured_readers.base_reader import BaseReader

log = logging.getLogger(__name__)


class BatchAPIReader(BaseReader):
    """
    Reads data from REST API endpoints defined in config["structured"]["batch"]["apis"].

    Responsibility: READ ONLY
    - Fetches raw data from configured API endpoints
    - Handles pagination, authentication, and incremental loads
    - Returns raw records without transformation
    - Transformation is delegated to the architecture layer
    - Writing is delegated to the user-selected writer

    Supports:
      - GET and POST methods
      - Auth: none, basic, bearer, api_key, oauth2
      - Pagination: none, page_number, cursor, offset
      - Incremental loads via incremental_param + checkpoint
      - Retry with exponential backoff on transient failures
    """

    def __init__(self, config: dict):
        super().__init__(config)

    # ── BaseReader contract ───────────────────────────────────────────────────

    def connect(self) -> None:
        """No persistent connection needed for HTTP APIs."""
        pass

    def read(self) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Yield raw record lists from each enabled API endpoint.
        Each yield is a list of dictionaries (raw API response records).
        No transformation is applied here.
        """
        apis = self.config.get("structured", {}).get("batch", {}).get("apis", [])
        if not apis:
            raise ValueError("No API config found under structured -> batch -> apis")

        for api in apis:
            if not api.get("enabled", False):
                log.info("BatchAPIReader: skipping disabled API '%s'", api.get("name"))
                continue

            log.info("BatchAPIReader: reading API '%s'", api.get("name"))
            records = self._read_single_api(api)
            if records:
                # Tag each record with the API source name for downstream tracing
                api_name = api.get("name", "api")
                for rec in records:
                    rec["_api_name"] = api_name
                yield records

    def close(self) -> None:
        """No persistent connection to close."""
        pass

    # ── Single API reader ─────────────────────────────────────────────────────

    def _read_single_api(self, api_config: dict) -> List[Dict[str, Any]]:
        """
        Fetch all pages from a single API endpoint and return raw records.

        Args:
            api_config: Configuration dict for a single API endpoint

        Returns:
            List of raw record dictionaries from the API, or empty list on failure
        """
        source     = api_config.get("source", {})
        load_cfg   = api_config.get("load", {})
        url        = source.get("url")
        method     = api_config.get("method", "GET").upper()
        headers    = dict(source.get("headers") or {})
        pagination = source.get("pagination", {})
        auth_cfg   = source.get("auth", {})

        if not url:
            log.error("BatchAPIReader: no URL configured for API '%s'", api_config.get("name"))
            return []

        # ── Build auth headers ────────────────────────────────────────────────
        headers = self._build_auth_headers(headers, auth_cfg)

        # ── Incremental load — add since/cursor param ─────────────────────────
        base_params = {}
        incremental_param = load_cfg.get("incremental_param")
        if load_cfg.get("mode") == "incremental" and incremental_param:
            checkpoint_value = self._read_checkpoint(api_config)
            if checkpoint_value:
                base_params[incremental_param] = checkpoint_value
                log.info("BatchAPIReader: incremental load from checkpoint '%s'", checkpoint_value)

        # ── Paginate and collect all records ──────────────────────────────────
        all_data   = []
        page_type  = pagination.get("type", "none")
        page_param = pagination.get("page_param")
        size_param = pagination.get("size_param")
        page_size  = pagination.get("size")
        page_num   = 1
        cursor     = None
        offset     = 0

        while True:
            params = dict(base_params)

            # Build pagination params based on type
            if page_type == "page_number" and page_param:
                params[page_param] = page_num
                if size_param and page_size:
                    params[size_param] = page_size

            elif page_type == "offset":
                params[page_param or "offset"] = offset
                if size_param and page_size:
                    params[size_param] = page_size

            elif page_type == "cursor" and cursor:
                params[page_param or "cursor"] = cursor

            # ── Make HTTP request with retry ──────────────────────────────────
            data = self._request_with_retry(method, url, headers, params, api_config)
            if data is None:
                break

            records = self._extract_records(data)
            if not records:
                log.info("BatchAPIReader: no more records on page %d", page_num)
                break

            all_data.extend(records)
            log.info("BatchAPIReader: page %d — fetched %d records (total: %d)",
                     page_num, len(records), len(all_data))

            # ── Advance pagination ────────────────────────────────────────────
            if page_type == "none":
                break
            elif page_type == "page_number":
                if page_size and len(records) < page_size:
                    break   # last page
                page_num += 1
            elif page_type == "offset":
                offset += len(records)
                if page_size and len(records) < page_size:
                    break
            elif page_type == "cursor":
                cursor = self._extract_cursor(data)
                if not cursor:
                    break
            else:
                break

        if not all_data:
            log.warning("BatchAPIReader: API '%s' returned no records", api_config.get("name"))
            return []

        # ── Update checkpoint ─────────────────────────────────────────────────
        if load_cfg.get("mode") == "incremental" and incremental_param and all_data:
            self._write_checkpoint(api_config, all_data)

        log.info("BatchAPIReader: API '%s' — total %d records", api_config.get("name"), len(all_data))
        return all_data

    # ── Auth helpers ──────────────────────────────────────────────────────────

    def _build_auth_headers(self, headers: dict, auth_cfg: dict) -> dict:
        """Build HTTP headers for the configured auth type."""
        auth_type = (auth_cfg.get("type") or "none").lower()

        if auth_type == "bearer":
            token = auth_cfg.get("token") or os.getenv("API_BEARER_TOKEN", "")
            headers["Authorization"] = f"Bearer {token}"

        elif auth_type == "basic":
            import base64
            username = auth_cfg.get("username", "")
            password = auth_cfg.get("password", "")
            creds    = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {creds}"

        elif auth_type == "api_key":
            key_name  = auth_cfg.get("key_name", "X-API-Key")
            key_value = auth_cfg.get("key_value") or os.getenv("API_KEY", "")
            headers[key_name] = key_value

        elif auth_type == "oauth2":
            token = self._get_oauth2_token(auth_cfg)
            if token:
                headers["Authorization"] = f"Bearer {token}"

        return headers

    def _get_oauth2_token(self, auth_cfg: dict) -> str:
        """Fetch OAuth2 access token via client credentials flow."""
        try:
            resp = requests.post(
                auth_cfg.get("token_url", ""),
                data={
                    "grant_type":    "client_credentials",
                    "client_id":     auth_cfg.get("client_id", ""),
                    "client_secret": auth_cfg.get("client_secret", ""),
                    "scope":         auth_cfg.get("scope", ""),
                },
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json().get("access_token", "")
        except Exception as exc:
            log.error("BatchAPIReader: OAuth2 token fetch failed: %s", exc)
            return ""

    # ── HTTP request with retry ───────────────────────────────────────────────

    def _request_with_retry(self, method: str, url: str, headers: dict,
                            params: dict, api_config: dict) -> Dict[str, Any]:
        """
        Make an HTTP request with exponential backoff retry.
        Returns parsed JSON or None on failure.
        """
        max_retries = self.config.get("settings", {}).get("retries", 2)
        retry_delay = self.config.get("settings", {}).get("retry_delay_seconds", 60)

        for attempt in range(max_retries + 1):
            try:
                if method == "POST":
                    body = api_config.get("source", {}).get("body") or {}
                    resp = requests.post(url, headers=headers, params=params,
                                         json=body, timeout=30)
                else:
                    resp = requests.get(url, headers=headers, params=params, timeout=30)

                resp.raise_for_status()
                return resp.json()

            except requests.exceptions.HTTPError as exc:
                # Don't retry on 4xx client errors — only on 5xx server errors
                if exc.response is not None and exc.response.status_code < 500:
                    log.error("BatchAPIReader: HTTP %d — not retrying: %s",
                              exc.response.status_code, url)
                    return None
                log.warning("BatchAPIReader: HTTP error attempt %d/%d: %s",
                            attempt + 1, max_retries + 1, exc)

            except requests.exceptions.RequestException as exc:
                log.warning("BatchAPIReader: request error attempt %d/%d: %s",
                            attempt + 1, max_retries + 1, exc)

            if attempt < max_retries:
                wait = retry_delay * (2 ** attempt)   # exponential backoff
                log.info("BatchAPIReader: retrying in %ds", wait)
                time.sleep(wait)

        log.error("BatchAPIReader: all %d attempts failed for %s", max_retries + 1, url)
        return None

    # ── Record extraction ─────────────────────────────────────────────────────

    def _extract_records(self, data) -> List[Dict[str, Any]]:
        """
        Extract a list of records from the API response.
        Handles common response envelope patterns.
        """
        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            # Try common envelope keys in order of prevalence
            for key in ("data", "results", "items", "records", "content", "value"):
                if key in data and isinstance(data[key], list):
                    return data[key]

            # If no list found, wrap the dict as a single record
            log.debug("BatchAPIReader: no list key found in response — wrapping as single record")
            return [data]

        return []

    def _extract_cursor(self, data) -> str:
        """Extract next cursor from paginated response."""
        if isinstance(data, dict):
            for key in ("next_cursor", "cursor", "next_page_token", "nextPageToken"):
                if data.get(key):
                    return str(data[key])
            # Handle nested pagination metadata
            meta = data.get("meta") or data.get("pagination") or {}
            if isinstance(meta, dict):
                for key in ("next_cursor", "cursor", "next"):
                    if meta.get(key):
                        return str(meta[key])
        return ""

    # ── Checkpoint helpers ────────────────────────────────────────────────────

    def _read_checkpoint(self, api_config: dict) -> str:
        """Read the last checkpoint value for incremental loads."""
        checkpoint_cfg = api_config.get("load", {}).get("checkpoint", {})
        if not checkpoint_cfg:
            return ""

        checkpoint_type = checkpoint_cfg.get("type")
        checkpoint_path = checkpoint_cfg.get("path")

        if not checkpoint_type or not checkpoint_path:
            return ""

        try:
            if checkpoint_type == "s3":
                import boto3
                bucket, key = checkpoint_path[len("s3://"):].split("/", 1)
                obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
                return obj["Body"].read().decode("utf-8").strip()

            elif checkpoint_type == "local":
                if os.path.exists(checkpoint_path):
                    with open(checkpoint_path) as f:
                        return f.read().strip()

        except Exception as exc:
            log.warning("BatchAPIReader: could not read checkpoint: %s", exc)

        return ""

    def _write_checkpoint(self, api_config: dict, records: List[Dict[str, Any]]) -> None:
        """Write checkpoint after successful incremental fetch."""
        checkpoint_cfg    = api_config.get("load", {}).get("checkpoint", {})
        incremental_param = api_config.get("load", {}).get("incremental_param")

        if not checkpoint_cfg or not incremental_param:
            return

        checkpoint_type = checkpoint_cfg.get("type")
        checkpoint_path = checkpoint_cfg.get("path")

        if not checkpoint_type or not checkpoint_path:
            return

        # Use the incremental_param value from the last record as the new checkpoint
        last_record = records[-1] if records else {}
        new_value   = str(last_record.get(incremental_param, ""))

        if not new_value:
            return

        try:
            if checkpoint_type == "s3":
                import boto3
                bucket, key = checkpoint_path[len("s3://"):].split("/", 1)
                boto3.client("s3").put_object(
                    Bucket=bucket, Key=key,
                    Body=new_value.encode("utf-8")
                )
                log.info("BatchAPIReader: checkpoint updated → %s", new_value)

            elif checkpoint_type == "local":
                os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
                with open(checkpoint_path, "w") as f:
                    f.write(new_value)
                log.info("BatchAPIReader: checkpoint updated → %s", new_value)

        except Exception as exc:
            log.warning("BatchAPIReader: could not write checkpoint: %s", exc)
