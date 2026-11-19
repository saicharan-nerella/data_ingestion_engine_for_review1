"""
structured_readers/batch_database_read.py
──────────────────────────────────────────
Batch reader for relational databases.

Reads tables from databases defined under config["batch"]["databases"],
handles full and incremental loads, and returns raw data records.

Transformation happens at the architecture layer.
Writing is handled by the user-selected writer component.

Registered in core/registry.py as:
  "reader": { "batch_database": "structured_readers.batch_database_read.BatchDatabaseReader" }
"""

import logging
from typing import Generator, List, Dict, Any
import sqlalchemy as sa
from sqlalchemy import create_engine, text

from structured_readers.base_reader import BaseReader

log = logging.getLogger(__name__)


class BatchDatabaseReader(BaseReader):
    """
    Reads data from relational databases.

    Responsibility: READ ONLY
    - Connects to configured database
    - Reads tables in batches (memory-safe)
    - Supports full and incremental loads
    - Returns raw records without transformation
    - Transformation is delegated to the architecture layer
    - Writing is delegated to the user-selected writer

    Supports:
      - Databases: MySQL, PostgreSQL, MSSQL, Oracle, Snowflake
      - Modes: Full load, Incremental (watermark-based)
      - Batch processing: Configurable batch size
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self._engine = None
        self._connection = None
        self._connected = False

    # ── BaseReader contract ───────────────────────────────────────────────────

    def connect(self) -> None:
        """Initialize database connection."""
        db_cfg = self.config.get("structured", {}).get("batch", {}).get("databases", [])
        
        if not db_cfg:
            raise ValueError(
                "No database configuration found in config.yaml under structured.batch.databases[]"
            )
        
        db_cfg = db_cfg[0]  # Get first database config
        
        if not db_cfg.get("enabled", True):
            raise ValueError("Database configuration is disabled (enabled: false)")
        
        db_type = db_cfg.get("type", "mysql").lower()
        
        source = db_cfg.get("source", {})
        host = source.get("host")
        port = source.get("port", 3306)
        database = source.get("database")
        username = source.get("username")
        password = source.get("password")
        
        # Validate credentials are not placeholders
        if not all([host, database, username, password]):
            raise ValueError(
                "Missing required database credentials in config.yaml. "
                "Required fields: host, database, username, password"
            )
        
        # Check for placeholder values
        placeholder_patterns = ["YOUR_", "PLACEHOLDER", "EXAMPLE", "CHANGE_ME"]
        for field_name, field_value in [("host", host), ("username", username), ("password", password)]:
            if any(pattern in str(field_value).upper() for pattern in placeholder_patterns):
                raise ValueError(
                    f"Database {field_name} appears to be a placeholder value: '{field_value}'. "
                    f"Please update config.yaml with actual credentials."
                )
        
        # Build connection string based on database type
        if db_type == "mysql":
            connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        
        elif db_type == "postgres":
            connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
        elif db_type == "mssql":
            connection_string = f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        
        elif db_type == "oracle":
            # Check for required Oracle driver
            try:
                import cx_Oracle
            except ImportError:
                raise ImportError(
                    "cx_Oracle is required for Oracle databases. "
                    "Install with: pip install cx_Oracle\n"
                    "Or use oracledb: pip install oracledb"
                )
            connection_string = f"oracle+cx_oracle://{username}:{password}@{host}:{port}/{database}"
        
        elif db_type == "snowflake":
            account = source.get("account")
            warehouse = source.get("warehouse")
            if not account or not warehouse:
                raise ValueError("Snowflake requires 'account' and 'warehouse' fields in source config")
            connection_string = f"snowflake://{username}:{password}@{account}/{database}?warehouse={warehouse}"
        
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        try:
            self._engine = create_engine(connection_string, echo=False)
            self._connection = self._engine.connect()
            self._connected = True
            log.info("BatchDatabaseReader: connected to %s://%s:%s/%s",
                     db_type, host, port, database)
        
        except Exception as exc:
            log.error("BatchDatabaseReader: connection failed: %s", exc)
            raise RuntimeError(
                f"Failed to connect to {db_type} database at {host}:{port}/{database}. "
                f"Error: {exc}"
            ) from exc

    def read(self) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Yield raw record lists from each configured table.
        Each yield is a list of dictionaries (table rows).
        No transformation is applied here.
        """
        if not self._connected:
            self.connect()
        
        db_cfg = self.config.get("structured", {}).get("batch", {}).get("databases", [])
        if not db_cfg:
            log.warning("BatchDatabaseReader: no database configuration found")
            return
        
        db_cfg = db_cfg[0]
        source = db_cfg.get("source", {})
        tables = source.get("tables", [])
        batch_size = db_cfg.get("load", {}).get("batch_size", 50000)
        
        if not tables:
            log.warning("BatchDatabaseReader: no tables configured in source.tables[]")
            return
        
        log.info("BatchDatabaseReader: reading %d tables", len(tables))
        
        # Process each table
        for table_name in tables:
            try:
                log.info("BatchDatabaseReader: reading table %s", table_name)
                
                # Check if table exists before attempting to read
                if not self._table_exists(table_name):
                    log.error("BatchDatabaseReader: table %s does not exist or is not accessible", table_name)
                    continue
                
                records = self._read_table(table_name, batch_size, db_cfg)

                if records:
                    log.info("BatchDatabaseReader: %s → %d records", table_name, len(records))
                    # Tag each record with its source table so downstream layers
                    # (Bronze's _bronze_source, Silver's dedup key) can tell records
                    # from different tables apart — every table has its own
                    # auto-increment id starting at 1, so ids alone are not unique
                    # across tables when multiple tables are read in one job.
                    for rec in records:
                        rec["_source_table"] = table_name
                    yield records
                else:
                    log.warning("BatchDatabaseReader: %s returned 0 records", table_name)
            
            except Exception as exc:
                log.error("BatchDatabaseReader: failed to read table %s: %s", table_name, exc)
                continue

    def close(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
        if self._engine:
            self._engine.dispose()
        self._connected = False
        log.info("BatchDatabaseReader: connection closed")

    # ── Database helpers ──────────────────────────────────────────────────────

    def _table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists and is accessible.
        
        Args:
            table_name: Name of the table to check
        
        Returns:
            True if table exists and is accessible, False otherwise
        """
        try:
            # Use SQLAlchemy's dialect-agnostic inspector rather than raw SQL —
            # a raw existence-check query (e.g. Oracle's ROWNUM) only works on
            # one dialect and silently returns False (via the except below) on
            # every other supported database (MySQL, PostgreSQL, MSSQL, Snowflake).
            return sa.inspect(self._engine).has_table(table_name)
        except Exception as exc:
            log.debug("BatchDatabaseReader: table check failed for %s: %s", table_name, exc)
            return False

    def _read_table(self, table_name: str, batch_size: int, 
                    db_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Read all rows from a table in batches.
        
        Args:
            table_name: Name of the table to read
            batch_size: Number of rows per batch
            db_cfg: Database configuration
        
        Returns:
            List of all rows as dictionaries
        """
        load_cfg = db_cfg.get("load", {})
        incremental_column = load_cfg.get("incremental_column")
        last_watermark = load_cfg.get("last_watermark")
        
        # Build query
        if incremental_column and last_watermark:
            # Incremental load: only rows where incremental_column > last_watermark
            query = f"SELECT * FROM {table_name} WHERE {incremental_column} > :watermark ORDER BY {incremental_column}"
            log.info("BatchDatabaseReader: incremental load from %s", last_watermark)
        else:
            # Full load: all rows
            query = f"SELECT * FROM {table_name}"
            log.info("BatchDatabaseReader: full table load")
        
        # Execute query and fetch all rows
        try:
            if incremental_column and last_watermark:
                result = self._connection.execute(
                    text(query),
                    {"watermark": last_watermark}
                )
            else:
                result = self._connection.execute(text(query))
            
            # Convert rows to list of dicts
            rows = result.fetchall()
            records = [dict(row._mapping) for row in rows]
            
            log.info("BatchDatabaseReader: fetched %d rows from %s", len(records), table_name)
            return records
        
        except Exception as exc:
            log.error("BatchDatabaseReader: query failed for %s: %s", table_name, exc)
            raise
