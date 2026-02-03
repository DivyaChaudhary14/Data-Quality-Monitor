"""
SQLite database connector for testing and lightweight deployments.

SQLite is useful for local development, testing, and demos without
requiring a full database server setup.
"""

import sqlite3
from pathlib import Path
from typing import Any

from src.connectors.base import BaseConnector, ConnectionError, QueryError


class SQLiteConnector(BaseConnector):
    """
    Connector for SQLite databases.
    
    Configuration options:
        path: Path to SQLite database file
        timeout: Connection timeout in seconds (default: 30)
        
    Example configuration:
    
        local_db:
          type: sqlite
          path: ./data/test.db
          
        memory_db:
          type: sqlite
          path: ":memory:"
    """
    
    connector_type = "sqlite"
    
    def __init__(self, config: dict):
        """Initialize SQLite connector."""
        super().__init__(config)
        self._cursor = None
    
    def connect(self) -> None:
        """Establish connection to SQLite database."""
        try:
            db_path = self.config.get('path', ':memory:')
            timeout = self.config.get('timeout', 30)
            
            # Create parent directories if needed (unless in-memory)
            if db_path != ':memory:':
                path = Path(db_path)
                path.parent.mkdir(parents=True, exist_ok=True)
            
            self._connection = sqlite3.connect(db_path, timeout=timeout)
            
            # Enable foreign keys
            self._connection.execute("PRAGMA foreign_keys = ON")
            
            # Return rows as sqlite3.Row for dict-like access
            self._connection.row_factory = sqlite3.Row
            
            self._cursor = self._connection.cursor()
            self._connected = True
            
        except sqlite3.Error as e:
            raise ConnectionError(f"Failed to connect to SQLite: {e}")
    
    def disconnect(self) -> None:
        """Close SQLite connection."""
        if self._cursor:
            try:
                self._cursor.close()
            except Exception:
                pass
            self._cursor = None
            
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None
            
        self._connected = False
    
    def execute_query(self, query: str, params: dict | None = None) -> list[dict]:
        """
        Execute SQL query and return results.
        
        Note: SQLite has limited SQL syntax compared to SQL Server.
        Some queries may need adjustment.
        """
        if not self._connected:
            self.connect()
        
        # Adapt SQL Server syntax to SQLite
        adapted_query = self._adapt_query(query)
        
        try:
            if params:
                self._cursor.execute(adapted_query, params)
            else:
                self._cursor.execute(adapted_query)
            
            rows = self._cursor.fetchall()
            return [dict(row) for row in rows]
            
        except sqlite3.Error as e:
            raise QueryError(f"Query execution failed: {e}\nQuery: {adapted_query[:500]}")
    
    def test_connection(self) -> bool:
        """Test if connection is working."""
        try:
            if not self._connected:
                self.connect()
            self._cursor.execute("SELECT 1 AS test")
            result = self._cursor.fetchone()
            return result is not None and result['test'] == 1
        except Exception:
            return False
    
    def quote_identifier(self, name: str) -> str:
        """Quote identifier using SQLite double quotes."""
        clean_name = name.strip('[]"\'`')
        return f'"{clean_name}"'
    
    def _adapt_query(self, query: str) -> str:
        """
        Adapt SQL Server syntax to SQLite.
        
        Handles common differences like:
        - [brackets] -> "quotes"
        - GETDATE() -> datetime('now')
        - TOP N -> LIMIT N
        - OFFSET/FETCH -> LIMIT/OFFSET
        """
        adapted = query
        
        # Replace bracket quoting with double quotes
        import re
        adapted = re.sub(r'\[([^\]]+)\]', r'"\1"', adapted)
        
        # Replace GETDATE() with SQLite equivalent
        adapted = adapted.replace('GETDATE()', "datetime('now')")
        adapted = adapted.replace('getdate()', "datetime('now')")
        
        # Replace DATEADD
        # DATEADD(month, -6, GETDATE()) -> datetime('now', '-6 months')
        dateadd_pattern = r"DATEADD\s*\(\s*(\w+)\s*,\s*(-?\d+)\s*,\s*(?:GETDATE\(\)|datetime\('now'\))\s*\)"
        def dateadd_replace(match):
            unit = match.group(1).lower()
            amount = match.group(2)
            # Map SQL Server units to SQLite
            unit_map = {'month': 'months', 'day': 'days', 'year': 'years', 'hour': 'hours'}
            sqlite_unit = unit_map.get(unit, unit + 's')
            return f"datetime('now', '{amount} {sqlite_unit}')"
        adapted = re.sub(dateadd_pattern, dateadd_replace, adapted, flags=re.IGNORECASE)
        
        # Handle TOP N (simple cases)
        top_pattern = r'SELECT\s+TOP\s+(\d+)\s+'
        def top_replace(match):
            return f'SELECT '
        top_match = re.search(top_pattern, adapted, re.IGNORECASE)
        if top_match:
            limit_val = top_match.group(1)
            adapted = re.sub(top_pattern, 'SELECT ', adapted, flags=re.IGNORECASE)
            # Add LIMIT at end if not already present
            if 'LIMIT' not in adapted.upper():
                adapted = adapted.rstrip(';') + f' LIMIT {limit_val}'
        
        # Handle OFFSET FETCH (convert to LIMIT OFFSET)
        offset_fetch_pattern = r'OFFSET\s+(\d+)\s+ROWS\s+FETCH\s+(?:NEXT|FIRST)\s+(\d+)\s+ROWS\s+ONLY'
        def offset_fetch_replace(match):
            offset = match.group(1)
            limit = match.group(2)
            return f'LIMIT {limit} OFFSET {offset}'
        adapted = re.sub(offset_fetch_pattern, offset_fetch_replace, adapted, flags=re.IGNORECASE)
        
        # Replace CONCAT_WS (SQLite doesn't have it directly)
        # This is a simplified replacement
        concat_ws_pattern = r"CONCAT_WS\s*\(\s*'([^']+)'\s*,\s*(.+?)\)"
        # For now, we'll skip this complex replacement and let it fail gracefully
        
        # Replace ISNULL with COALESCE
        adapted = re.sub(r'\bISNULL\s*\(', 'COALESCE(', adapted, flags=re.IGNORECASE)
        
        return adapted
    
    def get_tables(self) -> list[str]:
        """Get list of tables in the database."""
        query = """
            SELECT name 
            FROM sqlite_master 
            WHERE type = 'table' 
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """
        results = self.execute_query(query)
        return [r['name'] for r in results]
    
    def get_columns(self, table: str) -> list[dict]:
        """Get column information for a table."""
        query = f"PRAGMA table_info({self.quote_identifier(table)})"
        results = self.execute_query(query)
        return [
            {
                'COLUMN_NAME': r['name'],
                'DATA_TYPE': r['type'],
                'IS_NULLABLE': 'YES' if r['notnull'] == 0 else 'NO',
            }
            for r in results
        ]
