"""
Base connector class defining the interface for database connections.

All database connectors inherit from BaseConnector and implement
the connection and query execution methods.
"""

from abc import ABC, abstractmethod
from typing import Any


class ConnectionError(Exception):
    """Raised when database connection fails."""
    pass


class QueryError(Exception):
    """Raised when query execution fails."""
    pass


class BaseConnector(ABC):
    """
    Abstract base class for database connectors.
    
    Subclasses must implement:
        - connect(): Establish database connection
        - disconnect(): Close database connection
        - execute_query(): Run SQL and return results
        - test_connection(): Verify connection is working
    """
    
    connector_type: str = "base"  # Override in subclasses
    
    def __init__(self, config: dict):
        """
        Initialize connector with configuration.
        
        Args:
            config: Connection configuration dictionary
        """
        self.config = config
        self._connection = None
        self._connected = False
    
    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to the database.
        
        Raises:
            ConnectionError: If connection fails
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: dict | None = None) -> list[dict]:
        """
        Execute a SQL query and return results as list of dictionaries.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            List of dictionaries, one per row
            
        Raises:
            QueryError: If query execution fails
        """
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """
        Test if the connection is working.
        
        Returns:
            True if connection is valid, False otherwise
        """
        pass
    
    def quote_identifier(self, name: str) -> str:
        """
        Quote a table or column name for safe use in SQL.
        
        Default implementation uses SQL Server style [brackets].
        Override in subclasses for database-specific quoting.
        
        Args:
            name: Table or column name
            
        Returns:
            Safely quoted identifier
        """
        # Remove any existing quotes and re-quote
        clean_name = name.strip('[]"\'`')
        return f'[{clean_name}]'
    
    @property
    def is_connected(self) -> bool:
        """Check if currently connected."""
        return self._connected
    
    def __enter__(self):
        """Context manager entry - establish connection."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.disconnect()
        return False  # Don't suppress exceptions
    
    def _rows_to_dicts(self, cursor, rows: list) -> list[dict]:
        """
        Convert database rows to list of dictionaries.
        
        Args:
            cursor: Database cursor with description
            rows: List of row tuples
            
        Returns:
            List of dictionaries with column names as keys
        """
        if not rows:
            return []
            
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in rows]
