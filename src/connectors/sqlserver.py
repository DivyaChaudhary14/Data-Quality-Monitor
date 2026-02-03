"""
SQL Server database connector using pyodbc.

Supports both Windows Authentication (trusted connection) and
SQL Server Authentication (username/password).
"""

import pyodbc
from typing import Any

from src.connectors.base import BaseConnector, ConnectionError, QueryError


class SQLServerConnector(BaseConnector):
    """
    Connector for Microsoft SQL Server databases.
    
    Configuration options:
        host: Server hostname or IP address
        database: Database name
        port: Port number (default: 1433)
        trusted_connection: Use Windows Authentication (default: False)
        username: SQL Server login username
        password: SQL Server login password
        driver: ODBC driver name (default: auto-detect)
        timeout: Connection timeout in seconds (default: 30)
        
    Example configurations:
    
        # Windows Authentication
        sqlserver_prod:
          type: sqlserver
          host: server.domain.com
          database: production
          trusted_connection: true
          
        # SQL Authentication
        sqlserver_dev:
          type: sqlserver
          host: localhost
          database: development
          username: app_user
          password: ${DB_PASSWORD}
    """
    
    connector_type = "sqlserver"
    
    # Common ODBC drivers in preference order
    DRIVERS = [
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 17 for SQL Server',
        'SQL Server Native Client 11.0',
        'SQL Server',
    ]
    
    def __init__(self, config: dict):
        """Initialize SQL Server connector."""
        super().__init__(config)
        self._cursor = None
    
    def connect(self) -> None:
        """Establish connection to SQL Server."""
        try:
            connection_string = self._build_connection_string()
            self._connection = pyodbc.connect(connection_string)
            self._cursor = self._connection.cursor()
            self._connected = True
        except pyodbc.Error as e:
            raise ConnectionError(f"Failed to connect to SQL Server: {e}")
    
    def disconnect(self) -> None:
        """Close SQL Server connection."""
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
        
        Args:
            query: SQL query string
            params: Optional named parameters (not commonly used with pyodbc)
            
        Returns:
            List of row dictionaries
        """
        if not self._connected:
            self.connect()
            
        try:
            if params:
                # Convert named params to positional for pyodbc
                self._cursor.execute(query, list(params.values()))
            else:
                self._cursor.execute(query)
            
            # Check if query returns results
            if self._cursor.description:
                rows = self._cursor.fetchall()
                return self._rows_to_dicts(self._cursor, rows)
            else:
                return []
                
        except pyodbc.Error as e:
            raise QueryError(f"Query execution failed: {e}\nQuery: {query[:500]}")
    
    def test_connection(self) -> bool:
        """Test if connection is working."""
        try:
            if not self._connected:
                self.connect()
            self._cursor.execute("SELECT 1 AS test")
            result = self._cursor.fetchone()
            return result is not None and result[0] == 1
        except Exception:
            return False
    
    def _build_connection_string(self) -> str:
        """Build ODBC connection string from config."""
        host = self.config.get('host', 'localhost')
        database = self.config.get('database', '')
        port = self.config.get('port', 1433)
        trusted = self.config.get('trusted_connection', False)
        timeout = self.config.get('timeout', 30)
        
        # Find available driver
        driver = self.config.get('driver') or self._detect_driver()
        
        # Build base connection string
        parts = [
            f"DRIVER={{{driver}}}",
            f"SERVER={host},{port}",
            f"DATABASE={database}",
            f"Connection Timeout={timeout}",
        ]
        
        if trusted:
            parts.append("Trusted_Connection=yes")
        else:
            username = self.config.get('username', '')
            password = self.config.get('password', '')
            parts.append(f"UID={username}")
            parts.append(f"PWD={password}")
        
        # Handle encryption settings for newer drivers
        if 'ODBC Driver 18' in driver:
            # Driver 18 requires explicit encryption settings
            encrypt = self.config.get('encrypt', 'yes')
            trust_cert = self.config.get('trust_server_certificate', 'yes')
            parts.append(f"Encrypt={encrypt}")
            parts.append(f"TrustServerCertificate={trust_cert}")
        
        return ';'.join(parts)
    
    def _detect_driver(self) -> str:
        """Detect available ODBC driver."""
        available_drivers = pyodbc.drivers()
        
        for driver in self.DRIVERS:
            if driver in available_drivers:
                return driver
        
        # If no known driver found, try first available SQL Server driver
        for driver in available_drivers:
            if 'sql server' in driver.lower():
                return driver
        
        raise ConnectionError(
            f"No SQL Server ODBC driver found. Available drivers: {available_drivers}"
        )
    
    def quote_identifier(self, name: str) -> str:
        """Quote identifier using SQL Server brackets."""
        clean_name = name.strip('[]"\'`')
        return f'[{clean_name}]'
    
    def get_tables(self) -> list[str]:
        """Get list of tables in the database."""
        query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """
        results = self.execute_query(query)
        return [r['TABLE_NAME'] for r in results]
    
    def get_columns(self, table: str) -> list[dict]:
        """Get column information for a table."""
        query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        self._cursor.execute(query, [table])
        rows = self._cursor.fetchall()
        return self._rows_to_dicts(self._cursor, rows)
