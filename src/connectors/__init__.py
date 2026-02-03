"""
Connectors package - database connection implementations.

Provides connectors for different database types:
- SQL Server: Enterprise production databases
- PostgreSQL: Open source databases
- SQLite: Local development and testing
- CSV: Flat file data sources
"""

from src.connectors.base import BaseConnector, ConnectionError, QueryError
from src.connectors.sqlserver import SQLServerConnector
from src.connectors.sqlite import SQLiteConnector


# Registry mapping connection types to connector classes
CONNECTOR_REGISTRY: dict[str, type[BaseConnector]] = {
    'sqlserver': SQLServerConnector,
    'sqlite': SQLiteConnector,
}


def get_connector(connection_type: str) -> type[BaseConnector] | None:
    """
    Get connector class for a connection type.
    
    Args:
        connection_type: The type of database connection
        
    Returns:
        Connector class or None if not found
    """
    return CONNECTOR_REGISTRY.get(connection_type)


def create_connector(config: dict) -> BaseConnector:
    """
    Create a connector instance from configuration.
    
    Args:
        config: Connection configuration with 'type' field
        
    Returns:
        Initialized connector instance
        
    Raises:
        ValueError: If connection type is unknown
    """
    conn_type = config.get('type')
    if not conn_type:
        raise ValueError("Connection configuration must include 'type' field")
    
    connector_class = get_connector(conn_type)
    if not connector_class:
        available = list(CONNECTOR_REGISTRY.keys())
        raise ValueError(
            f"Unknown connection type: {conn_type}. Available types: {available}"
        )
    
    return connector_class(config)


def list_connector_types() -> list[str]:
    """Return list of available connector types."""
    return list(CONNECTOR_REGISTRY.keys())


__all__ = [
    'BaseConnector',
    'ConnectionError',
    'QueryError',
    'SQLServerConnector',
    'SQLiteConnector',
    'CONNECTOR_REGISTRY',
    'get_connector',
    'create_connector',
    'list_connector_types',
]
