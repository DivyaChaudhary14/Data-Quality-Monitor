"""
Configuration loader for data quality rules and database connections.

Handles YAML parsing, environment variable substitution, and validation.
"""

import os
import re
from pathlib import Path
from typing import Any

import yaml


class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing."""
    pass


class ConfigLoader:
    """Load and validate configuration from YAML files."""
    
    ENV_PATTERN = re.compile(r'\$\{([^}]+)\}')
    
    def __init__(self, config_path: str | Path, connections_path: str | Path | None = None):
        """
        Initialize configuration loader.
        
        Args:
            config_path: Path to rules configuration YAML
            connections_path: Path to connections YAML (optional, can be in config_path)
        """
        self.config_path = Path(config_path)
        self.connections_path = Path(connections_path) if connections_path else None
        
        self._config: dict = {}
        self._connections: dict = {}
        
    def load(self) -> dict:
        """Load and parse all configuration files."""
        self._config = self._load_yaml(self.config_path)
        
        # Load connections from separate file or embedded in config
        if self.connections_path:
            self._connections = self._load_yaml(self.connections_path)
        elif 'connections' in self._config:
            self._connections = {'connections': self._config.pop('connections')}
        else:
            # Look for connections.yaml in same directory
            default_conn_path = self.config_path.parent / 'connections.yaml'
            if default_conn_path.exists():
                self._connections = self._load_yaml(default_conn_path)
                
        self._validate_config()
        return self.get_full_config()
    
    def _load_yaml(self, path: Path) -> dict:
        """Load YAML file with environment variable substitution."""
        if not path.exists():
            raise ConfigurationError(f"Configuration file not found: {path}")
            
        with open(path, 'r') as f:
            content = f.read()
            
        # Substitute environment variables
        content = self._substitute_env_vars(content)
        
        try:
            return yaml.safe_load(content) or {}
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {path}: {e}")
    
    def _substitute_env_vars(self, content: str) -> str:
        """Replace ${VAR_NAME} patterns with environment variable values."""
        def replace_match(match):
            var_name = match.group(1)
            value = os.environ.get(var_name)
            if value is None:
                # Keep the placeholder if env var not set (will be caught in validation)
                return match.group(0)
            return value
            
        return self.ENV_PATTERN.sub(replace_match, content)
    
    def _validate_config(self) -> None:
        """Validate configuration structure and required fields."""
        # Validate rules
        rules = self._config.get('rules', [])
        if not rules:
            raise ConfigurationError("No rules defined in configuration")
            
        for i, rule in enumerate(rules):
            self._validate_rule(rule, i)
            
        # Validate connections
        connections = self._connections.get('connections', {})
        if not connections:
            raise ConfigurationError("No database connections defined")
            
        for name, conn in connections.items():
            self._validate_connection(name, conn)
    
    def _validate_rule(self, rule: dict, index: int) -> None:
        """Validate a single rule definition."""
        required_fields = ['name', 'type', 'severity']
        
        for field in required_fields:
            if field not in rule:
                raise ConfigurationError(
                    f"Rule at index {index} missing required field: {field}"
                )
                
        valid_severities = ['critical', 'high', 'medium', 'low']
        if rule['severity'] not in valid_severities:
            raise ConfigurationError(
                f"Rule '{rule['name']}' has invalid severity: {rule['severity']}. "
                f"Must be one of: {valid_severities}"
            )
            
        valid_types = [
            'completeness', 'referential_integrity', 'duplicates', 'uniqueness',
            'range', 'pattern', 'date_range', 'outliers', 'cross_field', 'custom_sql'
        ]
        if rule['type'] not in valid_types:
            raise ConfigurationError(
                f"Rule '{rule['name']}' has invalid type: {rule['type']}. "
                f"Must be one of: {valid_types}"
            )
            
        # Type-specific validation
        if rule['type'] == 'completeness' and 'columns' not in rule:
            raise ConfigurationError(
                f"Rule '{rule['name']}' (completeness) requires 'columns' field"
            )
            
        if rule['type'] == 'referential_integrity':
            required = ['column', 'reference_table', 'reference_column']
            for field in required:
                if field not in rule:
                    raise ConfigurationError(
                        f"Rule '{rule['name']}' (referential_integrity) requires '{field}' field"
                    )
                    
        if rule['type'] == 'custom_sql' and 'query' not in rule:
            raise ConfigurationError(
                f"Rule '{rule['name']}' (custom_sql) requires 'query' field"
            )
    
    def _validate_connection(self, name: str, conn: dict) -> None:
        """Validate a database connection definition."""
        if 'type' not in conn:
            raise ConfigurationError(f"Connection '{name}' missing 'type' field")
            
        valid_types = ['sqlserver', 'postgres', 'sqlite', 'csv']
        if conn['type'] not in valid_types:
            raise ConfigurationError(
                f"Connection '{name}' has invalid type: {conn['type']}. "
                f"Must be one of: {valid_types}"
            )
            
        # Check for unresolved environment variables
        for key, value in conn.items():
            if isinstance(value, str) and self.ENV_PATTERN.search(value):
                raise ConfigurationError(
                    f"Connection '{name}' has unresolved environment variable in '{key}': {value}"
                )
    
    def get_full_config(self) -> dict:
        """Return merged configuration with connections."""
        return {
            **self._config,
            'connections': self._connections.get('connections', {})
        }
    
    def get_rules(self) -> list[dict]:
        """Return list of validation rules."""
        return self._config.get('rules', [])
    
    def get_settings(self) -> dict:
        """Return global settings with defaults."""
        defaults = {
            'stop_on_critical': False,
            'sample_size': 5,
            'parallel_execution': True,
            'max_workers': 4,
            'output_dir': 'reports'
        }
        return {**defaults, **self._config.get('settings', {})}
    
    def get_notifications(self) -> dict:
        """Return notification settings."""
        return self._config.get('notifications', {})
    
    def get_connection(self, name: str) -> dict:
        """Get a specific connection configuration."""
        connections = self._connections.get('connections', {})
        if name not in connections:
            raise ConfigurationError(f"Connection not found: {name}")
        return connections[name]
    
    def get_connection_names(self) -> list[str]:
        """Return list of available connection names."""
        return list(self._connections.get('connections', {}).keys())


def load_config(config_path: str, connections_path: str | None = None) -> ConfigLoader:
    """
    Convenience function to load configuration.
    
    Args:
        config_path: Path to rules YAML
        connections_path: Path to connections YAML (optional)
        
    Returns:
        Loaded ConfigLoader instance
    """
    loader = ConfigLoader(config_path, connections_path)
    loader.load()
    return loader
