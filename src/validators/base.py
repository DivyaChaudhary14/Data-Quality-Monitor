"""
Base validator class defining the interface for all validators.

All validation types inherit from BaseValidator and implement the validate() method.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from src.connectors.base import BaseConnector


class Severity(Enum):
    """Validation rule severity levels."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    
    @classmethod
    def from_string(cls, value: str) -> "Severity":
        """Convert string to Severity enum."""
        return cls(value.lower())
    
    def __lt__(self, other: "Severity") -> bool:
        """Enable sorting by severity (critical > high > medium > low)."""
        order = [Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.LOW]
        return order.index(self) > order.index(other)


@dataclass
class ValidationResult:
    """
    Result of a single validation rule execution.
    
    Attributes:
        rule_name: Name of the rule that was executed
        rule_type: Type of validation (completeness, referential, etc.)
        severity: Severity level of the rule
        passed: Whether the validation passed (no violations found)
        violation_count: Number of records that violated the rule
        sample_records: Sample of violating records for review
        query: The SQL query used for validation
        table: Table that was validated
        description: Human-readable description of the rule
        execution_time_ms: Time taken to execute the validation
        error_message: Error message if validation failed to execute
        metadata: Additional context-specific data
    """
    rule_name: str
    rule_type: str
    severity: Severity
    passed: bool
    violation_count: int = 0
    sample_records: list[dict] = field(default_factory=list)
    query: str = ""
    table: str = ""
    description: str = ""
    execution_time_ms: float = 0.0
    error_message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    
    @property
    def failed(self) -> bool:
        """Convenience property for checking failure."""
        return not self.passed
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'rule_name': self.rule_name,
            'rule_type': self.rule_type,
            'severity': self.severity.value,
            'passed': self.passed,
            'violation_count': self.violation_count,
            'sample_records': self.sample_records,
            'query': self.query,
            'table': self.table,
            'description': self.description,
            'execution_time_ms': self.execution_time_ms,
            'error_message': self.error_message,
            'metadata': self.metadata
        }


@dataclass
class ValidationReport:
    """
    Aggregated report of all validation results.
    
    Attributes:
        connection_name: Name of the database connection used
        timestamp: When the validation was run
        duration_seconds: Total time to run all validations
        results: List of individual validation results
        settings: Configuration settings used
    """
    connection_name: str
    timestamp: datetime
    duration_seconds: float
    results: list[ValidationResult]
    settings: dict = field(default_factory=dict)
    
    @property
    def total_rules(self) -> int:
        """Total number of rules executed."""
        return len(self.results)
    
    @property
    def passed_count(self) -> int:
        """Number of rules that passed."""
        return sum(1 for r in self.results if r.passed)
    
    @property
    def failed_count(self) -> int:
        """Number of rules that failed."""
        return sum(1 for r in self.results if r.failed)
    
    @property
    def failed_results(self) -> list[ValidationResult]:
        """Get only failed results, sorted by severity."""
        failed = [r for r in self.results if r.failed]
        return sorted(failed, key=lambda r: r.severity)
    
    @property
    def critical_failures(self) -> list[ValidationResult]:
        """Get critical severity failures."""
        return [r for r in self.results if r.failed and r.severity == Severity.CRITICAL]
    
    @property
    def high_failures(self) -> list[ValidationResult]:
        """Get high severity failures."""
        return [r for r in self.results if r.failed and r.severity == Severity.HIGH]
    
    def failures_by_severity(self) -> dict[str, int]:
        """Count failures grouped by severity."""
        counts = {'critical': 0, 'high': 0, 'medium': 0, 'low': 0}
        for result in self.results:
            if result.failed:
                counts[result.severity.value] += 1
        return counts
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'connection_name': self.connection_name,
            'timestamp': self.timestamp.isoformat(),
            'duration_seconds': self.duration_seconds,
            'summary': {
                'total_rules': self.total_rules,
                'passed': self.passed_count,
                'failed': self.failed_count,
                'failures_by_severity': self.failures_by_severity()
            },
            'results': [r.to_dict() for r in self.results],
            'settings': self.settings
        }


class BaseValidator(ABC):
    """
    Abstract base class for all validators.
    
    Subclasses must implement:
        - validator_type: Class attribute identifying the validation type
        - validate(): Method that performs the actual validation
    """
    
    validator_type: str = "base"  # Override in subclasses
    
    def __init__(self, connector: "BaseConnector", settings: dict | None = None):
        """
        Initialize validator with database connector.
        
        Args:
            connector: Database connector for executing queries
            settings: Global settings (sample_size, etc.)
        """
        self.connector = connector
        self.settings = settings or {}
        self.sample_size = self.settings.get('sample_size', 5)
    
    @abstractmethod
    def validate(self, rule: dict) -> ValidationResult:
        """
        Execute validation for a single rule.
        
        Args:
            rule: Rule configuration dictionary
            
        Returns:
            ValidationResult with pass/fail status and details
        """
        pass
    
    def _build_result(
        self,
        rule: dict,
        passed: bool,
        violation_count: int = 0,
        sample_records: list[dict] | None = None,
        query: str = "",
        error_message: str | None = None,
        metadata: dict | None = None
    ) -> ValidationResult:
        """
        Helper to build a ValidationResult with common fields populated.
        
        Args:
            rule: The rule configuration
            passed: Whether validation passed
            violation_count: Number of violations found
            sample_records: Sample of violating records
            query: SQL query used
            error_message: Error if validation failed to execute
            metadata: Additional data
            
        Returns:
            Populated ValidationResult
        """
        return ValidationResult(
            rule_name=rule['name'],
            rule_type=rule['type'],
            severity=Severity.from_string(rule['severity']),
            passed=passed,
            violation_count=violation_count,
            sample_records=sample_records or [],
            query=query,
            table=rule.get('table', ''),
            description=rule.get('description', ''),
            error_message=error_message,
            metadata=metadata or {}
        )
    
    def _get_sample(self, records: list[dict]) -> list[dict]:
        """Return up to sample_size records."""
        return records[:self.sample_size]
    
    def _format_column_list(self, columns: list[str]) -> str:
        """Format column names for SQL query."""
        return ', '.join(f'[{col}]' for col in columns)
    
    def _quote_identifier(self, name: str) -> str:
        """Quote a table or column name for SQL."""
        # Use connector's quoting if available
        if hasattr(self.connector, 'quote_identifier'):
            return self.connector.quote_identifier(name)
        # Default to SQL Server style
        return f'[{name}]'
