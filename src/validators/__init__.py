"""
Validators package - data quality validation implementations.

Each validator type checks for a specific category of data quality issue:
- Completeness: Missing or empty values
- Referential integrity: Orphan foreign keys
- Duplicates: Repeated records
- Range: Values outside acceptable bounds
- Pattern: Format validation with regex
- Outliers: Statistical anomalies
- Custom SQL: Flexible user-defined checks
"""

from src.validators.base import (
    BaseValidator,
    ValidationResult,
    ValidationReport,
    Severity
)
from src.validators.completeness import CompletenessValidator
from src.validators.referential import ReferentialIntegrityValidator
from src.validators.duplicates import DuplicatesValidator
from src.validators.range_check import RangeValidator
from src.validators.pattern import PatternValidator
from src.validators.outliers import OutliersValidator
from src.validators.custom_sql import CustomSQLValidator


# Registry mapping rule types to validator classes
VALIDATOR_REGISTRY: dict[str, type[BaseValidator]] = {
    'completeness': CompletenessValidator,
    'referential_integrity': ReferentialIntegrityValidator,
    'duplicates': DuplicatesValidator,
    'uniqueness': DuplicatesValidator,  # Alias - duplicates with single column
    'range': RangeValidator,
    'pattern': PatternValidator,
    'outliers': OutliersValidator,
    'custom_sql': CustomSQLValidator,
}


def get_validator(rule_type: str) -> type[BaseValidator] | None:
    """
    Get validator class for a rule type.
    
    Args:
        rule_type: The type of validation rule
        
    Returns:
        Validator class or None if not found
    """
    return VALIDATOR_REGISTRY.get(rule_type)


def list_validator_types() -> list[str]:
    """Return list of available validator types."""
    return list(VALIDATOR_REGISTRY.keys())


__all__ = [
    'BaseValidator',
    'ValidationResult',
    'ValidationReport',
    'Severity',
    'CompletenessValidator',
    'ReferentialIntegrityValidator',
    'DuplicatesValidator',
    'RangeValidator',
    'PatternValidator',
    'OutliersValidator',
    'CustomSQLValidator',
    'VALIDATOR_REGISTRY',
    'get_validator',
    'list_validator_types',
]
    Returns:
        Validator class or None if not found
    """
    return VALIDATOR_REGISTRY.get(rule_type.lower())


def list_validators() -> list[str]:
    """Return list of available validator types."""
    return list(VALIDATOR_REGISTRY.keys())


__all__ = [
    'BaseValidator',
    'ValidationResult',
    'ValidationReport',
    'Severity',
    'CompletenessValidator',
    'ReferentialIntegrityValidator',
    'DuplicatesValidator',
    'RangeValidator',
    'PatternValidator',
    'OutliersValidator',
    'CustomSQLValidator',
    'VALIDATOR_REGISTRY',
    'get_validator',
    'list_validators',
]
