"""
Range validator - checks that numeric values fall within acceptable bounds.

Validates that values are within specified minimum and maximum thresholds,
useful for business rules like positive prices, valid percentages, etc.
"""

import time
from typing import Any

from src.validators.base import BaseValidator, ValidationResult


class RangeValidator(BaseValidator):
    """
    Validate that column values are within specified range.
    
    Configuration:
        table: Table to check
        column: Column to validate
        min: Minimum allowed value (optional)
        max: Maximum allowed value (optional)
        inclusive: Whether bounds are inclusive (default: True)
        
    Example rules:
        - name: valid_prices
          table: products
          type: range
          column: price
          min: 0
          severity: high
          
        - name: valid_percentage
          table: metrics
          type: range
          column: completion_pct
          min: 0
          max: 100
          severity: medium
    """
    
    validator_type = "range"
    
    def validate(self, rule: dict) -> ValidationResult:
        """Check that values are within specified range."""
        start_time = time.time()
        
        table = rule['table']
        column = rule['column']
        min_val = rule.get('min')
        max_val = rule.get('max')
        inclusive = rule.get('inclusive', True)
        
        if min_val is None and max_val is None:
            return self._build_result(
                rule=rule,
                passed=False,
                error_message="Range validation requires at least 'min' or 'max' to be specified"
            )
        
        quoted_col = self._quote_identifier(column)
        quoted_table = self._quote_identifier(table)
        
        # Build conditions
        conditions = []
        if min_val is not None:
            if inclusive:
                conditions.append(f'{quoted_col} < {min_val}')
            else:
                conditions.append(f'{quoted_col} <= {min_val}')
                
        if max_val is not None:
            if inclusive:
                conditions.append(f'{quoted_col} > {max_val}')
            else:
                conditions.append(f'{quoted_col} >= {max_val}')
        
        where_clause = ' OR '.join(conditions)
        
        query = f"""
            SELECT *
            FROM {quoted_table}
            WHERE {quoted_col} IS NOT NULL
              AND ({where_clause})
        """
        
        count_query = f"""
            SELECT COUNT(*) as violation_count
            FROM {quoted_table}
            WHERE {quoted_col} IS NOT NULL
              AND ({where_clause})
        """
        
        try:
            count_result = self.connector.execute_query(count_query)
            violation_count = count_result[0]['violation_count'] if count_result else 0
            
            sample_records = []
            if violation_count > 0:
                try:
                    sample_query = f"{query} OFFSET 0 ROWS FETCH NEXT {self.sample_size} ROWS ONLY"
                    sample_records = self.connector.execute_query(sample_query)
                except Exception:
                    try:
                        sample_query = f"SELECT TOP {self.sample_size} * FROM ({query}) AS t"
                        sample_records = self.connector.execute_query(sample_query)
                    except Exception:
                        all_records = self.connector.execute_query(query)
                        sample_records = all_records[:self.sample_size]
            
            execution_time = (time.time() - start_time) * 1000
            
            return self._build_result(
                rule=rule,
                passed=violation_count == 0,
                violation_count=violation_count,
                sample_records=self._serialize_records(sample_records),
                query=query.strip(),
                metadata={
                    'column': column,
                    'min': min_val,
                    'max': max_val,
                    'inclusive': inclusive
                }
            )
            
        except Exception as e:
            return self._build_result(
                rule=rule,
                passed=False,
                error_message=f"Query execution failed: {str(e)}",
                query=query.strip()
            )
    
    def _serialize_records(self, records: list[dict]) -> list[dict]:
        """Convert records to JSON-serializable format."""
        return [
            {k: self._serialize_value(v) for k, v in record.items()}
            for record in records
        ]
    
    def _serialize_value(self, value: Any) -> Any:
        """Convert value to JSON-serializable format."""
        if value is None:
            return None
        if isinstance(value, (int, float, str, bool)):
            return value
        return str(value)
