"""
Referential integrity validator - checks that foreign key relationships are valid.

Ensures that values in a column reference existing values in another table,
maintaining database integrity even when formal FK constraints aren't defined.
"""

import time
from typing import Any

from src.validators.base import BaseValidator, ValidationResult


class ReferentialIntegrityValidator(BaseValidator):
    """
    Validate that column values exist in a reference table.
    
    Configuration:
        table: Table containing the foreign key
        column: Column containing the reference value
        reference_table: Table being referenced
        reference_column: Column in reference table (usually primary key)
        allow_null: Whether NULL values are acceptable (default: True)
        
    Example rule:
        - name: orders_valid_customer
          table: orders
          type: referential_integrity
          column: customer_id
          reference_table: customers
          reference_column: id
          severity: critical
    """
    
    validator_type = "referential_integrity"
    
    def validate(self, rule: dict) -> ValidationResult:
        """Check that all values in column exist in reference table."""
        start_time = time.time()
        
        table = rule['table']
        column = rule['column']
        ref_table = rule['reference_table']
        ref_column = rule['reference_column']
        allow_null = rule.get('allow_null', True)
        
        # Build the orphan records query
        null_condition = f"AND t.{self._quote_identifier(column)} IS NOT NULL" if allow_null else ""
        
        query = f"""
            SELECT t.*
            FROM {self._quote_identifier(table)} t
            LEFT JOIN {self._quote_identifier(ref_table)} r 
                ON t.{self._quote_identifier(column)} = r.{self._quote_identifier(ref_column)}
            WHERE r.{self._quote_identifier(ref_column)} IS NULL
            {null_condition}
        """
        
        # Count query
        count_query = f"""
            SELECT COUNT(*) as violation_count
            FROM {self._quote_identifier(table)} t
            LEFT JOIN {self._quote_identifier(ref_table)} r 
                ON t.{self._quote_identifier(column)} = r.{self._quote_identifier(ref_column)}
            WHERE r.{self._quote_identifier(ref_column)} IS NULL
            {null_condition}
        """
        
        # Query to get distinct orphan values (useful for diagnosis)
        orphan_values_query = f"""
            SELECT DISTINCT t.{self._quote_identifier(column)} as orphan_value
            FROM {self._quote_identifier(table)} t
            LEFT JOIN {self._quote_identifier(ref_table)} r 
                ON t.{self._quote_identifier(column)} = r.{self._quote_identifier(ref_column)}
            WHERE r.{self._quote_identifier(ref_column)} IS NULL
            {null_condition}
        """
        
        try:
            # Get violation count
            count_result = self.connector.execute_query(count_query)
            violation_count = count_result[0]['violation_count'] if count_result else 0
            
            # Get sample records and orphan values if violations exist
            sample_records = []
            orphan_values = []
            
            if violation_count > 0:
                # Get sample of violating records
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
                
                # Get distinct orphan values (limit to 20 for readability)
                try:
                    orphan_query = f"{orphan_values_query} OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY"
                    orphan_results = self.connector.execute_query(orphan_query)
                except Exception:
                    try:
                        orphan_query = f"SELECT TOP 20 * FROM ({orphan_values_query}) AS t"
                        orphan_results = self.connector.execute_query(orphan_query)
                    except Exception:
                        orphan_results = self.connector.execute_query(orphan_values_query)[:20]
                
                orphan_values = [r['orphan_value'] for r in orphan_results]
            
            execution_time = (time.time() - start_time) * 1000
            
            return self._build_result(
                rule=rule,
                passed=violation_count == 0,
                violation_count=violation_count,
                sample_records=self._serialize_records(sample_records),
                query=query.strip(),
                metadata={
                    'source_table': table,
                    'source_column': column,
                    'reference_table': ref_table,
                    'reference_column': ref_column,
                    'orphan_values': [self._serialize_value(v) for v in orphan_values],
                    'allow_null': allow_null
                }
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
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
