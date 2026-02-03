"""
Completeness validator - checks for NULL or empty values in required fields.

This is one of the most common data quality checks, ensuring that mandatory
fields are populated.
"""

import time
from typing import Any

from src.validators.base import BaseValidator, ValidationResult


class CompletenessValidator(BaseValidator):
    """
    Validate that specified columns are not NULL or empty.
    
    Configuration:
        table: Table to validate
        columns: List of column names to check
        check_empty_strings: Also flag empty strings as violations (default: True)
        check_whitespace: Also flag whitespace-only strings (default: False)
        
    Example rule:
        - name: clients_required_fields
          table: clients
          type: completeness
          columns: [first_name, last_name, email]
          severity: high
          check_empty_strings: true
    """
    
    validator_type = "completeness"
    
    def validate(self, rule: dict) -> ValidationResult:
        """Check for NULL/empty values in specified columns."""
        start_time = time.time()
        
        table = rule['table']
        columns = rule['columns']
        check_empty = rule.get('check_empty_strings', True)
        check_whitespace = rule.get('check_whitespace', False)
        
        # Build WHERE conditions for each column
        conditions = []
        for col in columns:
            col_conditions = [f'{self._quote_identifier(col)} IS NULL']
            
            if check_empty:
                col_conditions.append(f'{self._quote_identifier(col)} = \'\'')
                
            if check_whitespace:
                col_conditions.append(f'LTRIM(RTRIM({self._quote_identifier(col)})) = \'\'')
            
            conditions.append(f'({" OR ".join(col_conditions)})')
        
        where_clause = ' OR '.join(conditions)
        
        # Build query to find violations
        # We want to know which column(s) are incomplete for each record
        case_expressions = []
        for col in columns:
            col_conds = [f'{self._quote_identifier(col)} IS NULL']
            if check_empty:
                col_conds.append(f'{self._quote_identifier(col)} = \'\'')
            if check_whitespace:
                col_conds.append(f'LTRIM(RTRIM({self._quote_identifier(col)})) = \'\'')
            
            case_expr = f"CASE WHEN {' OR '.join(col_conds)} THEN '{col}' ELSE NULL END"
            case_expressions.append(case_expr)
        
        # Query with violation details
        query = f"""
            SELECT 
                *,
                CONCAT_WS(', ', {', '.join(case_expressions)}) AS _incomplete_columns
            FROM {self._quote_identifier(table)}
            WHERE {where_clause}
        """
        
        # Count query (more efficient for large tables)
        count_query = f"""
            SELECT COUNT(*) as violation_count
            FROM {self._quote_identifier(table)}
            WHERE {where_clause}
        """
        
        try:
            # Get count first
            count_result = self.connector.execute_query(count_query)
            violation_count = count_result[0]['violation_count'] if count_result else 0
            
            # Get sample records if there are violations
            sample_records = []
            if violation_count > 0:
                sample_query = f"{query} OFFSET 0 ROWS FETCH NEXT {self.sample_size} ROWS ONLY"
                try:
                    sample_records = self.connector.execute_query(sample_query)
                except Exception:
                    # Fallback for databases that don't support OFFSET/FETCH
                    sample_query = f"SELECT TOP {self.sample_size} * FROM ({query}) AS t"
                    try:
                        sample_records = self.connector.execute_query(sample_query)
                    except Exception:
                        # Last resort: just get all and limit in Python
                        all_records = self.connector.execute_query(query)
                        sample_records = all_records[:self.sample_size]
            
            execution_time = (time.time() - start_time) * 1000
            
            return self._build_result(
                rule=rule,
                passed=violation_count == 0,
                violation_count=violation_count,
                sample_records=self._clean_sample_records(sample_records),
                query=query.strip(),
                metadata={
                    'columns_checked': columns,
                    'check_empty_strings': check_empty,
                    'check_whitespace': check_whitespace
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
    
    def _clean_sample_records(self, records: list[dict]) -> list[dict]:
        """Remove internal columns from sample records for cleaner output."""
        cleaned = []
        for record in records:
            cleaned_record = {
                k: self._serialize_value(v) 
                for k, v in record.items() 
                if not k.startswith('_')
            }
            # Add the incomplete columns info with a cleaner key
            if '_incomplete_columns' in record:
                cleaned_record['incomplete_fields'] = record['_incomplete_columns']
            cleaned.append(cleaned_record)
        return cleaned
    
    def _serialize_value(self, value: Any) -> Any:
        """Convert value to JSON-serializable format."""
        if value is None:
            return None
        if isinstance(value, (int, float, str, bool)):
            return value
        # Handle dates, decimals, etc.
        return str(value)
