"""
Duplicates validator - detects duplicate records based on specified columns.

Finds records that have the same values in the specified column combination,
which may indicate data entry errors or integration issues.
"""

import time
from typing import Any

from src.validators.base import BaseValidator, ValidationResult


class DuplicatesValidator(BaseValidator):
    """
    Detect duplicate records based on column combination.
    
    Configuration:
        table: Table to check
        columns: List of columns that should be unique together
        case_sensitive: Whether string comparison is case-sensitive (default: True)
        ignore_null: Whether to exclude NULL values from comparison (default: True)
        
    Example rule:
        - name: no_duplicate_clients
          table: clients
          type: duplicates
          columns: [first_name, last_name, date_of_birth]
          severity: high
          case_sensitive: false
    """
    
    validator_type = "duplicates"
    
    def validate(self, rule: dict) -> ValidationResult:
        """Find duplicate records based on specified columns."""
        start_time = time.time()
        
        table = rule['table']
        columns = rule['columns']
        case_sensitive = rule.get('case_sensitive', True)
        ignore_null = rule.get('ignore_null', True)
        
        # Build column expressions (with optional case handling)
        col_expressions = []
        for col in columns:
            if case_sensitive:
                col_expressions.append(self._quote_identifier(col))
            else:
                col_expressions.append(f'LOWER({self._quote_identifier(col)})')
        
        col_list = ', '.join(col_expressions)
        quoted_cols = ', '.join(self._quote_identifier(col) for col in columns)
        
        # Build NULL exclusion if needed
        null_conditions = ""
        if ignore_null:
            null_checks = [f'{self._quote_identifier(col)} IS NOT NULL' for col in columns]
            null_conditions = f"WHERE {' AND '.join(null_checks)}"
        
        # Find duplicate groups
        duplicates_query = f"""
            SELECT {quoted_cols}, COUNT(*) as duplicate_count
            FROM {self._quote_identifier(table)}
            {null_conditions}
            GROUP BY {col_list}
            HAVING COUNT(*) > 1
        """
        
        # Count total duplicate records (not groups)
        count_query = f"""
            SELECT SUM(cnt - 1) as violation_count
            FROM (
                SELECT COUNT(*) as cnt
                FROM {self._quote_identifier(table)}
                {null_conditions}
                GROUP BY {col_list}
                HAVING COUNT(*) > 1
            ) AS dups
        """
        
        # Get actual duplicate records with group info
        records_query = f"""
            WITH DuplicateGroups AS (
                SELECT {col_list}, COUNT(*) as dup_count
                FROM {self._quote_identifier(table)}
                {null_conditions}
                GROUP BY {col_list}
                HAVING COUNT(*) > 1
            )
            SELECT t.*, dg.dup_count as _duplicate_count
            FROM {self._quote_identifier(table)} t
            INNER JOIN DuplicateGroups dg ON {self._build_join_conditions(columns, case_sensitive)}
        """
        
        try:
            # Get count of excess records (total duplicates minus one per group)
            count_result = self.connector.execute_query(count_query)
            violation_count = int(count_result[0]['violation_count'] or 0) if count_result else 0
            
            # Get sample of duplicate groups
            sample_records = []
            duplicate_groups = []
            
            if violation_count > 0:
                # Get duplicate groups info
                try:
                    groups_query = f"{duplicates_query} ORDER BY COUNT(*) DESC OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY"
                    duplicate_groups = self.connector.execute_query(groups_query)
                except Exception:
                    try:
                        groups_query = f"SELECT TOP 10 * FROM ({duplicates_query}) AS t ORDER BY duplicate_count DESC"
                        duplicate_groups = self.connector.execute_query(groups_query)
                    except Exception:
                        all_groups = self.connector.execute_query(duplicates_query)
                        duplicate_groups = sorted(all_groups, key=lambda x: x.get('duplicate_count', 0), reverse=True)[:10]
                
                # Get sample records
                try:
                    sample_query = f"{records_query} ORDER BY t.{self._quote_identifier(columns[0])} OFFSET 0 ROWS FETCH NEXT {self.sample_size * 2} ROWS ONLY"
                    sample_records = self.connector.execute_query(sample_query)
                except Exception:
                    try:
                        sample_query = f"SELECT TOP {self.sample_size * 2} * FROM ({records_query}) AS t ORDER BY {self._quote_identifier(columns[0])}"
                        sample_records = self.connector.execute_query(sample_query)
                    except Exception:
                        all_records = self.connector.execute_query(records_query)
                        sample_records = all_records[:self.sample_size * 2]
            
            execution_time = (time.time() - start_time) * 1000
            
            return self._build_result(
                rule=rule,
                passed=violation_count == 0,
                violation_count=violation_count,
                sample_records=self._serialize_records(sample_records),
                query=duplicates_query.strip(),
                metadata={
                    'columns_checked': columns,
                    'case_sensitive': case_sensitive,
                    'ignore_null': ignore_null,
                    'duplicate_groups': self._serialize_records(duplicate_groups),
                    'group_count': len(duplicate_groups)
                }
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            return self._build_result(
                rule=rule,
                passed=False,
                error_message=f"Query execution failed: {str(e)}",
                query=duplicates_query.strip()
            )
    
    def _build_join_conditions(self, columns: list[str], case_sensitive: bool) -> str:
        """Build JOIN conditions for matching duplicates."""
        conditions = []
        for col in columns:
            quoted = self._quote_identifier(col)
            if case_sensitive:
                conditions.append(f't.{quoted} = dg.{quoted}')
            else:
                conditions.append(f'LOWER(t.{quoted}) = LOWER(dg.{quoted})')
        return ' AND '.join(conditions)
    
    def _serialize_records(self, records: list[dict]) -> list[dict]:
        """Convert records to JSON-serializable format."""
        cleaned = []
        for record in records:
            cleaned_record = {
                k: self._serialize_value(v) 
                for k, v in record.items()
                if not k.startswith('_')
            }
            if '_duplicate_count' in record:
                cleaned_record['group_size'] = record['_duplicate_count']
            if 'duplicate_count' in record:
                cleaned_record['group_size'] = record['duplicate_count']
            cleaned.append(cleaned_record)
        return cleaned
    
    def _serialize_value(self, value: Any) -> Any:
        """Convert value to JSON-serializable format."""
        if value is None:
            return None
        if isinstance(value, (int, float, str, bool)):
            return value
        return str(value)
