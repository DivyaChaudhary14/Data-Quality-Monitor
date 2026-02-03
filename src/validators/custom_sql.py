"""
Custom SQL validator - execute user-defined validation queries.

Provides maximum flexibility for complex business rules that can't be
expressed with the built-in validators.
"""

import time
from typing import Any

from src.validators.base import BaseValidator, ValidationResult


class CustomSQLValidator(BaseValidator):
    """
    Execute custom SQL queries for validation.
    
    The query should return records that VIOLATE the rule (i.e., records
    that have problems). If the query returns zero rows, the rule passes.
    
    Configuration:
        query: SQL query that returns violating records
        count_query: Optional separate query for counting (more efficient for large results)
        description: Human-readable explanation of what the rule checks
        
    Example rules:
        - name: active_clients_have_services
          type: custom_sql
          severity: medium
          description: "Active clients should have services in last 6 months"
          query: |
            SELECT c.client_id, c.first_name, c.last_name
            FROM clients c
            WHERE c.status = 'active'
              AND c.client_id NOT IN (
                SELECT DISTINCT client_id 
                FROM services 
                WHERE service_date >= DATEADD(month, -6, GETDATE())
              )
              
        - name: orphan_payments
          type: custom_sql
          severity: critical
          description: "All payments must link to valid invoices"
          query: |
            SELECT p.*
            FROM payments p
            LEFT JOIN invoices i ON p.invoice_id = i.invoice_id
            WHERE i.invoice_id IS NULL
          count_query: |
            SELECT COUNT(*) as violation_count
            FROM payments p
            LEFT JOIN invoices i ON p.invoice_id = i.invoice_id
            WHERE i.invoice_id IS NULL
    """
    
    validator_type = "custom_sql"
    
    def validate(self, rule: dict) -> ValidationResult:
        """Execute custom SQL query and check for violations."""
        start_time = time.time()
        
        query = rule.get('query', '').strip()
        count_query = rule.get('count_query', '').strip()
        
        if not query:
            return self._build_result(
                rule=rule,
                passed=False,
                error_message="Custom SQL validation requires 'query' field"
            )
        
        try:
            # Get violation count
            if count_query:
                # Use provided count query
                count_result = self.connector.execute_query(count_query)
                violation_count = self._extract_count(count_result)
            else:
                # Wrap query in COUNT
                wrapped_count = f"SELECT COUNT(*) as violation_count FROM ({query}) AS validation_results"
                try:
                    count_result = self.connector.execute_query(wrapped_count)
                    violation_count = self._extract_count(count_result)
                except Exception:
                    # If wrapping fails, execute query and count in Python
                    all_results = self.connector.execute_query(query)
                    violation_count = len(all_results)
            
            # Get sample records if there are violations
            sample_records = []
            if violation_count > 0:
                sample_records = self._get_sample_records(query)
            
            execution_time = (time.time() - start_time) * 1000
            
            return self._build_result(
                rule=rule,
                passed=violation_count == 0,
                violation_count=violation_count,
                sample_records=self._serialize_records(sample_records),
                query=query,
                metadata={
                    'custom_query': True,
                    'has_count_query': bool(count_query)
                }
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            return self._build_result(
                rule=rule,
                passed=False,
                error_message=f"Query execution failed: {str(e)}",
                query=query
            )
    
    def _extract_count(self, result: list[dict]) -> int:
        """Extract count from query result."""
        if not result:
            return 0
        
        row = result[0]
        
        # Try common column names for count
        for col in ['violation_count', 'count', 'cnt', 'total']:
            if col in row:
                return int(row[col] or 0)
        
        # If single column, assume it's the count
        if len(row) == 1:
            return int(list(row.values())[0] or 0)
        
        return 0
    
    def _get_sample_records(self, query: str) -> list[dict]:
        """Get sample of violating records."""
        # Try different pagination syntaxes
        try:
            # SQL Server / PostgreSQL OFFSET FETCH
            sample_query = f"{query} OFFSET 0 ROWS FETCH NEXT {self.sample_size} ROWS ONLY"
            return self.connector.execute_query(sample_query)
        except Exception:
            pass
        
        try:
            # SQL Server TOP
            # This is hacky but works for simple queries
            if query.upper().strip().startswith('SELECT'):
                sample_query = query.replace('SELECT', f'SELECT TOP {self.sample_size}', 1)
                return self.connector.execute_query(sample_query)
        except Exception:
            pass
        
        try:
            # PostgreSQL / SQLite LIMIT
            sample_query = f"{query} LIMIT {self.sample_size}"
            return self.connector.execute_query(sample_query)
        except Exception:
            pass
        
        try:
            # Subquery approach
            sample_query = f"SELECT TOP {self.sample_size} * FROM ({query}) AS t"
            return self.connector.execute_query(sample_query)
        except Exception:
            pass
        
        # Last resort: get all and slice
        try:
            all_records = self.connector.execute_query(query)
            return all_records[:self.sample_size]
        except Exception:
            return []
    
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
        # Handle dates, decimals, bytes, etc.
        return str(value)
