"""
Pattern validator - checks that values match expected formats using regex.

Validates string formats like email addresses, phone numbers, postal codes,
and other structured data that should follow specific patterns.
"""

import time
from typing import Any

from src.validators.base import BaseValidator, ValidationResult


class PatternValidator(BaseValidator):
    """
    Validate that column values match a regex pattern.
    
    Configuration:
        table: Table to check
        column: Column to validate
        pattern: Regular expression pattern
        match_null: Whether NULL values pass validation (default: True)
        inverse: If True, flag records that DO match (default: False)
        
    Example rules:
        - name: valid_email
          table: customers
          type: pattern
          column: email
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
          severity: high
          
        - name: valid_canadian_postal
          table: clients
          type: pattern
          column: postal_code
          pattern: "^[A-Z]\\d[A-Z] ?\\d[A-Z]\\d$"
          severity: low
    """
    
    validator_type = "pattern"
    
    # Common patterns for convenience
    COMMON_PATTERNS = {
        'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        'us_phone': r'^\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$',
        'us_zip': r'^\d{5}(-\d{4})?$',
        'ca_postal': r'^[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d$',
        'ssn': r'^\d{3}-?\d{2}-?\d{4}$',
        'url': r'^https?://[^\s/$.?#].[^\s]*$',
        'ipv4': r'^(\d{1,3}\.){3}\d{1,3}$',
        'date_iso': r'^\d{4}-\d{2}-\d{2}$',
        'uuid': r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    }
    
    def validate(self, rule: dict) -> ValidationResult:
        """Check that values match the specified pattern."""
        start_time = time.time()
        
        table = rule['table']
        column = rule['column']
        pattern = rule.get('pattern', '')
        match_null = rule.get('match_null', True)
        inverse = rule.get('inverse', False)
        
        # Check for named pattern
        if pattern in self.COMMON_PATTERNS:
            pattern = self.COMMON_PATTERNS[pattern]
        
        if not pattern:
            return self._build_result(
                rule=rule,
                passed=False,
                error_message="Pattern validation requires 'pattern' field"
            )
        
        quoted_col = self._quote_identifier(column)
        quoted_table = self._quote_identifier(table)
        
        # Different databases have different regex syntax
        # We'll try SQL Server syntax first, then adapt
        try:
            result = self._validate_sqlserver(quoted_table, quoted_col, pattern, match_null, inverse, rule)
            return result
        except Exception as e:
            # Try POSIX regex (PostgreSQL)
            try:
                result = self._validate_postgres(quoted_table, quoted_col, pattern, match_null, inverse, rule)
                return result
            except Exception:
                # Fall back to LIKE pattern (limited)
                return self._build_result(
                    rule=rule,
                    passed=False,
                    error_message=f"Pattern validation not supported by database: {str(e)}"
                )
    
    def _validate_sqlserver(
        self,
        table: str,
        column: str,
        pattern: str,
        match_null: bool,
        inverse: bool,
        rule: dict
    ) -> ValidationResult:
        """
        Validate using SQL Server pattern matching.
        
        Note: SQL Server doesn't have native regex, so we use a combination of
        LIKE and PATINDEX, or require CLR functions for full regex support.
        For simple patterns, we translate to LIKE; for complex ones, we note the limitation.
        """
        # For SQL Server, we'll use a simplified approach
        # Full regex requires CLR integration which may not be available
        
        null_condition = "" if match_null else f"AND {column} IS NOT NULL"
        
        # Check if pattern can be translated to LIKE
        simple_pattern = self._try_translate_to_like(pattern)
        
        if simple_pattern:
            match_operator = "NOT LIKE" if inverse else "LIKE"
            not_match_operator = "LIKE" if inverse else "NOT LIKE"
            
            query = f"""
                SELECT *
                FROM {table}
                WHERE {column} {not_match_operator} '{simple_pattern}'
                {null_condition}
            """
            
            count_query = f"""
                SELECT COUNT(*) as violation_count
                FROM {table}
                WHERE {column} {not_match_operator} '{simple_pattern}'
                {null_condition}
            """
        else:
            # Try PATINDEX (limited regex support in SQL Server)
            # PATINDEX returns 0 if no match
            if inverse:
                match_condition = f"PATINDEX('%{self._escape_patindex(pattern)}%', {column}) > 0"
            else:
                match_condition = f"PATINDEX('%{self._escape_patindex(pattern)}%', {column}) = 0"
            
            query = f"""
                SELECT *
                FROM {table}
                WHERE {column} IS NOT NULL
                  AND {match_condition}
                {null_condition}
            """
            
            count_query = f"""
                SELECT COUNT(*) as violation_count
                FROM {table}
                WHERE {column} IS NOT NULL
                  AND {match_condition}
                {null_condition}
            """
        
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
        
        return self._build_result(
            rule=rule,
            passed=violation_count == 0,
            violation_count=violation_count,
            sample_records=self._serialize_records(sample_records),
            query=query.strip(),
            metadata={
                'pattern': pattern,
                'match_null': match_null,
                'inverse': inverse,
                'pattern_type': 'like' if simple_pattern else 'patindex'
            }
        )
    
    def _validate_postgres(
        self,
        table: str,
        column: str,
        pattern: str,
        match_null: bool,
        inverse: bool,
        rule: dict
    ) -> ValidationResult:
        """Validate using PostgreSQL regex operators."""
        null_condition = "" if match_null else f"AND {column} IS NOT NULL"
        
        # PostgreSQL uses ~ for regex match, !~ for not match
        if inverse:
            match_condition = f"{column} ~ '{pattern}'"
        else:
            match_condition = f"{column} !~ '{pattern}'"
        
        query = f"""
            SELECT *
            FROM {table}
            WHERE {column} IS NOT NULL
              AND {match_condition}
            {null_condition}
        """
        
        count_query = f"""
            SELECT COUNT(*) as violation_count
            FROM {table}
            WHERE {column} IS NOT NULL
              AND {match_condition}
            {null_condition}
        """
        
        count_result = self.connector.execute_query(count_query)
        violation_count = count_result[0]['violation_count'] if count_result else 0
        
        sample_records = []
        if violation_count > 0:
            sample_query = f"{query} LIMIT {self.sample_size}"
            sample_records = self.connector.execute_query(sample_query)
        
        return self._build_result(
            rule=rule,
            passed=violation_count == 0,
            violation_count=violation_count,
            sample_records=self._serialize_records(sample_records),
            query=query.strip(),
            metadata={
                'pattern': pattern,
                'match_null': match_null,
                'inverse': inverse,
                'pattern_type': 'regex'
            }
        )
    
    def _try_translate_to_like(self, regex_pattern: str) -> str | None:
        """
        Try to translate simple regex to SQL LIKE pattern.
        Returns None if pattern is too complex.
        """
        # Only handle very simple patterns
        # ^ and $ anchors, literal characters, and basic wildcards
        
        simple_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@.-_ ')
        
        pattern = regex_pattern
        
        # Remove anchors (LIKE is implicitly anchored)
        if pattern.startswith('^'):
            pattern = pattern[1:]
        if pattern.endswith('$'):
            pattern = pattern[:-1]
        
        # Check if remaining pattern only has simple characters
        if all(c in simple_chars for c in pattern):
            return pattern
        
        # Pattern is too complex for LIKE
        return None
    
    def _escape_patindex(self, pattern: str) -> str:
        """Escape special characters for PATINDEX."""
        # PATINDEX uses a subset of regex: [ ] ^ - are special
        # This is a simplified escape - full regex not supported
        return pattern.replace("'", "''")
    
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
