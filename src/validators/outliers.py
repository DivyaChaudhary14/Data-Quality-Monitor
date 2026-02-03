"""
Outliers validator - detects statistical anomalies in numeric data.

Uses z-score or IQR methods to identify records with values significantly
outside the normal range, which may indicate data entry errors or fraud.
"""

import time
from typing import Any

from src.validators.base import BaseValidator, ValidationResult


class OutliersValidator(BaseValidator):
    """
    Detect statistical outliers in numeric columns.
    
    Configuration:
        table: Table to check
        column: Numeric column to analyze
        method: Detection method - 'zscore' or 'iqr' (default: 'zscore')
        threshold: Z-score threshold or IQR multiplier (default: 3 for zscore, 1.5 for IQR)
        direction: 'both', 'high', or 'low' (default: 'both')
        
    Example rule:
        - name: price_outliers
          table: products
          type: outliers
          column: price
          method: zscore
          threshold: 3
          severity: medium
    """
    
    validator_type = "outliers"
    
    def validate(self, rule: dict) -> ValidationResult:
        """Detect outliers using statistical methods."""
        start_time = time.time()
        
        table = rule['table']
        column = rule['column']
        method = rule.get('method', 'zscore').lower()
        direction = rule.get('direction', 'both').lower()
        
        # Set default threshold based on method
        if method == 'zscore':
            threshold = rule.get('threshold', 3.0)
        else:  # IQR
            threshold = rule.get('threshold', 1.5)
        
        quoted_col = self._quote_identifier(column)
        quoted_table = self._quote_identifier(table)
        
        try:
            if method == 'zscore':
                result = self._zscore_detection(quoted_table, quoted_col, threshold, direction, rule)
            elif method == 'iqr':
                result = self._iqr_detection(quoted_table, quoted_col, threshold, direction, rule)
            else:
                return self._build_result(
                    rule=rule,
                    passed=False,
                    error_message=f"Unknown outlier detection method: {method}. Use 'zscore' or 'iqr'."
                )
            
            return result
            
        except Exception as e:
            return self._build_result(
                rule=rule,
                passed=False,
                error_message=f"Outlier detection failed: {str(e)}"
            )
    
    def _zscore_detection(
        self, 
        table: str, 
        column: str, 
        threshold: float,
        direction: str,
        rule: dict
    ) -> ValidationResult:
        """Detect outliers using z-score method."""
        
        # First, get statistics
        stats_query = f"""
            SELECT 
                AVG(CAST({column} AS FLOAT)) as mean_val,
                STDEV(CAST({column} AS FLOAT)) as std_val,
                COUNT(*) as total_count
            FROM {table}
            WHERE {column} IS NOT NULL
        """
        
        stats = self.connector.execute_query(stats_query)
        if not stats or stats[0]['std_val'] is None or stats[0]['std_val'] == 0:
            return self._build_result(
                rule=rule,
                passed=True,
                violation_count=0,
                metadata={'note': 'No variance in data or insufficient records'}
            )
        
        mean_val = float(stats[0]['mean_val'])
        std_val = float(stats[0]['std_val'])
        
        # Build outlier conditions based on direction
        if direction == 'both':
            outlier_condition = f"ABS(({column} - {mean_val}) / {std_val}) > {threshold}"
        elif direction == 'high':
            outlier_condition = f"({column} - {mean_val}) / {std_val} > {threshold}"
        else:  # low
            outlier_condition = f"({column} - {mean_val}) / {std_val} < -{threshold}"
        
        # Find outliers
        query = f"""
            SELECT 
                *,
                ({column} - {mean_val}) / {std_val} as _zscore
            FROM {table}
            WHERE {column} IS NOT NULL
              AND {outlier_condition}
        """
        
        count_query = f"""
            SELECT COUNT(*) as violation_count
            FROM {table}
            WHERE {column} IS NOT NULL
              AND {outlier_condition}
        """
        
        count_result = self.connector.execute_query(count_query)
        violation_count = count_result[0]['violation_count'] if count_result else 0
        
        sample_records = []
        if violation_count > 0:
            try:
                sample_query = f"{query} ORDER BY ABS(({column} - {mean_val}) / {std_val}) DESC OFFSET 0 ROWS FETCH NEXT {self.sample_size} ROWS ONLY"
                sample_records = self.connector.execute_query(sample_query)
            except Exception:
                try:
                    sample_query = f"SELECT TOP {self.sample_size} * FROM ({query}) AS t ORDER BY ABS(_zscore) DESC"
                    sample_records = self.connector.execute_query(sample_query)
                except Exception:
                    all_records = self.connector.execute_query(query)
                    all_records.sort(key=lambda x: abs(x.get('_zscore', 0)), reverse=True)
                    sample_records = all_records[:self.sample_size]
        
        return self._build_result(
            rule=rule,
            passed=violation_count == 0,
            violation_count=violation_count,
            sample_records=self._serialize_records(sample_records, column),
            query=query.strip(),
            metadata={
                'method': 'zscore',
                'threshold': threshold,
                'direction': direction,
                'mean': round(mean_val, 4),
                'std_dev': round(std_val, 4),
                'lower_bound': round(mean_val - threshold * std_val, 4) if direction in ['both', 'low'] else None,
                'upper_bound': round(mean_val + threshold * std_val, 4) if direction in ['both', 'high'] else None
            }
        )
    
    def _iqr_detection(
        self,
        table: str,
        column: str,
        threshold: float,
        direction: str,
        rule: dict
    ) -> ValidationResult:
        """Detect outliers using IQR (Interquartile Range) method."""
        
        # Calculate quartiles
        # Using PERCENTILE_CONT which is widely supported
        quartiles_query = f"""
            SELECT 
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) as q1,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {column}) as median,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) as q3
            FROM {table}
            WHERE {column} IS NOT NULL
        """
        
        try:
            quartiles = self.connector.execute_query(quartiles_query)
        except Exception:
            # Fallback for databases without PERCENTILE_CONT
            return self._iqr_detection_fallback(table, column, threshold, direction, rule)
        
        if not quartiles or quartiles[0]['q1'] is None:
            return self._build_result(
                rule=rule,
                passed=True,
                violation_count=0,
                metadata={'note': 'Insufficient data for IQR calculation'}
            )
        
        q1 = float(quartiles[0]['q1'])
        q3 = float(quartiles[0]['q3'])
        median = float(quartiles[0]['median'])
        iqr = q3 - q1
        
        if iqr == 0:
            return self._build_result(
                rule=rule,
                passed=True,
                violation_count=0,
                metadata={'note': 'IQR is zero (no variation in middle 50% of data)'}
            )
        
        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr
        
        # Build outlier condition
        if direction == 'both':
            outlier_condition = f"({column} < {lower_bound} OR {column} > {upper_bound})"
        elif direction == 'high':
            outlier_condition = f"{column} > {upper_bound}"
        else:  # low
            outlier_condition = f"{column} < {lower_bound}"
        
        query = f"""
            SELECT *
            FROM {table}
            WHERE {column} IS NOT NULL
              AND {outlier_condition}
        """
        
        count_query = f"""
            SELECT COUNT(*) as violation_count
            FROM {table}
            WHERE {column} IS NOT NULL
              AND {outlier_condition}
        """
        
        count_result = self.connector.execute_query(count_query)
        violation_count = count_result[0]['violation_count'] if count_result else 0
        
        sample_records = []
        if violation_count > 0:
            try:
                # Order by distance from median
                sample_query = f"{query} ORDER BY ABS({column} - {median}) DESC OFFSET 0 ROWS FETCH NEXT {self.sample_size} ROWS ONLY"
                sample_records = self.connector.execute_query(sample_query)
            except Exception:
                try:
                    sample_query = f"SELECT TOP {self.sample_size} * FROM ({query}) AS t ORDER BY ABS({column} - {median}) DESC"
                    sample_records = self.connector.execute_query(sample_query)
                except Exception:
                    all_records = self.connector.execute_query(query)
                    sample_records = all_records[:self.sample_size]
        
        return self._build_result(
            rule=rule,
            passed=violation_count == 0,
            violation_count=violation_count,
            sample_records=self._serialize_records(sample_records, column),
            query=query.strip(),
            metadata={
                'method': 'iqr',
                'threshold': threshold,
                'direction': direction,
                'q1': round(q1, 4),
                'median': round(median, 4),
                'q3': round(q3, 4),
                'iqr': round(iqr, 4),
                'lower_bound': round(lower_bound, 4) if direction in ['both', 'low'] else None,
                'upper_bound': round(upper_bound, 4) if direction in ['both', 'high'] else None
            }
        )
    
    def _iqr_detection_fallback(
        self,
        table: str,
        column: str,
        threshold: float,
        direction: str,
        rule: dict
    ) -> ValidationResult:
        """Fallback IQR calculation for databases without PERCENTILE_CONT."""
        # This is less efficient but more compatible
        all_values_query = f"""
            SELECT {column} as val
            FROM {table}
            WHERE {column} IS NOT NULL
            ORDER BY {column}
        """
        
        try:
            all_values = self.connector.execute_query(all_values_query)
            values = [float(r['val']) for r in all_values]
            
            if len(values) < 4:
                return self._build_result(
                    rule=rule,
                    passed=True,
                    violation_count=0,
                    metadata={'note': 'Insufficient data for IQR calculation'}
                )
            
            # Calculate quartiles manually
            n = len(values)
            q1_idx = n // 4
            q3_idx = (3 * n) // 4
            
            q1 = values[q1_idx]
            q3 = values[q3_idx]
            median = values[n // 2]
            iqr = q3 - q1
            
            lower_bound = q1 - threshold * iqr
            upper_bound = q3 + threshold * iqr
            
            # Find outliers
            if direction == 'both':
                outliers = [v for v in values if v < lower_bound or v > upper_bound]
            elif direction == 'high':
                outliers = [v for v in values if v > upper_bound]
            else:
                outliers = [v for v in values if v < lower_bound]
            
            return self._build_result(
                rule=rule,
                passed=len(outliers) == 0,
                violation_count=len(outliers),
                sample_records=[{'value': v} for v in outliers[:self.sample_size]],
                metadata={
                    'method': 'iqr (fallback)',
                    'threshold': threshold,
                    'direction': direction,
                    'q1': round(q1, 4),
                    'median': round(median, 4),
                    'q3': round(q3, 4),
                    'iqr': round(iqr, 4),
                    'lower_bound': round(lower_bound, 4),
                    'upper_bound': round(upper_bound, 4)
                }
            )
            
        except Exception as e:
            return self._build_result(
                rule=rule,
                passed=False,
                error_message=f"IQR fallback calculation failed: {str(e)}"
            )
    
    def _serialize_records(self, records: list[dict], value_column: str) -> list[dict]:
        """Convert records to JSON-serializable format with zscore info."""
        cleaned = []
        for record in records:
            cleaned_record = {
                k: self._serialize_value(v)
                for k, v in record.items()
                if not k.startswith('_')
            }
            if '_zscore' in record:
                cleaned_record['zscore'] = round(float(record['_zscore']), 4)
            cleaned.append(cleaned_record)
        return cleaned
    
    def _serialize_value(self, value: Any) -> Any:
        """Convert value to JSON-serializable format."""
        if value is None:
            return None
        if isinstance(value, (int, float, str, bool)):
            return value
        return str(value)
