"""
Unit tests for data quality validators.

Run with: pytest tests/test_validators.py -v
"""

import pytest
import sqlite3
import tempfile
from pathlib import Path

from src.connectors.sqlite import SQLiteConnector
from src.validators import (
    CompletenessValidator,
    ReferentialIntegrityValidator,
    DuplicatesValidator,
    RangeValidator,
    OutliersValidator,
    CustomSQLValidator,
    Severity
)


@pytest.fixture
def test_db():
    """Create temporary test database."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create test tables
    cursor.executescript("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            age INTEGER,
            score REAL
        );
        
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount REAL,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        );
        
        -- Insert test data
        INSERT INTO customers VALUES (1, 'Alice', 'alice@test.com', 30, 85.5);
        INSERT INTO customers VALUES (2, 'Bob', NULL, 25, 92.0);
        INSERT INTO customers VALUES (3, 'Charlie', 'charlie@test.com', NULL, 78.0);
        INSERT INTO customers VALUES (4, 'Alice', 'alice2@test.com', 30, 88.0);  -- Duplicate name+age
        INSERT INTO customers VALUES (5, 'Eve', 'eve@test.com', 150, 95.0);  -- Invalid age
        INSERT INTO customers VALUES (6, 'Frank', 'frank@test.com', 35, 999.0);  -- Outlier score
        
        INSERT INTO orders VALUES (1, 1, 100.00);
        INSERT INTO orders VALUES (2, 2, 200.00);
        INSERT INTO orders VALUES (3, 999, 50.00);  -- Orphan order
    """)
    
    conn.commit()
    conn.close()
    
    yield db_path
    
    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def connector(test_db):
    """Create SQLite connector for test database."""
    config = {'type': 'sqlite', 'path': test_db}
    conn = SQLiteConnector(config)
    conn.connect()
    yield conn
    conn.disconnect()


class TestCompletenessValidator:
    """Tests for completeness validation."""
    
    def test_detects_null_values(self, connector):
        validator = CompletenessValidator(connector, {'sample_size': 5})
        rule = {
            'name': 'test_email_required',
            'type': 'completeness',
            'table': 'customers',
            'columns': ['email'],
            'severity': 'high'
        }
        
        result = validator.validate(rule)
        
        assert result.failed
        assert result.violation_count == 1  # Bob has NULL email
        assert result.severity == Severity.HIGH
    
    def test_passes_when_complete(self, connector):
        validator = CompletenessValidator(connector, {'sample_size': 5})
        rule = {
            'name': 'test_name_required',
            'type': 'completeness',
            'table': 'customers',
            'columns': ['name'],
            'severity': 'high'
        }
        
        result = validator.validate(rule)
        
        assert result.passed
        assert result.violation_count == 0


class TestReferentialIntegrityValidator:
    """Tests for referential integrity validation."""
    
    def test_detects_orphan_records(self, connector):
        validator = ReferentialIntegrityValidator(connector, {'sample_size': 5})
        rule = {
            'name': 'test_valid_customer',
            'type': 'referential_integrity',
            'table': 'orders',
            'column': 'customer_id',
            'reference_table': 'customers',
            'reference_column': 'id',
            'severity': 'critical'
        }
        
        result = validator.validate(rule)
        
        assert result.failed
        assert result.violation_count == 1  # Order with customer_id=999
        assert result.severity == Severity.CRITICAL
        assert 999 in result.metadata.get('orphan_values', [])


class TestDuplicatesValidator:
    """Tests for duplicate detection."""
    
    def test_detects_duplicates(self, connector):
        validator = DuplicatesValidator(connector, {'sample_size': 5})
        rule = {
            'name': 'test_no_duplicates',
            'type': 'duplicates',
            'table': 'customers',
            'columns': ['name', 'age'],
            'severity': 'high'
        }
        
        result = validator.validate(rule)
        
        assert result.failed
        assert result.violation_count >= 1  # Alice with age 30 appears twice


class TestRangeValidator:
    """Tests for range validation."""
    
    def test_detects_out_of_range(self, connector):
        validator = RangeValidator(connector, {'sample_size': 5})
        rule = {
            'name': 'test_valid_age',
            'type': 'range',
            'table': 'customers',
            'column': 'age',
            'min': 0,
            'max': 120,
            'severity': 'medium'
        }
        
        result = validator.validate(rule)
        
        assert result.failed
        assert result.violation_count == 1  # Eve has age 150


class TestOutliersValidator:
    """Tests for outlier detection."""
    
    def test_detects_zscore_outliers(self, connector):
        validator = OutliersValidator(connector, {'sample_size': 5})
        rule = {
            'name': 'test_score_outliers',
            'type': 'outliers',
            'table': 'customers',
            'column': 'score',
            'method': 'zscore',
            'threshold': 2,
            'severity': 'medium'
        }
        
        result = validator.validate(rule)
        
        # Frank's score of 999 should be detected as outlier
        assert result.failed
        assert result.violation_count >= 1


class TestCustomSQLValidator:
    """Tests for custom SQL validation."""
    
    def test_custom_query_finds_violations(self, connector):
        validator = CustomSQLValidator(connector, {'sample_size': 5})
        rule = {
            'name': 'test_custom',
            'type': 'custom_sql',
            'severity': 'high',
            'query': 'SELECT * FROM customers WHERE age > 100'
        }
        
        result = validator.validate(rule)
        
        assert result.failed
        assert result.violation_count == 1  # Eve has age 150
    
    def test_custom_query_passes_when_no_violations(self, connector):
        validator = CustomSQLValidator(connector, {'sample_size': 5})
        rule = {
            'name': 'test_custom_pass',
            'type': 'custom_sql',
            'severity': 'high',
            'query': 'SELECT * FROM customers WHERE name = \'NonExistent\''
        }
        
        result = validator.validate(rule)
        
        assert result.passed
        assert result.violation_count == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
