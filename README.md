# Data Quality Monitor

A configurable, database-agnostic data quality monitoring framework that automates validation checks, detects anomalies, and generates alerts.

Built to solve real operational problems: reducing manual data review, catching integrity issues before they impact reporting, and providing audit trails for data governance.

## Features

- **Rule-based validation**: Define quality rules in YAML—no code changes required
- **Multi-database support**: SQL Server, PostgreSQL, SQLite, CSV files
- **15+ built-in validators**: Completeness, referential integrity, duplicates, outliers, patterns, and more
- **Automated alerting**: Email, Slack, or custom webhook notifications
- **Dashboard-ready output**: Export results for Power BI, Tableau, or built-in Streamlit dashboard
- **Scheduling**: Built-in scheduler or integrate with cron/Task Scheduler
- **Detailed logging**: Full audit trail for compliance and debugging

## Quick Start

### Installation

```bash
git clone https://github.com/yourusername/data-quality-monitor.git
cd data-quality-monitor
pip install -r requirements.txt
```

### Basic Usage

1. **Configure your database connection** in `config/connections.yaml`:

```yaml
connections:
  production_db:
    type: sqlserver
    host: localhost
    database: your_database
    trusted_connection: true
```

2. **Define validation rules** in `config/rules.yaml`:

```yaml
rules:
  - name: customers_email_required
    table: customers
    type: completeness
    columns: [email]
    severity: high
    
  - name: orders_valid_customer
    table: orders
    type: referential_integrity
    column: customer_id
    reference_table: customers
    reference_column: id
    severity: critical
```

3. **Run validation**:

```bash
python -m src.main --config config/rules.yaml --connection production_db
```

### Output

```
================================================================================
DATA QUALITY REPORT - 2025-02-03 14:30:00
================================================================================

Connection: production_db
Rules executed: 12
Duration: 4.2 seconds

SUMMARY
-------
✓ Passed: 9
✗ Failed: 3
  - CRITICAL: 1
  - HIGH: 1
  - MEDIUM: 1

FAILED RULES
------------
[CRITICAL] orders_valid_customer
  Table: orders
  Issue: 23 records with invalid customer_id references
  Query: SELECT * FROM orders WHERE customer_id NOT IN (SELECT id FROM customers)

[HIGH] customers_email_required
  Table: customers  
  Issue: 156 records with NULL email
  Sample IDs: 1042, 1089, 1156, 1203, 1287

[MEDIUM] products_price_outliers
  Table: products
  Issue: 4 records with price > 3 standard deviations from mean
  Sample IDs: 892, 1204, 1567, 2001

Full report saved to: reports/dq_report_20250203_143000.json
```

## Validation Types

| Type | Description | Example Use Case |
|------|-------------|------------------|
| `completeness` | Check for NULL or empty values | Required fields validation |
| `referential_integrity` | Verify foreign key relationships | Order-customer linkage |
| `duplicates` | Find duplicate records | Prevent double-entry |
| `uniqueness` | Ensure column uniqueness | Email, SSN validation |
| `range` | Check numeric bounds | Age 0-120, price > 0 |
| `pattern` | Regex pattern matching | Phone format, postal codes |
| `date_range` | Validate date boundaries | No future dates |
| `outliers` | Statistical anomaly detection | Price spikes, unusual quantities |
| `cross_field` | Multi-column logic | end_date > start_date |
| `custom_sql` | Your own validation query | Complex business rules |

## Configuration Reference

### connections.yaml

```yaml
connections:
  # SQL Server with Windows Authentication
  sqlserver_prod:
    type: sqlserver
    host: server.domain.com
    database: production
    trusted_connection: true
    
  # SQL Server with SQL Authentication
  sqlserver_dev:
    type: sqlserver
    host: localhost
    database: development
    username: ${DB_USER}      # Environment variable
    password: ${DB_PASSWORD}
    
  # PostgreSQL
  postgres_analytics:
    type: postgres
    host: analytics.internal
    port: 5432
    database: analytics
    username: readonly
    password: ${POSTGRES_PASSWORD}
    
  # SQLite (for testing)
  local_test:
    type: sqlite
    path: ./sample_data/test.db
    
  # CSV files
  csv_source:
    type: csv
    path: ./data/
```

### rules.yaml

```yaml
# Global settings
settings:
  stop_on_critical: false
  sample_size: 5
  parallel_execution: true
  max_workers: 4

# Notification settings
notifications:
  email:
    enabled: true
    smtp_server: smtp.company.com
    recipients: [data-team@company.com]
    on_severity: [critical, high]
  slack:
    enabled: false
    webhook_url: ${SLACK_WEBHOOK}
    channel: "#data-alerts"

# Validation rules
rules:
  # Completeness check
  - name: client_required_fields
    table: clients
    type: completeness
    columns: [first_name, last_name, date_of_birth]
    severity: high
    description: "Core client fields must be populated"
    
  # Referential integrity
  - name: services_valid_client
    table: services
    type: referential_integrity
    column: client_id
    reference_table: clients
    reference_column: client_id
    severity: critical
    
  # Duplicate detection
  - name: no_duplicate_clients
    table: clients
    type: duplicates
    columns: [first_name, last_name, date_of_birth]
    severity: high
    
  # Range validation
  - name: valid_service_hours
    table: services
    type: range
    column: hours
    min: 0
    max: 24
    severity: medium
    
  # Pattern matching
  - name: valid_postal_code
    table: clients
    type: pattern
    column: postal_code
    pattern: "^[A-Z]\\d[A-Z] ?\\d[A-Z]\\d$"
    severity: low
    description: "Canadian postal code format"
    
  # Outlier detection
  - name: cost_outliers
    table: services
    type: outliers
    column: cost
    method: zscore
    threshold: 3
    severity: medium
    
  # Cross-field validation
  - name: valid_date_sequence
    table: programs
    type: cross_field
    expression: "end_date >= start_date"
    severity: high
    
  # Custom SQL
  - name: active_clients_have_services
    type: custom_sql
    severity: medium
    query: |
      SELECT c.client_id, c.first_name, c.last_name
      FROM clients c
      WHERE c.status = 'active'
        AND c.client_id NOT IN (
          SELECT DISTINCT client_id 
          FROM services 
          WHERE service_date >= DATEADD(month, -6, GETDATE())
        )
    description: "Active clients should have services in last 6 months"
```

## Project Structure

```
data-quality-monitor/
├── README.md
├── requirements.txt
├── setup.py
├── config/
│   ├── connections.yaml.example
│   └── rules.yaml.example
├── src/
│   ├── __init__.py
│   ├── main.py                 # Entry point
│   ├── config_loader.py        # YAML parsing, env substitution
│   ├── rule_engine.py          # Rule orchestration
│   ├── validators/
│   │   ├── __init__.py
│   │   ├── base.py             # Abstract validator
│   │   ├── completeness.py
│   │   ├── referential.py
│   │   ├── duplicates.py
│   │   ├── uniqueness.py
│   │   ├── range_check.py
│   │   ├── pattern.py
│   │   ├── date_range.py
│   │   ├── outliers.py
│   │   ├── cross_field.py
│   │   └── custom_sql.py
│   ├── connectors/
│   │   ├── __init__.py
│   │   ├── base.py             # Abstract connector
│   │   ├── sqlserver.py
│   │   ├── postgres.py
│   │   ├── sqlite.py
│   │   └── csv_connector.py
│   ├── reporters/
│   │   ├── __init__.py
│   │   ├── console.py
│   │   ├── json_report.py
│   │   ├── email_alert.py
│   │   └── slack_alert.py
│   └── scheduler.py
├── sql/
│   └── sample_rules.sql        # T-SQL examples
├── tests/
│   ├── __init__.py
│   ├── test_validators.py
│   ├── test_connectors.py
│   └── test_config.py
├── docs/
│   ├── architecture.md
│   ├── adding_validators.md
│   └── deployment.md
└── sample_data/
    ├── create_sample_db.py
    └── sample_data.csv
```

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Config Loader  │────▶│   Rule Engine   │────▶│    Reporters    │
│  (YAML + ENV)   │     │  (Orchestrator) │     │ (Console, JSON) │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                    ┌────────────┼────────────┐
                    ▼            ▼            ▼
            ┌───────────┐ ┌───────────┐ ┌───────────┐
            │ Validator │ │ Validator │ │ Validator │
            │   Pool    │ │   Pool    │ │   Pool    │
            └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
                  │             │             │
                  └─────────────┼─────────────┘
                                ▼
                    ┌─────────────────────┐
                    │   DB Connectors     │
                    │ (SQL Server, PG...) │
                    └─────────────────────┘
```

## Advanced Usage

### Running Specific Rules

```bash
# Run only critical rules
python -m src.main --config config/rules.yaml --severity critical

# Run specific rules by name
python -m src.main --config config/rules.yaml --rules client_required_fields,no_duplicate_clients

# Dry run (show what would execute)
python -m src.main --config config/rules.yaml --dry-run
```

### Scheduling

```bash
# Run with built-in scheduler (every 6 hours)
python -m src.scheduler --config config/rules.yaml --interval 6h

# Or use cron (Linux)
0 */6 * * * cd /path/to/data-quality-monitor && python -m src.main --config config/rules.yaml

# Or Task Scheduler (Windows)
schtasks /create /tn "DataQualityCheck" /tr "python -m src.main --config config/rules.yaml" /sc hourly /mo 6
```

### Extending with Custom Validators

```python
# src/validators/my_custom_validator.py
from src.validators.base import BaseValidator, ValidationResult

class MyCustomValidator(BaseValidator):
    """Check that manager_id references a senior employee."""
    
    validator_type = "manager_hierarchy"
    
    def validate(self, rule: dict) -> ValidationResult:
        query = f"""
            SELECT e.employee_id, e.name, e.manager_id
            FROM {rule['table']} e
            LEFT JOIN {rule['table']} m ON e.manager_id = m.employee_id
            WHERE e.manager_id IS NOT NULL
              AND (m.employee_id IS NULL OR m.level < e.level)
        """
        
        violations = self.connector.execute(query)
        
        return ValidationResult(
            rule_name=rule['name'],
            passed=len(violations) == 0,
            violation_count=len(violations),
            sample_records=violations[:self.sample_size],
            query=query
        )
```

## Integration with Power BI

The JSON report output is designed for easy Power BI ingestion:

1. Point Power BI to the `reports/` directory
2. Use the provided Power Query template in `docs/powerbi_template.pbit`
3. Build dashboards showing:
   - Rule pass/fail trends over time
   - Violation counts by severity
   - Most problematic tables/columns
   - Resolution time tracking

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Background

This project is inspired by real-world data quality challenges in managing large-scale databases with hundreds of tables and strict compliance requirements. The goal is to make data quality monitoring accessible, configurable, and actionable.
#   D a t a - Q u a l i t y - M o n i t o r  
 