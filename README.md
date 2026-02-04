# Data Quality Monitor

A configurable, database-agnostic data quality monitoring framework that automates validation checks, detects anomalies, and generates alerts.

Built to solve real operational problems: reducing manual data review, catching integrity issues before they impact reporting, and providing audit trails for data governance.

---

## Features

- **Rule-based validation** — Define quality rules in YAML, no code changes required
- **Multi-database support** — SQL Server, PostgreSQL, SQLite, CSV files
- **15+ built-in validators** — Completeness, referential integrity, duplicates, outliers, patterns, and more
- **Automated alerting** — Email, Slack, or custom webhook notifications
- **Dashboard-ready output** — Export results for Power BI, Tableau, or built-in Streamlit dashboard
- **Scheduling** — Built-in scheduler or integrate with cron/Task Scheduler
- **Detailed logging** — Full audit trail for compliance and debugging

---

## Quick Start

### Installation

```bash
git clone https://github.com/yourusername/data-quality-monitor.git
cd data-quality-monitor
pip install -r requirements.txt
```

### Basic Usage

**Step 1:** Configure your database connection in `config/connections.yaml`

```yaml
connections:
  production_db:
    type: sqlserver
    host: localhost
    database: your_database
    trusted_connection: true
```

**Step 2:** Define validation rules in `config/rules.yaml`

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

**Step 3:** Run validation

```bash
python -m src.main --config config/rules.yaml --connection production_db
```

---

## Sample Output

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

[HIGH] customers_email_required
  Table: customers
  Issue: 156 records with NULL email
  Sample IDs: 1042, 1089, 1156, 1203, 1287

[MEDIUM] products_price_outliers
  Table: products
  Issue: 4 records with price > 3 standard deviations from mean

Full report saved to: reports/dq_report_20250203_143000.json
```

---

## Validation Types

| Type                    | Description                      | Example Use Case       |
| ----------------------- | -------------------------------- | ---------------------- |
| `completeness`          | Check for NULL or empty values   | Required fields        |
| `referential_integrity` | Verify foreign key relationships | Order-customer linkage |
| `duplicates`            | Find duplicate records           | Prevent double-entry   |
| `uniqueness`            | Ensure column uniqueness         | Email, SSN validation  |
| `range`                 | Check numeric bounds             | Age 0-120, price > 0   |
| `pattern`               | Regex pattern matching           | Phone, postal codes    |
| `outliers`              | Statistical anomaly detection    | Price spikes           |
| `custom_sql`            | Your own validation query        | Complex business rules |

---

## Project Structure

```
data-quality-monitor/
├── README.md
├── requirements.txt
├── LICENSE
├── config/
│   ├── connections.yaml.example
│   ├── rules.yaml.example
│   └── sample_rules.yaml
├── src/
│   ├── main.py              # CLI entry point
│   ├── config_loader.py     # YAML parsing
│   ├── rule_engine.py       # Orchestration
│   ├── validators/          # Validation logic
│   │   ├── completeness.py
│   │   ├── referential.py
│   │   ├── duplicates.py
│   │   ├── range_check.py
│   │   ├── pattern.py
│   │   ├── outliers.py
│   │   └── custom_sql.py
│   ├── connectors/          # Database connections
│   │   ├── sqlserver.py
│   │   └── sqlite.py
│   └── reporters/           # Output formatting
│       ├── console.py
│       └── json_report.py
├── tests/
│   └── test_validators.py
├── docs/
│   └── architecture.md
└── sample_data/
    └── create_sample_db.py
```

---

## Configuration Examples

### Database Connections

```yaml
connections:
  # SQL Server with Windows Auth
  sqlserver_prod:
    type: sqlserver
    host: server.domain.com
    database: production
    trusted_connection: true

  # SQL Server with credentials
  sqlserver_dev:
    type: sqlserver
    host: localhost
    database: development
    username: ${DB_USER}
    password: ${DB_PASSWORD}

  # SQLite for testing
  local_test:
    type: sqlite
    path: ./sample_data/test.db
```

### Validation Rules

```yaml
settings:
  stop_on_critical: false
  sample_size: 5
  parallel_execution: true

rules:
  # Check required fields
  - name: client_required_fields
    table: clients
    type: completeness
    columns: [first_name, last_name, date_of_birth]
    severity: high

  # Verify foreign keys
  - name: services_valid_client
    table: services
    type: referential_integrity
    column: client_id
    reference_table: clients
    reference_column: client_id
    severity: critical

  # Detect duplicates
  - name: no_duplicate_clients
    table: clients
    type: duplicates
    columns: [first_name, last_name, date_of_birth]
    severity: high

  # Validate ranges
  - name: valid_service_hours
    table: services
    type: range
    column: hours
    min: 0
    max: 24
    severity: medium

  # Statistical outliers
  - name: cost_outliers
    table: services
    type: outliers
    column: cost
    method: zscore
    threshold: 3
    severity: medium

  # Custom business logic
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
```

---

## CLI Options

```bash
# Run all rules
python -m src.main --config config/rules.yaml

# Run only critical rules
python -m src.main --config config/rules.yaml --severity critical

# Dry run (preview without executing)
python -m src.main --config config/rules.yaml --dry-run

# Save report to file
python -m src.main --config config/rules.yaml --output reports/

# Specify connection
python -m src.main --config config/rules.yaml --connection prod_db
```

---

## Demo

```bash
# Generate test database with sample data
python sample_data/create_sample_db.py

# Run validation
python -m src.main --config config/sample_rules.yaml
```

---

## License

MIT License - see LICENSE file for details.
