# Data Quality Monitor

A rule-based data quality framework for SQL databases. You describe quality rules in YAML, it runs them against your database, and you get a severity-ranked report with sample failing records. Reports print to the console and save as JSON, ready to load into Power BI or Excel.

I built it from the checks I run as a database administrator and analyst on a privacy-regulated, multi-organization database: catch missing fields, orphan records, duplicates and outliers *before* they reach a report, and keep an audit trail of what was checked and when.

---

## What it does

- **Rules in YAML, no code changes.** Add or change a check by editing a config file.
- **8 rule types:** completeness, referential integrity, duplicates, uniqueness, range, regex pattern, statistical outliers (z-score / IQR) and custom SQL.
- **SQL Server and SQLite connectors.** SQL Server is the production target (Windows or SQL authentication via ODBC). SQLite runs the self-contained demo and the tests.
- **Severity levels** (critical, high, medium, low) with optional stop-on-critical.
- **Reports:** a console summary plus JSON output (`dq_report_*.json`, `dq_summary_*.json` and a running `dq_history.jsonl`) for dashboards and trend tracking.
- **CLI filters:** run by severity or by rule name, preview with `--dry-run`.

---

## Quick start (demo, no database server needed)

```bash
git clone https://github.com/DivyaChaudhary14/Data-Quality-Monitor.git
cd Data-Quality-Monitor
pip install -r requirements.txt

# Build a sample database with deliberately seeded data problems
python sample_data/create_sample_db.py

# Run all 13 sample rules and save JSON reports
python -m src.main --config config/sample_rules.yaml --output reports/
```

Real output from the demo (excerpt):

```
DATA QUALITY REPORT
================================================================================
Connection: sample_db
Rules executed: 13
Duration: 0.04 seconds

SUMMARY
----------------------------------------
Passed: 3
Failed: 10
  - CRITICAL: 1
  - HIGH: 3
  - MEDIUM: 4
  - LOW: 2

FAILED RULES
----------------------------------------
[CRITICAL] services_valid_client
  Table: services
  Violations: 2 records
  Sample IDs: 9999, 9998

[HIGH] no_duplicate_clients
  Table: clients
  Violations: 1 records
  Sample IDs: 201, 202

[MEDIUM] service_cost_outliers
  Table: services
  Violations: 15 records
  Sample IDs: 44, 79, 60, 23, 116
```

Every failure above is a problem the sample data generator plants on purpose, so the demo shows each rule type catching something.

---

## Using it on your own database

**1. Connections** (`config/connections.yaml`, copied from `connections.yaml.example`):

```yaml
connections:
  production_db:
    type: sqlserver
    host: localhost
    database: your_database
    trusted_connection: true      # or username/password via ${ENV_VARS}
```

**2. Rules** (`config/rules.yaml`, copied from `rules.yaml.example`):

```yaml
rules:
  - name: client_required_fields
    table: clients
    type: completeness
    columns: [first_name, last_name, date_of_birth]
    severity: high

  - name: services_valid_client
    table: services
    type: referential_integrity
    column: client_id
    reference_table: clients
    reference_column: client_id
    severity: critical

  - name: no_duplicate_clients
    table: clients
    type: duplicates
    columns: [first_name, last_name, date_of_birth]
    case_sensitive: false
    severity: high

  - name: cost_outliers
    table: services
    type: outliers
    column: cost
    method: zscore
    threshold: 3
    severity: medium

  - name: active_clients_have_recent_services
    type: custom_sql
    severity: medium
    query: |
      SELECT c.client_id
      FROM clients c
      WHERE c.status = 'active'
        AND c.client_id NOT IN (
          SELECT client_id FROM services
          WHERE service_date >= DATEADD(month, -6, GETDATE())
        )
```

**3. Run:**

```bash
python -m src.main --config config/rules.yaml --connections config/connections.yaml --connection production_db --output reports/
```

To run it on a schedule, call the same command from Windows Task Scheduler, SQL Server Agent or cron.

---

## Rule types

| Type | Checks for | Example |
|---|---|---|
| `completeness` | NULL or empty values | Required client fields |
| `referential_integrity` | Foreign keys with no parent row | Services pointing at deleted clients |
| `duplicates` | Repeated records across one or more columns | The same client entered twice |
| `uniqueness` | Repeated values in one column | Duplicate IDs |
| `range` | Numbers outside min/max bounds | Service hours between 0 and 24 |
| `pattern` | Values not matching a regex | Postal code format |
| `outliers` | Statistical anomalies (z-score or IQR) | Cost spikes |
| `custom_sql` | Any rows your own query returns | Business rules |

On SQL Server, `pattern` translates simple regexes to `LIKE` or `PATINDEX`, which cover a subset of regex. SQLite supports full Python regex.

---

## CLI options

```bash
python -m src.main --config config/rules.yaml                       # run all rules
python -m src.main --config config/rules.yaml --severity critical   # only critical rules
python -m src.main --config config/rules.yaml --rules no_duplicate_clients
python -m src.main --config config/rules.yaml --dry-run             # list rules without running
python -m src.main --config config/rules.yaml --output reports/     # save JSON reports
python -m src.main --config config/rules.yaml --quiet               # file output only
```

---

## Tests

```bash
pytest -v
```

The suite builds a temporary SQLite database with known problems and checks that each validator finds them.

---

## Project structure

```
src/
├── main.py             # CLI entry point
├── config_loader.py    # YAML loading and validation
├── rule_engine.py      # Runs rules, aggregates results
├── validators/         # One module per rule type
├── connectors/         # SQL Server (pyodbc) and SQLite
└── reporters/          # Console and JSON output
config/                 # Example connections, rules and sample rules
sample_data/            # Demo database generator
tests/                  # pytest suite
docs/architecture.md    # Design notes
```

---

## Roadmap

These are planned and not built yet:

- PostgreSQL connector
- Email and Slack alerts for critical failures (`notifications` settings in `rules.yaml.example` are ignored for now)
- A ready-made Power BI report over the JSON history

---

## License

MIT License. See `LICENSE`.
