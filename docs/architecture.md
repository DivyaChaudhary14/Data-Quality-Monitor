# Architecture Overview

## System Design

The Data Quality Monitor follows a modular, extensible architecture that separates concerns across distinct layers:

```
┌─────────────────────────────────────────────────────────────┐
│                      CLI / Entry Point                       │
│                        (src/main.py)                         │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    Configuration Layer                       │
│                   (src/config_loader.py)                     │
│  • YAML parsing                                              │
│  • Environment variable substitution                         │
│  • Validation of rules and connections                       │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                     Rule Engine                              │
│                   (src/rule_engine.py)                       │
│  • Orchestrates validation execution                         │
│  • Manages parallel/sequential execution                     │
│  • Aggregates results                                        │
│  • Handles critical failure stopping                         │
└──────────┬──────────────────────────────────┬───────────────┘
           │                                  │
┌──────────▼──────────┐          ┌───────────▼────────────────┐
│     Validators      │          │        Connectors          │
│  (src/validators/)  │          │     (src/connectors/)      │
│                     │          │                            │
│  • Completeness     │◄────────►│  • SQL Server              │
│  • Referential      │          │  • PostgreSQL              │
│  • Duplicates       │          │  • SQLite                  │
│  • Range            │          │  • CSV                     │
│  • Pattern          │          │                            │
│  • Outliers         │          │                            │
│  • Custom SQL       │          │                            │
└─────────────────────┘          └────────────────────────────┘
           │
┌──────────▼──────────────────────────────────────────────────┐
│                       Reporters                              │
│                    (src/reporters/)                          │
│  • Console (Rich/plain text)                                 │
│  • JSON (for BI tools)                                       │
│  • History tracking (JSONL)                                  │
└─────────────────────────────────────────────────────────────┘
```

## Component Details

### Configuration Layer

The configuration system uses YAML files with these features:

1. **Environment Variable Substitution**: Use `${VAR_NAME}` syntax for sensitive values
2. **Validation**: Rules and connections are validated before execution
3. **Flexible Structure**: Connections can be in a separate file or embedded

```yaml
# Environment variables are automatically substituted
connections:
  prod:
    password: ${DB_PASSWORD}  # Reads from environment
```

### Rule Engine

The rule engine is the central orchestrator:

- **Parallel Execution**: Uses ThreadPoolExecutor for concurrent rule evaluation
- **Progress Tracking**: Callback system for real-time progress updates
- **Graceful Failure Handling**: Continues after individual rule errors
- **Critical Stop**: Can halt execution on critical failures

### Validators

Each validator follows a consistent interface:

```python
class BaseValidator(ABC):
    @abstractmethod
    def validate(self, rule: dict) -> ValidationResult:
        """Execute validation and return result."""
        pass
```

**Validator Types:**

| Validator | Purpose | Key Config |
|-----------|---------|------------|
| Completeness | Check for NULL/empty | `columns`, `check_empty_strings` |
| Referential | FK integrity | `reference_table`, `reference_column` |
| Duplicates | Find repeated records | `columns`, `case_sensitive` |
| Range | Numeric bounds | `min`, `max`, `inclusive` |
| Pattern | Regex matching | `pattern`, `inverse` |
| Outliers | Statistical anomalies | `method`, `threshold`, `direction` |
| Custom SQL | Flexible queries | `query`, `count_query` |

### Connectors

Database connectors provide a unified interface:

```python
class BaseConnector(ABC):
    @abstractmethod
    def connect(self) -> None: ...
    
    @abstractmethod
    def execute_query(self, query: str) -> list[dict]: ...
    
    @abstractmethod
    def test_connection(self) -> bool: ...
```

The SQLite connector includes SQL dialect adaptation for cross-database compatibility.

### Reporters

Reporters consume `ValidationReport` objects and produce output:

- **Console**: Rich terminal formatting with severity coloring
- **JSON**: Structured output for BI tool ingestion
- **History**: Append-only JSONL for trend tracking

## Data Flow

```
1. CLI parses arguments
       │
2. ConfigLoader reads YAML files
       │
3. RuleEngine initializes with config
       │
4. Connector establishes database connection
       │
5. For each rule:
   │   ├─ Select appropriate Validator
   │   ├─ Execute validation query
   │   └─ Collect ValidationResult
       │
6. Aggregate results into ValidationReport
       │
7. Reporters output to console/files
```

## Extension Points

### Adding a New Validator

1. Create class in `src/validators/` extending `BaseValidator`
2. Implement `validate()` method
3. Register in `VALIDATOR_REGISTRY` in `__init__.py`

```python
class MyValidator(BaseValidator):
    validator_type = "my_check"
    
    def validate(self, rule: dict) -> ValidationResult:
        # Your validation logic
        return self._build_result(rule, passed=True)
```

### Adding a New Connector

1. Create class in `src/connectors/` extending `BaseConnector`
2. Implement required methods
3. Register in `CONNECTOR_REGISTRY` in `__init__.py`

### Adding a New Reporter

1. Create class in `src/reporters/`
2. Accept `ValidationReport` and produce output
3. Export from `__init__.py`

## Performance Considerations

- **Counting First**: Validators run COUNT queries before fetching samples
- **Pagination**: Large result sets are sampled, not fully loaded
- **Connection Reuse**: Single connection for all rules in a run
- **Parallel Execution**: Configurable worker pool for concurrent rules

## Error Handling

- Configuration errors fail fast with clear messages
- Individual rule failures don't stop the overall run (unless critical + stop_on_critical)
- Query errors are captured in ValidationResult.error_message
- All exceptions are logged with context
