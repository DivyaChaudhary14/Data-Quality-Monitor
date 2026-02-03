"""
Rule engine - orchestrates validation rule execution.

Manages the execution of validation rules, handling parallel execution,
error handling, and result aggregation.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Callable

from src.config_loader import ConfigLoader
from src.connectors import create_connector, BaseConnector
from src.validators import (
    VALIDATOR_REGISTRY,
    ValidationResult,
    ValidationReport,
    Severity,
    BaseValidator
)


class RuleEngine:
    """
    Orchestrates execution of data quality validation rules.
    
    Handles:
    - Loading and validating configuration
    - Creating database connections
    - Executing validators (optionally in parallel)
    - Aggregating results into reports
    - Stopping on critical failures if configured
    """
    
    def __init__(self, config: ConfigLoader, connection_name: str | None = None):
        """
        Initialize rule engine.
        
        Args:
            config: Loaded configuration
            connection_name: Name of connection to use (default: first available)
        """
        self.config = config
        self.settings = config.get_settings()
        
        # Determine connection to use
        if connection_name:
            self.connection_name = connection_name
        else:
            # Use first available connection
            names = config.get_connection_names()
            if not names:
                raise ValueError("No database connections configured")
            self.connection_name = names[0]
        
        self._connector: BaseConnector | None = None
        self._progress_callback: Callable[[str, int, int], None] | None = None
    
    def set_progress_callback(self, callback: Callable[[str, int, int], None]) -> None:
        """
        Set callback for progress updates.
        
        Args:
            callback: Function(rule_name, current, total) called after each rule
        """
        self._progress_callback = callback
    
    def run(
        self,
        rules: list[dict] | None = None,
        severity_filter: list[str] | None = None
    ) -> ValidationReport:
        """
        Execute validation rules and return report.
        
        Args:
            rules: Specific rules to run (default: all configured rules)
            severity_filter: Only run rules with these severity levels
            
        Returns:
            ValidationReport with all results
        """
        start_time = time.time()
        
        # Get rules to execute
        if rules is None:
            rules = self.config.get_rules()
        
        # Apply severity filter
        if severity_filter:
            filter_set = set(s.lower() for s in severity_filter)
            rules = [r for r in rules if r['severity'].lower() in filter_set]
        
        if not rules:
            return ValidationReport(
                connection_name=self.connection_name,
                timestamp=datetime.now(),
                duration_seconds=0,
                results=[],
                settings=self.settings
            )
        
        # Create connector
        conn_config = self.config.get_connection(self.connection_name)
        self._connector = create_connector(conn_config)
        
        try:
            self._connector.connect()
            
            # Execute rules
            if self.settings.get('parallel_execution', True):
                results = self._run_parallel(rules)
            else:
                results = self._run_sequential(rules)
            
            duration = time.time() - start_time
            
            return ValidationReport(
                connection_name=self.connection_name,
                timestamp=datetime.now(),
                duration_seconds=round(duration, 2),
                results=results,
                settings=self.settings
            )
            
        finally:
            self._connector.disconnect()
    
    def _run_sequential(self, rules: list[dict]) -> list[ValidationResult]:
        """Execute rules one at a time."""
        results = []
        stop_on_critical = self.settings.get('stop_on_critical', False)
        
        for i, rule in enumerate(rules):
            result = self._execute_rule(rule)
            results.append(result)
            
            # Progress callback
            if self._progress_callback:
                self._progress_callback(rule['name'], i + 1, len(rules))
            
            # Check for critical failure
            if stop_on_critical and result.failed and result.severity == Severity.CRITICAL:
                break
        
        return results
    
    def _run_parallel(self, rules: list[dict]) -> list[ValidationResult]:
        """Execute rules in parallel using thread pool."""
        max_workers = self.settings.get('max_workers', 4)
        stop_on_critical = self.settings.get('stop_on_critical', False)
        
        results = []
        completed = 0
        should_stop = False
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all rules
            future_to_rule = {
                executor.submit(self._execute_rule, rule): rule
                for rule in rules
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_rule):
                if should_stop:
                    future.cancel()
                    continue
                    
                rule = future_to_rule[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    completed += 1
                    if self._progress_callback:
                        self._progress_callback(rule['name'], completed, len(rules))
                    
                    # Check for critical failure
                    if stop_on_critical and result.failed and result.severity == Severity.CRITICAL:
                        should_stop = True
                        
                except Exception as e:
                    # Rule execution failed unexpectedly
                    results.append(ValidationResult(
                        rule_name=rule['name'],
                        rule_type=rule['type'],
                        severity=Severity.from_string(rule['severity']),
                        passed=False,
                        error_message=f"Execution failed: {str(e)}"
                    ))
                    completed += 1
        
        return results
    
    def _execute_rule(self, rule: dict) -> ValidationResult:
        """Execute a single validation rule."""
        rule_type = rule['type']
        
        # Get validator class
        validator_class = VALIDATOR_REGISTRY.get(rule_type)
        if not validator_class:
            return ValidationResult(
                rule_name=rule['name'],
                rule_type=rule_type,
                severity=Severity.from_string(rule['severity']),
                passed=False,
                error_message=f"Unknown validator type: {rule_type}"
            )
        
        # Create validator and execute
        try:
            validator = validator_class(self._connector, self.settings)
            start = time.time()
            result = validator.validate(rule)
            result.execution_time_ms = (time.time() - start) * 1000
            return result
        except Exception as e:
            return ValidationResult(
                rule_name=rule['name'],
                rule_type=rule_type,
                severity=Severity.from_string(rule['severity']),
                passed=False,
                error_message=f"Validator error: {str(e)}"
            )
    
    def run_single_rule(self, rule: dict) -> ValidationResult:
        """
        Execute a single rule (for testing or ad-hoc validation).
        
        Args:
            rule: Rule configuration dictionary
            
        Returns:
            ValidationResult
        """
        conn_config = self.config.get_connection(self.connection_name)
        self._connector = create_connector(conn_config)
        
        try:
            self._connector.connect()
            return self._execute_rule(rule)
        finally:
            self._connector.disconnect()
    
    def dry_run(self) -> list[dict]:
        """
        Preview what rules would be executed without running them.
        
        Returns:
            List of rule summaries
        """
        rules = self.config.get_rules()
        return [
            {
                'name': r['name'],
                'type': r['type'],
                'severity': r['severity'],
                'table': r.get('table', 'N/A'),
                'description': r.get('description', '')
            }
            for r in rules
        ]
