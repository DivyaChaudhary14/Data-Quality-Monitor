"""
JSON reporter - saves validation results to JSON files.

Outputs are designed for easy ingestion by BI tools like Power BI,
and for programmatic processing.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.validators.base import ValidationReport


class JSONReporter:
    """
    Save validation reports to JSON files.
    
    Output structure is optimized for:
    - Power BI / Tableau ingestion
    - Time series analysis of quality trends
    - Integration with alerting systems
    """
    
    def __init__(self, output_dir: str | Path = "reports"):
        """
        Initialize JSON reporter.
        
        Args:
            output_dir: Directory to save reports (created if needed)
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def save(
        self, 
        report: ValidationReport, 
        filename: str | None = None
    ) -> Path:
        """
        Save report to JSON file.
        
        Args:
            report: ValidationReport to save
            filename: Custom filename (default: auto-generated with timestamp)
            
        Returns:
            Path to saved file
        """
        if filename is None:
            timestamp = report.timestamp.strftime('%Y%m%d_%H%M%S')
            filename = f"dq_report_{timestamp}.json"
        
        filepath = self.output_dir / filename
        
        # Convert report to dict and serialize
        report_dict = self._serialize_report(report)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report_dict, f, indent=2, default=str)
        
        return filepath
    
    def save_summary(
        self,
        report: ValidationReport,
        filename: str | None = None
    ) -> Path:
        """
        Save condensed summary (for dashboards/alerts).
        
        Args:
            report: ValidationReport to summarize
            filename: Custom filename
            
        Returns:
            Path to saved file
        """
        if filename is None:
            timestamp = report.timestamp.strftime('%Y%m%d_%H%M%S')
            filename = f"dq_summary_{timestamp}.json"
        
        filepath = self.output_dir / filename
        
        summary = {
            'timestamp': report.timestamp.isoformat(),
            'connection': report.connection_name,
            'duration_seconds': report.duration_seconds,
            'total_rules': report.total_rules,
            'passed': report.passed_count,
            'failed': report.failed_count,
            'failures_by_severity': report.failures_by_severity(),
            'critical_count': len(report.critical_failures),
            'failed_rules': [
                {
                    'name': r.rule_name,
                    'severity': r.severity.value,
                    'table': r.table,
                    'violation_count': r.violation_count
                }
                for r in report.failed_results
            ]
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        
        return filepath
    
    def append_to_history(
        self,
        report: ValidationReport,
        history_file: str = "dq_history.jsonl"
    ) -> Path:
        """
        Append summary to JSONL history file (one record per line).
        
        Useful for tracking quality trends over time.
        
        Args:
            report: ValidationReport to append
            history_file: Name of history file
            
        Returns:
            Path to history file
        """
        filepath = self.output_dir / history_file
        
        # Create compact summary record
        record = {
            'timestamp': report.timestamp.isoformat(),
            'connection': report.connection_name,
            'total': report.total_rules,
            'passed': report.passed_count,
            'failed': report.failed_count,
            'critical': len(report.critical_failures),
            'high': len(report.high_failures),
            'duration': report.duration_seconds
        }
        
        # Append as single line
        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(json.dumps(record) + '\n')
        
        return filepath
    
    def _serialize_report(self, report: ValidationReport) -> dict:
        """Convert ValidationReport to JSON-serializable dict."""
        return {
            'metadata': {
                'connection_name': report.connection_name,
                'timestamp': report.timestamp.isoformat(),
                'duration_seconds': report.duration_seconds,
                'settings': report.settings
            },
            'summary': {
                'total_rules': report.total_rules,
                'passed_count': report.passed_count,
                'failed_count': report.failed_count,
                'failures_by_severity': report.failures_by_severity()
            },
            'results': [
                self._serialize_result(r) for r in report.results
            ]
        }
    
    def _serialize_result(self, result) -> dict:
        """Convert ValidationResult to dict."""
        return {
            'rule_name': result.rule_name,
            'rule_type': result.rule_type,
            'severity': result.severity.value,
            'passed': result.passed,
            'violation_count': result.violation_count,
            'table': result.table,
            'description': result.description,
            'execution_time_ms': result.execution_time_ms,
            'error_message': result.error_message,
            'sample_records': result.sample_records,
            'query': result.query,
            'metadata': result.metadata
        }


def save_report(
    report: ValidationReport,
    output_dir: str = "reports"
) -> tuple[Path, Path]:
    """
    Convenience function to save both full report and summary.
    
    Args:
        report: ValidationReport to save
        output_dir: Output directory
        
    Returns:
        Tuple of (full_report_path, summary_path)
    """
    reporter = JSONReporter(output_dir)
    full_path = reporter.save(report)
    summary_path = reporter.save_summary(report)
    reporter.append_to_history(report)
    return full_path, summary_path
