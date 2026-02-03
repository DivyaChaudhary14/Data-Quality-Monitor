"""
Console reporter - displays validation results in the terminal.

Uses the Rich library for formatted, colorful output when available,
with fallback to plain text.
"""

from datetime import datetime
from typing import TextIO
import sys

from src.validators.base import ValidationReport, ValidationResult, Severity

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class ConsoleReporter:
    """
    Report validation results to the console.
    
    Provides both rich formatted output (when Rich library available)
    and plain text fallback.
    """
    
    SEVERITY_COLORS = {
        Severity.CRITICAL: 'red',
        Severity.HIGH: 'orange1',
        Severity.MEDIUM: 'yellow',
        Severity.LOW: 'blue'
    }
    
    SEVERITY_SYMBOLS = {
        Severity.CRITICAL: '🔴',
        Severity.HIGH: '🟠',
        Severity.MEDIUM: '🟡',
        Severity.LOW: '🔵'
    }
    
    def __init__(self, use_rich: bool = True, output: TextIO | None = None):
        """
        Initialize console reporter.
        
        Args:
            use_rich: Use Rich formatting if available
            output: Output stream (default: stdout)
        """
        self.use_rich = use_rich and RICH_AVAILABLE
        self.output = output or sys.stdout
        
        if self.use_rich:
            self.console = Console(file=self.output)
    
    def report(self, report: ValidationReport) -> None:
        """Display validation report."""
        if self.use_rich:
            self._report_rich(report)
        else:
            self._report_plain(report)
    
    def _report_rich(self, report: ValidationReport) -> None:
        """Display report using Rich formatting."""
        # Header
        self.console.print()
        self.console.rule("[bold blue]DATA QUALITY REPORT[/bold blue]")
        self.console.print()
        
        # Summary info
        info_text = Text()
        info_text.append(f"Connection: ", style="dim")
        info_text.append(f"{report.connection_name}\n", style="cyan")
        info_text.append(f"Timestamp: ", style="dim")
        info_text.append(f"{report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n", style="cyan")
        info_text.append(f"Duration: ", style="dim")
        info_text.append(f"{report.duration_seconds:.2f} seconds\n", style="cyan")
        info_text.append(f"Rules Executed: ", style="dim")
        info_text.append(f"{report.total_rules}", style="cyan")
        self.console.print(Panel(info_text, title="Execution Info", border_style="blue"))
        
        # Summary stats
        self._print_summary_rich(report)
        
        # Failed rules details
        if report.failed_count > 0:
            self._print_failures_rich(report)
        
        # Passed rules (brief)
        passed_results = [r for r in report.results if r.passed]
        if passed_results:
            self.console.print()
            self.console.print("[green]✓ Passed Rules[/green]")
            for result in passed_results:
                self.console.print(f"  [dim]• {result.rule_name}[/dim]")
        
        self.console.print()
        self.console.rule()
    
    def _print_summary_rich(self, report: ValidationReport) -> None:
        """Print summary statistics with Rich."""
        self.console.print()
        
        # Create summary table
        table = Table(title="Summary", box=box.ROUNDED)
        table.add_column("Status", style="bold")
        table.add_column("Count", justify="right")
        
        table.add_row("[green]✓ Passed[/green]", str(report.passed_count))
        table.add_row("[red]✗ Failed[/red]", str(report.failed_count))
        
        # Breakdown by severity
        failures = report.failures_by_severity()
        if failures['critical'] > 0:
            table.add_row("  [red]• Critical[/red]", str(failures['critical']))
        if failures['high'] > 0:
            table.add_row("  [orange1]• High[/orange1]", str(failures['high']))
        if failures['medium'] > 0:
            table.add_row("  [yellow]• Medium[/yellow]", str(failures['medium']))
        if failures['low'] > 0:
            table.add_row("  [blue]• Low[/blue]", str(failures['low']))
        
        self.console.print(table)
    
    def _print_failures_rich(self, report: ValidationReport) -> None:
        """Print failed rule details with Rich."""
        self.console.print()
        self.console.print("[bold red]Failed Rules[/bold red]")
        self.console.print()
        
        for result in report.failed_results:
            severity_color = self.SEVERITY_COLORS.get(result.severity, 'white')
            symbol = self.SEVERITY_SYMBOLS.get(result.severity, '•')
            
            # Rule header
            self.console.print(
                f"{symbol} [{severity_color}][{result.severity.value.upper()}][/{severity_color}] "
                f"[bold]{result.rule_name}[/bold]"
            )
            
            # Details
            if result.table:
                self.console.print(f"  [dim]Table:[/dim] {result.table}")
            
            if result.error_message:
                self.console.print(f"  [red]Error:[/red] {result.error_message}")
            else:
                self.console.print(f"  [dim]Violations:[/dim] {result.violation_count:,} records")
            
            if result.description:
                self.console.print(f"  [dim]Description:[/dim] {result.description}")
            
            # Sample records
            if result.sample_records:
                self.console.print(f"  [dim]Sample violations:[/dim]")
                for i, record in enumerate(result.sample_records[:3]):
                    # Show key fields only
                    preview = self._format_record_preview(record)
                    self.console.print(f"    {i+1}. {preview}")
            
            # Query (truncated)
            if result.query:
                query_preview = result.query[:200].replace('\n', ' ')
                if len(result.query) > 200:
                    query_preview += "..."
                self.console.print(f"  [dim]Query:[/dim] {query_preview}")
            
            self.console.print()
    
    def _report_plain(self, report: ValidationReport) -> None:
        """Display report using plain text."""
        print("=" * 80, file=self.output)
        print(f"DATA QUALITY REPORT - {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}", file=self.output)
        print("=" * 80, file=self.output)
        print(file=self.output)
        
        print(f"Connection: {report.connection_name}", file=self.output)
        print(f"Rules executed: {report.total_rules}", file=self.output)
        print(f"Duration: {report.duration_seconds:.2f} seconds", file=self.output)
        print(file=self.output)
        
        # Summary
        print("SUMMARY", file=self.output)
        print("-" * 40, file=self.output)
        print(f"Passed: {report.passed_count}", file=self.output)
        print(f"Failed: {report.failed_count}", file=self.output)
        
        failures = report.failures_by_severity()
        if any(failures.values()):
            for severity, count in failures.items():
                if count > 0:
                    print(f"  - {severity.upper()}: {count}", file=self.output)
        
        print(file=self.output)
        
        # Failed rules
        if report.failed_count > 0:
            print("FAILED RULES", file=self.output)
            print("-" * 40, file=self.output)
            
            for result in report.failed_results:
                print(file=self.output)
                print(f"[{result.severity.value.upper()}] {result.rule_name}", file=self.output)
                
                if result.table:
                    print(f"  Table: {result.table}", file=self.output)
                
                if result.error_message:
                    print(f"  Error: {result.error_message}", file=self.output)
                else:
                    print(f"  Violations: {result.violation_count:,} records", file=self.output)
                
                if result.sample_records:
                    sample_ids = [self._get_id_from_record(r) for r in result.sample_records[:5]]
                    sample_ids = [str(id) for id in sample_ids if id]
                    if sample_ids:
                        print(f"  Sample IDs: {', '.join(sample_ids)}", file=self.output)
        
        print(file=self.output)
        print("=" * 80, file=self.output)
    
    def _format_record_preview(self, record: dict, max_fields: int = 4) -> str:
        """Format a record for preview display."""
        # Prioritize ID fields and important columns
        priority_fields = ['id', 'ID', 'client_id', 'customer_id', 'name', 'email']
        
        fields_to_show = []
        shown = set()
        
        # First, add priority fields
        for field in priority_fields:
            if field in record and field not in shown:
                value = record[field]
                if value is not None:
                    fields_to_show.append(f"{field}={self._truncate_value(value)}")
                    shown.add(field)
                    if len(fields_to_show) >= max_fields:
                        break
        
        # Then add remaining fields
        for key, value in record.items():
            if key not in shown and not key.startswith('_'):
                if value is not None:
                    fields_to_show.append(f"{key}={self._truncate_value(value)}")
                    if len(fields_to_show) >= max_fields:
                        break
        
        return ", ".join(fields_to_show)
    
    def _truncate_value(self, value, max_len: int = 30) -> str:
        """Truncate long values for display."""
        str_val = str(value)
        if len(str_val) > max_len:
            return str_val[:max_len-3] + "..."
        return str_val
    
    def _get_id_from_record(self, record: dict):
        """Extract ID field from record."""
        for field in ['id', 'ID', 'client_id', 'customer_id', 'record_id']:
            if field in record:
                return record[field]
        # Return first field value as fallback
        for key, value in record.items():
            if not key.startswith('_'):
                return value
        return None


def print_report(report: ValidationReport, use_rich: bool = True) -> None:
    """
    Convenience function to print a report to console.
    
    Args:
        report: ValidationReport to display
        use_rich: Use Rich formatting if available
    """
    reporter = ConsoleReporter(use_rich=use_rich)
    reporter.report(report)
