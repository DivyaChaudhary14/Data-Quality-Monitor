"""
Reporters package - output formatting for validation results.

Provides multiple output formats:
- Console: Rich terminal output with colors and formatting
- JSON: Machine-readable for BI tools and automation
"""

from src.reporters.console import ConsoleReporter, print_report
from src.reporters.json_report import JSONReporter, save_report


__all__ = [
    'ConsoleReporter',
    'print_report',
    'JSONReporter',
    'save_report',
]
