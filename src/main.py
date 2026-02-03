"""
Main entry point for data quality monitoring.

Provides CLI interface for running validations.
"""

import argparse
import sys
from pathlib import Path

from src.config_loader import load_config, ConfigurationError
from src.rule_engine import RuleEngine
from src.reporters import print_report, save_report


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Data Quality Monitor - Validate database quality rules',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Run all rules with default config
  python -m src.main --config config/rules.yaml
  
  # Run only critical rules
  python -m src.main --config config/rules.yaml --severity critical
  
  # Specify connection
  python -m src.main --config config/rules.yaml --connection prod_db
  
  # Dry run (show rules without executing)
  python -m src.main --config config/rules.yaml --dry-run
  
  # Save report to file
  python -m src.main --config config/rules.yaml --output reports/
'''
    )
    
    parser.add_argument(
        '--config', '-c',
        required=True,
        help='Path to rules configuration YAML file'
    )
    
    parser.add_argument(
        '--connections',
        help='Path to connections YAML file (optional if embedded in config)'
    )
    
    parser.add_argument(
        '--connection', '-n',
        help='Name of database connection to use'
    )
    
    parser.add_argument(
        '--severity', '-s',
        nargs='+',
        choices=['critical', 'high', 'medium', 'low'],
        help='Only run rules with specified severity levels'
    )
    
    parser.add_argument(
        '--rules', '-r',
        nargs='+',
        help='Specific rule names to run'
    )
    
    parser.add_argument(
        '--output', '-o',
        help='Directory to save JSON report'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what rules would run without executing'
    )
    
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress console output (only save to file)'
    )
    
    parser.add_argument(
        '--no-color',
        action='store_true',
        help='Disable colored output'
    )
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = load_config(args.config, args.connections)
        
        # Create rule engine
        engine = RuleEngine(config, args.connection)
        
        # Dry run - just show rules
        if args.dry_run:
            rules = engine.dry_run()
            print(f"\nWould execute {len(rules)} rules:\n")
            for rule in rules:
                print(f"  [{rule['severity'].upper():8}] {rule['name']}")
                if rule['table']:
                    print(f"             Table: {rule['table']}")
                if rule['description']:
                    print(f"             {rule['description']}")
            print()
            return 0
        
        # Progress callback
        def progress(rule_name: str, current: int, total: int):
            if not args.quiet:
                print(f"\r  Running: {current}/{total} - {rule_name}...", end='', flush=True)
        
        engine.set_progress_callback(progress)
        
        if not args.quiet:
            print(f"\nStarting data quality validation...")
            print(f"Connection: {engine.connection_name}")
            print()
        
        # Run validation
        report = engine.run(severity_filter=args.severity)
        
        if not args.quiet:
            print("\r" + " " * 60 + "\r", end='')  # Clear progress line
        
        # Display results
        if not args.quiet:
            print_report(report, use_rich=not args.no_color)
        
        # Save to file if requested
        if args.output:
            full_path, summary_path = save_report(report, args.output)
            if not args.quiet:
                print(f"Full report saved to: {full_path}")
                print(f"Summary saved to: {summary_path}")
        
        # Exit with error code if critical failures
        if report.critical_failures:
            return 1
        return 0
        
    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
