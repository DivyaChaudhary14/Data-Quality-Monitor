"""
Data Quality Monitor - Automated data validation framework.

A configurable, database-agnostic framework for monitoring data quality
through automated validation checks, anomaly detection, and alerting.

Basic usage:
    from src.config_loader import load_config
    from src.connectors import create_connector
    from src.rule_engine import RuleEngine
    from src.reporters import print_report
    
    # Load configuration
    config = load_config('config/rules.yaml')
    
    # Connect to database
    connector = create_connector(config['connections']['my_db'])
    connector.connect()
    
    # Run validations
    engine = RuleEngine(connector)
    report = engine.execute(config['rules'])
    
    # Display results
    print_report(report)
"""

__version__ = '1.0.0'
__author__ = 'Divya Chaudhary'
