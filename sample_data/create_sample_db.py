"""
Generate sample SQLite database for testing and demos.

Creates a realistic dataset with intentional data quality issues
to demonstrate the validation framework.
"""

import sqlite3
import random
from datetime import datetime, timedelta
from pathlib import Path


def create_sample_database(db_path: str = "sample_data/test.db"):
    """Create sample database with test data."""
    
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Remove existing database
    if path.exists():
        path.unlink()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables
    cursor.executescript("""
        -- Clients table
        CREATE TABLE clients (
            client_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone TEXT,
            date_of_birth DATE,
            postal_code TEXT,
            status TEXT DEFAULT 'active',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Programs table
        CREATE TABLE programs (
            program_id INTEGER PRIMARY KEY,
            program_name TEXT NOT NULL,
            start_date DATE,
            end_date DATE,
            capacity INTEGER,
            status TEXT DEFAULT 'active'
        );
        
        -- Services table
        CREATE TABLE services (
            service_id INTEGER PRIMARY KEY,
            client_id INTEGER,
            program_id INTEGER,
            service_date DATE,
            service_type TEXT,
            hours REAL,
            cost REAL,
            notes TEXT,
            FOREIGN KEY (client_id) REFERENCES clients(client_id),
            FOREIGN KEY (program_id) REFERENCES programs(program_id)
        );
        
        -- Staff table
        CREATE TABLE staff (
            staff_id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT,
            hire_date DATE,
            department TEXT,
            salary REAL
        );
    """)
    
    # Insert programs
    programs = [
        (1, 'Emergency Shelter', '2023-01-01', '2025-12-31', 50, 'active'),
        (2, 'Transitional Housing', '2023-01-01', '2025-12-31', 30, 'active'),
        (3, 'Permanent Supportive Housing', '2023-01-01', '2025-12-31', 100, 'active'),
        (4, 'Outreach Services', '2023-06-01', '2024-05-31', None, 'active'),
        (5, 'Youth Program', '2024-01-01', '2023-12-31', 25, 'active'),  # Invalid: end < start
    ]
    cursor.executemany(
        "INSERT INTO programs VALUES (?, ?, ?, ?, ?, ?)",
        programs
    )
    
    # Insert clients with various data quality issues
    clients = []
    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Lisa', 'James', 'Maria']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Wilson', 'Taylor']
    
    for i in range(1, 201):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        # Intentional issues:
        # - Some missing emails (completeness)
        # - Some invalid postal codes (pattern)
        # - Some duplicate names+DOB (duplicates)
        
        email = f"{first_name.lower()}.{last_name.lower()}{i}@email.com" if random.random() > 0.15 else None
        
        # Phone with various formats
        phone = f"204-555-{random.randint(1000, 9999)}" if random.random() > 0.1 else None
        
        # Date of birth
        dob = datetime(
            random.randint(1950, 2005),
            random.randint(1, 12),
            random.randint(1, 28)
        ).strftime('%Y-%m-%d')
        
        # Postal code - some invalid
        if random.random() > 0.1:
            postal = f"R{random.randint(1,3)}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')} {random.randint(1,9)}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(1,9)}"
        else:
            postal = f"INVALID{random.randint(1,99)}"  # Invalid format
        
        status = random.choice(['active', 'active', 'active', 'inactive', 'pending'])
        
        clients.append((i, first_name, last_name, email, phone, dob, postal, status, datetime.now().isoformat()))
    
    # Add duplicate entries (same name + DOB)
    clients.append((201, 'John', 'Smith', 'john.dup@email.com', '204-555-1234', '1985-03-15', 'R3A 1B2', 'active', datetime.now().isoformat()))
    clients.append((202, 'John', 'Smith', 'john.smith2@email.com', '204-555-5678', '1985-03-15', 'R3A 1B3', 'active', datetime.now().isoformat()))
    
    cursor.executemany(
        "INSERT INTO clients VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        clients
    )
    
    # Insert services with issues
    services = []
    service_types = ['Case Management', 'Housing Search', 'Benefits Assistance', 'Mental Health', 'Employment']
    
    for i in range(1, 501):
        client_id = random.randint(1, 200)
        program_id = random.randint(1, 5)
        service_date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
        service_type = random.choice(service_types)
        
        # Hours - some invalid (negative or > 24)
        if random.random() > 0.02:
            hours = round(random.uniform(0.5, 8), 1)
        else:
            hours = random.choice([-1, 25, 30])  # Invalid
        
        # Cost - some outliers
        if random.random() > 0.02:
            cost = round(random.uniform(50, 500), 2)
        else:
            cost = round(random.uniform(5000, 10000), 2)  # Outlier
        
        services.append((i, client_id, program_id, service_date, service_type, hours, cost, None))
    
    # Add orphan services (invalid client_id)
    services.append((501, 9999, 1, '2024-01-15', 'Case Management', 2.0, 150.00, 'Orphan record'))
    services.append((502, 9998, 2, '2024-02-20', 'Housing Search', 1.5, 100.00, 'Orphan record'))
    
    cursor.executemany(
        "INSERT INTO services VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        services
    )
    
    # Insert staff
    staff = [
        (1, 'Alice', 'Manager', 'alice@company.com', '2020-01-15', 'Administration', 75000),
        (2, 'Bob', 'Counselor', 'bob@company.com', '2021-03-01', 'Services', 55000),
        (3, 'Carol', 'Analyst', None, '2022-06-15', 'Data', 60000),  # Missing email
        (4, 'Dan', 'Coordinator', 'dan@company.com', '2023-01-10', 'Services', 50000),
        (5, 'Eve', 'Director', 'eve@company.com', '2019-05-01', 'Administration', 250000),  # Salary outlier
    ]
    cursor.executemany(
        "INSERT INTO staff VALUES (?, ?, ?, ?, ?, ?, ?)",
        staff
    )
    
    conn.commit()
    conn.close()
    
    print(f"Sample database created: {db_path}")
    print(f"  - 202 clients (with completeness and duplicate issues)")
    print(f"  - 5 programs (with date validation issue)")
    print(f"  - 502 services (with orphan records, range issues, outliers)")
    print(f"  - 5 staff members")


if __name__ == '__main__':
    create_sample_database()
