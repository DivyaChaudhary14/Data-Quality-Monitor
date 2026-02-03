-- ============================================================
-- Sample T-SQL Data Quality Validation Queries
-- ============================================================
-- These queries demonstrate validation patterns you can use
-- directly in SQL Server or adapt for custom_sql rules.
-- ============================================================


-- ============================================================
-- COMPLETENESS CHECKS
-- ============================================================

-- Find records with NULL required fields
SELECT 
    client_id,
    first_name,
    last_name,
    CASE 
        WHEN first_name IS NULL THEN 'first_name'
        WHEN last_name IS NULL THEN 'last_name'
        WHEN date_of_birth IS NULL THEN 'date_of_birth'
    END AS missing_field
FROM clients
WHERE first_name IS NULL 
   OR last_name IS NULL 
   OR date_of_birth IS NULL;

-- Count completeness by column
SELECT 
    'first_name' AS column_name,
    COUNT(*) AS total_records,
    SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) AS null_count,
    CAST(SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100 AS null_percentage
FROM clients
UNION ALL
SELECT 
    'email',
    COUNT(*),
    SUM(CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END),
    CAST(SUM(CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100
FROM clients;


-- ============================================================
-- REFERENTIAL INTEGRITY
-- ============================================================

-- Find orphan records (FK violations)
SELECT s.*
FROM services s
LEFT JOIN clients c ON s.client_id = c.client_id
WHERE c.client_id IS NULL;

-- Count orphans per table
SELECT 
    'services -> clients' AS relationship,
    COUNT(*) AS orphan_count
FROM services s
LEFT JOIN clients c ON s.client_id = c.client_id
WHERE c.client_id IS NULL
UNION ALL
SELECT 
    'services -> programs',
    COUNT(*)
FROM services s
LEFT JOIN programs p ON s.program_id = p.program_id
WHERE p.program_id IS NULL;


-- ============================================================
-- DUPLICATE DETECTION
-- ============================================================

-- Find exact duplicates
SELECT 
    first_name, 
    last_name, 
    date_of_birth,
    COUNT(*) AS duplicate_count
FROM clients
GROUP BY first_name, last_name, date_of_birth
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- Find potential duplicates with fuzzy matching
SELECT 
    c1.client_id AS client1_id,
    c2.client_id AS client2_id,
    c1.first_name,
    c1.last_name,
    c1.date_of_birth
FROM clients c1
INNER JOIN clients c2 
    ON c1.client_id < c2.client_id
    AND SOUNDEX(c1.first_name) = SOUNDEX(c2.first_name)
    AND SOUNDEX(c1.last_name) = SOUNDEX(c2.last_name)
    AND c1.date_of_birth = c2.date_of_birth;


-- ============================================================
-- RANGE VALIDATION
-- ============================================================

-- Find values outside acceptable range
SELECT *
FROM services
WHERE hours < 0 OR hours > 24;

-- Summary of range violations
SELECT 
    'hours < 0' AS violation_type,
    COUNT(*) AS count
FROM services WHERE hours < 0
UNION ALL
SELECT 
    'hours > 24',
    COUNT(*)
FROM services WHERE hours > 24
UNION ALL
SELECT 
    'cost < 0',
    COUNT(*)
FROM services WHERE cost < 0;


-- ============================================================
-- PATTERN VALIDATION
-- ============================================================

-- Find invalid email formats (SQL Server)
SELECT *
FROM clients
WHERE email IS NOT NULL
  AND email NOT LIKE '%_@_%.__%';

-- Find invalid Canadian postal codes
SELECT *
FROM clients
WHERE postal_code IS NOT NULL
  AND postal_code NOT LIKE '[A-Z][0-9][A-Z] [0-9][A-Z][0-9]';


-- ============================================================
-- OUTLIER DETECTION
-- ============================================================

-- Z-score outlier detection
WITH Stats AS (
    SELECT 
        AVG(CAST(cost AS FLOAT)) AS mean_cost,
        STDEV(CAST(cost AS FLOAT)) AS std_cost
    FROM services
    WHERE cost IS NOT NULL
)
SELECT 
    s.*,
    (s.cost - st.mean_cost) / st.std_cost AS zscore
FROM services s
CROSS JOIN Stats st
WHERE ABS((s.cost - st.mean_cost) / st.std_cost) > 3;

-- IQR outlier detection
WITH Quartiles AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cost) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY cost) AS q3
    FROM services
    WHERE cost IS NOT NULL
),
Bounds AS (
    SELECT 
        q1,
        q3,
        q3 - q1 AS iqr,
        q1 - 1.5 * (q3 - q1) AS lower_bound,
        q3 + 1.5 * (q3 - q1) AS upper_bound
    FROM Quartiles
)
SELECT s.*
FROM services s
CROSS JOIN Bounds b
WHERE s.cost < b.lower_bound OR s.cost > b.upper_bound;


-- ============================================================
-- DATE VALIDATION
-- ============================================================

-- Find invalid date sequences
SELECT *
FROM programs
WHERE end_date < start_date;

-- Find future dates that shouldn't exist
SELECT *
FROM services
WHERE service_date > GETDATE();

-- Find dates outside reasonable range
SELECT *
FROM clients
WHERE date_of_birth < '1900-01-01'
   OR date_of_birth > GETDATE();


-- ============================================================
-- CROSS-TABLE CONSISTENCY
-- ============================================================

-- Active clients without recent activity
SELECT c.*
FROM clients c
WHERE c.status = 'active'
  AND c.client_id NOT IN (
    SELECT DISTINCT client_id 
    FROM services 
    WHERE service_date >= DATEADD(month, -6, GETDATE())
  );

-- Services after client inactive date
SELECT s.*
FROM services s
INNER JOIN clients c ON s.client_id = c.client_id
WHERE c.status = 'inactive'
  AND s.service_date > c.updated_at;


-- ============================================================
-- DATA QUALITY DASHBOARD QUERIES
-- ============================================================

-- Overall quality score by table
SELECT 
    'clients' AS table_name,
    COUNT(*) AS total_records,
    SUM(CASE 
        WHEN first_name IS NULL 
          OR last_name IS NULL 
          OR date_of_birth IS NULL 
        THEN 1 ELSE 0 
    END) AS records_with_issues,
    CAST(100.0 - (SUM(CASE 
        WHEN first_name IS NULL 
          OR last_name IS NULL 
          OR date_of_birth IS NULL 
        THEN 1 ELSE 0 
    END) * 100.0 / COUNT(*)) AS DECIMAL(5,2)) AS quality_score
FROM clients
UNION ALL
SELECT 
    'services',
    COUNT(*),
    SUM(CASE 
        WHEN s.client_id NOT IN (SELECT client_id FROM clients)
          OR hours < 0 OR hours > 24
        THEN 1 ELSE 0 
    END),
    CAST(100.0 - (SUM(CASE 
        WHEN s.client_id NOT IN (SELECT client_id FROM clients)
          OR hours < 0 OR hours > 24
        THEN 1 ELSE 0 
    END) * 100.0 / COUNT(*)) AS DECIMAL(5,2))
FROM services s;

-- Quality trend over time (for tracking improvements)
SELECT 
    CAST(created_at AS DATE) AS check_date,
    COUNT(*) AS records_checked,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS missing_email,
    SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) AS missing_phone
FROM clients
GROUP BY CAST(created_at AS DATE)
ORDER BY check_date DESC;
