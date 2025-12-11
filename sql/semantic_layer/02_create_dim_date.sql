-- ============================================================================
-- Create Date Dimension Table
-- ============================================================================
-- Description: Creates comprehensive date dimension with calendar and fiscal
--              attributes for time-based analysis
-- Date Range: 2020-01-01 to 2030-12-31 (11 years, ~4,018 rows)
-- Key Format: YYYYMMDD as INT (e.g., 20240115 = January 15, 2024)
-- ============================================================================

CREATE OR REPLACE TABLE acme_supermarkets.edw_gold.dim_date AS
WITH date_spine AS (
  SELECT explode(sequence(
    to_date('2020-01-01'), 
    to_date('2030-12-31'), 
    interval 1 day
  )) AS full_date
)
SELECT
  -- Surrogate key (YYYYMMDD format for easy joins and readability)
  CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
  
  -- Full date
  full_date,
  
  -- Day attributes
  dayofweek(full_date) AS day_of_week,
  date_format(full_date, 'EEEE') AS day_name,
  date_format(full_date, 'EEE') AS day_abbr,
  dayofmonth(full_date) AS day_of_month,
  dayofyear(full_date) AS day_of_year,
  
  -- Week attributes
  weekofyear(full_date) AS week_of_year,
  date_trunc('week', full_date) AS week_start_date,
  date_add(date_trunc('week', full_date), 6) AS week_end_date,
  
  -- Month attributes
  month(full_date) AS month_num,
  date_format(full_date, 'MMMM') AS month_name,
  date_format(full_date, 'MMM') AS month_abbr,
  date_trunc('month', full_date) AS month_start_date,
  last_day(full_date) AS month_end_date,
  
  -- Quarter attributes
  quarter(full_date) AS quarter_num,
  CONCAT('Q', quarter(full_date)) AS quarter_name,
  CONCAT(year(full_date), '-Q', quarter(full_date)) AS quarter_year,
  date_trunc('quarter', full_date) AS quarter_start_date,
  
  -- Year attributes
  year(full_date) AS year,
  
  -- Fiscal attributes (assuming fiscal year starts July 1)
  -- Adjust if your fiscal year starts on a different date
  CASE 
    WHEN month(full_date) >= 7 THEN year(full_date) + 1 
    ELSE year(full_date) 
  END AS fiscal_year,
  
  CASE 
    WHEN month(full_date) BETWEEN 7 AND 9 THEN 1
    WHEN month(full_date) BETWEEN 10 AND 12 THEN 2
    WHEN month(full_date) BETWEEN 1 AND 3 THEN 3
    ELSE 4
  END AS fiscal_quarter,
  
  CASE 
    WHEN month(full_date) >= 7 THEN month(full_date) - 6
    ELSE month(full_date) + 6
  END AS fiscal_month,
  
  -- Business flags
  CASE WHEN dayofweek(full_date) IN (7, 1) THEN TRUE ELSE FALSE END AS is_weekend,
  CASE WHEN dayofweek(full_date) NOT IN (7, 1) THEN TRUE ELSE FALSE END AS is_weekday,
  
  -- Holiday flag (initialized as FALSE, can be updated later with actual holidays)
  FALSE AS is_holiday,
  CAST(NULL AS STRING) AS holiday_name,
  
  -- Business day (weekday and not a holiday)
  CASE WHEN dayofweek(full_date) NOT IN (7, 1) THEN TRUE ELSE FALSE END AS is_business_day,
  
  -- Relative periods (useful for dashboard filters)
  datediff(full_date, current_date()) AS days_from_today,
  
  CASE 
    WHEN year(full_date) = year(current_date()) 
    AND month(full_date) = month(current_date()) 
    THEN TRUE 
    ELSE FALSE 
  END AS is_current_month,
  
  CASE 
    WHEN year(full_date) = year(add_months(current_date(), -1)) 
    AND month(full_date) = month(add_months(current_date(), -1)) 
    THEN TRUE 
    ELSE FALSE 
  END AS is_last_month,
  
  CASE 
    WHEN year(full_date) = year(current_date()) 
    AND quarter(full_date) = quarter(current_date()) 
    THEN TRUE 
    ELSE FALSE 
  END AS is_current_quarter,
  
  CASE 
    WHEN year(full_date) = year(add_months(current_date(), -3)) 
    AND quarter(full_date) = quarter(add_months(current_date(), -3)) 
    THEN TRUE 
    ELSE FALSE 
  END AS is_last_quarter,
  
  CASE 
    WHEN year(full_date) = year(current_date()) 
    THEN TRUE 
    ELSE FALSE 
  END AS is_current_year,
  
  CASE 
    WHEN year(full_date) = year(current_date()) - 1 
    THEN TRUE 
    ELSE FALSE 
  END AS is_last_year
  
FROM date_spine;

-- Optimize the table for better query performance
OPTIMIZE acme_supermarkets.edw_gold.dim_date;

-- Display row count and sample rows
SELECT COUNT(*) AS total_rows FROM acme_supermarkets.edw_gold.dim_date;

SELECT 
  date_key,
  full_date,
  day_name,
  month_name,
  quarter_name,
  year,
  fiscal_year,
  is_weekend,
  is_current_month
FROM acme_supermarkets.edw_gold.dim_date 
WHERE year = 2024 AND month_num = 1
LIMIT 5;

