-- ============================================================================
-- Create Employee Dimension View (Conformed)
-- ============================================================================
-- Description: Creates conformed employee dimension with role categorization
--              and store assignment
-- Source: dim_sap_user (with join to location for store name)
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.dim_employee AS
SELECT
    -- Keys
    u.user_key AS employee_key,
    u.user_id AS employee_id,
    
    -- Employee attributes
    u.username,
    u.first_name,
    u.last_name,
    CONCAT(u.first_name, ' ', u.last_name) AS full_name,
    u.email,
    
    -- Role information
    u.role AS employee_role,
    CASE 
        WHEN u.role = 'manager' THEN 'Management'
        WHEN u.role = 'cashier' THEN 'Sales'
        WHEN u.role = 'picker' THEN 'Fulfillment'
        ELSE 'Other'
    END AS role_category,
    
    -- Store assignment
    u.store_id AS assigned_location_id,
    l.name AS assigned_location_name,
    l.location_type AS assigned_location_type,
    l.city AS assigned_location_city,
    l.state AS assigned_location_state,
    
    -- Status
    u.status AS employee_status,
    
    -- Tenure
    u.created_at AS hired_date,
    DATEDIFF(CURRENT_DATE(), u.created_at) AS tenure_days,
    FLOOR(DATEDIFF(CURRENT_DATE(), u.created_at) / 365) AS tenure_years,
    
    -- Audit
    u.updated_at AS employee_updated_at,
    
    -- SCD Type 2 fields
    u.__START_AT AS effective_from_date,
    u.__END_AT AS effective_to_date,
    CASE WHEN u.__END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM acme_supermarkets.edw_silver.dim_sap_user u
LEFT JOIN acme_supermarkets.edw_silver.dim_ncr_location l 
    ON u.store_id = l.location_id 
    AND l.__END_AT IS NULL;

-- Test the view
SELECT 
  employee_key,
  full_name,
  employee_role,
  role_category,
  assigned_location_name,
  tenure_years,
  is_current
FROM acme_supermarkets.edw_gold.dim_employee
WHERE is_current = TRUE
LIMIT 5;

-- Count employees by role
SELECT 
  employee_role,
  role_category,
  COUNT(*) AS employee_count
FROM acme_supermarkets.edw_gold.dim_employee
WHERE is_current = TRUE
GROUP BY employee_role, role_category
ORDER BY employee_count DESC;

