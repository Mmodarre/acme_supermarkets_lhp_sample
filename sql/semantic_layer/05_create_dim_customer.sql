-- ============================================================================
-- Create Customer Dimension View (Conformed)
-- ============================================================================
-- Description: Creates conformed customer dimension with calculated age groups,
--              tenure metrics, and denormalized default address
-- Source: dim_sfcc_cust (with join to customer_address)
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.dim_customer AS
SELECT
    -- Keys
    c.cust_key AS customer_key,
    c.customer_id,
    c.external_ref,
    
    -- Name attributes
    c.first_name,
    c.last_name,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name,
    
    -- Contact information
    c.email,
    c.phone,
    
    -- Demographics
    c.dob AS date_of_birth,
    YEAR(CURRENT_DATE()) - YEAR(c.dob) AS age,
    CASE 
        WHEN c.dob IS NULL THEN 'Unknown'
        WHEN YEAR(CURRENT_DATE()) - YEAR(c.dob) < 18 THEN 'Under 18'
        WHEN YEAR(CURRENT_DATE()) - YEAR(c.dob) < 25 THEN '18-24'
        WHEN YEAR(CURRENT_DATE()) - YEAR(c.dob) < 35 THEN '25-34'
        WHEN YEAR(CURRENT_DATE()) - YEAR(c.dob) < 45 THEN '35-44'
        WHEN YEAR(CURRENT_DATE()) - YEAR(c.dob) < 55 THEN '45-54'
        WHEN YEAR(CURRENT_DATE()) - YEAR(c.dob) < 65 THEN '55-64'
        ELSE '65+'
    END AS age_group,
    
    -- Segmentation
    c.segment AS customer_segment,
    c.status AS customer_status,
    
    -- Tenure metrics
    c.created_at AS customer_since_date,
    DATEDIFF(CURRENT_DATE(), c.created_at) AS customer_tenure_days,
    FLOOR(DATEDIFF(CURRENT_DATE(), c.created_at) / 30) AS customer_tenure_months,
    FLOOR(DATEDIFF(CURRENT_DATE(), c.created_at) / 365) AS customer_tenure_years,
    
    -- Default shipping address (denormalized)
    a.address_id AS default_address_id,
    a.line1 AS address_line1,
    a.line2 AS address_line2,
    a.city,
    a.state,
    a.postcode,
    a.country,
    CONCAT_WS(', ', a.line1, a.city, a.state, a.postcode) AS full_address,
    
    -- Audit columns
    c.updated_at AS customer_updated_at,
    
    -- SCD Type 2 fields
    c.__START_AT AS effective_from_date,
    c.__END_AT AS effective_to_date,
    CASE WHEN c.__END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM acme_supermarkets.edw_silver.dim_sfcc_cust c
LEFT JOIN acme_supermarkets.edw_silver.dim_sfcc_cust_addr a 
    ON c.customer_id = a.customer_id 
    AND a.is_default_shipping = TRUE
    AND a.__END_AT IS NULL;

-- Test the view
SELECT 
  customer_key,
  full_name,
  age_group,
  customer_segment,
  customer_tenure_months,
  city,
  state,
  is_current
FROM acme_supermarkets.edw_gold.dim_customer
WHERE is_current = TRUE
LIMIT 5;

-- Count customers
SELECT 
  COUNT(*) AS total_customers,
  COUNT(CASE WHEN is_current THEN 1 END) AS current_customers
FROM acme_supermarkets.edw_gold.dim_customer;

