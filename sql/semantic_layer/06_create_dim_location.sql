-- ============================================================================
-- Create Location Dimension View (Conformed)
-- ============================================================================
-- Description: Creates conformed location dimension with derived region
--              attributes for geographic analysis
-- Source: dim_ncr_location
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.dim_location AS
SELECT
    -- Keys
    location_key,
    location_id,
    
    -- Location attributes
    location_type,
    name AS location_name,
    
    -- Address
    address_line1,
    address_line2,
    city,
    state,
    zip_code,
    country,
    
    -- Derived attributes (adjust region logic as needed)
    CASE 
        WHEN state IN ('CA', 'OR', 'WA', 'NV', 'ID', 'MT', 'WY', 'UT', 'CO', 'AK', 'HI') THEN 'West'
        WHEN state IN ('TX', 'AZ', 'NM', 'OK') THEN 'Southwest'
        WHEN state IN ('IL', 'IN', 'MI', 'OH', 'WI', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') THEN 'Midwest'
        WHEN state IN ('NY', 'NJ', 'PA', 'MA', 'CT', 'RI', 'VT', 'NH', 'ME') THEN 'Northeast'
        WHEN state IN ('FL', 'GA', 'NC', 'SC', 'VA', 'WV', 'MD', 'DE', 'DC', 'KY', 'TN', 'AL', 'MS', 'AR', 'LA') THEN 'Southeast'
        ELSE 'Other'
    END AS region,
    
    CASE 
        WHEN state IN ('CA', 'OR', 'WA') THEN 'Pacific'
        WHEN state IN ('AZ', 'NM', 'NV', 'UT', 'CO') THEN 'Mountain'
        WHEN state IN ('TX', 'OK', 'AR', 'LA') THEN 'South Central'
        WHEN state IN ('IL', 'IN', 'MI', 'OH', 'WI') THEN 'Great Lakes'
        WHEN state IN ('MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') THEN 'Plains'
        WHEN state IN ('NY', 'PA', 'NJ') THEN 'Mid-Atlantic'
        WHEN state IN ('MA', 'CT', 'RI', 'VT', 'NH', 'ME') THEN 'New England'
        WHEN state IN ('FL', 'GA', 'NC', 'SC', 'VA', 'WV', 'MD', 'DE', 'DC') THEN 'Atlantic South'
        WHEN state IN ('KY', 'TN', 'AL', 'MS') THEN 'Deep South'
        ELSE 'Other'
    END AS sub_region,
    
    -- Status
    status AS location_status,
    
    -- SCD Type 2 fields
    __START_AT AS effective_from_date,
    __END_AT AS effective_to_date,
    CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM acme_supermarkets.edw_silver.dim_ncr_location;

-- Test the view
SELECT 
  location_key,
  location_name,
  location_type,
  city,
  state,
  region,
  sub_region,
  is_current
FROM acme_supermarkets.edw_gold.dim_location
WHERE is_current = TRUE
LIMIT 5;

-- Count locations by type and region
SELECT 
  location_type,
  region,
  COUNT(*) AS location_count
FROM acme_supermarkets.edw_gold.dim_location
WHERE is_current = TRUE
GROUP BY location_type, region
ORDER BY location_type, region;

