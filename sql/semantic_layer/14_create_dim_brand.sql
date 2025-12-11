-- ============================================================================
-- Create Brand Dimension View (Conformed)
-- ============================================================================
-- Description: Creates conformed brand dimension for product analytics
--              and brand performance tracking
-- Source: dim_sap_brand
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.dim_brand AS
SELECT
    -- Keys
    b.brand_key,
    b.brand_id,
    
    -- Brand attributes
    b.name AS brand_name,
    b.status AS brand_status,
    
    -- Audit columns
    b.created_at AS brand_created_at,
    b.updated_at AS brand_updated_at,
    
    -- SCD Type 2 fields
    b.__START_AT AS effective_from_date,
    b.__END_AT AS effective_to_date,
    CASE WHEN b.__END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM acme_supermarkets.edw_silver.dim_sap_brand b;

-- ============================================================================
-- Test the view
-- ============================================================================

-- Count brands
SELECT 
  COUNT(*) AS total_brands,
  COUNT(CASE WHEN is_current THEN 1 END) AS current_brands,
  COUNT(CASE WHEN is_current = FALSE THEN 1 END) AS historical_brands
FROM acme_supermarkets.edw_gold.dim_brand;

-- Sample brands
SELECT 
  brand_key,
  brand_id,
  brand_name,
  brand_status,
  brand_created_at
FROM acme_supermarkets.edw_gold.dim_brand
WHERE is_current = TRUE
ORDER BY brand_name
LIMIT 20;

-- Brand status distribution
SELECT 
  brand_status,
  COUNT(*) AS brand_count
FROM acme_supermarkets.edw_gold.dim_brand
WHERE is_current = TRUE
GROUP BY brand_status
ORDER BY brand_count DESC;

