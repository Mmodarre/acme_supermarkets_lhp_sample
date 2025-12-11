-- ============================================================================
-- Create Warehouse Dimension View (Conformed)
-- ============================================================================
-- Description: Creates conformed warehouse dimension for supply chain and
--              e-commerce fulfillment analytics
-- Source: dim_sap_whse (with location details)
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.dim_warehouse AS
SELECT
    -- Keys
    w.whse_key AS warehouse_key,
    w.warehouse_id,
    
    -- Warehouse attributes
    w.name AS warehouse_name,
    w.status AS warehouse_status,
    
    -- Location linkage and attributes (denormalized)
    w.location_id,
    l.name AS location_name,
    l.location_type,
    l.address_line1,
    l.address_line2,
    l.city,
    l.state,
    l.zip_code,
    l.country,
    
    -- Derived geographic attributes
    CASE 
        WHEN l.state IN ('WA', 'OR', 'CA') THEN 'West'
        ELSE 'Other'
    END AS region,
    
    -- Audit columns
    w.last_update_dttm AS warehouse_last_update,
    
    -- SCD Type 2 fields
    w.__START_AT AS effective_from_date,
    w.__END_AT AS effective_to_date,
    CASE WHEN w.__END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM acme_supermarkets.edw_silver.dim_sap_whse w
LEFT JOIN acme_supermarkets.edw_silver.dim_ncr_location l 
    ON w.location_id = l.location_id
    AND l.__END_AT IS NULL;

-- ============================================================================
-- Test the view
-- ============================================================================

-- Count warehouses
SELECT 
  COUNT(*) AS total_warehouses,
  COUNT(CASE WHEN is_current THEN 1 END) AS current_warehouses,
  COUNT(CASE WHEN is_current = FALSE THEN 1 END) AS historical_warehouses
FROM acme_supermarkets.edw_gold.dim_warehouse;

-- List current warehouses with location details
SELECT 
  warehouse_key,
  warehouse_id,
  warehouse_name,
  warehouse_status,
  city,
  state,
  region
FROM acme_supermarkets.edw_gold.dim_warehouse
WHERE is_current = TRUE
ORDER BY warehouse_name;

-- Warehouse distribution by region
SELECT 
  region,
  COUNT(*) AS warehouse_count
FROM acme_supermarkets.edw_gold.dim_warehouse
WHERE is_current = TRUE
GROUP BY region
ORDER BY warehouse_count DESC;

