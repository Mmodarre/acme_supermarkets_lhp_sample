-- ============================================================================
-- Create Product Dimension View (Conformed)
-- ============================================================================
-- Description: Creates conformed product dimension with denormalized brand,
--              category, and UOM attributes for easier querying
-- Source: dim_sap_prd (with joins to brand, category, uom)
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.dim_product AS
SELECT
    -- Keys
    p.prd_key AS product_key,
    p.product_id,
    p.sku,
    p.upc,
    
    -- Product attributes
    p.name AS product_name,
    p.packaging,
    p.quantity,
    p.nutrition_data,
    p.status AS product_status,
    
    -- Brand attributes (denormalized)
    p.brand_id,
    b.name AS brand_name,
    b.status AS brand_status,
    
    -- Category attributes (denormalized with hierarchy)
    p.category_id,
    c.name AS category_name,
    c.parent_category_id,
    c.level AS category_level,
    c.path AS category_path,
    
    -- UOM attributes (denormalized)
    p.uom_id,
    u.name AS uom_name,
    u.code AS uom_code,
    u.uom_type,
    
    -- Pricing and cost
    p.base_cost,
    p.base_price,
    ROUND(p.base_price - p.base_cost, 2) AS unit_margin,
    ROUND((p.base_price - p.base_cost) / NULLIF(p.base_price, 0) * 100, 2) AS margin_pct,
    
    -- Inventory attributes
    p.reorder_quantity,
    p.shelf_life_days,
    
    -- Audit columns
    p.created_at AS product_created_at,
    p.updated_at AS product_updated_at,
    
    -- SCD Type 2 fields
    p.__START_AT AS effective_from_date,
    p.__END_AT AS effective_to_date,
    CASE WHEN p.__END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM acme_supermarkets.edw_silver.dim_sap_prd p
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_brand b 
    ON p.brand_id = b.brand_id 
    AND b.__END_AT IS NULL
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_cat c 
    ON p.category_id = c.category_id
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_uom u 
    ON p.uom_id = u.uom_id;

-- Test the view
SELECT 
  product_key,
  product_name,
  brand_name,
  category_name,
  base_price,
  base_cost,
  margin_pct,
  is_current
FROM acme_supermarkets.edw_gold.dim_product
WHERE is_current = TRUE
LIMIT 5;

-- Count products
SELECT 
  COUNT(*) AS total_products,
  COUNT(CASE WHEN is_current THEN 1 END) AS current_products,
  COUNT(CASE WHEN is_current = FALSE THEN 1 END) AS historical_products
FROM acme_supermarkets.edw_gold.dim_product;

