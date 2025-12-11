-- ============================================================================
-- Create Category Dimension View (Conformed)
-- ============================================================================
-- Description: Creates conformed category dimension with full hierarchy for
--              product classification and merchandising analytics
-- Source: dim_sap_cat
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.dim_category AS
SELECT
    -- Keys
    c.cat_key AS category_key,
    c.category_id,
    
    -- Category attributes
    c.name AS category_name,
    c.parent_category_id,
    c.level AS category_level,
    c.path AS category_path,
    
    -- Parent category name (self-join)
    parent.name AS parent_category_name,
    
    -- Category type classification
    CASE 
        WHEN c.level = 1 THEN 'Department'
        WHEN c.level = 2 THEN 'Category'
        WHEN c.level = 3 THEN 'Sub-Category'
        ELSE 'Item'
    END AS category_type,
    
    -- Audit columns
    c.created_at AS category_created_at,
    c.updated_at AS category_updated_at,
    
    -- SCD Type fields (Note: dim_sap_cat is SCD Type 1, but keeping for consistency)
    c.__START_AT AS effective_from_date,
    c.__END_AT AS effective_to_date,
    CASE WHEN c.__END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM acme_supermarkets.edw_silver.dim_sap_cat c
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_cat parent 
    ON c.parent_category_id = parent.category_id;

-- ============================================================================
-- Test the view
-- ============================================================================

-- Count categories
SELECT 
  COUNT(*) AS total_categories,
  COUNT(CASE WHEN is_current THEN 1 END) AS current_categories
FROM acme_supermarkets.edw_gold.dim_category;

-- Category distribution by level
SELECT 
  category_level,
  category_type,
  COUNT(*) AS category_count
FROM acme_supermarkets.edw_gold.dim_category
WHERE is_current = TRUE
GROUP BY category_level, category_type
ORDER BY category_level;

-- Sample categories with hierarchy
SELECT 
  category_key,
  category_id,
  category_name,
  parent_category_name,
  category_level,
  category_type,
  category_path
FROM acme_supermarkets.edw_gold.dim_category
WHERE is_current = TRUE
  AND category_level <= 2
ORDER BY category_path
LIMIT 20;

-- Top-level departments
SELECT 
  category_id,
  category_name,
  category_path
FROM acme_supermarkets.edw_gold.dim_category
WHERE is_current = TRUE
  AND category_level = 1
ORDER BY category_name;

