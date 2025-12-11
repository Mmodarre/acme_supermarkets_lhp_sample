-- ============================================================================
-- Create Inventory Snapshot Fact View
-- ============================================================================
-- Description: Creates inventory snapshot fact for inventory analytics,
--              turnover, DIO, GMROI, and stock coverage metrics
-- Source: fct_ncr_str_invtry
-- Grain: One row per store/warehouse + product + snapshot date
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.fact_inventory_snapshot AS
SELECT
    -- Date key (for time-series analysis)
    CAST(date_format(i.last_update_dttm, 'yyyyMMdd') AS INT) AS snapshot_date_key,
    i.last_update_dttm AS snapshot_datetime,
    
    -- Dimension foreign keys
    i.store_id AS location_key,
    i.product_id AS product_key,
    
    -- Degenerate dimensions
    i.store_inventory_id,
    'NCR' AS source_system,
    
    -- Location type (to distinguish stores vs warehouses)
    l.location_type,
    CASE 
        WHEN l.location_type = 'Warehouse' THEN TRUE 
        ELSE FALSE 
    END AS is_warehouse,
    
    -- Quantity measures
    i.on_hand_qty,
    i.reserved_qty,
    i.safety_stock_qty,
    i.reorder_point,
    
    -- Available quantity (on-hand minus reserved)
    i.on_hand_qty - COALESCE(i.reserved_qty, 0) AS available_qty,
    
    -- Product cost attributes (from product dimension)
    p.base_cost AS unit_cost,
    p.base_price AS unit_price,
    
    -- Inventory value measures
    i.on_hand_qty * p.base_cost AS inventory_value_at_cost,
    i.on_hand_qty * p.base_price AS inventory_value_at_retail,
    (i.on_hand_qty - COALESCE(i.reserved_qty, 0)) * p.base_cost AS available_inventory_value,
    
    -- Potential profit if all sold
    i.on_hand_qty * (p.base_price - p.base_cost) AS potential_gross_profit,
    
    -- Stock status flags
    CASE 
        WHEN i.on_hand_qty = 0 THEN 'Out of Stock'
        WHEN i.on_hand_qty <= i.reorder_point THEN 'Reorder Needed'
        WHEN i.on_hand_qty <= i.safety_stock_qty THEN 'Below Safety Stock'
        ELSE 'In Stock'
    END AS stock_status,
    
    -- SCD Type 2 fields
    i.__START_AT AS effective_from_date,
    i.__END_AT AS effective_to_date,
    CASE WHEN i.__END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
    
FROM acme_supermarkets.edw_silver.fct_ncr_str_invtry i
LEFT JOIN acme_supermarkets.edw_silver.dim_ncr_location l 
    ON i.store_id = l.location_id
    AND l.__END_AT IS NULL
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_prd p 
    ON i.product_id = p.product_id
    AND p.__END_AT IS NULL;

-- ============================================================================
-- Test the view
-- ============================================================================

-- Overall inventory summary
SELECT 
  COUNT(*) AS total_inventory_records,
  COUNT(CASE WHEN is_current THEN 1 END) AS current_records,
  SUM(CASE WHEN is_current THEN on_hand_qty ELSE 0 END) AS total_units,
  ROUND(SUM(CASE WHEN is_current THEN inventory_value_at_cost ELSE 0 END), 2) AS total_inventory_value
FROM acme_supermarkets.edw_gold.fact_inventory_snapshot;

-- Inventory by location type
SELECT 
  location_type,
  COUNT(DISTINCT location_key) AS location_count,
  COUNT(DISTINCT product_key) AS product_count,
  SUM(on_hand_qty) AS total_units,
  ROUND(SUM(inventory_value_at_cost), 2) AS total_value
FROM acme_supermarkets.edw_gold.fact_inventory_snapshot
WHERE is_current = TRUE
GROUP BY location_type
ORDER BY total_value DESC;

-- Stock status distribution
SELECT 
  stock_status,
  COUNT(*) AS product_location_count,
  SUM(on_hand_qty) AS total_units,
  ROUND(SUM(inventory_value_at_cost), 2) AS total_value
FROM acme_supermarkets.edw_gold.fact_inventory_snapshot
WHERE is_current = TRUE
GROUP BY stock_status
ORDER BY total_value DESC;

-- Top 10 products by inventory value
SELECT 
  p.product_name,
  p.sku,
  SUM(f.on_hand_qty) AS total_units,
  ROUND(SUM(f.inventory_value_at_cost), 2) AS inventory_value,
  ROUND(SUM(f.potential_gross_profit), 2) AS potential_profit
FROM acme_supermarkets.edw_gold.fact_inventory_snapshot f
LEFT JOIN acme_supermarkets.edw_gold.dim_product p 
    ON f.product_key = p.product_id AND p.is_current = TRUE
WHERE f.is_current = TRUE
GROUP BY p.product_name, p.sku
ORDER BY inventory_value DESC
LIMIT 10;

