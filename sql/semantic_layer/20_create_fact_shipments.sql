-- ============================================================================
-- Create E-commerce Shipments Fact View
-- ============================================================================
-- Description: Creates shipment fact for carrier performance, delivery time,
--              and multi-warehouse order analytics
-- Sources: fct_sfcc_shmpt_hdr_snapshot + fct_sfcc_shmpt_ln
-- Grain: One row per shipment line (product level)
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.fact_shipments AS
SELECT
    -- Date keys
    CAST(date_format(h.ship_date, 'yyyyMMdd') AS INT) AS ship_date_key,
    CAST(date_format(h.expected_delivery_date, 'yyyyMMdd') AS INT) AS expected_delivery_date_key,
    CAST(date_format(h.actual_delivery_date, 'yyyyMMdd') AS INT) AS actual_delivery_date_key,
    CAST(date_format(h.delivered_date, 'yyyyMMdd') AS INT) AS delivered_date_key,
    
    -- Date/time stamps
    h.ship_date,
    h.expected_delivery_date,
    h.actual_delivery_date,
    h.delivered_date,
    h.in_transit_date,
    h.out_for_delivery_date,
    
    -- Dimension foreign keys
    l.shipment_id,
    l.shipment_line_id,
    h.order_id,
    l.order_line_id,
    h.warehouse_id AS warehouse_key,
    h.carrier_id AS carrier_key,
    l.product_id AS product_key,
    
    -- Degenerate dimensions
    h.tracking_number,
    h.current_status AS shipment_status,
    'SFCC' AS source_system,
    
    -- Quantity measures
    l.qty AS shipped_quantity,
    
    -- Product cost (from product dimension)
    p.base_cost AS unit_cost,
    l.qty * p.base_cost AS shipment_value_at_cost,
    
    -- Shipment timing measures (in days and hours)
    CASE 
        WHEN h.in_transit_date IS NOT NULL AND h.ship_date IS NOT NULL 
        THEN ROUND((UNIX_TIMESTAMP(h.in_transit_date) - UNIX_TIMESTAMP(h.ship_date)) / 3600.0, 2)
        ELSE NULL 
    END AS ship_to_transit_hours,
    
    CASE 
        WHEN h.delivered_date IS NOT NULL AND h.ship_date IS NOT NULL 
        THEN DATEDIFF(h.delivered_date, h.ship_date)
        ELSE NULL 
    END AS ship_to_delivery_days,
    
    CASE 
        WHEN h.actual_delivery_date IS NOT NULL AND h.ship_date IS NOT NULL 
        THEN DATEDIFF(h.actual_delivery_date, h.ship_date)
        ELSE NULL 
    END AS actual_transit_days,
    
    CASE 
        WHEN h.expected_delivery_date IS NOT NULL AND h.ship_date IS NOT NULL 
        THEN DATEDIFF(h.expected_delivery_date, h.ship_date)
        ELSE NULL 
    END AS expected_transit_days,
    
    CASE 
        WHEN h.actual_delivery_date IS NOT NULL AND h.expected_delivery_date IS NOT NULL 
        THEN DATEDIFF(h.actual_delivery_date, h.expected_delivery_date)
        ELSE NULL 
    END AS delivery_variance_days,
    
    -- Shipment performance flags
    CASE 
        WHEN h.current_status IN ('delivered', 'completed') 
        THEN TRUE 
        ELSE FALSE 
    END AS is_delivered,
    
    CASE 
        WHEN h.actual_delivery_date IS NOT NULL 
             AND h.expected_delivery_date IS NOT NULL
             AND h.actual_delivery_date <= h.expected_delivery_date
        THEN TRUE
        WHEN h.actual_delivery_date IS NULL
        THEN NULL  -- Still in transit
        ELSE FALSE
    END AS is_on_time_delivery,
    
    CASE 
        WHEN h.actual_delivery_date IS NOT NULL 
             AND h.expected_delivery_date IS NOT NULL
             AND h.actual_delivery_date > h.expected_delivery_date
        THEN TRUE
        ELSE FALSE
    END AS is_late_delivery,
    
    CASE 
        WHEN h.current_status = 'in_transit' 
        THEN TRUE 
        ELSE FALSE 
    END AS is_in_transit,
    
    -- Shipment counts (for aggregation)
    1 AS shipment_line_count,
    
    -- Audit columns
    h.last_update_dttm
    
FROM acme_supermarkets.edw_silver.fct_sfcc_shmpt_ln l
INNER JOIN acme_supermarkets.edw_silver.fct_sfcc_shmpt_hdr_snapshot h 
    ON l.shipment_id = h.shipment_id
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_prd p 
    ON l.product_id = p.product_id
    AND p.__END_AT IS NULL;

-- ============================================================================
-- Test the view
-- ============================================================================

-- Overall shipment summary
SELECT 
  COUNT(DISTINCT shipment_id) AS total_shipments,
  COUNT(*) AS total_shipment_lines,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(shipped_quantity) AS total_units_shipped,
  ROUND(SUM(shipment_value_at_cost), 2) AS total_shipment_value,
  ROUND(AVG(actual_transit_days), 2) AS avg_transit_days,
  COUNT(CASE WHEN is_on_time_delivery THEN 1 END) AS on_time_shipments,
  ROUND(COUNT(CASE WHEN is_on_time_delivery THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN is_delivered THEN 1 END), 0), 2) AS on_time_pct
FROM acme_supermarkets.edw_gold.fact_shipments;

-- Carrier performance
SELECT 
  c.carrier_name,
  COUNT(DISTINCT s.shipment_id) AS shipment_count,
  SUM(s.shipped_quantity) AS total_units,
  ROUND(AVG(s.actual_transit_days), 2) AS avg_transit_days,
  ROUND(AVG(s.delivery_variance_days), 2) AS avg_variance_days,
  COUNT(CASE WHEN s.is_on_time_delivery THEN 1 END) AS on_time_count,
  ROUND(COUNT(CASE WHEN s.is_on_time_delivery THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN s.is_delivered THEN 1 END), 0), 2) AS on_time_pct,
  COUNT(CASE WHEN s.is_late_delivery THEN 1 END) AS late_count
FROM acme_supermarkets.edw_gold.fact_shipments s
LEFT JOIN acme_supermarkets.edw_gold.dim_carrier c 
    ON s.carrier_key = c.carrier_id AND c.is_current = TRUE
GROUP BY c.carrier_name
ORDER BY shipment_count DESC;

-- Shipments by warehouse
SELECT 
  w.warehouse_name,
  w.city,
  w.state,
  COUNT(DISTINCT s.shipment_id) AS shipment_count,
  SUM(s.shipped_quantity) AS total_units,
  ROUND(AVG(s.actual_transit_days), 2) AS avg_transit_days,
  ROUND(COUNT(CASE WHEN s.is_on_time_delivery THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN s.is_delivered THEN 1 END), 0), 2) AS on_time_pct
FROM acme_supermarkets.edw_gold.fact_shipments s
LEFT JOIN acme_supermarkets.edw_gold.dim_warehouse w 
    ON s.warehouse_key = w.warehouse_id AND w.is_current = TRUE
GROUP BY w.warehouse_name, w.city, w.state
ORDER BY shipment_count DESC;

-- Shipment status distribution
SELECT 
  shipment_status,
  COUNT(DISTINCT shipment_id) AS shipment_count,
  COUNT(*) AS line_count,
  SUM(shipped_quantity) AS total_units,
  ROUND(SUM(shipment_value_at_cost), 2) AS total_value
FROM acme_supermarkets.edw_gold.fact_shipments
GROUP BY shipment_status
ORDER BY shipment_count DESC;

-- Multi-warehouse orders (orders fulfilled from multiple warehouses)
SELECT 
  order_id,
  COUNT(DISTINCT warehouse_key) AS warehouse_count,
  COUNT(DISTINCT shipment_id) AS shipment_count,
  SUM(shipped_quantity) AS total_units
FROM acme_supermarkets.edw_gold.fact_shipments
GROUP BY order_id
HAVING COUNT(DISTINCT warehouse_key) > 1
ORDER BY warehouse_count DESC, total_units DESC
LIMIT 20;

-- Top products shipped
SELECT 
  p.product_name,
  p.sku,
  COUNT(DISTINCT s.shipment_id) AS shipment_count,
  SUM(s.shipped_quantity) AS total_units,
  ROUND(SUM(s.shipment_value_at_cost), 2) AS total_value
FROM acme_supermarkets.edw_gold.fact_shipments s
LEFT JOIN acme_supermarkets.edw_gold.dim_product p 
    ON s.product_key = p.product_id AND p.is_current = TRUE
GROUP BY p.product_name, p.sku
ORDER BY total_units DESC
LIMIT 10;

