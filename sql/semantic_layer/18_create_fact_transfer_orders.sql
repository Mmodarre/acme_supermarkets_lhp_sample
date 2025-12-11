-- ============================================================================
-- Create Transfer Orders Fact View
-- ============================================================================
-- Description: Creates transfer order fact for warehouse-to-store transfer
--              efficiency, replenishment cycle time, and network optimization
-- Sources: fct_sap_xfer_ord_hdr_snapshot + fct_sap_xfer_ord_ln
-- Grain: One row per transfer order line (product level)
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.fact_transfer_orders AS
SELECT
    -- Date keys
    CAST(date_format(h.created_at, 'yyyyMMdd') AS INT) AS created_date_key,
    CAST(date_format(h.shipped_date, 'yyyyMMdd') AS INT) AS shipped_date_key,
    CAST(date_format(h.received_at, 'yyyyMMdd') AS INT) AS received_date_key,
    
    -- Date/time stamps
    h.created_at AS transfer_created_at,
    h.shipped_at AS transfer_shipped_at,
    h.shipped_date,
    h.received_at AS transfer_received_at,
    h.pending_date,
    h.delivered_date,
    
    -- Dimension foreign keys
    l.transfer_id,
    l.transfer_line_id,
    l.product_id AS product_key,
    h.source_location_id AS source_warehouse_key,
    h.dest_location_id AS destination_location_key,
    l.uom_id,
    
    -- Degenerate dimensions
    h.current_status AS transfer_status,
    'SAP' AS source_system,
    
    -- Quantity measures
    l.qty AS transfer_quantity,
    
    -- Product cost (from product dimension)
    p.base_cost AS unit_cost,
    l.qty * p.base_cost AS transfer_value_at_cost,
    
    -- Transfer timing measures (in days and hours)
    CASE 
        WHEN h.shipped_at IS NOT NULL AND h.created_at IS NOT NULL 
        THEN ROUND((UNIX_TIMESTAMP(h.shipped_at) - UNIX_TIMESTAMP(h.created_at)) / 86400.0, 2)
        ELSE NULL 
    END AS created_to_shipped_days,
    
    CASE 
        WHEN h.received_at IS NOT NULL AND h.shipped_at IS NOT NULL 
        THEN ROUND((UNIX_TIMESTAMP(h.received_at) - UNIX_TIMESTAMP(h.shipped_at)) / 86400.0, 2)
        ELSE NULL 
    END AS shipped_to_received_days,
    
    CASE 
        WHEN h.received_at IS NOT NULL AND h.created_at IS NOT NULL 
        THEN ROUND((UNIX_TIMESTAMP(h.received_at) - UNIX_TIMESTAMP(h.created_at)) / 86400.0, 2)
        ELSE NULL 
    END AS total_cycle_time_days,
    
    -- Transfer performance flags
    CASE 
        WHEN h.current_status IN ('completed', 'delivered') 
        THEN TRUE 
        ELSE FALSE 
    END AS is_completed,
    
    CASE 
        WHEN h.received_at IS NOT NULL 
             AND ROUND((UNIX_TIMESTAMP(h.received_at) - UNIX_TIMESTAMP(h.shipped_at)) / 86400.0, 2) <= 2 
        THEN TRUE 
        ELSE FALSE 
    END AS is_on_time_delivery,
    
    CASE 
        WHEN h.received_at IS NOT NULL 
        THEN TRUE 
        ELSE FALSE 
    END AS is_received,
    
    -- Transfer order counts (for aggregation)
    1 AS transfer_line_count,
    
    -- Audit columns
    h.last_update_dttm
    
FROM acme_supermarkets.edw_silver.fct_sap_xfer_ord_ln l
INNER JOIN acme_supermarkets.edw_silver.fct_sap_xfer_ord_hdr_snapshot h 
    ON l.transfer_id = h.transfer_id
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_prd p 
    ON l.product_id = p.product_id
    AND p.__END_AT IS NULL;

-- ============================================================================
-- Test the view
-- ============================================================================

-- Overall transfer order summary
SELECT 
  COUNT(DISTINCT transfer_id) AS total_transfer_orders,
  COUNT(*) AS total_transfer_lines,
  SUM(transfer_quantity) AS total_units_transferred,
  ROUND(SUM(transfer_value_at_cost), 2) AS total_transfer_value,
  ROUND(AVG(total_cycle_time_days), 2) AS avg_cycle_time_days,
  COUNT(CASE WHEN is_on_time_delivery THEN 1 END) AS on_time_transfers,
  ROUND(COUNT(CASE WHEN is_on_time_delivery THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN is_received THEN 1 END), 0), 2) AS on_time_pct
FROM acme_supermarkets.edw_gold.fact_transfer_orders;

-- Transfers by source warehouse
SELECT 
  src_w.warehouse_name AS source_warehouse,
  COUNT(DISTINCT t.transfer_id) AS transfer_count,
  SUM(t.transfer_quantity) AS total_units,
  ROUND(AVG(t.total_cycle_time_days), 2) AS avg_cycle_days,
  ROUND(SUM(t.transfer_value_at_cost), 2) AS total_value
FROM acme_supermarkets.edw_gold.fact_transfer_orders t
LEFT JOIN acme_supermarkets.edw_gold.dim_warehouse src_w 
    ON t.source_warehouse_key = src_w.warehouse_id AND src_w.is_current = TRUE
GROUP BY src_w.warehouse_name
ORDER BY transfer_count DESC;

-- Transfers by destination location
SELECT 
  dest_l.location_name AS destination,
  dest_l.location_type,
  COUNT(DISTINCT t.transfer_id) AS transfer_count,
  SUM(t.transfer_quantity) AS total_units,
  ROUND(AVG(t.total_cycle_time_days), 2) AS avg_cycle_days,
  COUNT(CASE WHEN t.is_on_time_delivery THEN 1 END) AS on_time_count
FROM acme_supermarkets.edw_gold.fact_transfer_orders t
LEFT JOIN acme_supermarkets.edw_gold.dim_location dest_l 
    ON t.destination_location_key = dest_l.location_id AND dest_l.is_current = TRUE
GROUP BY dest_l.location_name, dest_l.location_type
ORDER BY transfer_count DESC
LIMIT 10;

-- Transfer status distribution
SELECT 
  transfer_status,
  COUNT(DISTINCT transfer_id) AS order_count,
  COUNT(*) AS line_count,
  SUM(transfer_quantity) AS total_units,
  ROUND(SUM(transfer_value_at_cost), 2) AS total_value
FROM acme_supermarkets.edw_gold.fact_transfer_orders
GROUP BY transfer_status
ORDER BY order_count DESC;

-- Top products transferred
SELECT 
  p.product_name,
  p.sku,
  COUNT(DISTINCT t.transfer_id) AS transfer_count,
  SUM(t.transfer_quantity) AS total_units,
  ROUND(SUM(t.transfer_value_at_cost), 2) AS total_value
FROM acme_supermarkets.edw_gold.fact_transfer_orders t
LEFT JOIN acme_supermarkets.edw_gold.dim_product p 
    ON t.product_key = p.product_id AND p.is_current = TRUE
GROUP BY p.product_name, p.sku
ORDER BY total_units DESC
LIMIT 10;

