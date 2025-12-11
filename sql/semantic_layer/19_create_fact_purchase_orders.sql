-- ============================================================================
-- Create Purchase Orders Fact View
-- ============================================================================
-- Description: Creates purchase order fact for supplier performance, PO
--              accuracy, DPO estimation, and procurement analytics
-- Sources: fct_sap_prch_ord_hdr_hist + fct_sap_prch_ord_ln
-- Grain: One row per purchase order line (product level)
-- ============================================================================

CREATE OR REPLACE VIEW acme_supermarkets.edw_gold.fact_purchase_orders AS
SELECT
    -- Date keys
    CAST(date_format(h.order_date, 'yyyyMMdd') AS INT) AS order_date_key,
    CAST(date_format(h.expected_date, 'yyyyMMdd') AS INT) AS expected_date_key,
    CAST(date_format(l.due_date, 'yyyyMMdd') AS INT) AS due_date_key,
    CAST(date_format(h.order_date, 'yyyyMMdd') AS INT) AS received_date_key,  -- Using order_date as proxy
    
    -- Date/time stamps
    h.order_date,
    h.expected_date,
    l.due_date,
    NULL AS pending_date,  -- Not available in hist
    h.order_date AS received_date,  -- Using order_date as proxy
    
    -- Dimension foreign keys
    l.po_id,
    l.po_line_id,
    h.vendor_id AS supplier_key,
    l.product_id AS product_key,
    h.ship_to_location_id AS destination_location_key,
    l.uom_id,
    
    -- Degenerate dimensions
    h.status AS po_status,
    l.status AS line_status,
    h.currency,
    'SAP' AS source_system,
    
    -- Quantity measures
    l.ordered_qty,
    
    -- Financial measures
    l.unit_price,
    l.tax_rate,
    l.ordered_qty * l.unit_price AS line_subtotal,
    l.ordered_qty * l.unit_price * (1 + COALESCE(l.tax_rate, 0) / 100) AS line_total,
    l.ordered_qty * l.unit_price * (COALESCE(l.tax_rate, 0) / 100) AS line_tax_amount,
    
    -- Product cost (from product dimension for comparison)
    p.base_cost AS product_base_cost,
    
    -- Cost variance
    l.unit_price - p.base_cost AS unit_cost_variance,
    (l.unit_price - p.base_cost) * l.ordered_qty AS total_cost_variance,
    
    -- PO timing measures (in days)
    CASE 
        WHEN h.expected_date IS NOT NULL AND h.order_date IS NOT NULL 
        THEN DATEDIFF(h.expected_date, h.order_date)
        ELSE NULL 
    END AS order_to_receipt_days,
    
    CASE 
        WHEN h.expected_date IS NOT NULL AND h.order_date IS NOT NULL 
        THEN DATEDIFF(h.expected_date, h.order_date)
        ELSE NULL 
    END AS expected_lead_time_days,
    
    CAST(NULL AS INT) AS delivery_variance_days,  -- Not calculable without received_date
    
    -- PO performance flags
    CASE 
        WHEN h.status IN ('received', 'completed') 
        THEN TRUE 
        ELSE FALSE 
    END AS is_received,
    
    CAST(NULL AS BOOLEAN) AS is_on_time_delivery,  -- Not calculable without received_date
    
    CASE 
        WHEN l.status = 'completed' 
        THEN TRUE 
        ELSE FALSE 
    END AS is_line_completed,
    
    -- PO counts (for aggregation)
    1 AS po_line_count,
    
    -- Audit columns
    h.last_update_dttm
    
FROM acme_supermarkets.edw_silver.fct_sap_prch_ord_ln l
INNER JOIN acme_supermarkets.edw_silver.fct_sap_prch_ord_hdr_hist h 
    ON l.po_id = h.po_id
    AND h.__END_AT IS NULL
LEFT JOIN acme_supermarkets.edw_silver.dim_sap_prd p 
    ON l.product_id = p.product_id
    AND p.__END_AT IS NULL;

-- ============================================================================
-- Test the view
-- ============================================================================

-- Overall purchase order summary
SELECT 
  COUNT(DISTINCT po_id) AS total_purchase_orders,
  COUNT(*) AS total_po_lines,
  SUM(ordered_qty) AS total_units_ordered,
  ROUND(SUM(line_total), 2) AS total_po_value,
  ROUND(AVG(order_to_receipt_days), 2) AS avg_lead_time_days,
  COUNT(CASE WHEN is_on_time_delivery THEN 1 END) AS on_time_pos,
  ROUND(COUNT(CASE WHEN is_on_time_delivery THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN is_received THEN 1 END), 0), 2) AS on_time_pct
FROM acme_supermarkets.edw_gold.fact_purchase_orders;

-- POs by supplier
SELECT 
  s.supplier_name,
  s.payment_terms_days,
  COUNT(DISTINCT po.po_id) AS po_count,
  SUM(po.ordered_qty) AS total_units,
  ROUND(SUM(po.line_total), 2) AS total_spend,
  ROUND(AVG(po.order_to_receipt_days), 2) AS avg_lead_days,
  ROUND(COUNT(CASE WHEN po.is_on_time_delivery THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN po.is_received THEN 1 END), 0), 2) AS on_time_pct
FROM acme_supermarkets.edw_gold.fact_purchase_orders po
LEFT JOIN acme_supermarkets.edw_gold.dim_supplier s 
    ON po.supplier_key = s.supplier_id AND s.is_current = TRUE
GROUP BY s.supplier_name, s.payment_terms_days
ORDER BY total_spend DESC;

-- PO status distribution
SELECT 
  po_status,
  COUNT(DISTINCT po_id) AS po_count,
  COUNT(*) AS line_count,
  SUM(ordered_qty) AS total_units,
  ROUND(SUM(line_total), 2) AS total_value
FROM acme_supermarkets.edw_gold.fact_purchase_orders
GROUP BY po_status
ORDER BY po_count DESC;

-- Top products purchased
SELECT 
  p.product_name,
  p.sku,
  COUNT(DISTINCT po.po_id) AS po_count,
  SUM(po.ordered_qty) AS total_units,
  ROUND(AVG(po.unit_price), 2) AS avg_unit_price,
  ROUND(SUM(po.line_total), 2) AS total_spend
FROM acme_supermarkets.edw_gold.fact_purchase_orders po
LEFT JOIN acme_supermarkets.edw_gold.dim_product p 
    ON po.product_key = p.product_id AND p.is_current = TRUE
GROUP BY p.product_name, p.sku
ORDER BY total_spend DESC
LIMIT 10;

-- Cost variance analysis
SELECT 
  CASE 
    WHEN unit_cost_variance < -0.50 THEN 'Below Base Cost (-$0.50+)'
    WHEN unit_cost_variance >= -0.50 AND unit_cost_variance < 0 THEN 'Slightly Below Base Cost'
    WHEN unit_cost_variance = 0 THEN 'At Base Cost'
    WHEN unit_cost_variance > 0 AND unit_cost_variance <= 0.50 THEN 'Slightly Above Base Cost'
    ELSE 'Above Base Cost (+$0.50+)'
  END AS cost_variance_bucket,
  COUNT(*) AS po_line_count,
  ROUND(SUM(line_total), 2) AS total_value,
  ROUND(AVG(unit_cost_variance), 2) AS avg_variance
FROM acme_supermarkets.edw_gold.fact_purchase_orders
WHERE unit_cost_variance IS NOT NULL
GROUP BY cost_variance_bucket
ORDER BY avg_variance;

